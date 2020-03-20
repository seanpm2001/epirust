use core::borrow::Borrow;
use core::borrow::BorrowMut;
use std::any::Any;
use std::collections::HashMap;
use std::time::{Instant, SystemTime};

use chrono::{DateTime, Local};
use fxhash::{FxBuildHasher, FxHashMap};
use rand::Rng;

use crate::{allocation_map, events, constants};
use crate::allocation_map::AgentLocationMap;
use crate::config::{Config, Population};
use crate::csv_service::CsvListener;
use crate::disease::Disease;
use crate::disease_tracker::Hotspot;
use crate::events::{Counts, Listener};
use crate::geography;
use crate::geography::{Grid, Point};
use crate::kafka_service::KafkaProducer;
use crate::random_wrapper::RandomWrapper;
use crate::agent::Citizen;
use crate::interventions::{Intervention, BuildNewHospital};

pub struct Epidemiology {
    pub agent_location_map: allocation_map::AgentLocationMap,
    pub write_agent_location_map: allocation_map::AgentLocationMap,
    pub grid: Grid,
    pub disease: Disease,
    is_city_under_quarantine: bool,
}

impl Epidemiology {
    pub fn new(config: &Config) -> Epidemiology {
        let start = Instant::now();
        let disease = config.get_disease();
        let grid = geography::define_geography(config.get_grid_size());
        let mut rng = RandomWrapper::new();
        let (start_locations, agent_list) = match config.get_population() {
            Population::Csv(csv_pop) => {
                grid.read_population(&csv_pop)
            }
            Population::Auto(auto_pop) => {
                grid.generate_population(&auto_pop, &mut rng)
            }
        };

        let agent_location_map = allocation_map::AgentLocationMap::new(config.get_grid_size(), &agent_list, &start_locations);
        let write_agent_location_map = allocation_map::AgentLocationMap::new(config.get_grid_size(), &agent_list, &start_locations);
        let is_city_under_quarantine = false;

        println!("Initialization completed in {} seconds", start.elapsed().as_secs_f32());
        Epidemiology { agent_location_map, write_agent_location_map, grid, disease, is_city_under_quarantine }
    }

    fn stop_simulation(row: Counts) -> bool {
        row.get_infected() == 0 && row.get_quarantined() == 0
    }

    pub fn run(&mut self, config: &Config) {
        let now: DateTime<Local> = SystemTime::now().into();
        let output_file_prefix = config.get_output_file().unwrap_or("simulation".to_string());
        let output_file_name = format!("{}_{}.csv", output_file_prefix, now.format("%Y-%m-%dT%H:%M:%S"));
        let csv_listener = CsvListener::new(output_file_name);
        let kafka_listener = KafkaProducer::new(self.agent_location_map.agent_cell.len(),
                                                config.enable_citizen_state_messages());
        let hotspot_tracker = Hotspot::new();
        let mut listeners = Listeners::from(vec![Box::new(csv_listener), Box::new(kafka_listener), Box::new(hotspot_tracker)]);

        let mut counts_at_hr = Counts::new((self.agent_location_map.agent_cell.len() - 1) as i32, 1);
        let mut rng = RandomWrapper::new();
        let start_time = Instant::now();

        self.write_agent_location_map.agent_cell = FxHashMap::with_capacity_and_hasher(self.agent_location_map.agent_cell.len(), FxBuildHasher::default());

        let vaccinations = Epidemiology::prepare_vaccinations(config);
        let hospital_intervention = Intervention::get_hospital_intervention(config);

        for simulation_hour in 1..config.get_hours() {
            counts_at_hr.increment_hour();

            let mut read_buffer_reference = self.agent_location_map.borrow();
            let mut write_buffer_reference = self.write_agent_location_map.borrow_mut();

            if simulation_hour % 2 == 0 {
                read_buffer_reference = self.write_agent_location_map.borrow();
                write_buffer_reference = self.agent_location_map.borrow_mut();
            }

            match hospital_intervention  {
                Some(x) if x.at_hour == simulation_hour => {
                    println!("Increasing the hospital size");
                    self.grid.increase_hospital_size(config.get_grid_size(), x.new_scale_factor);
                },
                _ => {},
            }

            Epidemiology::simulate(&mut counts_at_hr, simulation_hour, read_buffer_reference, write_buffer_reference,
                                   &self.grid, &mut listeners, &mut rng, &self.disease);
            listeners.counts_updated(counts_at_hr);

            let should_lock_down = !self.is_city_under_quarantine && Epidemiology::check_lock_down(&counts_at_hr);

            if should_lock_down {
                self.is_city_under_quarantine = true;
                println!("Locking the city");
                Epidemiology::lock_city(&mut write_buffer_reference);
            }

            match vaccinations.get(&simulation_hour) {
                Some(vac_percent) => {
                    println!("Vaccination");
                    Epidemiology::vaccinate(*vac_percent, &mut write_buffer_reference, &mut rng);
                }
                _ => {}
            };

            if Epidemiology::stop_simulation(counts_at_hr) {
                break;
            }

            if simulation_hour % 100 == 0 {
                println!("Throughput: {} iterations/sec; simulation hour {} of {}",
                         simulation_hour as f32 / start_time.elapsed().as_secs_f32(),
                         simulation_hour, config.get_hours());
            }
        }
        let elapsed_time = start_time.elapsed().as_secs_f32();
        println!("Number of iterations: {}, Total Time taken {} seconds", counts_at_hr.get_hour(), elapsed_time);
        println!("Iterations/sec: {}", counts_at_hr.get_hour() as f32 / elapsed_time);
        listeners.simulation_ended();
    }

    fn prepare_vaccinations(config: &Config) -> HashMap<i32, f64> {
        let mut vaccinations: HashMap<i32, f64> = HashMap::new();
        config.get_interventions().iter().filter_map(|i| {
            match i {
                Intervention::Vaccinate(v) => Some(v),
                _ => None,
            }
        }).for_each(|v| {
            vaccinations.insert(v.at_hour, v.percent);
        });
        vaccinations
    }

    fn vaccinate(vaccination_percentage: f64, write_buffer_reference: &mut AgentLocationMap, rng: &mut RandomWrapper) {
        for (_v, agent) in write_buffer_reference.agent_cell.iter_mut() {
            if agent.is_susceptible() && rng.get().gen_bool(vaccination_percentage) {
                agent.set_vaccination(true);
            }
        }
    }

    fn simulate(mut csv_record: &mut events::Counts, simulation_hour: i32, read_buffer: &AgentLocationMap,
                write_buffer: &mut AgentLocationMap, grid: &Grid, listeners: &mut Listeners,
                rng: &mut RandomWrapper, disease: &Disease) {
        write_buffer.agent_cell.clear();
        for (cell, agent) in read_buffer.agent_cell.iter() {
            let mut current_agent = *agent;
            let infection_status = current_agent.is_infected();
            let point = current_agent.perform_operation(*cell, simulation_hour, &grid, read_buffer, &mut csv_record, rng, disease);

            if infection_status == false && current_agent.is_infected() == true {
                listeners.citizen_got_infected(&cell);
            }

            let agent_option = write_buffer.agent_cell.get(&point);
            let new_location = match agent_option {
                Some(mut _agent) => cell, //occupied
                _ => &point
            };
            write_buffer.agent_cell.insert(*new_location, current_agent);
            listeners.citizen_state_updated(simulation_hour, &current_agent, new_location);
        }
    }

    fn check_lock_down(csv_record: &events::Counts) -> bool {
        csv_record.get_infected() > constants::CITY_LOCK_DOWN_THRESHOLD
    }

    fn lock_city(write_buffer_reference: &mut AgentLocationMap) {
        for (_v, agent) in write_buffer_reference.agent_cell.iter_mut() {
            agent.set_isolation(true);
        }
    }
}

struct Listeners {
    listeners: Vec<Box<dyn Listener>>,
}

impl Listeners {
    fn from(listeners: Vec<Box<dyn Listener>>) -> Listeners {
        Listeners { listeners }
    }
}

impl Listener for Listeners {
    fn counts_updated(&mut self, counts: Counts) {
        self.listeners.iter_mut().for_each(|listener| { listener.counts_updated(counts) });
    }

    fn simulation_ended(&mut self) {
        self.listeners.iter_mut().for_each(|listener| { listener.simulation_ended() });
    }

    fn citizen_got_infected(&mut self, cell: &Point) {
        self.listeners.iter_mut().for_each(|listener| { listener.citizen_got_infected(cell) });
    }

    fn citizen_state_updated(&mut self, hr: i32, citizen: &Citizen, location: &Point) {
        self.listeners.iter_mut().for_each(|listener| {
            listener.citizen_state_updated(hr, citizen, location);
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::config::AutoPopulation;
    use crate::interventions::Vaccinate;
    use crate::geography::Area;
    use crate::geography::Point;

    use super::*;

    #[test]
    fn should_init() {
        let pop = AutoPopulation {
            number_of_agents: 10,
            public_transport_percentage: 1.0,
            working_percentage: 1.0,
        };
        let disease = Disease::new(0, 0, 0, 0.0, 0.0, 0.0);
        let vac = Vaccinate {
            at_hour: 5000,
            percent: 0.2,
        };
        let config = Config::new(Population::Auto(pop), disease, vec![], 20, 10000,
                                 vec![Intervention::Vaccinate(vac)], None);
        let epidemiology: Epidemiology = Epidemiology::new(&config);
        let expected_housing_area = Area::new(Point::new(0, 0), Point::new(7, 19));
        assert_eq!(epidemiology.grid.housing_area, expected_housing_area);

        let expected_transport_area = Area::new(Point::new(8, 0), Point::new(9, 19));
        assert_eq!(epidemiology.grid.transport_area, expected_transport_area);

        let expected_hospital_area = Area::new(Point::new(10, 0), Point::new(11, 19));
        assert_eq!(epidemiology.grid.hospital_area, expected_hospital_area);

        let expected_work_area = Area::new(Point::new(12, 0), Point::new(19, 19));
        assert_eq!(epidemiology.grid.work_area, expected_work_area);

        assert_eq!(epidemiology.agent_location_map.agent_cell.len(), 10);
    }

    struct MockListener {
        calls_counts_updated: u32,
        calls_simulation_ended: u32,
        calls_citizen_got_infected: u32,
    }

    impl MockListener {
        fn new() -> MockListener {
            MockListener {
                calls_counts_updated: 0,
                calls_simulation_ended: 0,
                calls_citizen_got_infected: 0,
            }
        }
    }

    impl Listener for MockListener {
        fn counts_updated(&mut self, _counts: Counts) {
            self.calls_counts_updated += 1;
        }

        fn simulation_ended(&mut self) {
            self.calls_simulation_ended += 1;
        }

        fn citizen_got_infected(&mut self, _cell: &Point) {
            self.calls_citizen_got_infected += 1;
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn should_notify_all_listeners() {
        let mock1 = Box::new(MockListener::new());
        let mock2 = Box::new(MockListener::new());

        let mocks: Vec<Box<dyn Listener>> = vec![mock1, mock2];
        let mut listeners = Listeners::from(mocks);


        listeners.counts_updated(Counts::new(10, 1));
        listeners.citizen_got_infected(&Point::new(1, 1));
        listeners.simulation_ended();

        for i in 0..=1 {
            //ownership has moved. We need to read the value from the struct, and downcast to MockListener to assert
            let mock = listeners.listeners.get(i).unwrap().as_any().downcast_ref::<MockListener>().unwrap();
            assert_eq!(mock.calls_counts_updated, 1);
            assert_eq!(mock.calls_citizen_got_infected, 1);
            assert_eq!(mock.calls_simulation_ended, 1);
        }
    }
}
