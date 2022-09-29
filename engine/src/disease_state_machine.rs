/*
 * EpiRust
 * Copyright (c) 2020  ThoughtWorks, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
use rand::Rng;
use rand::seq::SliceRandom;

use crate::disease::Disease;
use crate::utils::RandomWrapper;
use crate::models::constants;
use crate::models::custom_types::{Day, Hour};

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum State {
    Susceptible {},
    Exposed { at_hour: Hour },
    Infected { symptoms: bool, severity: InfectionSeverity },
    Recovered {},
    Deceased {},
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum InfectionSeverity {
    Pre { at_hour: Hour },
    Mild,
    Severe,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiseaseStateMachine {
    pub state: State,
    infection_day: Day,
}

impl DiseaseStateMachine {
    pub fn new() -> Self {
        DiseaseStateMachine { state: State::Susceptible {}, infection_day: 0 }
    }

    pub fn get_infection_day(self) -> Day {
        match self.state {
            State::Infected { .. } => self.infection_day,
            _ => 0,
        }
    }

    pub fn expose(&mut self, current_hour: Hour) {
        match self.state {
            State::Susceptible {} => self.state = State::Exposed { at_hour: current_hour },
            _ => {
                panic!("Invalid state transition!")
            }
        }
    }

    pub fn infect(&mut self, rng: &mut RandomWrapper, sim_hr: Hour, disease: &Disease) -> bool {
        match self.state {
            State::Exposed { at_hour } => {
                let option = constants::RANGE_FOR_EXPOSED.choose(rng.get());
                // info!("option: {:?}", option);
                let random_factor = *option.unwrap();
                if sim_hr - at_hour >= (disease.get_exposed_duration() as i32 + random_factor) as Hour {
                    let symptoms = rng.get().gen_bool(1.0 - disease.get_percentage_asymptomatic_population());
                    let mut severity = InfectionSeverity::Pre { at_hour: sim_hr };
                    if !symptoms {
                        severity = InfectionSeverity::Mild {};
                    }
                    self.state = State::Infected { symptoms, severity };
                    return true;
                }
                false
            }
            _ => {
                panic!("Invalid state transition!")
            }
        }
    }

    pub fn change_infection_severity(&mut self, current_hour: Hour, rng: &mut RandomWrapper, disease: &Disease) {
        match self.state {
            State::Infected { symptoms: true, severity } => {
                if let InfectionSeverity::Pre { at_hour } = severity {
                    if current_hour - at_hour >= disease.get_pre_symptomatic_duration() {
                        let mut severity = InfectionSeverity::Mild {};
                        let severe = rng.get().gen_bool(disease.get_percentage_severe_infected_population());
                        if severe {
                            severity = InfectionSeverity::Severe {};
                        }
                        self.state = State::Infected { symptoms: true, severity };
                    }
                }
            }
            _ => {
                panic!("Invalid state transition!")
            }
        }
    }

    pub fn hospitalize(&mut self, disease: &Disease, immunity: i32) -> bool {
        match self.state {
            State::Infected { symptoms: true, severity: InfectionSeverity::Severe } =>
            // why we are adding immunity in infection day
            {
                disease.is_to_be_hospitalized((self.infection_day as i32 + immunity) as Day)
            }
            State::Infected { .. } => false,
            _ => {
                panic!("Invalid state transition!")
            }
        }
    }

    pub fn decease(&mut self, rng: &mut RandomWrapper, disease: &Disease) -> (i32, i32) {
        match self.state {
            State::Infected { symptoms: true, severity: InfectionSeverity::Severe {} } => {
                if self.infection_day == disease.get_disease_last_day() {
                    if disease.is_to_be_deceased(rng) {
                        self.state = State::Deceased {};
                        return (1, 0);
                    }
                    self.state = State::Recovered {};
                    return (0, 1);
                }
            }
            State::Infected { symptoms: true, severity: InfectionSeverity::Mild {} } => {
                if self.infection_day == constants::MILD_INFECTED_LAST_DAY {
                    self.state = State::Recovered {};
                    return (0, 1);
                }
            }
            State::Infected { .. } => {
                if self.infection_day == constants::ASYMPTOMATIC_LAST_DAY {
                    self.state = State::Recovered {};
                    return (0, 1);
                }
            }
            _ => {
                panic!("Invalid state transition!")
            }
        }
        (0, 0)
    }

    pub fn is_susceptible(&self) -> bool {
        matches!(self.state, State::Susceptible {})
    }

    pub fn is_exposed(&self) -> bool {
        matches!(self.state, State::Exposed { .. })
    }

    pub fn is_infected(&self) -> bool {
        matches!(self.state, State::Infected { .. })
    }

    pub fn is_pre_symptomatic(&self) -> bool {
        matches!(self.state, State::Infected { symptoms: _, severity: InfectionSeverity::Pre { .. } })
    }

    pub fn is_symptomatic(&self) -> bool {
        match self.state {
            State::Infected { symptoms: true, severity } => !matches!(severity, InfectionSeverity::Pre { .. }),
            _ => false,
        }
    }

    pub fn is_deceased(&self) -> bool {
        matches!(self.state, State::Deceased {})
    }

    pub fn increment_infection_day(&mut self) {
        self.infection_day += 1;
    }

    // should be called only during initialization
    pub fn set_mild_asymptomatic(&mut self) {
        self.state = State::Infected { symptoms: false, severity: InfectionSeverity::Mild };
        self.infection_day = 1
    }

    // should be called only during initialization
    pub fn set_mild_symptomatic(&mut self) {
        self.state = State::Infected { symptoms: true, severity: InfectionSeverity::Mild };
        self.infection_day = 1
    }

    // should be called only during initialization
    pub fn set_severe_infected(&mut self) {
        self.state = State::Infected { symptoms: true, severity: InfectionSeverity::Severe };
        self.infection_day = 1
    }

    #[cfg(test)]
    pub fn is_mild_asymptomatic(&self) -> bool {
        matches!(self.state, State::Infected { symptoms: false, severity: InfectionSeverity::Mild })
    }

    pub fn is_mild_symptomatic(&self) -> bool {
        matches!(self.state, State::Infected { symptoms: true, severity: InfectionSeverity::Mild })
    }

    pub fn is_infected_severe(&self) -> bool {
        matches!(self.state, State::Infected { symptoms: true, severity: InfectionSeverity::Severe })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_initialize() {
        let machine = DiseaseStateMachine::new();

        let result = matches!(machine.state, State::Susceptible {});
        assert!(result);
        assert_eq!(machine.get_infection_day(), 0);
    }

    #[test]
    fn should_infect() {
        let mut machine = DiseaseStateMachine::new();
        let disease = Disease::new(10, 20, 40, 9, 12, 0.025, 0.25, 0.02, 0.3, 0.3, 24, 24);
        machine.expose(100);
        machine.infect(&mut RandomWrapper::new(), 140, &disease);

        let result = matches!(
            machine.state,
            State::Infected { symptoms: false, severity: InfectionSeverity::Mild {} }
                | State::Infected { symptoms: true, severity: InfectionSeverity::Pre { at_hour: 140 } }
        );

        assert!(result);
    }

    #[test]
    fn should_not_infect() {
        let mut machine = DiseaseStateMachine::new();
        let disease = Disease::new(10, 20, 40, 9, 12, 0.025, 0.25, 0.02, 0.3, 0.3, 24, 24);

        machine.expose(100);
        machine.infect(&mut RandomWrapper::new(), 110, &disease);

        let result = matches!(machine.state, State::Exposed { .. });

        assert!(result);
    }

    #[test]
    #[should_panic]
    fn should_panic() {
        let disease = Disease::init("config/diseases.yaml", &String::from("small_pox"));
        let mut machine = DiseaseStateMachine::new();
        machine.hospitalize(&disease, 2);
    }

    #[test]
    fn should_change_infection_severity() {
        let mut machine = DiseaseStateMachine::new();
        let disease = Disease::new(10, 20, 40, 9, 12, 0.025, 0.25, 0.02, 0.3, 0.3, 24, 24);
        let mut rng = RandomWrapper::new();

        machine.state = State::Infected { symptoms: true, severity: InfectionSeverity::Pre { at_hour: 100 } };

        machine.change_infection_severity(140, &mut rng, &disease);

        let result = match machine.state {
            State::Infected { symptoms: true, severity } => !matches!(severity, InfectionSeverity::Pre { .. }),
            _ => false,
        };

        assert!(result);
    }

    #[test]
    fn should_not_change_infection_severity() {
        let mut machine = DiseaseStateMachine::new();
        let disease = Disease::new(10, 20, 40, 9, 12, 0.025, 0.25, 0.02, 0.3, 0.3, 24, 24);
        let mut rng = RandomWrapper::new();

        machine.state = State::Infected { symptoms: true, severity: InfectionSeverity::Pre { at_hour: 100 } };

        machine.change_infection_severity(120, &mut rng, &disease);

        let result = match machine.state {
            State::Infected { symptoms: true, severity } => matches!(severity, InfectionSeverity::Pre { at_hour: 100 }),
            _ => false,
        };

        assert!(result);
    }

    #[test]
    fn should_check_if_pre_symptomatic() {
        let mut machine = DiseaseStateMachine::new();

        machine.state = State::Infected { symptoms: true, severity: InfectionSeverity::Pre { at_hour: 100 } };
        assert!(machine.is_pre_symptomatic());

        machine.state = State::Infected { symptoms: true, severity: InfectionSeverity::Mild {} };
        assert_eq!(machine.is_pre_symptomatic(), false);
    }

    #[test]
    fn should_set_mild_asymptomatic() {
        let mut machine = DiseaseStateMachine::new();
        machine.set_mild_asymptomatic();
        assert_eq!(machine.state, State::Infected { symptoms: false, severity: InfectionSeverity::Mild });
        assert_eq!(machine.infection_day, 1);
    }

    #[test]
    fn should_set_mild_symptomatic() {
        let mut machine = DiseaseStateMachine::new();
        machine.set_mild_symptomatic();
        assert_eq!(machine.state, State::Infected { symptoms: true, severity: InfectionSeverity::Mild });
        assert_eq!(machine.infection_day, 1);
    }

    #[test]
    fn should_set_severe_infected() {
        let mut machine = DiseaseStateMachine::new();
        machine.set_severe_infected();
        assert_eq!(machine.state, State::Infected { symptoms: true, severity: InfectionSeverity::Severe });
        assert_eq!(machine.infection_day, 1);
    }

    #[test]
    fn should_check_if_symptomatic() {
        let mut machine = DiseaseStateMachine::new();

        machine.state = State::Infected { symptoms: true, severity: InfectionSeverity::Mild };
        assert!(machine.is_symptomatic());

        machine.state = State::Infected { symptoms: true, severity: InfectionSeverity::Severe };
        assert!(machine.is_symptomatic());

        machine.state = State::Infected { symptoms: false, severity: InfectionSeverity::Mild };
        assert!(!machine.is_symptomatic());

        machine.state = State::Infected { symptoms: true, severity: InfectionSeverity::Pre { at_hour: 100 } };
        assert!(!machine.is_symptomatic());
    }
}
