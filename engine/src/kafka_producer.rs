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

use rdkafka::producer::{FutureProducer, FutureRecord, DeliveryFuture};
use rdkafka::ClientConfig;
use crate::commute::CommutersByRegion;
use crate::custom_types::Hour;
use crate::environment;
use crate::travel_plan::MigratorsByRegion;
use crate::listeners::events::counts::Counts;

const TICK_ACKS_TOPIC: &str = "ticks_ack";
pub const MIGRATION_TOPIC: &str = "migration";
pub const COMMUTE_TOPIC: &str = "commute";

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new() -> KafkaProducer {
        let kafka_url = environment::kafka_url();
        KafkaProducer {
            producer: ClientConfig::new()
                .set("bootstrap.servers", kafka_url.as_str())
                .create()
                .expect("Could not create Kafka Producer")
        }
    }

    pub fn send_ack(&mut self, tick: &TickAck) -> DeliveryFuture {
        let tick_string = serde_json::to_string(&tick).unwrap();
        let record: FutureRecord<String, String> = FutureRecord::to(TICK_ACKS_TOPIC)
            .payload(&tick_string);
        self.producer.send(record, 0)
    }

    pub fn send_migrators(&mut self, outgoing: Vec<MigratorsByRegion>) {
        outgoing.iter().for_each(|out_region| {
            let payload = serde_json::to_string(out_region).unwrap();
            debug!("Sending migrators: {} to region: {}", payload, out_region.to_engine_id());
            let record: FutureRecord<String, String> = FutureRecord::to(MIGRATION_TOPIC)
                .payload(&payload);
            self.producer.send(record, 0);
        });
    }

    pub fn send_commuters(&mut self, outgoing: Vec<CommutersByRegion>) {
        outgoing.iter().for_each(|out_region| {
            let payload = serde_json::to_string(out_region).unwrap();
            debug!("Sending commuters: {} to region: {}", payload, out_region.to_engine_id());
            let record: FutureRecord<String, String> = FutureRecord::to(COMMUTE_TOPIC)
                .payload(&payload);
            self.producer.send(record, 0);
        });
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TickAck {
    pub engine_id: String,
    pub hour: Hour,
    pub counts: Counts,
    pub locked_down: bool,
}
