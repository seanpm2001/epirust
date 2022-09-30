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

use crate::geography::Point;
use crate::kafka::travel_consumer;
use crate::models::constants;
use crate::travel::commute::Commuter;
use common::models::custom_types::Hour;
use futures::StreamExt;
use rdkafka::consumer::MessageStream;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CommutersByRegion {
    pub(in crate::travel::commute) to_engine_id: String,
    pub commuters: Vec<Commuter>,
}

impl CommutersByRegion {
    pub fn to_engine_id(&self) -> &String {
        &self.to_engine_id
    }

    pub fn get_commuters(self) -> Vec<Commuter> {
        self.commuters
    }

    pub(crate) async fn receive_commuters_from_region(
        message_stream: &mut MessageStream<'_>,
        engine_id: &String,
    ) -> Option<CommutersByRegion> {
        let msg = message_stream.next().await;
        let mut maybe_commuters = travel_consumer::read_commuters(msg);
        while maybe_commuters.is_none()
            || (maybe_commuters.as_ref().unwrap().commuters.is_empty()
                && maybe_commuters.as_ref().unwrap().to_engine_id() == engine_id)
        {
            let next_msg = message_stream.next().await;
            maybe_commuters = travel_consumer::read_commuters(next_msg);
        }
        maybe_commuters
    }

    pub fn get_commuters_by_region(
        regions: &[String],
        commuters: &Vec<(Point, Commuter)>,
        simulation_hour: Hour,
    ) -> Vec<CommutersByRegion> {
        let mut commuters_by_region: Vec<CommutersByRegion> = Vec::new();
        for region in regions {
            let mut commuters_for_region: Vec<Commuter> = Vec::new();
            for (_point, commuter) in commuters {
                if simulation_hour % 24 == constants::ROUTINE_TRAVEL_START_TIME && commuter.work_location.location_id == *region {
                    commuters_for_region.push(commuter.clone())
                }
                if simulation_hour % 24 == constants::ROUTINE_TRAVEL_END_TIME && commuter.home_location.location_id == *region {
                    commuters_for_region.push(commuter.clone())
                }
            }
            commuters_by_region.push(CommutersByRegion { to_engine_id: region.clone(), commuters: commuters_for_region })
        }
        commuters_by_region
    }
}
