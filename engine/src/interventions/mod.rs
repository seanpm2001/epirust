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

use crate::interventions::hospital::BuildNewHospital;
use crate::interventions::lockdown::LockdownIntervention;
use crate::interventions::vaccination::VaccinateIntervention;

pub mod hospital;
pub mod intervention_type;
pub mod lockdown;
pub mod vaccination;

pub struct Interventions {
    pub vaccinate: VaccinateIntervention,
    pub lockdown: LockdownIntervention,
    pub build_new_hospital: BuildNewHospital,
}
