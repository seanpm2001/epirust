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

const KafkaServices = require('../services/kafka');
const config = require("../config");
const {SimulationStatus} = require("../db/models/Simulation");
const SimulationService = require('../db/services/SimulationService');
const CountService = require('../db/services/CountService');
const through2 = require("through2");


class SimulationCountsConsumer {
  constructor() {
    this.kafkaConsumer =
      new KafkaServices.KafkaGroupConsumer(config.KAFKA_URL, config.COUNTS_TOPIC, config.KAFKA_GROUP);
  }

  async start() {
    const commitStream = this.kafkaConsumer.consumerStream.createCommitStream();

    const handleMessage = this.handleMessage.bind(this);
    this.kafkaConsumer.consumerStream
      .pipe(through2.obj(async function (data, enc, cb) {
        await handleMessage(data);
        cb(null, data);
      }))
      .pipe(commitStream);
  }

  async handleMessage(data) {
    const parsedMessage = JSON.parse(data.value);

    let simulationId = parseInt(data.key.toString());

    const simulationEnded = "simulation_ended" in parsedMessage;
    const isInterventionMessage = "intervention" in parsedMessage;

    if (simulationEnded) {
      await SimulationService.updateSimulationStatus(simulationId, SimulationStatus.FINISHED);
      console.log("Consumed all messages for ", simulationId);
    } else if (isInterventionMessage) {
      console.log("handling intervention");
      await CountService.addIntervention(simulationId, parsedMessage);
    } else {
      await this.handleCountMessage(parsedMessage, simulationId);
    }
  }

  async handleCountMessage(parsedMessage, simulationId) {
    if (parsedMessage.hour === 1)
      SimulationService.updateSimulationStatus(simulationId, SimulationStatus.RUNNING);

    if(parsedMessage.hour % 100 === 0)
      console.log(simulationId, parsedMessage.hour);

    await CountService.upsertCount(simulationId, parsedMessage);
  }
}

module.exports = {SimulationCountsConsumer};
