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

use clap::Parser;
use mpi::traits::Communicator;
use opentelemetry::sdk::trace::{config, Span};
use opentelemetry::sdk::Resource;
use opentelemetry::trace::noop::NoopTracerProvider;
use opentelemetry::trace::{FutureExt, TraceContextExt, Tracer, TracerProvider};
use opentelemetry::{global, Context, KeyValue};

use engine::config::configuration::{Configuration, EngineConfig};
use engine::config::{Config, TravelPlanConfig};
use engine::disease::Disease;
use engine::{EngineApp, RunMode};

use crate::file_logger::FileLogger;

mod file_logger;

const STANDALONE_ENGINE_ID: &str = "standalone";
const BUFFER_SIZE: usize = 50 * 1024 * 1024;

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, value_name = "FILE", help = "Use a config file to run the simulation")]
    config: Option<String>,

    #[arg(short, long, default_value_t = false)]
    #[arg(help = "Start the engine in daemon mode. It will wait for messages from Kafka. \
            Specifying this flag will cause the config argument to be ignored")]
    standalone: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "start the tracing")]
    tracing: bool,

    #[arg(short, long, default_value_t = 4)]
    #[arg(help = "Number of parallel threads for data parallelization")]
    threads: u32,
}

fn init_tracer(enable: bool) -> Context {
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

    if !enable {
        let tracer_provider = NoopTracerProvider::new();
        let tracer = tracer_provider.tracer("my-noop-tracer");
        let noop_span = tracer.start("noop");
        Context::current_with_span(noop_span)
    } else {
        let _tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_auto_split_batch(true)
            .with_max_packet_size(9216)
            .with_service_name("epirust-trace")
            .with_trace_config(config().with_resource(Resource::new(vec![KeyValue::new("exporter", "otlp-jaeger")])))
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();

        let span: Span = _tracer.start("root");
        Context::current_with_span(span)
    }
}

#[tokio::main]
async fn main() {
    // env_logger::init();

    let args = Args::parse();
    let number_of_threads = args.threads;
    let standalone = args.standalone;
    let tracing = args.tracing;

    let disease_handler: Option<Disease> = None;

    let cx: Context = init_tracer(tracing);

    println!("println logging is working");

    if standalone {
        println!("its here in standalone");
        let default_config_path = "engine/config/default.json".to_string();
        let config_path = args.config.unwrap_or(default_config_path);
        let engine_config: Config = Config::read(&config_path).expect("Failed to read config file");
        let run_mode = RunMode::Standalone;
        let engine_id = STANDALONE_ENGINE_ID.to_string();
        FileLogger::init(engine_id.to_string()).unwrap();
        EngineApp::start(engine_id, engine_config, &run_mode, None, disease_handler, number_of_threads).with_context(cx).await;
    } else {
        println!("in multi-engine mode");
        let mut universe = mpi::initialize().unwrap();
        // Try to attach a buffer.
        universe.set_buffer_size(BUFFER_SIZE);
        assert_eq!(universe.buffer_size(), BUFFER_SIZE);

        let world = universe.world();
        let rank = world.rank();
        let default_config_path = "engine/config/simulation.json".to_string();
        let config_path = args.config.unwrap_or(default_config_path);
        let config = Configuration::read(&config_path).expect("Error while reading config");
        config.validate();
        let config_per_engine = config.get_engine_configs();
        let index: usize = (rank) as usize;
        let self_config: &EngineConfig = config_per_engine.get(index).unwrap();
        let travel_plan: &TravelPlanConfig = config.get_travel_plan();
        let engine_config = &self_config.config;
        let engine_id = String::from(&self_config.engine_id);
        FileLogger::init(engine_id.to_string()).unwrap();
        let run_mode = RunMode::MultiEngine;
        EngineApp::start(
            engine_id.clone(),
            engine_config.clone(),
            &run_mode,
            Some(travel_plan.clone()),
            disease_handler,
            number_of_threads,
        )
        .with_context(cx)
        .await;
    }
}
