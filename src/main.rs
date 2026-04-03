use chrono::DateTime;
use clap::Parser;
use glob::glob;
use rerun::{
    AnnotationContext, RecordingStream, Scalars,
    blueprint::{Blueprint, ContainerLike, Grid, Tabs, TimePanel, TimeSeriesView, Vertical},
    datatypes::ClassDescriptionMapElem,
    external::{
        re_log,
        re_sdk_types::blueprint::components::{LoopMode, PanelState, PlayState},
    },
};
use rust_ocpp::v1_6::types::Phase;
use std::fs;

use regex::Regex;

use rusqlite::Connection;

#[derive(Debug, clap::Parser)]
#[clap(author, version, about)]
pub struct Args {
    #[command(flatten)]
    exclusive: Exclusive,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
struct Exclusive {
    /// Path to the trace file.
    #[arg(short, long)]
    trace_file_directory: Option<String>,

    /// Path to SQLite DB
    #[arg(short, long)]
    sqlite_db_path: Option<String>,
}

fn read_file(file_directory: &String, file_extension: &str) -> Vec<String> {
    let mut contents: Vec<String> = Vec::new();
    for entry in glob(format!("{}/**/*.{}", file_directory, file_extension).as_str())
        .expect("Failed to read glob pattern")
    {
        match entry {
            Ok(path) => {
                match fs::read_to_string(&path) {
                    // If successful return the files text as `contents`.
                    // `c` is a local variable.
                    Ok(c) => c.split("\n").map(|s| s.to_owned()).for_each(|e| {
                        contents.push(e);
                    }),
                    // Handle the `error` case.
                    Err(_) => {
                        // Write `msg` to `stderr`.
                        panic!("Could not read file `{}`", file_directory);
                    }
                };
            }
            Err(e) => println!("{:?}", e),
        }
    }

    contents
}

fn plot_current_import(rec: &RecordingStream, phase: Phase, value: f64) -> anyhow::Result<()> {
    rec.log(
        format!("current/import/{:?}", phase),
        &Scalars::single(value),
    )?;

    Ok(())
}

fn plot_voltage(rec: &RecordingStream, phase: Phase, value: f64) -> anyhow::Result<()> {
    rec.log(format!("voltage/{:?}", phase), &Scalars::single(value))?;

    Ok(())
}

fn plot_current_offered(rec: &RecordingStream, value: f64) -> anyhow::Result<()> {
    rec.log("current/offered", &Scalars::single(value))?;

    Ok(())
}

fn plot_power_offered(rec: &RecordingStream, value: f64) -> anyhow::Result<()> {
    rec.log("power/offered", &Scalars::single(value))?;

    Ok(())
}

fn plot_power_active_import(rec: &RecordingStream, phase: Phase, value: f64) -> anyhow::Result<()> {
    rec.log(
        format!("power/active/import/{:?}", phase),
        &Scalars::single(value),
    )?;

    Ok(())
}

fn plot_power_active_import_sum(rec: &RecordingStream, value: f64) -> anyhow::Result<()> {
    rec.log("power/active/import/sum", &Scalars::single(value))?;

    Ok(())
}

fn plot_pv_production(rec: &RecordingStream, value: f64) -> anyhow::Result<()> {
    rec.log("/log/pv_production", &Scalars::single(value))?;

    Ok(())
}

fn plot_battery_load(rec: &RecordingStream, value: f64) -> anyhow::Result<()> {
    rec.log("/log/battery_load", &Scalars::single(value))?;

    Ok(())
}

fn plot_ev_import(rec: &RecordingStream, value: f64) -> anyhow::Result<()> {
    rec.log("/log/ev_import", &Scalars::single(value))?;

    Ok(())
}

fn plot_load_overall(rec: &RecordingStream, value: f64) -> anyhow::Result<()> {
    rec.log("/log/load_overall", &Scalars::single(value))?;

    Ok(())
}

fn plot_overproduction(rec: &RecordingStream, value: f64) -> anyhow::Result<()> {
    rec.log("/log/overproduction", &Scalars::single(value))?;

    Ok(())
}

fn plot_meter_value_readings(
    rec: &RecordingStream,
    trace_file_entry: &TraceFileEntry,
) -> anyhow::Result<()> {
    rec.set_timestamp_secs_since_epoch("time", trace_file_entry.timestamp as f64);

    plot_current_import(rec, Phase::L1, trace_file_entry.current_import.l1)?;
    plot_current_import(rec, Phase::L2, trace_file_entry.current_import.l2)?;
    plot_current_offered(rec, trace_file_entry.current_offered)?;
    plot_power_offered(rec, trace_file_entry.power_offered)?;

    plot_voltage(rec, Phase::L1, trace_file_entry.voltage.l1)?;
    plot_voltage(rec, Phase::L2, trace_file_entry.voltage.l2)?;
    plot_voltage(rec, Phase::L3, trace_file_entry.voltage.l3)?;

    plot_power_active_import(rec, Phase::L1, trace_file_entry.power_active_import.l1)?;
    plot_power_active_import(rec, Phase::L2, trace_file_entry.power_active_import.l2)?;
    plot_power_active_import(rec, Phase::L3, trace_file_entry.power_active_import.l3)?;

    plot_power_active_import_sum(
        rec,
        trace_file_entry.power_active_import.l2
            + trace_file_entry.power_active_import.l2
            + trace_file_entry.power_active_import.l3,
    )?;

    Ok(())
}

fn plot_log_file_entry(rec: &RecordingStream, log_file_entry: &LogFileEntry) -> anyhow::Result<()> {
    rec.set_timestamp_secs_since_epoch("time", log_file_entry.timestamp);

    plot_pv_production(rec, log_file_entry.pv_overproduction)?;
    plot_battery_load(rec, log_file_entry.battery_load)?;
    plot_ev_import(rec, log_file_entry.ev_import)?;
    plot_load_overall(rec, log_file_entry.load_overall)?;
    plot_overproduction(rec, log_file_entry.overproduction)?;

    Ok(())
}

struct CurrentImport {
    l1: f64,
    l2: f64,
}

struct Voltage {
    l1: f64,
    l2: f64,
    l3: f64,
}

struct PowerActiveImport {
    l1: f64,
    l2: f64,
    l3: f64,
}

struct TraceFileEntry {
    timestamp: f64,
    current_import: CurrentImport,
    current_offered: f64,
    power_offered: f64,
    voltage: Voltage,
    power_active_import: PowerActiveImport,
}

fn parse_meter_value_readings(contents: &Vec<String>) -> anyhow::Result<Vec<TraceFileEntry>> {
    let mut trace_file_entries: Vec<TraceFileEntry> = Vec::new();
    for line in contents {
        let line_parts = line
            .split(char::is_whitespace)
            .map(|s| s.to_owned())
            .collect::<Vec<_>>();

        let (date, time, json) = if line_parts.len() == 10 {
            (
                line_parts[0].clone(),
                line_parts[1].clone(),
                line_parts[9].clone(),
            )
        } else if line_parts.len() == 9 {
            (
                line_parts[0].clone(),
                line_parts[1].clone(),
                line_parts[8].clone(),
            )
        } else {
            continue;
        };

        let date_time = format!("{} {} +00:00", date.as_str().replace("[", ""), time);
        if date_time.is_empty() {
            continue;
        }

        let timestamp = match DateTime::parse_from_str(date_time.as_str(), "%Y-%m-%d %H:%M:%S %z") {
            Ok(d) => d,
            _ => continue,
        };

        if let Ok(meter_vaules_request) = serde_json::from_str::<
            rust_ocpp::v1_6::messages::meter_values::MeterValuesRequest,
        >(json.as_str())
        {
            let mut current_import_l1: Option<f64> = None;
            let mut current_import_l2: Option<f64> = None;

            let mut current_offered: Option<f64> = None;
            let mut power_offered: Option<f64> = None;

            let mut voltage_l1: Option<f64> = None;
            let mut voltage_l2: Option<f64> = None;
            let mut voltage_l3: Option<f64> = None;

            let mut power_active_import_l1: Option<f64> = None;
            let mut power_active_import_l2: Option<f64> = None;
            let mut power_active_import_l3: Option<f64> = None;

            for meter_value in &meter_vaules_request.meter_value {
                for sampled_value in &meter_value.sampled_value {
                    match sampled_value.measurand {
                        Some(rust_ocpp::v1_6::types::Measurand::CurrentImport) => {
                            match sampled_value.phase {
                                Some(rust_ocpp::v1_6::types::Phase::L1) => {
                                    current_import_l1 =
                                        Some(sampled_value.value.parse::<f64>().unwrap_or(0.0))
                                }
                                Some(rust_ocpp::v1_6::types::Phase::L2) => {
                                    current_import_l2 =
                                        Some(sampled_value.value.parse::<f64>().unwrap_or(0.0))
                                }
                                _ => {}
                            }
                        }
                        Some(rust_ocpp::v1_6::types::Measurand::CurrentOffered) => {
                            current_offered =
                                Some(sampled_value.value.parse::<f64>().unwrap_or(0.0));
                        }
                        Some(rust_ocpp::v1_6::types::Measurand::PowerOffered) => {
                            power_offered = Some(sampled_value.value.parse::<f64>().unwrap_or(0.0));
                        }
                        Some(rust_ocpp::v1_6::types::Measurand::PowerActiveImport) => {
                            match sampled_value.phase {
                                Some(rust_ocpp::v1_6::types::Phase::L1) => {
                                    power_active_import_l1 =
                                        Some(sampled_value.value.parse::<f64>().unwrap_or(0.0))
                                }
                                Some(rust_ocpp::v1_6::types::Phase::L2) => {
                                    power_active_import_l2 =
                                        Some(sampled_value.value.parse::<f64>().unwrap_or(0.0))
                                }
                                Some(rust_ocpp::v1_6::types::Phase::L3) => {
                                    power_active_import_l3 =
                                        Some(sampled_value.value.parse::<f64>().unwrap_or(0.0))
                                }
                                _ => {}
                            }
                        }
                        Some(rust_ocpp::v1_6::types::Measurand::Voltage) => {
                            match sampled_value.phase {
                                Some(rust_ocpp::v1_6::types::Phase::L1) => {
                                    voltage_l1 =
                                        Some(sampled_value.value.parse::<f64>().unwrap_or(0.0))
                                }
                                Some(rust_ocpp::v1_6::types::Phase::L2) => {
                                    voltage_l2 =
                                        Some(sampled_value.value.parse::<f64>().unwrap_or(0.0))
                                }
                                Some(rust_ocpp::v1_6::types::Phase::L3) => {
                                    voltage_l3 =
                                        Some(sampled_value.value.parse::<f64>().unwrap_or(0.0))
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }

            trace_file_entries.push(TraceFileEntry {
                timestamp: timestamp.timestamp() as f64,
                current_import: CurrentImport {
                    l1: current_import_l1.unwrap_or(0.0),
                    l2: current_import_l2.unwrap_or(0.0),
                },
                current_offered: current_offered.unwrap_or(0.0),
                power_offered: power_offered.unwrap_or(0.0),
                voltage: Voltage {
                    l1: voltage_l1.unwrap_or(0.0),
                    l2: voltage_l2.unwrap_or(0.0),
                    l3: voltage_l3.unwrap_or(0.0),
                },
                power_active_import: PowerActiveImport {
                    l1: power_active_import_l1.unwrap_or(0.0),
                    l2: power_active_import_l2.unwrap_or(0.0),
                    l3: power_active_import_l3.unwrap_or(0.0),
                },
            });
        }
    }

    Ok(trace_file_entries)
}

struct LogFileEntry {
    timestamp: f64,
    pv_overproduction: f64,
    battery_load: f64,
    ev_import: f64,
    load_overall: f64,
    overproduction: f64,
}

fn parse_log_file_entries(contents: &Vec<String>) -> anyhow::Result<Vec<LogFileEntry>> {
    let mut log_file_entries: Vec<LogFileEntry> = Vec::new();

    let re = Regex::new(r"([a-zA-Z]+) (-?[0-9]+(\.[0-9]+)?) \+ (-?[0-9]+(\.[0-9]+)?) \+ (-?[0-9]+(\.[0-9]+)?) \+ (-?[0-9]+(\.[0-9]+)?) = (-?[0-9]+(\.[0-9]+)?)").unwrap();
    let mut average_count = 0;
    let mut pv_overproduction_average = 0.0;
    let mut battery_load_average = 0.0;
    let mut ev_import_average = 0.0;
    let mut load_overall_average = 0.0;
    let mut overproduction_average = 0.0;

    for line in contents {
        let line_parts = line
            .split(char::is_whitespace)
            .map(|s| s.to_owned())
            .collect::<Vec<_>>();

        if line_parts.len() < 2 {
            continue;
        }
        let (date, time) = (line_parts[0].clone(), line_parts[1].clone());
        let date_time = format!("{} {} +00:00", date.as_str().replace("[", ""), time);
        if date_time.is_empty() {
            continue;
        }

        let timestamp = match DateTime::parse_from_str(date_time.as_str(), "%Y-%m-%d %H:%M:%S %z") {
            Ok(d) => d,
            _ => continue,
        };

        let Some(caps) = re.captures(line.as_str()) else {
            continue;
        };

        let pv_overproduction = caps[2].parse::<f64>()?;
        let battery_load = caps[6].parse::<f64>()?;
        let ev_import = caps[8].parse::<f64>()?;
        let load_overall = (caps[4].parse::<f64>()? + caps[8].parse::<f64>()?).abs();
        let overproduction = if caps[10].parse::<f64>()? >= 0.0 {
            caps[10].parse::<f64>()?
        } else {
            0.0
        };

        if log_file_entries.is_empty() {
            log_file_entries.push(LogFileEntry {
                timestamp: timestamp.timestamp() as f64,
                pv_overproduction,
                battery_load,
                ev_import,
                load_overall,
                overproduction,
            });
        } else {
            let time_delta =
                timestamp.timestamp() as f64 - log_file_entries.last().unwrap().timestamp;
            let threshold = chrono::Duration::minutes(5).as_seconds_f64();

            if time_delta >= threshold {
                log_file_entries.push(LogFileEntry {
                    timestamp: timestamp.timestamp() as f64,
                    pv_overproduction: pv_overproduction_average / average_count as f64,
                    battery_load: battery_load_average / average_count as f64,
                    ev_import: ev_import_average / average_count as f64,
                    load_overall: load_overall_average / average_count as f64,
                    overproduction: overproduction_average / average_count as f64,
                });

                average_count = 0;
                pv_overproduction_average = 0.0;
                battery_load_average = 0.0;
                ev_import_average = 0.0;
                load_overall_average = 0.0;
                overproduction_average = 0.0;
            } else {
                pv_overproduction_average += pv_overproduction;
                battery_load_average += battery_load;
                ev_import_average += ev_import;
                load_overall_average += load_overall;
                overproduction_average += overproduction;

                average_count += 1;
            }
        }
    }

    Ok(log_file_entries)
}

fn setup_blueprint() -> Blueprint {
    Blueprint::new(Grid::new(vec![ContainerLike::from(Tabs::new(vec![
        ContainerLike::from(Vertical::new(vec![
            ContainerLike::from(Grid::new(vec![
                ContainerLike::from(
                    TimeSeriesView::new("Current")
                        .with_origin("/")
                        .with_contents(vec!["current/**"]),
                ),
                ContainerLike::from(
                    TimeSeriesView::new("Power")
                        .with_origin("/")
                        .with_contents(vec!["power/**"]),
                ),
            ])),
            ContainerLike::from(
                TimeSeriesView::new("Voltage")
                    .with_origin("/")
                    .with_contents(vec!["voltage/**"]),
            ),
        ])),
        ContainerLike::from(Grid::new(vec![ContainerLike::from(
            TimeSeriesView::new("Log")
                .with_origin("/")
                .with_contents(vec!["/log/**"]),
        )])),
    ]))]))
    .with_time_panel(
        TimePanel::new()
            .with_state(PanelState::Collapsed)
            .with_timeline("time")
            .with_loop_mode(LoopMode::Selection)
            .with_play_state(PlayState::Following),
    )
}

fn main() -> anyhow::Result<()> {
    re_log::setup_logging();
    let args = Args::parse();
    let rec = rerun::RecordingStreamBuilder::new("OcppMeter values")
        .with_blueprint(setup_blueprint())
        .spawn()?;

    if let Some(trace_file_directory) = args.exclusive.trace_file_directory {
        let trace_contents: Vec<String> = read_file(&trace_file_directory, "trace");
        let log_contents: Vec<String> = read_file(&trace_file_directory, "log");

        parse_meter_value_readings(&trace_contents)?
            .iter()
            .try_for_each(|trace_file_entry| plot_meter_value_readings(&rec, trace_file_entry))?;

        parse_log_file_entries(&log_contents)?
            .iter()
            .try_for_each(|log_file_entry| plot_log_file_entry(&rec, log_file_entry))?;
    } else if let Some(sqlite_db_path) = args.exclusive.sqlite_db_path {
        let connection = Connection::open(sqlite_db_path)?;

        let mut stmt =
            connection.prepare("SELECT name, timestamp, value, phase FROM meter_readings;")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let (name, timestamp, value, phase) = (
                row.get::<usize, String>(0)?,
                row.get::<usize, i64>(1)?,
                row.get::<usize, f64>(2)?,
                if row.get::<usize, String>(3)? == "L1".to_string() {
                    Some(Phase::L1)
                } else if row.get::<usize, String>(3)? == "L2".to_string() {
                    Some(Phase::L2)
                } else if row.get::<usize, String>(3)? == "L3".to_string() {
                    Some(Phase::L3)
                } else if row.get::<usize, String>(3)? == "N".to_string() {
                    Some(Phase::N)
                } else if row.get::<usize, String>(3)? == "L1-N".to_string() {
                    Some(Phase::L1N)
                } else if row.get::<usize, String>(3)? == "L2-N".to_string() {
                    Some(Phase::L2N)
                } else if row.get::<usize, String>(3)? == "L3-N".to_string() {
                    Some(Phase::L3N)
                } else if row.get::<usize, String>(3)? == "L1-L2".to_string() {
                    Some(Phase::L1L2)
                } else if row.get::<usize, String>(3)? == "L2-L3".to_string() {
                    Some(Phase::L2L3)
                } else if row.get::<usize, String>(3)? == "L3-L1".to_string() {
                    Some(Phase::L3L1)
                } else {
                    None
                },
            );

            rec.set_timestamp_secs_since_epoch(
                "time",
                chrono::Duration::milliseconds(timestamp).as_seconds_f64(),
            );

            if name == "CurrentImport"
                && let Some(phase) = phase
            {
                plot_current_import(&rec, phase, value)?;
            } else if name == "CurrentOffered" {
                plot_current_offered(&rec, value)?;
            } else if name == "PowerActiveImport"
                && let Some(phase) = phase
            {
                plot_power_active_import(&rec, phase, value)?;
            } else if name == "PowerOffered" {
                plot_power_offered(&rec, value)?;
            } else if name == "Voltage"
                && let Some(phase) = phase
            {
                plot_voltage(&rec, phase, value)?;
            }
        }

        let mut power_active_import_sum_stmt =
            connection.prepare("SELECT timestamp, SUM(value) FROM meter_readings WHERE name = 'PowerActiveImport' GROUP BY timestamp;")?;
        let mut power_active_import_sum = power_active_import_sum_stmt.query([])?;

        while let Some(row) = power_active_import_sum.next()? {
            let (timestamp, value) = (row.get::<usize, i64>(0)?, row.get::<usize, f64>(1)?);
            rec.set_timestamp_secs_since_epoch(
                "time",
                chrono::Duration::milliseconds(timestamp).as_seconds_f64(),
            );

            plot_power_active_import_sum(&rec, value)?;
        }

        /*
        let mut status_notification_stmt =
            connection.prepare("SELECT connector_id, error_code, info, status, timestamp")?;
        let mut status_notification_rows = status_notification_stmt.query([])?;

        while let Some(row) = status_notification_rows.next()? {
            rec.log(
                "*",
                &AnnotationContext::new([ClassDescription {
                    info: (0, "connector_id").into(),
                    kerow.get(0)?,
                }]),
            )?;
        }
        */
    }

    Ok(())
}
