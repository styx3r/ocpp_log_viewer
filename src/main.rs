use chrono::DateTime;
use clap::Parser;
use glob::glob;
use rerun::external::re_log;
use rerun::Scalars;
use std::fs;

#[derive(Debug, clap::Parser)]
#[clap(author, version, about)]
pub struct Args {
    /// Path to the trace file.
    #[arg(short, long)]
    trace_file_directory: String,
}

fn main() -> anyhow::Result<()> {
    re_log::setup_logging();
    let args = Args::parse();

    let mut contents: Vec<String> = Vec::new();
    for entry in glob(format!("{}/**/*.trace", &args.trace_file_directory).as_str())
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
                        panic!("Could not read file `{}`", args.trace_file_directory);
                    }
                };
            }
            Err(e) => println!("{:?}", e),
        }
    }

    let rec = rerun::RecordingStreamBuilder::new("OcppMeter values").spawn()?;

    for line in &contents {
        let line_parts = line
            .split(char::is_whitespace)
            .map(|s| s.to_owned())
            .collect::<Vec<_>>();
        if line_parts.len() != 10 {
            continue;
        }

        let (date, time, json) = (
            line_parts[0].clone(),
            line_parts[1].clone(),
            line_parts[9].clone(),
        );

        let date_time = format!("{} {} +00:00", date, time);
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

            rec.log_static(
                format!("{}/current/import/L1", date),
                &rerun::SeriesLines::new()
                    .with_colors([[255, 0, 0]])
                    .with_names(["Current.Import(L1)"])
                    .with_widths([2.0]),
            )?;

            rec.set_timestamp_secs_since_epoch("time", timestamp.timestamp() as f64);
            rec.log(
                format!("{}/current/import/L1", date),
                &Scalars::single(current_import_l1.unwrap_or(0.0)),
            )?;

            rec.log(
                "current/import/L2",
                &Scalars::single(current_import_l2.unwrap_or(0.0)),
            )?;

            rec.log(
                "current/offered",
                &Scalars::single(current_offered.unwrap_or(0.0)),
            )?;

            rec.log(
                "power/offered",
                &Scalars::single(power_offered.unwrap_or(0.0)),
            )?;

            rec.log("voltage/L1", &Scalars::single(voltage_l1.unwrap_or(0.0)))?;

            rec.log("voltage/L2", &Scalars::single(voltage_l2.unwrap_or(0.0)))?;

            rec.log("voltage/L3", &Scalars::single(voltage_l3.unwrap_or(0.0)))?;

            rec.log(
                "power/active/import/L1",
                &Scalars::single(power_active_import_l1.unwrap_or(0.0)),
            )?;

            rec.log(
                "power/active/import/L2",
                &Scalars::single(power_active_import_l2.unwrap_or(0.0)),
            )?;

            rec.log(
                "power/active/import/L3",
                &Scalars::single(power_active_import_l3.unwrap_or(0.0)),
            )?;

            rec.log(
                "power/active/import/sum",
                &Scalars::single(
                    power_active_import_l1.unwrap_or(0.0)
                        + power_active_import_l2.unwrap_or(0.0)
                        + power_active_import_l3.unwrap_or(0.0),
                ),
            )?;
        }
    }

    Ok(())
}
