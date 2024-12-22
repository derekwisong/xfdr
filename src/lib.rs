pub mod detection;
pub mod fdr;
pub mod garmin;

use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[doc(hidden)]
pub fn resource_path(filename: &str) -> std::path::PathBuf {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("resources");
    d.push(filename);
    d
}

/// Export an X-Plane Flight Data Recorder (FDR) file from an avionics log file.
///
/// FDR files may be replayed in X-Plane to visualize flight path and telemetry data. This is useful as a post-flight
/// debriefing and analysis tool, for creating videos, or for sharing flight data with others.
#[derive(Parser, Debug)]
#[command(version, about, long_about)]
pub struct Args {
    /// The source of the avionics log file, otherwise auto-detect source
    #[arg(short, long, value_enum)]
    pub source: Option<AviationLogSourceOption>,

    /// The path to an aircraft file, relative to the X-Plane root, to be used as the aircraft model during replay
    #[arg(short, long, default_value = "Aircraft/Laminar Research/Cirrus SR22/Cirrus SR22.acf")]
    pub aircraft: String,

    /// Optionally override the aircraft tail number, if any, that was discovered in the avionics log
    #[arg(short, long)]
    pub tail_number: Option<String>,

    /// Path to an avionics log file
    pub input: PathBuf,

    /// Path to output a FDR file. If not specified, output is written to stdout
    pub output: Option<PathBuf>,

    /// If set, do not ignore unknown data fields in the avionics log
    #[arg(long, default_value = "false")]
    pub strict: bool,
}

/// Supported avionics log sources that can be used as command line arguments
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum AviationLogSourceOption {
    /// Flight data logs from Garmin Engine Indication System (EIS) products. One such example is the G500 TXi EIS
    Garmin,
    // .. add more sources here as they become known (Avidyne, etc.)
}

#[cfg(test)]
mod tests {
    use super::*;
    const APP_NAME: &str = "xfdr";

    #[test]
    fn test_args_parse() -> Result<(), String> {
        let args = Args::parse_from(vec![APP_NAME, "input.csv"]);

        assert_eq!(args.input.to_str().unwrap(), "input.csv");
        assert_eq!(args.output, None);
        Ok(())
    }
}
