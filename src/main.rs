//! Export an X-Plane Flight Data Recorder (FDR) file from an avionics log file.
//!
//! The FDR file format is a simple csv-like text format which is described inside example files in the "Instructions"
//! directory of the X-Plane installation.

use clap::Parser;
use std::fs::File;
use std::io::ErrorKind;
use xfdr::detection::{detect_source, read_avionics_log};
use xfdr::fdr::{self, FDRConfigurationBuilder, FDRWriter};
use xfdr::Args;

/// Entrypoint for the xfdr binary
fn main() {
    let args = Args::parse();

    // auto-detect the source if it wasn't provided
    let source = args.source.unwrap_or_else(|| {
        detect_source(&args.input).unwrap_or_else(|e| {
            eprintln!("Unable to detect source: {}", e);
            std::process::exit(1);
        })
    });

    // read the avionics log file into a data structure
    let data = read_avionics_log(&source, &args.input).unwrap_or_else(|e| {
        eprintln!("Unable to read avionics log: {}", e);
        std::process::exit(1);
    });

    // config tells the writer how to format the output
    let config = FDRConfigurationBuilder::default()
        .aircraft_model(args.aircraft)
        .tail_number_override(args.tail_number)
        .strict(args.strict)
        .auto_drefs(args.auto_drefs)
        .allow_nulls(args.allow_nulls)
        .build();

    // open the output file for writing
    let mut output: Box<dyn std::io::Write> = args.output.as_ref().map_or_else(
        || Box::new(std::io::stdout()) as Box<dyn std::io::Write>,
        |p| {
            Box::new(File::create(&p).unwrap_or_else(|e| {
                eprintln!("Unable to create output file: {}", e);
                std::process::exit(1);
            }))
        },
    );

    // write the FDR file or handle errors
    if let Err(e) = FDRWriter::new(config).write(data, &mut output) {
        use fdr::FDRWriteError;
        match e {
            FDRWriteError::IO(ref e) if (args.output.is_none()) && e.kind() == ErrorKind::BrokenPipe => {
                // ignore broken pipe errors (created when stdout piped to `head` or `tail` in linux, etc.)
                std::process::exit(0);
            }
            FDRWriteError::IO(e) => {
                eprintln!("IO error: {}", e);
                std::process::exit(1);
            }
            FDRWriteError::Polars(e) => {
                eprintln!("Data handling error: {}", e);
                std::process::exit(1);
            }
            FDRWriteError::FlightDataError(err) => {
                eprintln!("Flight data error: {}", err);
                std::process::exit(1);
            }
        }
    }
}
