use crate::{fdr::FlightDataSource, garmin, AviationLogSourceOption};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    path::Path,
};

/// Error type for source detection
#[derive(Debug)]
pub enum SourceDetectionError {
    UnrecognizedSource,
}

impl Error for SourceDetectionError {}

impl Display for SourceDetectionError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Unrecognized source")
    }
}

/// Detect the source of an avionics log file
///
/// This is useful to determine the correct parser to use for the log file
pub fn detect_source(_path: &Path) -> Result<AviationLogSourceOption, SourceDetectionError> {
    // This function is a placeholder for future implementation of source auto-detection
    // Currently, only Garmin logs are supported
    Ok(AviationLogSourceOption::Garmin)
}

/// Read an avionics log file into a data structure
///
/// This is where all logic about how to build each source type belongs
pub fn read_avionics_log(
    source: &AviationLogSourceOption,
    path: &Path,
) -> Result<Box<dyn FlightDataSource>, Box<dyn Error>> {
    match source {
        AviationLogSourceOption::Garmin => Ok(Box::new(garmin::GarminLogFile::new(path)?)),
    }
}
