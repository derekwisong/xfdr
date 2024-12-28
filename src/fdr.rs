use chrono::Utc;
use core::fmt;
use polars::prelude::*;
use std::error::Error;
use std::io::Write;

#[derive(Debug, Clone)]
/// A reference to a data value in the X-Plane simulator and a scaling factor to convert the value
/// provided by the source to the value expected by the simulator.
pub struct DataRef {
    pub path: String,
    pub scale: f64,
}

impl DataRef {
    /// Create a new DataRef with a scaling factor of 1.0
    pub fn new(path: String) -> Self {
        Self { path, scale: 1.0 }
    }

    /// Set the scaling factor for the data reference
    pub fn with_scale(mut self, scale: f64) -> Self {
        self.scale = scale;
        self
    }
}

/// A block of flight data to be written to an FDR file
///
/// The data block contains a list of DREFs and a DataFrame containing the flight data.
/// The DataFrame must have at least 7 columns, which are the required fields for the FDR file.
/// The remaining columns must have a corresponding DREF in the DREF list.
pub struct FlightDataBlock {
    pub drefs: Vec<DataRef>,
    pub data: DataFrame,
}

#[derive(Debug)]
/// Error type for FlightDataBlock
pub enum FlightDataError {
    MissingDrefs(Vec<String>),
    UnknownColumn(String),
    InsufficientData,
}

impl Error for FlightDataError {}

impl std::fmt::Display for FlightDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlightDataError::MissingDrefs(drefs) => {
                write!(f, "Missing DREFs for columns: {:?}", drefs)
            }
            FlightDataError::UnknownColumn(column) => {
                write!(f, "Unknown column: {}", column)
            }
            FlightDataError::InsufficientData => {
                write!(f, "Insufficient data")
            }
        }
    }
}

impl FlightDataBlock {
    /// Create a new FlightDataBlock
    pub fn new(drefs: Vec<DataRef>, data: DataFrame) -> Result<Self, FlightDataError> {
        if (data.width() - 7) != drefs.len() {
            // - data should have at least 7 columns
            // - the first 7 colums are expected to be the required fields
            // - the remaining columns should have a corresponding dref
            let missing_drefs: Vec<String> = data.get_column_names().iter().skip(7).map(|c| c.to_string()).collect();
            return Err(FlightDataError::MissingDrefs(missing_drefs));
        }

        Ok(Self { drefs, data })
    }
}

/// The minimum schema required for the data block
fn required_schema() -> Schema {
    Schema::from_iter(vec![
        Field::new("timestamp".into(), DataType::Datetime(TimeUnit::Nanoseconds, None)),
        Field::new("longitude".into(), DataType::Float64),
        Field::new("latitude".into(), DataType::Float64),
        Field::new("altitude".into(), DataType::Float64),
        Field::new("heading".into(), DataType::Float64),
        Field::new("pitch".into(), DataType::Float64),
        Field::new("roll".into(), DataType::Float64),
    ])
}

/// Types that implement FlightDataSource can provide flight data for the FDRWriter
pub trait FlightDataSource {
    /// The tail number of the aircraft, used for the TAIL field in the FDR file
    fn tail_number(&self) -> Option<String>;

    /// The timestamp of the flight data, used for the TIME and DATE fields in the FDR file
    fn timestamp(&self) -> Option<chrono::DateTime<Utc>>;

    /// The data, and their DREF entries, to be written to the FDR file
    fn data_block(&self, _config: &FDRConfiguration) -> Result<FlightDataBlock, FlightDataError> {
        // default implementation returns an empty data block with minimum required data
        let schema = required_schema();

        Ok(FlightDataBlock {
            drefs: Vec::new(),
            data: DataFrame::empty_with_schema(&schema),
        })
    }
}

#[derive(Debug, Clone)]
pub struct FDRConfiguration {
    pub aircraft_model: String,
    pub defaut_tail_number: String,
    pub tail_number_override: Option<String>,
    pub strict: bool,
    pub auto_drefs: bool,
    pub allow_nulls: bool,
}

impl FDRConfiguration {
    pub fn tail_number(&self, source: &Box<dyn FlightDataSource>) -> String {
        match &self.tail_number_override {
            Some(tail_number) => tail_number.to_string(),
            None => source.tail_number().unwrap_or_else(|| self.defaut_tail_number.clone()),
        }
    }
}

/// Builder for FDRConfiguration, this is the preferred way to create a new FDRConfiguration.
///
/// ## Example
/// ```
/// let cfg = xfdr::fdr::FDRConfigurationBuilder::default()
///     .aircraft_model("Aircraft/Laminar Research/Cirrus SR22/Cirrus SR22.acf".to_string())
///     .build();
/// assert_eq!(cfg.aircraft_model, "Aircraft/Laminar Research/Cirrus SR22/Cirrus SR22.acf");
/// ```
#[derive(Debug, Clone)]
pub struct FDRConfigurationBuilder {
    aircraft_model: Option<String>,
    default_tail_number: String,
    tail_number_override: Option<String>,
    strict: bool,
    auto_drefs: bool,
    allow_nulls: bool,
}

impl Default for FDRConfigurationBuilder {
    fn default() -> Self {
        Self {
            aircraft_model: None,
            default_tail_number: "N12345".to_string(),
            tail_number_override: None,
            strict: false,
            auto_drefs: false,
            allow_nulls: false,
        }
    }
}

impl FDRConfigurationBuilder {
    /// The path to an aircraft file, relative to the X-Plane root, to be used as the aircraft model during replay
    pub fn aircraft_model(mut self, model: String) -> Self {
        self.aircraft_model = Some(model);
        self
    }

    /// If set, allow data records with null values to be written to the FDR file
    pub fn allow_nulls(mut self, allow_nulls: bool) -> Self {
        self.allow_nulls = allow_nulls;
        self
    }

    /// If set, automatically map fields in the data source to X-Plane datarefs
    pub fn auto_drefs(mut self, auto_drefs: bool) -> Self {
        self.auto_drefs = auto_drefs;
        self
    }

    /// Set the default tail number to use if no tail number is found in the data source
    pub fn default_tail_number(mut self, default_tail_number: String) -> Self {
        self.default_tail_number = default_tail_number;
        self
    }

    /// Optionally override the aircraft tail number, if any, that was discovered in the avionics log
    pub fn tail_number_override(mut self, tail_number: Option<String>) -> Self {
        self.tail_number_override = tail_number;
        self
    }

    /// If set, do not ignore unknown data fields in the avionics log
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Consume the builder and return a new FDRConfiguration
    pub fn build(self) -> FDRConfiguration {
        FDRConfiguration {
            aircraft_model: self.aircraft_model.unwrap_or_default(),
            defaut_tail_number: self.default_tail_number,
            tail_number_override: self.tail_number_override,
            strict: self.strict,
            auto_drefs: self.auto_drefs,
            allow_nulls: self.allow_nulls,
        }
    }
}

pub struct FDRWriter {
    config: FDRConfiguration,
}

impl FDRWriter {
    pub fn new(config: FDRConfiguration) -> Self {
        Self { config }
    }
}

#[derive(Debug)]
pub enum FDRWriteError {
    IO(std::io::Error),
    Polars(polars::error::PolarsError),
    FlightDataError(FlightDataError),
}

impl From<std::io::Error> for FDRWriteError {
    fn from(err: std::io::Error) -> Self {
        FDRWriteError::IO(err)
    }
}

impl From<polars::error::PolarsError> for FDRWriteError {
    fn from(err: polars::error::PolarsError) -> Self {
        match err {
            PolarsError::IO { error, msg } => FDRWriteError::IO(std::io::Error::new(
                error.kind(),
                msg.map_or_else(|| error.to_string(), |m| m.to_string()),
            )),
            _ => FDRWriteError::Polars(err),
        }
    }
}

impl fmt::Display for FDRWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FDRWriteError::IO(err) => write!(f, "IO error: {}", err),
            FDRWriteError::Polars(err) => write!(f, "Polars error: {}", err),
            FDRWriteError::FlightDataError(err) => write!(f, "Flight data error: {}", err),
        }
    }
}

impl From<FlightDataError> for FDRWriteError {
    fn from(err: FlightDataError) -> Self {
        FDRWriteError::FlightDataError(err)
    }
}

impl FDRWriter {
    pub fn write<W: Write>(&self, source: Box<dyn FlightDataSource>, writer: &mut W) -> Result<(), FDRWriteError> {
        //let mut writer = BufWriter::new(std::fs::File::create(path)?);
        writeln!(writer, "A")?;
        writeln!(writer, "4")?;

        // write the fields
        writeln!(writer, "ACFT,{}", self.config.aircraft_model)?;
        writeln!(writer, "TAIL,{}", self.config.tail_number(&source))?;

        // write the drefs
        let data_block = source.data_block(&self.config)?;

        for dref in data_block.drefs.iter() {
            writeln!(writer, "DREF,{},{}", dref.path, dref.scale)?;
        }

        // prepare csv data for writing
        let mut df = data_block.data;
        if let Ok(ts) = df.column("timestamp")?.datetime()?.strftime("%H:%M:%S") {
            df.with_column(ts)?;
        }
        
        if !self.config.allow_nulls {
            df = df.drop_nulls::<String>(None)?;
        }

        // write the csv data
        CsvWriter::new(writer).include_header(false).finish(&mut df)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::BufWriter, path::PathBuf};

    use crate::{detection::read_avionics_log, AviationLogSourceOption};

    use super::*;

    const SAMPLE_CSV_FILE: &str = "log_231104_084813_KPOU.csv";
    fn sample_csv() -> String {
        crate::resource_path(SAMPLE_CSV_FILE).to_str().unwrap().to_string()
    }

    #[test]
    fn test_fdr_configuration_builder() {
        let cfg = FDRConfigurationBuilder::default()
            .aircraft_model("Aircraft/Laminar Research/Cirrus SR22/Cirrus SR22.acf".to_string())
            .tail_number_override(Some("N12345".to_string()))
            .default_tail_number("N54321".to_string())
            .build();
        assert_eq!(
            cfg.aircraft_model,
            "Aircraft/Laminar Research/Cirrus SR22/Cirrus SR22.acf"
        );
        assert_eq!(cfg.tail_number_override, Some("N12345".to_string()));
        assert_eq!(cfg.defaut_tail_number, "N54321".to_string());
    }

    #[test]
    fn test_fdr_writer() -> Result<(), Box<dyn std::error::Error>> {
        let cfg = FDRConfigurationBuilder::default().build();
        let writer = FDRWriter::new(cfg);
        let path = PathBuf::from(sample_csv());
        let data = read_avionics_log(&AviationLogSourceOption::Garmin, &path)?;
        let mut buffer = BufWriter::new(Vec::new());
        writer.write(data, &mut buffer).unwrap();
        let contents = buffer.into_inner().unwrap();
        assert_eq!(contents.is_empty(), false); // 22 is temporary, actual value will vary
        Ok(())
    }
}
