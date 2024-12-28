use crate::fdr::{DataRef, FDRConfiguration, FlightDataBlock, FlightDataError, FlightDataSource};
use chrono::Utc;
use polars::prelude::*;
use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    io::{BufRead, Read},
    path::Path,
};

pub struct GarminLogFile {
    header: GarminEISLogHeader,
    data: DataFrame,
}

#[derive(Debug)]
pub enum GarminLogFileParseError {
    IO(std::io::Error),
    Polars(polars::error::PolarsError),
}

impl Error for GarminLogFileParseError {}

impl Display for GarminLogFileParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GarminLogFileParseError::IO(e) => write!(f, "IO error: {}", e),
            GarminLogFileParseError::Polars(e) => write!(f, "Polars error: {}", e),
        }
    }
}

impl From<std::io::Error> for GarminLogFileParseError {
    fn from(e: std::io::Error) -> Self {
        GarminLogFileParseError::IO(e)
    }
}

impl From<polars::error::PolarsError> for GarminLogFileParseError {
    fn from(e: polars::error::PolarsError) -> Self {
        GarminLogFileParseError::Polars(e)
    }
}

impl GarminLogFile {
    pub fn new(path: &Path) -> Result<Self, GarminLogFileParseError> {
        let log = GarminEISLog::from_csv(&path)?;
        Ok(Self {
            header: log.header,
            data: log.data,
        })
    }
}

impl FlightDataSource for GarminLogFile {
    fn tail_number(&self) -> Option<String> {
        self.header
            .metadata
            .get("tail_number")
            .map_or(None, |s| Some(s.clone()))
    }

    fn timestamp(&self) -> Option<chrono::DateTime<Utc>> {
        match self.data.column("timestamp") {
            Ok(Column::Series(s)) => Some(
                s.datetime()
                    .unwrap()
                    .as_datetime_iter()
                    .next()
                    .unwrap()
                    .unwrap()
                    .and_utc(),
            ),
            _ => None,
        }
    }

    fn data_block(&self, config: &FDRConfiguration) -> Result<FlightDataBlock, FlightDataError> {
        const MANDATORY_COLS: usize = 7;

        return if !config.auto_drefs {
            // select the MANDATORY_COLUMNS
            match self.data.select(
                self.data
                    .get_column_names()
                    .iter()
                    .take(MANDATORY_COLS)
                    .map(|s| s.as_str())
                    .collect::<Vec<&str>>(),
            ) {
                Ok(data) => Ok(FlightDataBlock::new(vec![], data)?),
                Err(_) => Err(FlightDataError::InsufficientData),
            }
        } else {
            let dref_map = build_dref_map();
            // get the datarefs for the columns we care about, None for entries that dont map
            let drefs: Vec<Option<DataRef>> = self
                .data
                .get_column_names()
                .iter()
                .skip(MANDATORY_COLS)
                .map(|name| dref_map.get(name.as_str()).map_or(None, |dref| Some(dref.clone())))
                .collect();

            // indices of missing drefs
            let mising_idx: Vec<usize> = drefs
                .iter()
                .enumerate()
                .filter(|(_, dref)| dref.is_none())
                .map(|(idx, _)| idx + MANDATORY_COLS)
                .collect();

            if config.strict && !mising_idx.is_empty() {
                return Err(FlightDataError::MissingDrefs(
                    mising_idx
                        .iter()
                        .map(|i| self.data.get_column_names()[*i].to_string())
                        .collect(),
                ));
            }

            // remove missing drefs and missing columns
            let missing_names: Vec<&str> = mising_idx
                .iter()
                .map(|i| self.data.get_column_names()[*i].as_str())
                .collect();

            let data = self.data.clone().drop_many(missing_names);
            let drefs = drefs.into_iter().filter_map(|x| x).collect();
            Ok(FlightDataBlock::new(drefs, data)?)
        };
    }
}

#[derive(Debug)]
pub struct GarminEISColumn {
    name: String,
    unit: String,
}

/// Clean up a raw column name by trimming whitespace
fn clean_column_name(name: &str) -> &str {
    name.trim()
}

// Clean the column names of a dataframe using clean_column_name
pub fn strip_column_names(mut df: DataFrame) -> Result<DataFrame, PolarsError> {
    df.set_column_names(
        &df.get_columns()
            .iter()
            .map(|s| clean_column_name(s.name()).to_string())
            .collect::<Vec<String>>(),
    )?;
    Ok(df)
}

/// drop rows where all values in that row are null
fn remove_empty_rows(mut df: DataFrame) -> Result<DataFrame, PolarsError> {
    let mask = df
        .get_columns()
        .iter()
        .fold(None, |acc, s| match acc {
            None => Some(s.is_not_null()),
            Some(mask) => Some(mask | s.is_not_null()),
        })
        .unwrap();
    df = df.filter(&mask)?;
    Ok(df)
}

fn clean_strings(mut df: DataFrame) -> PolarsResult<DataFrame> {
    // get names of string columns
    let string_columns = df
        .get_columns()
        .iter()
        .filter(|s| s.dtype() == &DataType::String)
        .map(|s| s.name().to_string())
        .collect::<Vec<String>>();

    // trim whitespace from string columns
    for col in string_columns {
        df.replace(
            &col,
            Series::new(
                col.clone().into(), // this was once fine using &col, but update of polars broke it
                df.column(&col)?
                    .str()?
                    .into_iter()
                    .map(|opt_s| opt_s.map(|s| s.trim().to_string()))
                    .collect::<Vec<Option<String>>>(),
            ),
        )?;
    }

    Ok(df)
}

pub fn clean_dataframe(mut df: DataFrame) -> Result<DataFrame, PolarsError> {
    df = strip_column_names(df)?;
    df = clean_strings(df)?;
    df = remove_empty_rows(df)?;

    // move the "timestamp" column to the front
    const REQUIRED_COLS: [&str; 7] = ["timestamp", "Longitude", "Latitude", "AltB", "HDG", "Pitch", "Roll"];
    df = df
        .lazy()
        .select(vec![cols(REQUIRED_COLS), col("*").exclude(REQUIRED_COLS)])
        .collect()?;

    Ok(df)
}

impl GarminEISColumn {
    pub fn name(&self) -> &str {
        clean_column_name(&self.name)
    }

    pub fn raw_name(&self) -> &str {
        &self.name
    }

    pub fn unit(&self) -> &str {
        &self.unit
    }
}

struct GarminEISLogHeader {
    pub metadata: HashMap<String, String>,
    pub columns: Vec<GarminEISColumn>,
}

struct GarminEISLog {
    pub header: GarminEISLogHeader,
    pub data: DataFrame,
}

impl GarminEISLogHeader {
    pub fn from_csv(path: &std::path::Path) -> Result<Self, std::io::Error> {
        let file = std::fs::File::open(path)?;
        let mut metadata = HashMap::new();
        let mut columns = Vec::new();

        // all header data can be found in the first 3 rows of the file.
        // row 1 starts with a comment char and has metadata entries in the form of key="value" separated by commas
        // row 2 starts with a comment char and has column units separated by commas
        // row 3 lists the column names separated by commas

        let mut lines = std::io::BufReader::new(file).lines();
        let metadata_line = lines.next().expect("No lines in file")?;

        let units_line = lines.next().expect("No units line in file")?;
        let units = units_line.trim_start_matches('#').split(",");

        let names_line = lines.next().expect("No names line in file")?;
        let names = names_line.split(',');

        for entry in metadata_line.trim_start_matches('#').split(',') {
            let mut parts = entry.split('=');
            if let Some(key) = parts.next() {
                if let Some(value) = parts.next() {
                    metadata.insert(key.trim().to_string(), value.trim_matches('"').to_string());
                }
            }
        }

        for (unit, name) in units.zip(names) {
            columns.push(GarminEISColumn {
                name: name.to_string(),
                unit: unit.trim().to_string(),
            });
        }

        Ok(Self { metadata, columns })
    }

    pub fn build_schema(&self) -> Schema {
        Schema::from_iter(
            self.columns
                .iter()
                .map(|c| {
                    let dtype = match c.unit() {
                        "yyy-mm-dd" => DataType::Date,
                        "bool" => DataType::Int64,
                        "enum" => DataType::String,
                        "MHz" => DataType::Float64,
                        "degrees" => DataType::Float64,
                        "ft" => DataType::Float64,
                        "nm" => DataType::Float64,
                        "fsd" => DataType::Float64, // indication of full scale deflection?
                        "mt" => DataType::Float64,  // WAAS performance numbers
                        "ft wgs" => DataType::Float64,
                        "ft Baro" => DataType::Float64,
                        "ft msl" => DataType::Float64,
                        "kt" => DataType::Float64,
                        "fpm" => DataType::Float64,
                        "deg" => DataType::Float64,
                        "ft/min" => DataType::Float64,
                        "deg F/min" => DataType::Float64,
                        "kts" => DataType::Float64,
                        "lbs" => DataType::Float64,
                        "gals" => DataType::Float64,
                        "volts" => DataType::Float64,
                        "amps" => DataType::Float64,
                        "gph" => DataType::Float64,
                        "psi" => DataType::Float64,
                        "degF" => DataType::Float64,
                        "deg F" => DataType::Float64,
                        "deg C" => DataType::Float64,
                        "%" => DataType::Float64,
                        "rpm" => DataType::Float64,
                        "inch" => DataType::Float64,
                        "Hg" => DataType::Float64,
                        "G" => DataType::Float64,
                        "#" => DataType::Int64,
                        "s" => DataType::Float64,
                        "crc16" => DataType::String,
                        _ => DataType::String,
                    };
                    Field::new(c.name().into(), dtype)
                })
                .collect::<Vec<_>>(),
        )
    }
}

impl GarminEISLog {
    fn read_bytes(path: &std::path::Path) -> std::io::Result<std::io::Cursor<Vec<u8>>> {
        let mut file = std::fs::File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        // remove trailing null bytes from buffer
        while buffer.last() == Some(&0) {
            buffer.pop();
        }
        Ok(std::io::Cursor::new(buffer))
    }

    fn read_df(path: &std::path::Path, schema: &Schema) -> PolarsResult<LazyFrame> {
        // read the file bytes into a buffer
        const SKIP_ROWS: usize = 2;
        // read into dataframe
        let reader = CsvReadOptions::default()
            .with_has_header(true)
            .with_schema(Some(Arc::new(schema.clone())))
            .with_skip_rows(SKIP_ROWS)
            .into_reader_with_file_handle(Self::read_bytes(path)?);
        Ok(reader.finish()?.lazy())
    }

    pub fn from_csv(path: &std::path::Path) -> PolarsResult<Self> {
        let header = GarminEISLogHeader::from_csv(path)?;
        let schema = header.build_schema();
        let data = Self::read_df(path, &schema)?;
        let data = parse_datetime(data, "Lcl Date", "Lcl Time", "UTCOfst", "timestamp", true)?;
        let data = data.collect()?;
        let data = clean_dataframe(data)?;
        Ok(Self { header, data })
    }
}

fn parse_datetime(
    lazy: LazyFrame,
    date_col: &str,
    time_col: &str,
    offset_col: &str,
    new_timestamp_col: &str,
    drop_source_cols: bool,
) -> PolarsResult<LazyFrame> {
    let mut lazy = lazy.with_column(
        concat_str(
            vec![
                col(date_col).dt().strftime("%Y-%m-%d"),
                lit("T"),
                col(time_col),
                col(offset_col),
            ],
            "",
            false,
        )
        .str()
        .to_datetime(
            Some(TimeUnit::Microseconds),
            Some("UTC".into()),
            StrptimeOptions {
                format: Some("%Y-%m-%dT%H:%M:%S%z".into()),
                ..Default::default()
            },
            lit("raise"),
        )
        .alias(new_timestamp_col),
    );

    if drop_source_cols {
        lazy = lazy.drop(vec![date_col, time_col, offset_col]);
    }

    Ok(lazy)
}

fn build_dref_map() -> HashMap<&'static str, DataRef> {
    let mut map = HashMap::new();
    // map.insert("AtvWpt", DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()));
    map.insert(
        "BaroA",
        DataRef::new("sim/cockpit2/gauges/actuators/barometer_setting_in_hg_pilot".to_string()),
    );
    map.insert(
        "AltMSL",
        DataRef::new("sim/cockpit2/gauges/indicators/altitude_ft_pilot".to_string()),
    );
    map.insert(
        "OAT",
        DataRef::new("sim/cockpit2/temperature/outside_air_temp_degc".to_string()),
    );
    map.insert(
        "IAS",
        DataRef::new("sim/cockpit2/gauges/indicators/airspeed_kts_pilot".to_string()),
    );
    map.insert(
        "GndSpd",
        DataRef::new("sim/cockpit2/gauges/indicators/ground_speed_kt".to_string()),
    );
    map.insert(
        "TAS",
        DataRef::new("sim/cockpit2/gauges/indicators/true_airspeed_kts_pilot".to_string()),
    );
    map.insert(
        "VSpd",
        DataRef::new("sim/cockpit2/gauges/indicators/vvi_fpm_pilot".to_string()),
    );
    map.insert(
        "TRK",
        DataRef::new("sim/cockpit2/gauges/indicators/ground_track_true_pilot".to_string()),
    );
    map.insert(
        "bus1volts",
        DataRef::new("sim/cockpit2/electrical/bus_volts[0]".to_string()),
    );
    map.insert(
        "alt1amps",
        DataRef::new("sim/cockpit2/electrical/generator_amps".to_string()),
    );
    map.insert(
        "FQtyLlbs",
        DataRef::new("sim/flightmodel/weight/m_fuel[0]".to_string()).with_scale(0.45359237),
    ); // lbs -> kg
    map.insert(
        "FQtyRlbs",
        DataRef::new("sim/flightmodel/weight/m_fuel[1]".to_string()).with_scale(0.45359237),
    ); // lbs -> kg
    map.insert(
        "FQtyL",
        DataRef::new("sim/cockpit2/fuel/fuel_quantity[0]".to_string()).with_scale(2.73062384),
    ); // gal -> kg
    map.insert(
        "FQtyR",
        DataRef::new("sim/cockpit2/fuel/fuel_quantity[1]".to_string()).with_scale(2.73062384),
    ); // gal -> kg
       // insert the rest with a placeholder path
       // map.insert("LatAc", DataRef::new("???".to_string()));
       // map.insert("NormAc", DataRef::new("???".to_string()));
    map.insert(
        "E1 FFlow",
        DataRef::new("sim/cockpit2/engine/indicators/fuel_flow_kg_sec[0]".to_string()).with_scale(1.0),
    ); // gph -> kg/s
    map.insert(
        "E1 FPres",
        DataRef::new("sim/cockpit2/engine/indicators/fuel_pressure_psi[0]".to_string()),
    );
    map.insert(
        "E1 OilT",
        DataRef::new("sim/cockpit2/engine/indicators/oil_temperature_deg_C[0]".to_string()),
    );
    map.insert(
        "E1 OilP",
        DataRef::new("sim/cockpit2/engine/indicators/oil_pressure_psi[0]".to_string()),
    );
    // map.insert(
    //     "E1 MAP",
    //     DataRef::new("???".to_string()),
    // );
    map.insert(
        "E1 RPM",
        DataRef::new("sim/cockpit2/engine/indicators/engine_speed_rpm[0]".to_string()),
    );
    map.insert(
        "E1 %Pwr",
        DataRef::new("sim/cockpit2/engine/indicators/N1_percent".to_string()),
    );
    map.insert(
        "E1 CHT1",
        DataRef::new("sim/cockpit2/engine/indicators/CHT_CYL_deg_F[0]".to_string()),
    );
    map.insert(
        "E1 CHT2",
        DataRef::new("sim/cockpit2/engine/indicators/CHT_CYL_deg_F[1]".to_string()),
    );
    map.insert(
        "E1 CHT3",
        DataRef::new("sim/cockpit2/engine/indicators/CHT_CYL_deg_F[2]".to_string()),
    );
    map.insert(
        "E1 CHT4",
        DataRef::new("sim/cockpit2/engine/indicators/CHT_CYL_deg_F[3]".to_string()),
    );
    // map.insert(
    //     "E1 CHT CLD",
    //     DataRef::new("???".to_string()),
    // );
    map.insert(
        "E1 EGT1",
        DataRef::new("sim/cockpit2/engine/indicators/EGT_CYL_deg_F[0]".to_string()),
    );
    map.insert(
        "E1 EGT2",
        DataRef::new("sim/cockpit2/engine/indicators/EGT_CYL_deg_F[1]".to_string()),
    );
    map.insert(
        "E1 EGT3",
        DataRef::new("sim/cockpit2/engine/indicators/EGT_CYL_deg_F[2]".to_string()),
    );
    map.insert(
        "E1 EGT4",
        DataRef::new("sim/cockpit2/engine/indicators/EGT_CYL_deg_F[3]".to_string()),
    );
    // map.insert(
    //     "AltGPS",
    //     DataRef::new("???".to_string()),
    // );
    // map.insert(
    //     "HSIS",
    //     DataRef::new("???".to_string()),
    // );
    // map.insert(
    //     "CRS",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "NAV1",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "NAV2",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "HCDI",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "VCDI",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "WndSpd",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "WndDr",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "WptDst",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "WptBrg",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "MagVar",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "AfcsOn",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "RollM",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "PitchM",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "RollC",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "PitchC",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "VSpdG",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "GPSfix",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "HAL",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "VAL",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "HPLwas",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "HPLfd",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "VPLwas",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "AltPress",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "OnGrnd",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "LogIdx",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "SysTime",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "LonAc",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    // map.insert(
    //     "LogCheck",
    //     DataRef::new("sim/cockpit2/gauges/actuators/placeholder".to_string()),
    // );
    map
}
