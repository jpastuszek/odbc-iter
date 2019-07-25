/*!
Fetching data from ODBC Cursor and conversion to Rust native types.
!*/

use error_context::prelude::*;
use odbc::ffi::SqlDataType;
use odbc::{ColumnDescriptor, DiagnosticRecord, OdbcType};
use odbc::{SqlDate, SqlSsTime2, SqlTime, SqlTimestamp};
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::string::FromUtf16Error;
use std::convert::TryInto;

/// This error can be returned if database provided column type does not match type requested by
/// client
#[derive(Debug)]
pub struct SqlDataTypeMismatch {
    requested: &'static str,
    queried: SqlDataType,
}

impl fmt::Display for SqlDataTypeMismatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "requested SQL column data type '{:?}' does not match queried data type '{:?}'",
            self.requested, self.queried
        )
    }
}

impl Error for SqlDataTypeMismatch {}

/// This error can be returned if database provided column of type that currently cannot be mapped to `Value` type.
#[derive(Debug)]
pub struct UnsupportedSqlDataType(SqlDataType);

impl fmt::Display for UnsupportedSqlDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "unsupported SQL data type: {:?}", self.0)
    }
}

impl Error for UnsupportedSqlDataType {}

/// Errors related to datum access of ODBC cursor.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum DatumAccessError {
    OdbcCursorError(DiagnosticRecord),
    SqlDataTypeMismatch(SqlDataTypeMismatch),
    FromUtf16Error(FromUtf16Error, &'static str),
    #[cfg(feature = "serde_json")]
    JsonError(serde_json::Error),
}

impl fmt::Display for DatumAccessError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DatumAccessError::OdbcCursorError(_) => {
                write!(f, "failed to access data in ODBC cursor")
            }
            DatumAccessError::SqlDataTypeMismatch(_) => {
                write!(f, "failed to handle data type conversion")
            }
            DatumAccessError::FromUtf16Error(_, context) => write!(
                f,
                "failed to create String from UTF-16 column data while {}",
                context
            ),
            #[cfg(feature = "serde_json")]
            DatumAccessError::JsonError(_) => write!(f, "failed to convert data to JSON Value"),
        }
    }
}

impl Error for DatumAccessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DatumAccessError::OdbcCursorError(err) => Some(err),
            DatumAccessError::SqlDataTypeMismatch(err) => Some(err),
            DatumAccessError::FromUtf16Error(err, _) => Some(err),
            #[cfg(feature = "serde_json")]
            DatumAccessError::JsonError(err) => Some(err),
        }
    }
}

impl From<ErrorContext<FromUtf16Error, &'static str>> for DatumAccessError {
    fn from(err: ErrorContext<FromUtf16Error, &'static str>) -> DatumAccessError {
        DatumAccessError::FromUtf16Error(err.error, err.context)
    }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Error> for DatumAccessError {
    fn from(err: serde_json::Error) -> DatumAccessError {
        DatumAccessError::JsonError(err)
    }
}

/// Description of column type, name and nullability properties used to represent row schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnType {
    /// Supported type of datum that this column holds. See `DatumType` documentation of usage of corresponding `Column::into_*()` functions.
    pub datum_type: DatumType,
    /// ODBC SQL Data Type as returned by the driver.
    pub odbc_type: SqlDataType,
    /// `true` if column can contain `NULL` value. If `false` the `Column::into_*()` functions should always return `Some` value.
    pub nullable: bool,
    /// Name of the column as provided by the ODBC driver.
    pub name: String,
}

/// Types of values that column can be converted to.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DatumType {
    /// Use `Column::into_bool()` to get column value.
    Bit,
    /// Use `Column::into_i8()` to get column value.
    Tinyint,
    /// Use `Column::into_i16()` to get column value.
    Smallint,
    /// Use `Column::into_i32()` to get column value.
    Integer,
    /// Use `Column::into_i64()` to get column value.
    Bigint,
    /// Use `Column::into_f32()` to get column value.
    Float,
    /// Use `Column::into_f64()` to get column value.
    Double,
    /// Use `Column::into_string()` to get column value.
    String,
    /// Use `Column::into_timestamp()` to get column value.
    Timestamp,
    /// Use `Column::into_date()` to get column value.
    Date,
    /// Use `Column::into_time()` to get column value.
    Time,
    #[cfg(feature = "serde_json")]
    /// Use `Column::into_json()` to parse as `serde_json::Value` or `Column::into_string()` to get it as `String`.
    Json,
}

impl DatumType {
    /// Static string describing type of column datum.
    pub fn description(self) -> &'static str {
        match self {
            DatumType::Bit => "BIT",
            DatumType::Tinyint => "TINYINT",
            DatumType::Smallint => "SMALLINT",
            DatumType::Integer => "INTEGER",
            DatumType::Bigint => "BIGINT",
            DatumType::Float => "FLOAT",
            DatumType::Double => "DOUBLE",
            DatumType::String => "STRING",
            DatumType::Timestamp => "TIMESTAMP",
            DatumType::Date => "DATE",
            DatumType::Time => "TIME",
            #[cfg(feature = "serde_json")]
            DatumType::Json => "JSON",
        }
    }
}

impl TryFrom<ColumnDescriptor> for ColumnType {
    type Error = UnsupportedSqlDataType;

    fn try_from(column_descriptor: ColumnDescriptor) -> Result<ColumnType, UnsupportedSqlDataType> {
        use SqlDataType::*;
        let datum_type = match column_descriptor.data_type {
            SQL_EXT_BIT => DatumType::Bit,
            SQL_EXT_TINYINT => DatumType::Tinyint,
            SQL_SMALLINT => DatumType::Smallint,
            SQL_INTEGER => DatumType::Integer,
            SQL_EXT_BIGINT => DatumType::Bigint,
            SQL_FLOAT | SQL_REAL => DatumType::Float,
            SQL_DOUBLE => DatumType::Double,
            SQL_CHAR | SQL_VARCHAR | SQL_EXT_LONGVARCHAR | SQL_EXT_WCHAR | SQL_EXT_WVARCHAR
            | SQL_EXT_WLONGVARCHAR => DatumType::String,
            SQL_TIMESTAMP => DatumType::Timestamp,
            SQL_DATE => DatumType::Date,
            SQL_TIME | SQL_SS_TIME2 => DatumType::Time,
            SQL_UNKNOWN_TYPE => {
                #[cfg(feature = "serde_json")]
                {
                    DatumType::Json
                }
                #[cfg(not(feature = "serde_json"))]
                {
                    DatumType::String
                }
            }
            _ => return Err(UnsupportedSqlDataType(column_descriptor.data_type)),
        };

        Ok(ColumnType {
            datum_type,
            odbc_type: column_descriptor.data_type,
            nullable: column_descriptor.nullable.unwrap_or(true),
            name: column_descriptor.name,
        })
    }
}

/// Represents SQL table column which can be converted to Rust native type.
pub struct Column<'r, 's, 'c, S> {
    column_type: &'r ColumnType,
    cursor: &'r mut odbc::Cursor<'s, 'c, 'c, S>,
    index: u16,
    utf_16_strings: bool,
}

impl<'r, 's, 'c, S> Column<'r, 's, 'c, S> {
    fn into<T: OdbcType<'r>>(self) -> Result<Option<T>, DatumAccessError> {
        self.cursor
            .get_data::<T>(self.index + 1)
            .map_err(DatumAccessError::OdbcCursorError)
    }

    pub fn column_type(&self) -> &ColumnType {
        self.column_type
    }

    // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-2017

    pub fn into_bool(self) -> Result<Option<bool>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_EXT_BIT => self.into::<u8>()?.map(|byte| byte != 0),
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "BIT",
                    queried,
                }))
            }
        })
    }

    pub fn into_i8(self) -> Result<Option<i8>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_EXT_TINYINT => self.into::<i8>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "TINYINT",
                    queried,
                }))
            }
        })
    }

    pub fn into_i16(self) -> Result<Option<i16>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_SMALLINT => self.into::<i16>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "SMALLINT",
                    queried,
                }))
            }
        })
    }

    pub fn into_i32(self) -> Result<Option<i32>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_INTEGER => self.into::<i32>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "INTEGER",
                    queried,
                }))
            }
        })
    }

    pub fn into_i64(self) -> Result<Option<i64>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_EXT_BIGINT => self.into::<i64>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "BIGINT",
                    queried,
                }))
            }
        })
    }

    pub fn into_f32(self) -> Result<Option<f32>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_REAL | SqlDataType::SQL_FLOAT => self.into::<f32>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "FLOAT",
                    queried,
                }))
            }
        })
    }

    pub fn into_f64(self) -> Result<Option<f64>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_DOUBLE => self.into::<f64>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "DOUBLE",
                    queried,
                }))
            }
        })
    }

    pub fn into_string(self) -> Result<Option<String>, DatumAccessError> {
        use SqlDataType::*;
        Ok(match self.column_type.odbc_type {
            SQL_CHAR | SQL_VARCHAR | SQL_EXT_LONGVARCHAR => self.into::<String>()?,
            SQL_EXT_WCHAR | SQL_EXT_WVARCHAR | SQL_EXT_WLONGVARCHAR |
            SQL_UNKNOWN_TYPE => {
                if self.utf_16_strings {
                    //TODO: map + transpose
                    if let Some(bytes) = self.into::<&[u16]>()? {
                        Some(String::from_utf16(bytes)
                            .wrap_error_while("getting UTF-16 string (SQL_EXT_WCHAR | SQL_EXT_WVARCHAR | SQL_EXT_WLONGVARCHAR)")?)
                    } else {
                        None
                    }
                } else {
                    self.into::<String>()?
                }
            }
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "STRING",
                    queried,
                }))
            }
        })
    }

    pub fn into_timestamp(self) -> Result<Option<SqlTimestamp>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_TIMESTAMP => self.into::<SqlTimestamp>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "TIMESTAMP",
                    queried,
                }))
            }
        })
    }

    pub fn into_date(self) -> Result<Option<SqlDate>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_DATE => self.into::<SqlDate>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "DATE",
                    queried,
                }))
            }
        })
    }

    pub fn into_time(self) -> Result<Option<SqlSsTime2>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_TIME => self.into::<SqlTime>()?.map(|ss| SqlSsTime2 {
                hour: ss.hour,
                minute: ss.minute,
                second: ss.second,
                fraction: 0,
            }),
            SqlDataType::SQL_SS_TIME2 => self.into::<SqlSsTime2>()?,
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "TIME",
                    queried,
                }))
            }
        })
    }

    #[cfg(feature = "serde_json")]
    pub fn into_json(self) -> Result<Option<serde_json::Value>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            queried @ SqlDataType::SQL_UNKNOWN_TYPE => {
                self.into::<String>()?
                    .map(|data| {
                        // MonetDB can only store arrays or objects as top level JSON values so check if data looks like JSON in case we are not talking to MonetDB
                        if (data.starts_with("[") && data.ends_with("]"))
                            || (data.starts_with("{") && data.ends_with("}"))
                        {
                            serde_json::from_str(&data).map_err(Into::into)
                        } else {
                            //TOOD: better error?
                            return Err(DatumAccessError::SqlDataTypeMismatch(
                                SqlDataTypeMismatch {
                                    requested: "JSON",
                                    queried,
                                },
                            ));
                        }
                    })
                    .transpose()?
            }
            queried => {
                return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                    requested: "JSON",
                    queried,
                }))
            }
        })
    }
}

/// Represents SQL table row of Column objects.
pub struct Row<'r, 's, 'c, S> {
    schema: &'r [ColumnType],
    cursor: odbc::Cursor<'s, 'c, 'c, S>,
    index: u16,
    columns: u16,
    utf_16_strings: bool,
}

impl<'r, 's, 'c, S> Row<'r, 's, 'c, S> {
    pub fn new(
        cursor: odbc::Cursor<'s, 'c, 'c, S>,
        schema: &'r [ColumnType],
        utf_16_strings: bool,
    ) -> Row<'r, 's, 'c, S> {
        Row {
            schema,
            cursor,
            index: 0,
            columns: schema.len() as u16,
            utf_16_strings,
        }
    }

    pub fn shift_column<'i>(&'i mut self) -> Option<Column<'i, 's, 'c, S>> {
        self.schema
            .get(self.index as usize)
            .map(move |column_type| {
                let column = Column {
                    column_type,
                    cursor: &mut self.cursor,
                    index: self.index,
                    utf_16_strings: self.utf_16_strings,
                };

                self.index += 1;
                column
            })
    }

    pub fn columns(&self) -> u16 {
        self.columns
    }
}


/// Column values can be converted to types implementing this trait.
///
/// This trait is implemented for primitive Rust types, `String` and `chrono` date and time types.
pub trait TryFromColumn: Sized {
    type Error: Error + 'static;
    /// Create `Self` from row column.
    fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S>) -> Result<Self, Self::Error>;
}

/// This traits allow for conversion of `Row` type representing ODBC cursor used internally by `ResultSet` iterator to any other type returned as `Item` that implements it.
///
/// This trait is implemented for Rust tuple type enabling conversion of rows to tuples of types implementing `TryFromValue`.
/// Also this trait implementation allows to convert single column rows to types implementing `TryFromColumn`.
///
/// This trait can be implemented for custom objects. This will enable them to be queried directly from database as `Item` of `ResultSet` iterator.
pub trait TryFromRow: Sized {
    type Error: Error + 'static;
    /// Given `ColumnType` convert from `Row` to other type of value representing table row.
    fn try_from_row<'r, 's, 'c, S>(row: Row<'r, 's, 'c, S>) -> Result<Self, Self::Error>;
}

/// Error type that represents different problems when converting column values to specific types.
#[derive(Debug)]
pub enum ColumnConvertError {
    UnexpectedNullValue(&'static str),
    DatumAccessError(DatumAccessError),
    ValueOutOfRange {
        expected: &'static str,
    },
}

impl From<DatumAccessError> for ColumnConvertError {
    fn from(err: DatumAccessError) -> ColumnConvertError {
        ColumnConvertError::DatumAccessError(err)
    }
}

impl fmt::Display for ColumnConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ColumnConvertError::UnexpectedNullValue(t) => {
                write!(f, "expecting value of type {} but got NULL", t)
            }
            ColumnConvertError::DatumAccessError(_) => {
                write!(f, "problem accessing datum")
            }
            ColumnConvertError::ValueOutOfRange { expected } => {
                write!(f, "value is out of range for type {}", expected)
            }
        }
    }
}

impl Error for ColumnConvertError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ColumnConvertError::DatumAccessError(err) => Some(err),
            ColumnConvertError::UnexpectedNullValue(_) |
            ColumnConvertError::ValueOutOfRange { .. } => None,
        }
    }
}

macro_rules! try_from_row_not_null {
    ($t:ty) => {
        impl TryFromColumn for $t {
            type Error = ColumnConvertError;
            fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S>) -> Result<Self, Self::Error> {
                let value: Option<$t> = TryFromColumn::try_from_column(column)?;
                value.ok_or_else(|| ColumnConvertError::UnexpectedNullValue(stringify!($t)))
            }
        }
    }
}

macro_rules! try_from_row {
    ($t:ty, $f:ident) => {
        impl TryFromColumn for Option<$t> {
            type Error = ColumnConvertError;
            fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S>) -> Result<Self, Self::Error> {
                column.$f().map_err(Into::into)
            }
        }

        try_from_row_not_null!($t);
    };
}

macro_rules! try_from_row_unsigned {
    ($it:ty, $t:ty) => {
        impl TryFromColumn for Option<$t> {
            type Error = ColumnConvertError;
            fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S>) -> Result<Self, Self::Error> {
                let value: Option<$it> = TryFromColumn::try_from_column(column)?;
                value.map(|value|
                    value
                    .try_into()
                    .map_err(|_| ColumnConvertError::ValueOutOfRange {
                        expected: stringify!($t),
                    })
                ).transpose()
            }
        }

        try_from_row_not_null!($t);
    };
}

try_from_row![bool, into_bool];
try_from_row![i8, into_i8];
try_from_row_unsigned![i8, u8];
try_from_row![i16, into_i16];
try_from_row_unsigned![i16, u16];
try_from_row![i32, into_i32];
try_from_row_unsigned![i32, u32];
try_from_row![i64, into_i64];
try_from_row_unsigned![i64, u64];
try_from_row![f32, into_f32];
try_from_row![f64, into_f64];
try_from_row![String, into_string];
try_from_row![SqlTimestamp, into_timestamp];
try_from_row![SqlDate, into_date];
try_from_row![SqlSsTime2, into_time];
#[cfg(feature = "serde_json")]
try_from_row![serde_json::Value, into_json];

#[cfg(feature = "chrono")]
use chrono::{NaiveDateTime, NaiveDate, NaiveTime};

#[cfg(feature = "chrono")]
impl TryFromColumn for Option<NaiveDateTime> {
    type Error = ColumnConvertError;
    fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S>) -> Result<Self, Self::Error> {
        let value: Option<SqlTimestamp> = TryFromColumn::try_from_column(column)?;

        Ok(value.map(|value| {
            NaiveDate::from_ymd(
                i32::from(value.year),
                u32::from(value.month),
                u32::from(value.day),
            )
            .and_hms_nano(
                u32::from(value.hour),
                u32::from(value.minute),
                u32::from(value.second),
                value.fraction,
            )
        }))
    }
}

#[cfg(feature = "chrono")]
try_from_row_not_null!(NaiveDateTime);

#[cfg(feature = "chrono")]
impl TryFromColumn for Option<NaiveDate> {
    type Error = ColumnConvertError;
    fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S>) -> Result<Self, Self::Error> {
        let value: Option<SqlDate> = TryFromColumn::try_from_column(column)?;

        Ok(value.map(|value| {
            NaiveDate::from_ymd(
                i32::from(value.year),
                u32::from(value.month),
                u32::from(value.day),
            )
        }))
    }
}

#[cfg(feature = "chrono")]
try_from_row_not_null!(NaiveDate);

#[cfg(feature = "chrono")]
impl TryFromColumn for Option<NaiveTime> {
    type Error = ColumnConvertError;
    fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S>) -> Result<Self, Self::Error> {
        let value: Option<SqlSsTime2> = TryFromColumn::try_from_column(column)?;

        Ok(value.map(|value| {
            NaiveTime::from_hms_nano(
                u32::from(value.hour),
                u32::from(value.minute),
                u32::from(value.second),
                value.fraction,
            )
        }))
    }
}

#[cfg(feature = "chrono")]
try_from_row_not_null!(NaiveTime);

/// Errors that may happen during conversion of `ValueRow` to given type.
#[derive(Debug)]
pub enum RowConvertError {
    UnexpectedNullValue(&'static str),
    UnexpectedValue,
    UnexpectedNumberOfColumns { expected: u16, got: u16 },
    ColumnConvertError(Box<dyn Error>),
}

impl From<ColumnConvertError> for RowConvertError {
    fn from(err: ColumnConvertError) -> RowConvertError {
        RowConvertError::ColumnConvertError(Box::new(err))
    }
}

impl fmt::Display for RowConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RowConvertError::UnexpectedNullValue(t) => {
                write!(f, "expecting value of type {} but got NULL", t)
            }
            RowConvertError::UnexpectedValue => write!(f, "expecting no data (unit) but got a row"),
            RowConvertError::UnexpectedNumberOfColumns { expected, got } => write!(
                f,
                "unexpected number of columns: expected {} but got {}",
                expected, got
            ),
            RowConvertError::ColumnConvertError(_) => {
                write!(f, "failed to convert column value to target type")
            }
        }
    }
}

impl Error for RowConvertError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RowConvertError::UnexpectedNullValue(_)
            | RowConvertError::UnexpectedValue
            | RowConvertError::UnexpectedNumberOfColumns { .. } => None,
            RowConvertError::ColumnConvertError(err) => Some(err.as_ref()),
        }
    }
}

/// Unit can be used to signal that no rows of data should be produced.
impl TryFromRow for () {
    type Error = RowConvertError;
    fn try_from_row<'r, 's, 'c, S>(_row: Row<'r, 's, 'c, S>) -> Result<Self, Self::Error> {
        Err(RowConvertError::UnexpectedValue)
    }
}

/// Convert row with single column to any type implementing `TryFromColumn`.
impl<T> TryFromRow for T
where
    T: TryFromColumn,
{
    type Error = RowConvertError;
    fn try_from_row<'r, 's, 'c, S>(mut row: Row<'r, 's, 'c, S>) -> Result<Self, Self::Error> {
        if row.columns() != 1 {
            return Err(RowConvertError::UnexpectedNumberOfColumns {
                expected: 1,
                got: row.columns(),
            });
        }

        let column = row.shift_column().unwrap();

        TryFromColumn::try_from_column(column)
            .map_err(|e| RowConvertError::ColumnConvertError(Box::new(e)))

    }
}

/// Errors that my arise when converting rows to tuples.
#[derive(Debug)]
pub enum RowConvertTupleError {
    UnexpectedNumberOfColumns { expected: u16, tuple: &'static str },
    ValueConvertError(Box<dyn Error>),
}

impl fmt::Display for RowConvertTupleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RowConvertTupleError::UnexpectedNumberOfColumns { expected, tuple } => write!(
                f,
                "failed to convert row with {} columns to tuple {}",
                expected, tuple
            ),
            RowConvertTupleError::ValueConvertError(_) => {
                write!(f, "failed to convert column value to target type")
            }
        }
    }
}

impl Error for RowConvertTupleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RowConvertTupleError::UnexpectedNumberOfColumns { .. } => None,
            RowConvertTupleError::ValueConvertError(err) => Some(err.as_ref()),
        }
    }
}

macro_rules! count {
    () => (0u16);
    ( $x:tt $($xs:tt)* ) => (1u16 + count!($($xs)*));
}

macro_rules! try_from_tuple {
    ($(
        $Tuple:ident {
            $(($idx:tt) -> $T:ident)+
        }
    )+) => {
        $(
            impl<$($T: TryFromColumn),+> TryFromRow for ($($T,)+) {
                type Error = RowConvertTupleError;
                fn try_from_row<'r, 's, 'c, S>(mut row: Row<'r, 's, 'c, S>) -> Result<($($T,)+), Self::Error> {
                    if row.columns() != count!($($T)+) {
                        return Err(RowConvertTupleError::UnexpectedNumberOfColumns { expected: row.columns(), tuple: stringify![($($T,)+)] })
                    }
                    Ok(($({ let x: $T = $T::try_from_column(row.shift_column().unwrap()).map_err(|err| RowConvertTupleError::ValueConvertError(Box::new(err)))?; x},)+))
                }
            }
        )+
    }
}

try_from_tuple! {
    Tuple1 {
        (0) -> A
    }
    Tuple2 {
        (0) -> A
        (1) -> B
    }
    Tuple3 {
        (0) -> A
        (1) -> B
        (2) -> C
    }
    Tuple4 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
    }
    Tuple5 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
    }
    Tuple6 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
    }
    Tuple7 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
    }
    Tuple8 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
    }
    Tuple9 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
    }
    Tuple10 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
    }
    Tuple11 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
    }
    Tuple12 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
    }
}
