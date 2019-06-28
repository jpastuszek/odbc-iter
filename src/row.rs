/*!
Fetching data from ODBC Cursor and conversion to Rust native types.
!*/

use error_context::prelude::*;
use odbc::ffi::SqlDataType;
use odbc::{SqlDate, SqlSsTime2, SqlTime, SqlTimestamp};
use odbc::{
    ColumnDescriptor, DiagnosticRecord, OdbcType
};
use std::fmt;
use std::error::Error;
use std::string::FromUtf16Error;
use std::convert::TryFrom;

/// This error can be returned if database provided column type does not match type requested by
/// client
#[derive(Debug)]
pub struct SqlDataTypeMismatch {
    requested: &'static str,
    queried: SqlDataType,
}

impl fmt::Display for SqlDataTypeMismatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "requested SQL column data type '{:?}' does not match queried data type '{:?}'", self.requested, self.queried)
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
    pub datum_type: DatumType,
    pub odbc_type: SqlDataType,
    pub nullable: bool,
    pub name: String,
}

/// Types of values that column can be converted to.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DatumType {
    Bit,
    Tinyint,
    Smallint,
    Integer,
    Bigint,
    Float,
    Double,
    String,
    Timestamp,
    Date,
    Time,
    #[cfg(feature = "serde_json")]
    Json,
}

impl DatumType {
    /// Static string describing type of column datum.
    pub fn description(&self) -> &'static str {
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

/// Represents SQL table column which can ba converted to Rust native type.
pub struct Column<'r, 's, 'c, S> {
    column_type: &'r ColumnType,
    cursor: &'r mut odbc::Cursor<'s, 'c, 'c, S>,
    index: u16,
    utf_16_strings: bool,
}

impl<'r, 's, 'c, S> Column<'r, 's, 'c, S> {
    fn into<T: OdbcType<'r>>(self) -> Result<Option<T>, DatumAccessError> {
        self.cursor.get_data::<T>(self.index + 1).map_err(DatumAccessError::OdbcCursorError)
    }

    pub fn column_type(&self) -> &ColumnType {
        self.column_type
    }

    // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-2017

    pub fn as_bool(self) -> Result<Option<bool>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_EXT_BIT => {
                self.into::<u8>()?
                    .map(|byte| if byte == 0 { false } else { true })
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "BIT",
                queried,
            }))
        })
    }

    pub fn as_i8(self) -> Result<Option<i8>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_EXT_TINYINT => {
                self.into::<i8>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "TINYINT",
                queried,
            }))
        })
    }

    pub fn as_i16(self) -> Result<Option<i16>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_SMALLINT => {
                self.into::<i16>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "SMALLINT",
                queried,
            }))
        })
    }

    pub fn as_i32(self) -> Result<Option<i32>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_INTEGER => {
                self.into::<i32>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "INTEGER",
                queried,
            }))
        })
    }

    pub fn as_i64(self) -> Result<Option<i64>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_EXT_BIGINT => {
                self.into::<i64>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "BIGINT",
                queried,
            }))
        })
    }

    pub fn as_f32(self) -> Result<Option<f32>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_REAL |
            SqlDataType::SQL_FLOAT => {
                self.into::<f32>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "FLOAT",
                queried,
            }))
        })
    }

    pub fn as_f64(self) -> Result<Option<f64>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_DOUBLE => {
                self.into::<f64>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "DOUBLE",
                queried,
            }))
        })
    }

    pub fn as_string(self) -> Result<Option<String>, DatumAccessError> {
        use SqlDataType::*;
        Ok(match self.column_type.odbc_type {
            SQL_CHAR | SQL_VARCHAR | SQL_EXT_LONGVARCHAR => {
                self.into::<String>()?
            }
            SQL_EXT_WCHAR | SQL_EXT_WVARCHAR | SQL_EXT_WLONGVARCHAR => {
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
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "STRING",
                queried,
            }))
        })
    }

    pub fn as_timestamp(self) -> Result<Option<SqlTimestamp>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_TIMESTAMP => {
                self.into::<SqlTimestamp>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "TIMESTAMP",
                queried,
            }))
        })
    }

    pub fn as_date(self) -> Result<Option<SqlDate>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_DATE => {
                self.into::<SqlDate>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "DATE",
                queried,
            }))
        })
    }

    pub fn as_time(self) -> Result<Option<SqlSsTime2>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            SqlDataType::SQL_TIME=> {
                self.into::<SqlTime>()?.map(|ss| SqlSsTime2 {
                    hour: ss.hour,
                    minute: ss.minute,
                    second: ss.second,
                    fraction: 0
                })
            }
            SqlDataType::SQL_SS_TIME2 => {
                self.into::<SqlSsTime2>()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "TIME",
                queried,
            }))
        })
    }

    #[cfg(feature = "serde_json")]
    pub fn as_json(self) -> Result<Option<serde_json::Value>, DatumAccessError> {
        Ok(match self.column_type.odbc_type {
            queried @ SqlDataType::SQL_UNKNOWN_TYPE => {
                self.into::<String>()?.map(|data| {
                    // MonetDB can only store arrays or objects as top level JSON values so check if data looks like JSON in case we are not talking to MonetDB
                    if (data.starts_with("[") && data.ends_with("]")) || (data.starts_with("{") && data.ends_with("}")) {
                        serde_json::from_str(&data).map_err(Into::into)
                    } else {
                        //TOOD: better error?
                        return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                            requested: "JSON",
                            queried,
                        }))
                    }
                }).transpose()?
            }
            queried => return Err(DatumAccessError::SqlDataTypeMismatch(SqlDataTypeMismatch {
                requested: "JSON",
                queried,
            }))
        })
    }
}

/// Represents SQL table row of Column objects.
pub struct Row<'r, 's, 'c, S> {
    schema: &'r[ColumnType],
    cursor: odbc::Cursor<'s, 'c, 'c, S>,
    index: u16,
    columns: u16,
    utf_16_strings: bool,
}

impl<'r, 's, 'c, S> Row<'r, 's, 'c, S> {
    pub fn new(cursor: odbc::Cursor<'s, 'c, 'c, S>, schema: &'r[ColumnType], utf_16_strings: bool) -> Row<'r, 's, 'c, S> {
        Row {
            schema: schema,
            cursor: cursor,
            index: 0,
            columns: schema.len() as u16,
            utf_16_strings,
        }
    }

    pub fn shift_column<'i>(&'i mut self) -> Option<Column<'i, 's, 'c, S>> {
        self.schema.get(self.index as usize).map(move |column_type| {
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
