use crate::row::{Configuration, DatumType, Column, TryFromColumn, ColumnConvertError};
use odbc::{SqlDate, SqlSsTime2, SqlTime, SqlTimestamp};
use std::convert::{Infallible, TryInto};
use std::error::Error;
use std::fmt;

#[cfg(feature = "chrono")]
pub use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
#[cfg(feature = "chrono")]
pub use chrono::{Datelike, Timelike};
#[cfg(feature = "rust_decimal")]
pub use rust_decimal::Decimal;
#[cfg(feature = "serde_json")]
pub use serde_json::Value as Json;

/// Representation of every supported column value.
#[derive(Clone, PartialEq)]
pub enum Value {
    Bit(bool),
    Tinyint(i8),
    Smallint(i16),
    Integer(i32),
    Bigint(i64),
    Float(f32),
    Double(f64),
    #[cfg(feature = "rust_decimal")]
    Decimal(Decimal),
    String(String),
    Timestamp(SqlTimestamp),
    Date(SqlDate),
    Time(SqlSsTime2),
    #[cfg(feature = "serde_json")]
    Json(Json),
}

/// Note that `as_` methods return reference so values can be parameter-bound to a query.
/// Use `to_` or `into_` methods to get values cheaply.
impl Value {
    pub fn as_bool(&self) -> Option<&bool> {
        match self {
            Value::Bit(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_bool(&self) -> Option<bool> {
        self.as_bool().cloned()
    }

    pub fn as_i8(&self) -> Option<&i8> {
        match self {
            Value::Tinyint(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_i8(&self) -> Option<i8> {
        self.as_i8().cloned()
    }

    pub fn as_i16(&self) -> Option<&i16> {
        match self {
            Value::Smallint(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_i16(&self) -> Option<i16> {
        self.as_i16().cloned()
    }

    pub fn as_i32(&self) -> Option<&i32> {
        match self {
            Value::Integer(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_i32(&self) -> Option<i32> {
        self.as_i32().cloned()
    }

    pub fn as_i64(&self) -> Option<&i64> {
        match self {
            Value::Bigint(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        self.as_i64().cloned()
    }

    pub fn as_f32(&self) -> Option<&f32> {
        match self {
            Value::Float(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_f32(&self) -> Option<f32> {
        self.as_f32().cloned()
    }

    pub fn as_f64(&self) -> Option<&f64> {
        match self {
            Value::Double(value) => Some(value),
            _ => None,
        }
    }

    #[cfg(feature = "rust_decimal")]
    pub fn as_decimal(&self) -> Option<&Decimal> {
        match self {
            Value::Decimal(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_f64(&self) -> Option<f64> {
        self.as_f64().cloned()
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn into_string(self) -> Result<String, Value> {
        match self {
            Value::String(value) => Ok(value),
            _ => Err(self),
        }
    }

    pub fn as_timestamp(&self) -> Option<&SqlTimestamp> {
        match self {
            Value::Timestamp(value) => Some(value),
            _ => None,
        }
    }

    pub fn into_timestamp(self) -> Result<SqlTimestamp, Value> {
        match self {
            Value::Timestamp(value) => Ok(value),
            _ => Err(self),
        }
    }

    #[cfg(feature = "chrono")]
    pub fn to_naive_date_time(&self) -> Option<NaiveDateTime> {
        self.as_timestamp().map(|value| {
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
        })
    }

    pub fn as_date(&self) -> Option<&SqlDate> {
        match self {
            Value::Date(value) => Some(value),
            _ => None,
        }
    }

    pub fn into_date(self) -> Result<SqlDate, Value> {
        match self {
            Value::Date(value) => Ok(value),
            _ => Err(self),
        }
    }

    #[cfg(feature = "chrono")]
    pub fn to_naive_date(&self) -> Option<NaiveDate> {
        self.as_date().map(|value| {
            NaiveDate::from_ymd(
                i32::from(value.year),
                u32::from(value.month),
                u32::from(value.day),
            )
        })
    }

    pub fn as_time(&self) -> Option<&SqlSsTime2> {
        match self {
            Value::Time(value) => Some(value),
            _ => None,
        }
    }

    pub fn into_time(self) -> Result<SqlSsTime2, Value> {
        match self {
            Value::Time(value) => Ok(value),
            _ => Err(self),
        }
    }

    #[cfg(feature = "chrono")]
    pub fn to_naive_time(&self) -> Option<NaiveTime> {
        self.as_time().map(|value| {
            NaiveTime::from_hms_nano(
                u32::from(value.hour),
                u32::from(value.minute),
                u32::from(value.second),
                value.fraction,
            )
        })
    }

    #[cfg(feature = "serde_json")]
    pub fn as_json(&self) -> Option<&Json> {
        match self {
            Value::Json(value) => Some(value),
            _ => None,
        }
    }

    #[cfg(feature = "serde_json")]
    pub fn into_json(self) -> Result<Json, Value> {
        match self {
            Value::Json(value) => Ok(value),
            _ => Err(self),
        }
    }

    /// Type of this value.
    pub fn datum_type(&self) -> DatumType {
        match self {
            Value::Bit(_) => DatumType::Bit,
            Value::Tinyint(_) => DatumType::Tinyint,
            Value::Smallint(_) => DatumType::Smallint,
            Value::Integer(_) => DatumType::Integer,
            Value::Bigint(_) => DatumType::Bigint,
            Value::Float(_) => DatumType::Float,
            Value::Double(_) => DatumType::Double,
            #[cfg(feature = "rust_decimal")]
            Value::Decimal(_) => DatumType::Decimal,
            Value::String(_) => DatumType::String,
            Value::Timestamp(_) => DatumType::Timestamp,
            Value::Date(_) => DatumType::Date,
            Value::Time(_) => DatumType::Time,
            #[cfg(feature = "serde_json")]
            Value::Json(_) => DatumType::Json,
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Value {
        Value::Bit(value)
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Value {
        Value::Tinyint(value)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Value {
        Value::Smallint(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Value {
        Value::Integer(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Value {
        Value::Bigint(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Value {
        Value::Float(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Value {
        Value::Double(value)
    }
}

#[cfg(feature = "rust_decimal")]
impl From<Decimal> for Value {
    fn from(value: Decimal) -> Value {
        Value::Decimal(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value::String(value)
    }
}

#[cfg(feature = "chrono")]
impl From<NaiveDateTime> for Value {
    fn from(value: NaiveDateTime) -> Value {
        Value::Timestamp(SqlTimestamp {
            day: value.day() as u16,
            month: value.month() as u16,
            year: value.year() as i16,
            hour: value.hour() as u16,
            minute: value.minute() as u16,
            second: value.second() as u16,
            fraction: value.nanosecond(),
        })
    }
}

impl From<SqlTimestamp> for Value {
    fn from(value: SqlTimestamp) -> Value {
        Value::Timestamp(value)
    }
}

#[cfg(feature = "chrono")]
use crate::odbc_type::UnixTimestamp;
#[cfg(feature = "chrono")]
impl From<UnixTimestamp> for Value {
    fn from(value: UnixTimestamp) -> Value {
        Value::Timestamp(value.into_inner())
    }
}

#[cfg(feature = "chrono")]
impl From<NaiveDate> for Value {
    fn from(value: NaiveDate) -> Value {
        Value::Date(SqlDate {
            day: value.day() as u16,
            month: value.month() as u16,
            year: value.year() as i16,
        })
    }
}

impl From<SqlDate> for Value {
    fn from(value: SqlDate) -> Value {
        Value::Date(value)
    }
}

#[cfg(feature = "chrono")]
impl From<NaiveTime> for Value {
    fn from(value: NaiveTime) -> Value {
        Value::Time(SqlSsTime2 {
            hour: value.hour() as u16,
            minute: value.minute() as u16,
            second: value.second() as u16,
            fraction: value.nanosecond(),
        })
    }
}

impl From<SqlTime> for Value {
    fn from(value: SqlTime) -> Value {
        Value::Time(SqlSsTime2 {
            hour: value.hour,
            minute: value.minute,
            second: value.second,
            fraction: 0,
        })
    }
}

impl From<SqlSsTime2> for Value {
    fn from(value: SqlSsTime2) -> Value {
        Value::Time(value)
    }
}

#[cfg(feature = "serde_json")]
impl From<Json> for Value {
    fn from(value: Json) -> Value {
        Value::Json(value)
    }
}

impl<C: Configuration> TryFromColumn<C> for Option<Value> {
    type Error = ColumnConvertError;

    fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S, C>) -> Result<Self, Self::Error> {
        Ok(match column.column_type.datum_type {
            DatumType::Bit => column.into_bool()?.map(Value::from),
            DatumType::Tinyint => column.into_i8()?.map(Value::from),
            DatumType::Smallint => column.into_i16()?.map(Value::from),
            DatumType::Integer => column.into_i32()?.map(Value::from),
            DatumType::Bigint => column.into_i64()?.map(Value::from),
            DatumType::Float => column.into_f32()?.map(Value::from),
            DatumType::Double => column.into_f64()?.map(Value::from),
            #[cfg(feature = "rust_decimal")]
            DatumType::Decimal => column.into_decimal()?.map(Value::from),
            DatumType::String => column.into_string()?.map(Value::from),
            DatumType::Timestamp => column.into_timestamp()?.map(Value::from),
            DatumType::Date => column.into_date()?.map(Value::from),
            DatumType::Time => column.into_time()?.map(Value::from),
            #[cfg(feature = "serde_json")]
            DatumType::Json => column.into_json()?.map(Value::from),
        })
    }
}

impl<C: Configuration> TryFromColumn<C> for Value {
    type Error = ColumnConvertError;

    fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S, C>) -> Result<Self, Self::Error> {
        let value: Option<Value> = TryFromColumn::try_from_column(column)?;
        value.ok_or_else(|| ColumnConvertError::UnexpectedNullValue("Value"))
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Bit(ref b) => fmt::Display::fmt(b, f),
            Value::Tinyint(ref n) => fmt::Display::fmt(n, f),
            Value::Smallint(ref n) => fmt::Display::fmt(n, f),
            Value::Integer(ref n) => fmt::Display::fmt(n, f),
            Value::Bigint(ref n) => fmt::Display::fmt(n, f),
            Value::Float(ref n) => fmt::Display::fmt(n, f),
            Value::Double(ref n) => fmt::Display::fmt(n, f),
            #[cfg(feature = "rust_decimal")]
            Value::Decimal(ref n) => fmt::Display::fmt(n, f),
            Value::String(ref s) => fmt::Display::fmt(s, f),
            Value::Timestamp(ref timestamp) => write!(
                f,
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
                timestamp.year,
                timestamp.month,
                timestamp.day,
                timestamp.hour,
                timestamp.minute,
                timestamp.second,
                timestamp.fraction / 1_000_000
            ),
            Value::Date(ref date) => {
                write!(f, "{:04}-{:02}-{:02}", date.year, date.month, date.day)
            }
            Value::Time(ref time) => write!(
                f,
                "{:02}:{:02}:{:02}.{:03}",
                time.hour,
                time.minute,
                time.second,
                time.fraction / 1_000_000
            ),
            #[cfg(feature = "serde_json")]
            Value::Json(ref json) => write!(f, "{}", json),
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Bit(ref b) => f.debug_tuple("Bit").field(b).finish(),
            Value::Tinyint(ref n) => f.debug_tuple("Tinyint").field(n).finish(),
            Value::Smallint(ref n) => f.debug_tuple("Smallint").field(n).finish(),
            Value::Integer(ref n) => f.debug_tuple("Integer").field(n).finish(),
            Value::Bigint(ref n) => f.debug_tuple("Bigint").field(n).finish(),
            Value::Float(ref n) => f.debug_tuple("Float").field(n).finish(),
            Value::Double(ref n) => f.debug_tuple("Double").field(n).finish(),
            #[cfg(feature = "rust_decimal")]
            Value::Decimal(ref n) => f.debug_tuple("Decimal").field(n).finish(),
            Value::String(ref s) => f.debug_tuple("String").field(s).finish(),
            timestamp @ Value::Timestamp(_) => f
                .debug_tuple("Timestamp")
                .field(&format_args!("{}", timestamp))
                .finish(),
            date @ Value::Date(_) => f
                .debug_tuple("Date")
                .field(&format_args!("{}", date))
                .finish(),
            time @ Value::Time(_) => f
                .debug_tuple("Time")
                .field(&format_args!("{}", time))
                .finish(),
            #[cfg(feature = "serde_json")]
            Value::Json(ref j) => f.debug_tuple("Json").field(j).finish(),
        }
    }
}

/// Wrapper type that can be used to display nullable column value represented as `Option<Value>`.
#[derive(Debug, Clone, PartialEq)]
pub struct NullableValue<'i>(&'i Option<Value>, &'static str);

impl<'i> fmt::Display for NullableValue<'i> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Some(ref v) => fmt::Display::fmt(v, f),
            None => write!(f, "{}", self.1),
        }
    }
}

pub trait AsNullable {
    /// Convert to `NullableValue` that implements `Display` representing no value as "NULL".
    fn as_nullable(&self) -> NullableValue {
        self.as_nullable_as("NULL")
    }

    /// Convert to `NullableValue` that implements `Display` representing no value as given string.
    fn as_nullable_as(&self, null: &'static str) -> NullableValue;
}

/// Represent `None` as "NULL" and `Some(Value)` as value.
impl AsNullable for Option<Value> {
    fn as_nullable_as(&self, null: &'static str) -> NullableValue {
        NullableValue(&self, null)
    }
}

/// Column values can be converted to types implementing this trait.
///
/// This trait is implemented for primitive Rust types, `String` and `chrono` date and time types.
pub trait TryFromValue: Sized {
    type Error: Error + 'static;
    fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error>;
}

/// Error type that represents different problems when converting column values to specific types.
#[derive(Debug)]
pub enum ValueConvertError {
    UnexpectedNullValue(&'static str),
    UnexpectedType {
        expected: &'static str,
        got: &'static str,
    },
    ValueOutOfRange {
        expected: &'static str,
    },
}

impl fmt::Display for ValueConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueConvertError::UnexpectedNullValue(t) => {
                write!(f, "expecting value of type {} but got NULL", t)
            }
            ValueConvertError::UnexpectedType { expected, got } => {
                write!(f, "expecting value of type {} but got {}", expected, got)
            }
            ValueConvertError::ValueOutOfRange { expected } => {
                write!(f, "value is out of range for type {}", expected)
            }
        }
    }
}

impl Error for ValueConvertError {}

impl TryFromValue for Value {
    type Error = ValueConvertError;
    fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
        value.ok_or_else(|| ValueConvertError::UnexpectedNullValue("Value"))
    }
}

impl TryFromValue for Option<Value> {
    type Error = Infallible;
    fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
        Ok(value)
    }
}

macro_rules! try_from_value_copy {
    ($t:ty, $f:ident) => {
        impl TryFromValue for $t {
            type Error = ValueConvertError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                let value =
                    value.ok_or_else(|| ValueConvertError::UnexpectedNullValue(stringify!($t)))?;
                value.$f().ok_or_else(|| ValueConvertError::UnexpectedType {
                    expected: stringify!($t),
                    got: value.datum_type().description(),
                })
            }
        }

        impl TryFromValue for Option<$t> {
            type Error = ValueConvertError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                value
                    .map(|value| TryFromValue::try_from_value(Some(value)))
                    .transpose()
            }
        }
    };
}

macro_rules! try_from_value_unsigned {
    ($it:ty, $t:ty) => {
        impl TryFromValue for $t {
            type Error = ValueConvertError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                let value: $it = TryFromValue::try_from_value(value)?;
                value
                    .try_into()
                    .map_err(|_| ValueConvertError::ValueOutOfRange {
                        expected: stringify!($t),
                    })
            }
        }

        impl TryFromValue for Option<$t> {
            type Error = ValueConvertError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                value
                    .map(|value| TryFromValue::try_from_value(Some(value)))
                    .transpose()
            }
        }
    };
}

macro_rules! try_from_value_owned {
    ($t:ty, $f:ident) => {
        impl TryFromValue for $t {
            type Error = ValueConvertError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                let value =
                    value.ok_or_else(|| ValueConvertError::UnexpectedNullValue(stringify!($t)))?;
                value
                    .$f()
                    .map_err(|value| ValueConvertError::UnexpectedType {
                        expected: stringify!($t),
                        got: value.datum_type().description(),
                    })
            }
        }

        impl TryFromValue for Option<$t> {
            type Error = ValueConvertError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                value
                    .map(|value| {
                        value
                            .$f()
                            .map_err(|value| ValueConvertError::UnexpectedType {
                                expected: stringify!($t),
                                got: value.datum_type().description(),
                            })
                    })
                    .transpose()
            }
        }
    };
}

try_from_value_copy![bool, to_bool];
try_from_value_copy![i8, to_i8];
try_from_value_unsigned![i8, u8];
try_from_value_copy![i16, to_i16];
try_from_value_unsigned![i16, u16];
try_from_value_copy![i32, to_i32];
try_from_value_unsigned![i32, u32];
try_from_value_copy![i64, to_i64];
try_from_value_unsigned![i64, u64];
try_from_value_copy![f32, to_f32];
try_from_value_copy![f64, to_f64];
try_from_value_owned![String, into_string];
try_from_value_owned![SqlTimestamp, into_timestamp];
#[cfg(feature = "chrono")]
try_from_value_copy![NaiveDateTime, to_naive_date_time];
try_from_value_owned![SqlDate, into_date];
#[cfg(feature = "chrono")]
try_from_value_copy![NaiveDate, to_naive_date];
try_from_value_owned![SqlSsTime2, into_time];
#[cfg(feature = "chrono")]
try_from_value_copy![NaiveTime, to_naive_time];
#[cfg(feature = "serde_json")]
try_from_value_owned![Json, into_json];

#[cfg(feature = "serde")]
mod ser {
    use super::*;
    use serde::{self, Serialize};

    impl Serialize for Value {
        #[inline]
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: ::serde::Serializer,
        {
            match *self {
                Value::Bit(b) => serializer.serialize_bool(b),
                Value::Tinyint(n) => serializer.serialize_i8(n),
                Value::Smallint(n) => serializer.serialize_i16(n),
                Value::Integer(n) => serializer.serialize_i32(n),
                Value::Bigint(n) => serializer.serialize_i64(n),
                Value::Float(n) => serializer.serialize_f32(n),
                Value::Double(n) => serializer.serialize_f64(n),
                #[cfg(feature = "rust_decimal")]
                Value::Decimal(n) => Serialize::serialize(&n, serializer),
                Value::String(ref s) => serializer.serialize_str(s),
                ref value @ Value::Timestamp(_)
                | ref value @ Value::Date(_)
                | ref value @ Value::Time(_) => serializer.serialize_str(&value.to_string()),
                #[cfg(feature = "serde_json")]
                Value::Json(ref j) => j.serialize(serializer),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn serialize_value_primitive() {
            assert_eq!(&serde_json::to_string(&Value::Bit(true)).unwrap(), "true");
            assert_eq!(&serde_json::to_string(&Value::Bit(false)).unwrap(), "false");

            assert_eq!(&serde_json::to_string(&Value::Integer(-1)).unwrap(), "-1");
            assert_eq!(&serde_json::to_string(&Value::Integer(22)).unwrap(), "22");

            assert_eq!(
                &serde_json::to_string(&Value::Double(-1.1)).unwrap(),
                "-1.1"
            );
            assert_eq!(
                &serde_json::to_string(&Value::Double(33.22)).unwrap(),
                "33.22"
            );

            assert_eq!(
                &serde_json::to_string(&Value::String("foo".to_owned())).unwrap(),
                "\"foo\""
            );
            assert_eq!(
                &serde_json::to_string(&Value::String("bar baz".to_owned())).unwrap(),
                "\"bar baz\""
            );
        }

        #[cfg(feature = "rust_decimal")]
        #[test]
        fn serialize_value_decimal() {
            use std::str::FromStr;

            assert_eq!(
                &serde_json::to_string(&Value::Decimal(Decimal::from_str("-1.1").unwrap())).unwrap(),
                "\"-1.1\""
            );
            assert_eq!(
                &serde_json::to_string(&Value::Decimal(Decimal::from_str("33.22").unwrap())).unwrap(),
                "\"33.22\""
            );
            assert_eq!(
                &serde_json::to_string(&Value::Decimal(Decimal::from_str("10.9231213232423424323423234234").unwrap())).unwrap(),
                "\"10.923121323242342432342323423\""
            );
        }

        #[cfg(feature = "chrono")]
        #[test]
        fn serialize_value_timestamp() {
            assert_eq!(
                &serde_json::to_string(&Value::from(
                    NaiveDate::from_ymd(2016, 7, 8).and_hms_milli(9, 10, 11, 23)
                ))
                .unwrap(),
                "\"2016-07-08 09:10:11.023\""
            );
            assert_eq!(
                &serde_json::to_string(&Value::from(
                    NaiveDate::from_ymd(2016, 12, 8).and_hms_milli(19, 1, 1, 0)
                ))
                .unwrap(),
                "\"2016-12-08 19:01:01.000\""
            );
        }

        #[cfg(feature = "chrono")]
        #[test]
        fn serialize_value_date() {
            assert_eq!(
                &serde_json::to_string(&Value::from(NaiveDate::from_ymd(2016, 7, 8))).unwrap(),
                "\"2016-07-08\""
            );
            assert_eq!(
                &serde_json::to_string(&Value::from(NaiveDate::from_ymd(2016, 12, 8))).unwrap(),
                "\"2016-12-08\""
            );
        }

        #[cfg(feature = "chrono")]
        #[test]
        fn serialize_value_time() {
            assert_eq!(
                &serde_json::to_string(&Value::from(NaiveTime::from_hms_milli(9, 10, 11, 23)))
                    .unwrap(),
                "\"09:10:11.023\""
            );
            assert_eq!(
                &serde_json::to_string(&Value::from(NaiveTime::from_hms_milli(19, 1, 1, 0)))
                    .unwrap(),
                "\"19:01:01.000\""
            );
        }
    }
}
