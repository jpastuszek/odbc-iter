use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
use chrono::{Datelike, Timelike};
use odbc::{SqlDate, SqlSsTime2, SqlTime, SqlTimestamp};

pub type ValueRow = Vec<Option<Value>>;

#[derive(Debug)]
pub enum Value {
    Bit(bool),
    Tinyint(i8),
    Smallint(i16),
    Integer(i32),
    Bigint(i64),
    Float(f32),
    Double(f64),
    String(String),
    Timestamp(SqlTimestamp),
    Date(SqlDate),
    Time(SqlSsTime2),
}

/// Note that `as_` methods return reference so values can be parameter-bound to a query
/// Use `to_` or `into_` methods to get values cheaply
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

    pub fn to_naive_date_time(&self) -> Option<NaiveDateTime> {
        self.as_timestamp().map(|value| {
            NaiveDate::from_ymd(value.year as i32, value.month as u32, value.day as u32)
                .and_hms_nano(
                    value.hour as u32,
                    value.minute as u32,
                    value.second as u32,
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

    pub fn to_naive_date(&self) -> Option<NaiveDate> {
        self.as_date().map(|value| {
            NaiveDate::from_ymd(value.year as i32, value.month as u32, value.day as u32)
        })
    }

    pub fn as_time(&self) -> Option<&SqlSsTime2> {
        match self {
            Value::Time(value) => Some(value),
            _ => None,
        }
    }

    pub fn to_naive_time(&self) -> Option<NaiveTime> {
        self.as_time().map(|value| {
            NaiveTime::from_hms_nano(
                value.hour as u32,
                value.minute as u32,
                value.second as u32,
                value.fraction,
            )
        })
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

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value::String(value)
    }
}

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

use crate::odbc_type::UnixTimestamp;
impl From<UnixTimestamp> for Value {
    fn from(value: UnixTimestamp) -> Value {
        Value::Timestamp(value.into_inner())
    }
}

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
