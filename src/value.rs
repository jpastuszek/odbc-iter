use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
use odbc::{SqlDate, SqlSsTime2, SqlTime, SqlTimestamp};

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
    Timestamp(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
}

impl Value {
    pub fn as_bit(&self) -> Option<bool> {
        match self {
            Value::Bit(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_tinyint(&self) -> Option<i8> {
        match self {
            Value::Tinyint(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_smallint(&self) -> Option<i16> {
        match self {
            Value::Smallint(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i32> {
        match self {
            Value::Integer(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_bigint(&self) -> Option<i64> {
        match self {
            Value::Bigint(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f32> {
        match self {
            Value::Float(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_dobule(&self) -> Option<f64> {
        match self {
            Value::Double(value) => Some(*value),
            _ => None,
        }
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

    pub fn as_timestamp(&self) -> Option<NaiveDateTime> {
        match self {
            Value::Timestamp(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<NaiveDate> {
        match self {
            Value::Date(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_time(&self) -> Option<NaiveTime> {
        match self {
            Value::Time(value) => Some(*value),
            _ => None,
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

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value::String(value)
    }
}

impl From<NaiveDateTime> for Value {
    fn from(value: NaiveDateTime) -> Value {
        Value::Timestamp(value)
    }
}

impl From<SqlTimestamp> for Value {
    fn from(value: SqlTimestamp) -> Value {
        Value::Timestamp(NaiveDateTime::new(
            NaiveDate::from_ymd(value.year as i32, value.month as u32, value.day as u32),
            NaiveTime::from_hms_nano(
                value.hour as u32,
                value.minute as u32,
                value.second as u32,
                value.fraction,
            ),
        ))
    }
}

impl From<NaiveDate> for Value {
    fn from(value: NaiveDate) -> Value {
        Value::Date(value)
    }
}

impl From<SqlDate> for Value {
    fn from(value: SqlDate) -> Value {
        Value::Date(NaiveDate::from_ymd(
            value.year as i32,
            value.month as u32,
            value.day as u32,
        ))
    }
}

impl From<NaiveTime> for Value {
    fn from(value: NaiveTime) -> Value {
        Value::Time(value)
    }
}

impl From<SqlTime> for Value {
    fn from(value: SqlTime) -> Value {
        Value::Time(NaiveTime::from_hms(
            value.hour as u32,
            value.minute as u32,
            value.second as u32,
        ))
    }
}

impl From<SqlSsTime2> for Value {
    fn from(value: SqlSsTime2) -> Value {
        Value::Time(NaiveTime::from_hms_nano(
            value.hour as u32,
            value.minute as u32,
            value.second as u32,
            value.fraction,
        ))
    }
}

pub type ValueRow = Vec<Option<Value>>;
