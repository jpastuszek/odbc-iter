//! Extra types that represent SQL data values but with extra from/to impls that implement `OdbcType` so they can be bound to query parameter
use chrono::naive::{NaiveDate, NaiveDateTime};
use chrono::{Datelike, Timelike};
use odbc::SqlTimestamp;

pub use odbc::ffi;
pub use odbc::OdbcType;

#[derive(Debug)]
pub struct UnixTimestamp(SqlTimestamp);

impl UnixTimestamp {
    pub fn as_naive_date_time(&self) -> NaiveDateTime {
        NaiveDate::from_ymd(self.0.year as i32, self.0.month as u32, self.0.day as u32)
            .and_hms_nano(
                self.0.hour as u32,
                self.0.minute as u32,
                self.0.second as u32,
                self.0.fraction,
            )
    }

    pub fn into_inner(self) -> SqlTimestamp {
        self.0
    }
}

impl From<f64> for UnixTimestamp {
    fn from(ts: f64) -> UnixTimestamp {
        let ts = NaiveDateTime::from_timestamp(ts as i64, (ts.fract() * 1_000_000_000.0) as u32);
        ts.into()
    }
}

impl From<NaiveDateTime> for UnixTimestamp {
    fn from(value: NaiveDateTime) -> UnixTimestamp {
        UnixTimestamp(SqlTimestamp {
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

unsafe impl<'a> OdbcType<'a> for UnixTimestamp {
    fn sql_data_type() -> ffi::SqlDataType {
        SqlTimestamp::sql_data_type()
    }
    fn c_data_type() -> ffi::SqlCDataType {
        SqlTimestamp::c_data_type()
    }

    fn convert(buffer: &'a [u8]) -> Self {
        UnixTimestamp(SqlTimestamp::convert(buffer))
    }

    fn column_size(&self) -> ffi::SQLULEN {
        self.0.column_size()
    }
    fn value_ptr(&self) -> ffi::SQLPOINTER {
        self.0.value_ptr()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;

    #[test]
    fn test_timestamp() {
        let ts: UnixTimestamp = 1547115460.2291234.into();

        assert_eq!(ts.0.year, 2019);
        assert_eq!(ts.0.month, 1);
        assert_eq!(ts.0.day, 10);
        assert_eq!(ts.0.hour, 10);
        assert_eq!(ts.0.minute, 17);
        assert_eq!(ts.0.second, 40);
        assert_eq!(ts.0.fraction / 1000, 229123); // need to round it up as precision is not best
    }

    #[test]
    fn test_timestamp_as_date_time() {
        let ts: UnixTimestamp = 1547115460.2291234.into();
        assert_eq!(ts.as_naive_date_time().timestamp_millis(), 1547115460229);
    }
}
