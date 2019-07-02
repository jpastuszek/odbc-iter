//! Extra types that represent SQL data values but with extra from/to implementations for `OdbcType` so they can be bound to query parameter

// Allow for custom type implementation
pub use odbc::{ffi, OdbcType};

#[cfg(feature = "chrono")]
mod sql_timestamp {
    use super::*;
    use chrono::naive::{NaiveDate, NaiveDateTime};
    use chrono::{Datelike, Timelike};
    use odbc::SqlTimestamp;

    /// `SqlTimestamp` type that can be created from number of seconds since epoch as represented by `f64` value.
    #[derive(Debug)]
    pub struct UnixTimestamp(SqlTimestamp);

    impl UnixTimestamp {
        pub fn as_naive_date_time(&self) -> NaiveDateTime {
            NaiveDate::from_ymd(
                i32::from(self.0.year),
                u32::from(self.0.month),
                u32::from(self.0.day),
            )
            .and_hms_nano(
                u32::from(self.0.hour),
                u32::from(self.0.minute),
                u32::from(self.0.second),
                self.0.fraction,
            )
        }

        pub fn into_inner(self) -> SqlTimestamp {
            self.0
        }
    }

    impl From<f64> for UnixTimestamp {
        fn from(ts: f64) -> UnixTimestamp {
            let ts =
                NaiveDateTime::from_timestamp(ts as i64, (ts.fract() * 1_000_000_000.0) as u32);
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
}

#[cfg(feature = "chrono")]
pub use sql_timestamp::*;
