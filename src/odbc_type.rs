//! Extra types that represent SQL data values but with extra from/to implementations for `OdbcType` so they can be bound to query parameter

use std::fmt;

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

use std::borrow::Cow;

/// Owned or borrowed string that can be bound as statement parameter.
#[derive(PartialEq, Eq, Debug)]
pub struct CowString<'s>(pub Cow<'s, str>);

impl<'s> From<String> for CowString<'s> {
    fn from(s: String) -> CowString<'static> {
        CowString(Cow::Owned(s))
    }
}

impl<'s> From<&'s str> for CowString<'s> {
    fn from(s: &'s str) -> CowString<'s> {
        CowString(Cow::Borrowed(s))
    }
}

impl<'s> From<Cow<'s, str>> for CowString<'s> {
    fn from(s: Cow<'s, str>) -> CowString<'s> {
        CowString(s)
    }
}

unsafe impl<'s> OdbcType<'s> for CowString<'s> {
    fn sql_data_type() -> ffi::SqlDataType {
        String::sql_data_type()
    }
    fn c_data_type() -> ffi::SqlCDataType {
        String::c_data_type()
    }

    fn convert(buffer: &'s [u8]) -> Self {
        CowString(Cow::Owned(String::convert(buffer)))
    }

    fn column_size(&self) -> ffi::SQLULEN {
        self.0.as_ref().column_size()
    }

    fn value_ptr(&self) -> ffi::SQLPOINTER {
        self.0.as_ref().value_ptr()
    }
}

/// UTF-16 encoded string that can be bound as statement parameter.
#[derive(PartialEq, Eq)]
pub struct StringUtf16(pub Vec<u16>);

impl fmt::Debug for StringUtf16 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "N{:?}", String::from_utf16(&self.0).expect("StringUtf16 is not valid UTF-16 encoded string"))
    }
}

impl From<String> for StringUtf16 {
    fn from(s: String) -> StringUtf16 {
        s.as_str().into()
    }
}

impl From<&str> for StringUtf16 {
    fn from(s: &str) -> StringUtf16 {
        StringUtf16(s.encode_utf16().collect())
    }
}

unsafe impl<'a> OdbcType<'a> for StringUtf16 {
    fn sql_data_type() -> ffi::SqlDataType {
        <&[u16]>::sql_data_type()
    }
    fn c_data_type() -> ffi::SqlCDataType {
        <&[u16]>::c_data_type()
    }

    fn convert(buffer: &[u8]) -> Self {
        StringUtf16(<&[u16]>::convert(buffer).to_owned())
    }

    fn column_size(&self) -> ffi::SQLULEN {
        <&[u16]>::column_size(&self.0.as_slice())
    }

    fn value_ptr(&self) -> ffi::SQLPOINTER {
        self.0.as_ptr() as *const &[u16] as ffi::SQLPOINTER
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::de::Deserialize<'de> for StringUtf16 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<StringUtf16, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        String::deserialize(deserializer)
            .map(From::from)
    }
}
