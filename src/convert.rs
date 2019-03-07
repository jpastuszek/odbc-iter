use std::fmt;
use std::error::Error;
use crate::value::{ValueRow, Value};
use crate::Schema;
use crate::{SqlTimestamp, SqlDate, SqlSsTime2};
use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};

//TODO: use ! type when it is stable
#[derive(Debug)]
pub struct NoError;

impl fmt::Display for NoError {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        panic!("unexpected error")
    }
}

impl Error for NoError {}

/// Convert from ODBC schema to other type of schema
pub trait TryFromSchema: Sized {
    type Error: Error + 'static;
    fn try_from_schema(schema: &Schema) -> Result<Self, Self::Error>;
}

impl TryFromSchema for () {
    type Error = NoError;
    fn try_from_schema(_schema: &Schema) -> Result<Self, Self::Error> {
        Ok(())
    }
}

impl TryFromSchema for Schema {
    type Error = NoError;
    fn try_from_schema(schema: &Schema) -> Result<Self, Self::Error> {
        Ok(schema.clone())
    }
}

/// Convert single column value to given type
pub trait TryFromValue: Sized {
    type Error: Error + 'static;
    fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error>;
}

impl TryFromValue for Option<Value> {
    type Error = NoError;
    fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
        Ok(value)
    }
}

#[derive(Debug)]
pub enum TryFromValueError {
    UnexpectedNullValue,
    UnexpectedType { 
        expected: &'static str, 
        got: &'static str 
    },
}

impl fmt::Display for TryFromValueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TryFromValueError::UnexpectedNullValue => write!(f, "expecting value but got NULL"),
            TryFromValueError::UnexpectedType { expected, got } => write!(f, "expecting value of type {} but got {}", expected, got),
        }
    }
}

impl Error for TryFromValueError {}

impl TryFromValue for Value {
    type Error = TryFromValueError;
    fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
        value.ok_or_else(|| TryFromValueError::UnexpectedNullValue)
    }
}

macro_rules! try_from_value_copy {
    ($t:ty, $f:ident) => { 
        impl TryFromValue for $t {
            type Error = TryFromValueError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                let value = value.ok_or_else(|| TryFromValueError::UnexpectedNullValue)?;
                value.$f().ok_or_else(|| TryFromValueError::UnexpectedType { expected: stringify!($t), got: value.description() })
            }
        }

        impl TryFromValue for Option<$t> {
            type Error = TryFromValueError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                value.map(|value| value.$f().ok_or_else(|| TryFromValueError::UnexpectedType { expected: stringify!($t), got: value.description() })).transpose()
            }
        }
    }
}

macro_rules! try_from_value_owned {
    ($t:ty, $f:ident) => { 
        impl TryFromValue for $t {
            type Error = TryFromValueError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                let value = value.ok_or_else(|| TryFromValueError::UnexpectedNullValue)?;
                value.$f().map_err(|value| TryFromValueError::UnexpectedType { expected: stringify!($t), got: value.description() })
            }
        }

        impl TryFromValue for Option<$t> {
            type Error = TryFromValueError;
            fn try_from_value(value: Option<Value>) -> Result<Self, Self::Error> {
                value.map(|value| value.$f().map_err(|value| TryFromValueError::UnexpectedType { expected: stringify!($t), got: value.description() })).transpose()
            }
        }
    }
}

try_from_value_copy![bool, to_bool];
try_from_value_copy![i8, to_i8];
try_from_value_copy![i16, to_i16];
try_from_value_copy![i32, to_i32];
try_from_value_copy![i64, to_i64];
try_from_value_copy![f32, to_f32];
try_from_value_copy![f64, to_f64];
try_from_value_owned![String, into_string];
try_from_value_owned![SqlTimestamp, into_timestamp];
try_from_value_copy![NaiveDateTime, to_naive_date_time];
try_from_value_owned![SqlDate, into_date];
try_from_value_copy![NaiveDate, to_naive_date];
try_from_value_owned![SqlSsTime2, into_time];
try_from_value_copy![NaiveTime, to_naive_time];

/// Convert from ODBC row to other type of value
pub trait TryFromRow: Sized {
    /// Type of schema for the target value
    type Schema: TryFromSchema;
    type Error: Error + 'static;
    fn try_from_row(values: ValueRow, schema: &Self::Schema) -> Result<Self, Self::Error>;
}

impl TryFromRow for ValueRow {
    type Schema = Schema;
    type Error = NoError;
    fn try_from_row(values: ValueRow, _schema: &Self::Schema) -> Result<Self, Self::Error> {
        Ok(values)
    }
}

#[derive(Debug)]
pub enum TryFromRowError<V> {
    UnexpectedNumberOfColumns { 
        expected: u16,
        got: u16,
    },
    ValueConversionError(V),
}

impl<V> fmt::Display for TryFromRowError<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TryFromRowError::UnexpectedNumberOfColumns { expected, got } => write!(f, "unexpected number of columns: expected {} but got {}", expected, got),
            TryFromRowError::ValueConversionError(_) => write!(f, "failed to convert column value to target type"),
        }
    }
}

impl<V: Error + 'static> Error for TryFromRowError<V> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TryFromRowError::UnexpectedNumberOfColumns { .. } => None,
            TryFromRowError::ValueConversionError(err) => Some(err),
        }
    }
}

impl<T> TryFromRow for T where T: TryFromValue {
    type Schema = Schema;
    type Error = TryFromRowError<<T as TryFromValue>::Error>;
    fn try_from_row(mut values: ValueRow, _schema: &Self::Schema) -> Result<Self, Self::Error> {
        if values.len() != 1 {
            return Err(TryFromRowError::UnexpectedNumberOfColumns { expected: 1, got: values.len() as u16 })
        }
        T::try_from_value(values.pop().unwrap()).map_err(|err| TryFromRowError::ValueConversionError(err))
    }
}

#[derive(Debug)]
pub enum TryFromRowTupleError {
    UnexpectedNumberOfColumns { 
        expected: u16,
        tuple: &'static str,
    },
    ValueConversionError(Box<dyn Error + 'static>),
}

impl fmt::Display for TryFromRowTupleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TryFromRowTupleError::UnexpectedNumberOfColumns { expected, tuple } => write!(f, "failed to convert row with {} columns to tuple {}", expected, tuple),
            TryFromRowTupleError::ValueConversionError(_) => write!(f, "failed to convert column value to target type"),
        }
    }
}

impl Error for TryFromRowTupleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TryFromRowTupleError::UnexpectedNumberOfColumns { .. } => None,
            TryFromRowTupleError::ValueConversionError(err) => Some(err.as_ref()),
        }
    }
}

macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! try_from_tuple {
    ($(
        $Tuple:ident {
            $(($idx:tt) -> $T:ident)+
        }
    )+) => {
        $(
            impl<$($T:TryFromValue),+> TryFromRow for ($($T,)+) {
                type Schema = Schema;
                type Error = TryFromRowTupleError;
                fn try_from_row(values: ValueRow, _schema: &Self::Schema) -> Result<($($T,)+), Self::Error> {
                    if values.len() != count!($($T)+) {
                        return Err(TryFromRowTupleError::UnexpectedNumberOfColumns { expected: 1, tuple: stringify![($($T,)+)] })
                    }
                    let mut values = values.into_iter();
                    Ok(($({ let x: $T = $T::try_from_value(values.next().unwrap()).map_err(|err| TryFromRowTupleError::ValueConversionError(Box::new(err)))?; x},)+))
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

#[cfg(test)]
#[cfg(feature = "test-monetdb")]
mod tests {
    use crate::Odbc;
    use super::*;
    #[allow(unused_imports)]
    use assert_matches::assert_matches;

    #[derive(Debug)]
    struct Foo {
        val: i64,
    }

    impl TryFromRow for Foo {
        type Schema = Schema;
        type Error = NoError;
        fn try_from_row(mut values: ValueRow, _schema: &Schema) -> Result<Self, NoError> {
            Ok(values
                .pop()
                .map(|val| Foo {
                    val: val.and_then(|v| v.to_i64()).expect("val to be an bigint"),
                })
                .expect("value"))
        }
    }

    #[test]
    fn test_custom_type() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let foo: Foo = db
            .handle()
            .query("SELECT CAST(42 AS BIGINT) AS val;")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(foo.val, 42);
    }

    #[test]
    fn test_single_value() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: Value = db
            .handle()
            .query("SELECT CAST(42 AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.to_i64().unwrap(), 42);
    }

    #[test]
    fn test_single_nullable_value() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: Option<Value> = db
            .handle()
            .query("SELECT CAST(42 AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_some());
        assert_eq!(value.unwrap().to_i64().unwrap(), 42);

        let value: Option<Value> = db
            .handle()
            .query("SELECT CAST(NULL AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_none());
    }

    #[test]
    fn test_value_row() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: ValueRow = db
            .handle()
            .query("SELECT CAST(42 AS BIGINT), CAST(22 AS INTEGER)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.len(), 2);
        assert_eq!(value[0].as_ref().unwrap().to_i64().unwrap(), 42);
        assert_eq!(value[1].as_ref().unwrap().to_i32().unwrap(), 22);
    }

    #[test]
    fn test_single_copy() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: bool = db
            .handle()
            .query("SELECT true")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value, true);

        let value: Option<bool> = db
            .handle()
            .query("SELECT true")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.unwrap(), true);

        let value: Option<bool> = db
            .handle()
            .query("SELECT CAST(NULL AS BOOL)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_none());

        let value: i64 = db
            .handle()
            .query("SELECT CAST(42 AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value, 42);

        let value: Option<i64> = db
            .handle()
            .query("SELECT CAST(42 AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.unwrap(), 42);

        let value: Option<i64> = db
            .handle()
            .query("SELECT CAST(NULL AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_none());
    }

    #[test]
    fn test_single_string() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: String = db
            .handle()
            .query("SELECT 'foo'")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(&value, "foo");

        let value: Option<String> = db
            .handle()
            .query("SELECT 'foo'")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(&value.unwrap(), "foo");

        let value: Option<String> = db
            .handle()
            .query("SELECT CAST(NULL AS STRING)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_none());
    }

    #[test]
    fn test_single_date() {
        use chrono::Datelike;

        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: NaiveDate = db
            .handle()
            .query("SELECT CAST('2019-04-02' AS DATE)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.year(), 2019);
        assert_eq!(value.month(), 4);
        assert_eq!(value.day(), 2);

        let value: Option<NaiveDate> = db
            .handle()
            .query("SELECT CAST('2019-04-02' AS DATE)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.unwrap().year(), 2019);
        assert_eq!(value.unwrap().month(), 4);
        assert_eq!(value.unwrap().day(), 2);

        let value: Option<NaiveDate> = db
            .handle()
            .query("SELECT CAST(NULL AS DATE)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_none());
    }

    #[test]
    fn test_tuple_value() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: (String, i64, bool) = db
            .handle()
            .query("SELECT 'foo', CAST(42 AS BIGINT), true")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(&value.0, "foo");
        assert_eq!(value.1, 42);
        assert_eq!(value.2, true);

        let value: (Option<String>, i64, Option<bool>) = db
            .handle()
            .query("SELECT 'foo', CAST(42 AS BIGINT), true")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(&value.0.unwrap(), "foo");
        assert_eq!(value.1, 42);
        assert_eq!(value.2.unwrap(), true);

        let value: (Option<String>, i64, Option<bool>) = db
            .handle()
            .query("SELECT CAST(NULL AS STRING), CAST(42 AS BIGINT), CAST(NULL AS BOOL)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(&value.0.is_none());
        assert_eq!(value.1, 42);
        assert!(value.2.is_none());
    }
}