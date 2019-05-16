use std::fmt;
use std::error::Error;
use std::convert::{Infallible, TryFrom};
use crate::value::{ValueRow, Value, TryFromValueError};
use crate::Schema;

//TODO: implement TryFrom/TryInto for Value and ValueRow in value.rs for Rust types and use that impls in here

/// This traits allow for convetion of ValueRow type used intarnally by Rows iterator to any
/// other type returned as Item.
/// Because ValueRow does not provide names of columns the schema can be used to provide that
/// information when needed (e.g. when converting to Avro record or JSON map).

#[derive(Debug)]
pub struct ValueRowWithNames<'n>(pub ValueRow, pub &'n[String]);

#[derive(Debug)]
pub enum TryFromRowError {
    UnexpectedNullValue(&'static str),
    UnexpectedValue,
    UnexpectedNumberOfColumns {
        expected: u16,
        got: u16,
    },
    ColumnValueError(TryFromValueError),
}

impl fmt::Display for TryFromRowError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TryFromRowError::UnexpectedNullValue(t) => write!(f, "expecting value of type {} but got NULL", t),
            TryFromRowError::UnexpectedValue => write!(f, "expecting no data (unit) but got a row"),
            TryFromRowError::UnexpectedNumberOfColumns { expected, got } => write!(f, "unexpected number of columns: expected {} but got {}", expected, got),
            TryFromRowError::ColumnValueError(_) => write!(f, "failed to convert column value to target type"),
        }
    }
}

impl Error for TryFromRowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TryFromRowError::UnexpectedNullValue(_) |
            TryFromRowError::UnexpectedValue |
            TryFromRowError::UnexpectedNumberOfColumns { .. } => None,
            TryFromRowError::ColumnValueError(err) => Some(err),
        }
    }
}

//TODO: TryFrom.. for T: TryFrom<..>

impl<'n>TryFrom<ValueRowWithNames<'n>> for Value {
    type Error = TryFromRowError;
    fn try_from(values: ValueRowWithNames<'n>) -> Result<Self, Self::Error> {
        if values.0.len() != 1 {
            return TryFromRowError::UnexpectedNumberOfColumns { expected: 1, got: values.len() }
        }
        Ok(values.0[0].ok_or_else(|| TryFromRowError::UnexpectedNullValue("Value")))
    }
}

impl<'n>TryFrom<ValueRowWithNames<'n>> for Option<Value> {
    type Error = TryFromRowError;
    fn try_from(values: ValueRowWithNames<'n>) -> Result<Self, Self::Error> {
        if values.0.len() != 1 {
            return TryFromRowError::UnexpectedNumberOfColumns { expected: 1, got: values.len() }
        }
        Ok(values.0[0])
    }
}

impl<'n, T: TryFrom<ValueRowWithNames<'n>>> TryFrom<ValueRowWithNames<'n>> for T {
    type Error = TryFromRowError;
    fn try_from(values: ValueRowWithNames<'n>) -> Result<Self, Self::Error> {
        if values.0.len() != 1 {
            return TryFromRowError::UnexpectedNumberOfColumns { expected: 1, got: values.len() }
        }
        Ok(values.0[0].ok_or_else(|| TryFromRowError::UnexpectedNullValue("Value")).try_into())
    }
}

impl<'n, T: TryFrom<ValueRowWithNames<'n>>> TryFrom<ValueRowWithNames<'n>> for Option<Value> {
    type Error = TryFromRowError;
    fn try_from(values: ValueRowWithNames<'n>) -> Result<Self, Self::Error> {
        if values.0.len() != 1 {
            return TryFromRowError::UnexpectedNumberOfColumns { expected: 1, got: values.len() }
        }
        Ok(values.0[0].try_into())
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
            impl<'n, $($T:TryFrom<ValueRowWithNames<'n>>),+> TryFrom<ValueRowWithNames<'n>> for ($($T,)+) {
                type Error = TryFromRowTupleError;
                fn try_from(values: ValueRowWithNames<'n>) -> Result<($($T,)+), Self::Error> {
                    if values.0.len() != count!($($T)+) {
                        return Err(TryFromRowTupleError::UnexpectedNumberOfColumns { expected: values.len() as u16, tuple: stringify![($($T,)+)] })
                    }
                    let mut values = values.0.into_iter();
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

//TODO: this tests should not need DB
#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use crate::Odbc;
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use assert_matches::assert_matches;

    #[derive(Debug)]
    struct Foo {
        val: i64,
    }

    impl<'n>TryFrom<ValueRowWithNames<'n>> for Foo {
        type Error = Infallible;
        fn try_from(mut values: ValueRowWithNames<'n>) -> Result<Self, Infallible> {
            Ok(values
                .pop()
                .map(|val| Foo {
                    val: val.and_then(|v| v.to_i64()).expect("val to be an bigint"),
                })
                .expect("value"))
        }
    }

    #[test]
    #[cfg(feature = "test-monetdb")]
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
    #[cfg(feature = "test-monetdb")]
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
    #[cfg(feature = "test-monetdb")]
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
    #[cfg(feature = "test-monetdb")]
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
    #[cfg(feature = "test-monetdb")]
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

        assert_eq!(value.unwrap(), 42i64);

        let value: Option<i64> = db
            .handle()
            .query("SELECT CAST(NULL AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_none());
    }

    #[test]
    #[cfg(feature = "test-monetdb")]
    fn test_single_unsigned() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: Option<u64> = db
            .handle()
            .query("SELECT CAST(42 AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.unwrap(), 42u64);
    }

    #[test]
    #[cfg(feature = "test-monetdb")]
    #[should_panic(expected = "ValueOutOfRange")]
    fn test_single_unsigned_err() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let _value: Option<u64> = db
            .handle()
            .query("SELECT CAST(-666 AS BIGINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");
    }

    #[test]
    #[cfg(feature = "test-monetdb")]
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
    #[cfg(feature = "test-monetdb")]
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
    #[cfg(feature = "test-monetdb")]
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

    #[test]
    fn test_value_row_conversions() {
        let test_row: ValueRow = vec![Some(Value::Bit(true)), Some(Value::Integer(42)), None];
        type Test = (Option<bool>, Option<u32>, Option<String>);

        //let (b, u, s): (Option<bool>, Option<u32>, Option<String>) = test_row.try_into().unwrap();
        let (b, u, s) = Test::try_from_row(test_row, &()).unwrap();
        assert_eq!(b, Some(true));
        assert_eq!(u, Some(42));
        assert_eq!(s, None);
    }
}
