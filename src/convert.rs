use std::fmt;
use std::error::Error;
use crate::value::{ValueRow, Value};
use crate::Schema;

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

macro_rules! try_from_value_primitive {
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

try_from_value_primitive![bool, to_bool];
try_from_value_primitive![i8, to_i8];
try_from_value_primitive![i16, to_i16];
try_from_value_primitive![i32, to_i32];
try_from_value_primitive![i64, to_i64];
try_from_value_primitive![f32, to_f32];
try_from_value_primitive![f64, to_f64];

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
    fn test_single_primitive() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(crate::tests::monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let value: i8 = db
            .handle()
            .query("SELECT CAST(42 AS TINYINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value, 42);

        let value: Option<i8> = db
            .handle()
            .query("SELECT CAST(42 AS TINYINT)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.unwrap(), 42);

        let value: Option<i8> = db
            .handle()
            .query("SELECT CAST(NULL AS TINYINT)")
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
}