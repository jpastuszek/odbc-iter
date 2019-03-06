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
pub enum TryFromRowError {
    UnexpectedNumberOfColumns { 
        have: u16,
        expected: u16,
    },
    UnexpectedNullValue,
}

impl fmt::Display for TryFromRowError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TryFromRowError::UnexpectedNumberOfColumns { have, expected } => write!(f, "unexpected number of columns: have {}, expected {}", have, expected),
            TryFromRowError::UnexpectedNullValue => write!(f, "expecting value but got NULL"),
        }
    }
}

impl Error for TryFromRowError {}

impl TryFromRow for Option<Value> {
    type Schema = Schema;
    type Error = TryFromRowError;
    fn try_from_row(mut values: ValueRow, _schema: &Self::Schema) -> Result<Self, Self::Error> {
        if values.len() != 1 {
            return Err(TryFromRowError::UnexpectedNumberOfColumns { have: values.len() as u16, expected: 1 })
        }
        Ok(values.pop().unwrap())
    }
}

impl TryFromRow for Value {
    type Schema = Schema;
    type Error = TryFromRowError;
    fn try_from_row(mut values: ValueRow, _schema: &Self::Schema) -> Result<Self, Self::Error> {
        if values.len() != 1 {
            return Err(TryFromRowError::UnexpectedNumberOfColumns { have: values.len() as u16, expected: 1 })
        }
        values.pop().unwrap().ok_or_else(|| TryFromRowError::UnexpectedNullValue)
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
}