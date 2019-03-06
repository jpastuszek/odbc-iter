use std::fmt;
use std::error::Error;
use crate::value::ValueRow;
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