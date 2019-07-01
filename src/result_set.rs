use error_context::prelude::*;
use odbc::{
    ColumnDescriptor, DiagnosticRecord, Prepared, Executed, ResultSetState
};
use log::{debug, log_enabled, trace};
use std::fmt;
use std::error::Error;
use std::convert::TryFrom;
use std::marker::PhantomData;

use crate::{OdbcError, Handle, PreparedStatement};
use crate::row::{DatumAccessError, ColumnType, DatumType, Row, UnsupportedSqlDataType};
use crate::value_row::TryFromValueRow;
use crate::value::Value;

/// Error crating ResultSet iterator.
#[derive(Debug)]
pub enum ResultSetError {
    OdbcError(DiagnosticRecord, &'static str),
    UnsupportedSqlDataType(UnsupportedSqlDataType),
}

impl fmt::Display for ResultSetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResultSetError::OdbcError(_, context) => {
                write!(f, "ODBC call failed while {}", context)
            }
            ResultSetError::UnsupportedSqlDataType(_) => {
                write!(f, "query schema has unsupported data type")
            }
        }
    }
}

impl Error for ResultSetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ResultSetError::OdbcError(err, _) => Some(err),
            ResultSetError::UnsupportedSqlDataType(err) => Some(err),
        }
    }
}

impl From<ErrorContext<DiagnosticRecord, &'static str>> for ResultSetError {
    fn from(err: ErrorContext<DiagnosticRecord, &'static str>) -> ResultSetError {
        ResultSetError::OdbcError(err.error, err.context)
    }
}

impl From<UnsupportedSqlDataType> for ResultSetError {
    fn from(err: UnsupportedSqlDataType) -> ResultSetError {
        ResultSetError::UnsupportedSqlDataType(err)
    }
}

/// Errors related to data access of query result set.
///
/// This error can happen when iterating rows of executed query result set.
/// For convenience this error can be converted into `QueryError`.
#[derive(Debug)]
pub enum DataAccessError {
    OdbcError(DiagnosticRecord, &'static str),
    DatumAccessError(DatumAccessError),
    FromRowError(Box<dyn Error>),
    UnexpectedNumberOfRows(&'static str),
}

impl fmt::Display for DataAccessError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataAccessError::OdbcError(_, context) => {
                write!(f, "ODBC call failed while {}", context)
            }
            DataAccessError::DatumAccessError(_) => {
                write!(f, "failed to access datum in ODBC cursor")
            }
            DataAccessError::FromRowError(_) => {
                write!(f, "failed to convert table row to target type")
            }
            DataAccessError::UnexpectedNumberOfRows(context) => write!(
                f,
                "unexpected number of rows returned by query: {}",
                context
            ),
        }
    }
}

impl Error for DataAccessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DataAccessError::OdbcError(err, _) => Some(err),
            DataAccessError::DatumAccessError(err) => Some(err),
            DataAccessError::FromRowError(err) => Some(err.as_ref()),
            DataAccessError::UnexpectedNumberOfRows(_) => None,
        }
    }
}

impl From<ErrorContext<DiagnosticRecord, &'static str>> for DataAccessError {
    fn from(err: ErrorContext<DiagnosticRecord, &'static str>) -> DataAccessError {
        DataAccessError::OdbcError(err.error, err.context)
    }
}

impl From<DatumAccessError> for DataAccessError {
    fn from(err: DatumAccessError) -> DataAccessError {
        DataAccessError::DatumAccessError(err)
    }
}

/// Iterator over result set rows.
///
/// Items of this iterator can be of any type that implements `TryFromValueRow` that includes common Rust types and tuples.
pub struct ResultSet<'h, 'c, V, S> {
    statement: Option<ExecutedStatement<'c, S>>,
    schema: Vec<ColumnType>,
    columns: i16,
    utf_16_strings: bool,
    phantom: PhantomData<&'h V>,
}

impl<'h, 'c, V, S> fmt::Debug for ResultSet<'h, 'c, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ResultSet")
            .field("schema", &self.schema)
            .field("columns", &self.columns)
            .field("utf_16_strings", &self.utf_16_strings)
            .finish()
    }
}

impl<'h, 'c, V, S> Drop for ResultSet<'h, 'c, V, S> {
    fn drop(&mut self) {
        // We need to make sure statement is dropped; implementing Drop forces use of drop(row_iter) if not consumed before another query
        // Should Statement not impl Drop itself?
        drop(self.statement.take())
    }
}

enum ExecutedStatement<'c, S> {
    HasResult(odbc::Statement<'c, 'c, S, odbc::HasResult>),
    NoResult(odbc::Statement<'c, 'c, S, odbc::NoResult>),
}

impl<'h, 'c: 'h, V, S> ResultSet<'h, 'c, V, S>
where
    V: TryFromValueRow,
{
    pub(crate) fn from_result(
        _handle: &'h Handle<'c>,
        result: ResultSetState<'c, '_, S>,
        utf_16_strings: bool,
    ) -> Result<ResultSet<'h, 'c, V, S>, ResultSetError> {
        let (odbc_schema, columns, statement) = match result {
            ResultSetState::Data(statement) => {
                let columns = statement
                    .num_result_cols()
                    .wrap_error_while("getting number of result columns")?;
                let odbc_schema = (1..columns + 1)
                    .map(|i| statement.describe_col(i as u16))
                    .collect::<Result<Vec<ColumnDescriptor>, _>>()
                    .wrap_error_while("getting column descriptiors")?;
                let statement = statement
                    .reset_parameters()
                    .wrap_error_while("reseting bound parameters on statement")?; // don't reference parameter data any more

                if log_enabled!(::log::Level::Debug) {
                    if odbc_schema.len() == 0 {
                        debug!("Got empty data set");
                    } else {
                        debug!(
                            "Got data with columns: {}",
                            odbc_schema
                                .iter()
                                .map(|cd| cd.name.clone())
                                .collect::<Vec<String>>()
                                .join(", ")
                        );
                    }
                }

                (
                    odbc_schema,
                    columns,
                    ExecutedStatement::HasResult(statement),
                )
            }
            ResultSetState::NoData(statement) => {
                debug!("No data");
                let statement = statement
                    .reset_parameters()
                    .wrap_error_while("reseting bound parameters on statement")?; // don't reference parameter data any more
                (Vec::new(), 0, ExecutedStatement::NoResult(statement))
            }
        };

        if log_enabled!(::log::Level::Trace) {
            for cd in &odbc_schema {
                trace!("ODBC query result schema: {} [{:?}] size: {:?} nullable: {:?} decimal_digits: {:?}", cd.name, cd.data_type, cd.column_size, cd.nullable, cd.decimal_digits);
            }
        }

        // convert schema here so that when iterating rows we can pass reference to it per row for row type conversion
        let schema = odbc_schema
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResultSet {
            statement: Some(statement),
            schema,
            columns,
            phantom: PhantomData,
            utf_16_strings,
        })
    }

    /// Information about column types.
    pub fn schema(&self) -> &[ColumnType] {
        self.schema.as_slice()
    }

    /// Get exactly one row from the result set.
    /// This function will fail if zero or more than one rows would be provided.
    pub fn single(mut self) -> Result<V, DataAccessError> {
        let value = self.next().ok_or(DataAccessError::UnexpectedNumberOfRows(
            "expected single row but got no rows",
        ))?;
        if self.next().is_some() {
            return Err(DataAccessError::UnexpectedNumberOfRows(
                "expected single row but got more rows",
            ));
        }
        value
    }

    /// Get first row from the result set.
    /// Any following rows are discarded.
    /// This function will fail no rows were provided.
    pub fn first(mut self) -> Result<V, DataAccessError> {
        self.next().ok_or(DataAccessError::UnexpectedNumberOfRows(
            "expected at least one row but got no rows",
        ))?
    }

    /// Assert that the query returned no rows.
    /// This function will fail if there was at least one row provided.
    /// This is useful when working with SQL statements that produce no rows like "INSERT".
    pub fn no_result(mut self) -> Result<(), DataAccessError> {
        if self.next().is_some() {
            return Err(DataAccessError::UnexpectedNumberOfRows(
                "exepcted no rows but got at least one",
            ));
        }
        Ok(())
    }
}

impl<'h, 'c: 'h, V> ResultSet<'h, 'c, V, Prepared>
where
    V: TryFromValueRow,
{
    /// Close the result set and discard any not consumed rows.
    pub fn close(mut self) -> Result<PreparedStatement<'c>, OdbcError> {
        match self.statement.take().unwrap() {
            ExecutedStatement::HasResult(statement) => Ok(PreparedStatement(
                statement
                    .close_cursor()
                    .wrap_error_while("closing cursor on executed prepared statement")?,
            )),
            ExecutedStatement::NoResult(statement) => Ok(PreparedStatement(statement)),
        }
    }

    /// When available provides information on number of rows affected by query (e.g. "DELETE" statement).
    pub fn affected_rows(&self) -> Result<Option<i64>, OdbcError> {
        match &self.statement.as_ref().unwrap() {
            ExecutedStatement::HasResult(statement) => {
                let rows = statement.affected_row_count().wrap_error_while(
                    "getting affected row count from prepared statemnt with result",
                )?;
                Ok(if rows >= 0 { Some(rows) } else { None })
            }
            ExecutedStatement::NoResult(_) => Ok(None),
        }
    }
}

impl<'h, 'c: 'h, V> ResultSet<'h, 'c, V, Executed>
where
    V: TryFromValueRow,
{
    /// Close the result set and discard any not consumed rows.
    pub fn close(mut self) -> Result<(), OdbcError> {
        if let ExecutedStatement::HasResult(statement) = self.statement.take().unwrap() {
            statement
                .close_cursor()
                .wrap_error_while("closing cursor on executed statement")?;
        }
        Ok(())
    }

    /// When available provides information on number of rows affected by query (e.g. "DELETE" statement).
    pub fn affected_rows(&self) -> Result<Option<i64>, OdbcError> {
        let rows = match &self.statement.as_ref().unwrap() {
            ExecutedStatement::HasResult(statement) => {
                statement.affected_row_count().wrap_error_while(
                    "getting affected row count from allocated statemnt with result",
                )?
            }
            ExecutedStatement::NoResult(statement) => {
                statement.affected_row_count().wrap_error_while(
                    "getting affected row count from allocated statemnt with no result",
                )?
            }
        };
        Ok(if rows >= 0 { Some(rows) } else { None })
    }
}

impl<'h, 'c: 'h, V, S> Iterator for ResultSet<'h, 'c, V, S>
where
    V: TryFromValueRow,
{
    type Item = Result<V, DataAccessError>;

    fn next(&mut self) -> Option<Self::Item> {
        let statement = match self.statement.as_mut().unwrap() {
            ExecutedStatement::HasResult(statement) => statement,
            ExecutedStatement::NoResult(_) => return None,
        };

        // Invalid cursor
        if self.columns == 0 {
            return None;
        }

        let utf_16_strings = self.utf_16_strings;
        let schema = &self.schema;

        statement.fetch().wrap_error_while("fetching row").transpose().map(|cursor| {
            let mut row = Row::new(cursor?, schema, utf_16_strings);

            let mut value_row = Vec::with_capacity(row.columns() as usize);

            loop {
                if let Some(column) = row.shift_column() {
                    let value = match column.column_type().datum_type {
                        DatumType::Bit => column.as_bool()?.map(Value::from),
                        DatumType::Tinyint => column.as_i8()?.map(Value::from),
                        DatumType::Smallint => column.as_i16()?.map(Value::from),
                        DatumType::Integer => column.as_i32()?.map(Value::from),
                        DatumType::Bigint => column.as_i64()?.map(Value::from),
                        DatumType::Float => column.as_f32()?.map(Value::from),
                        DatumType::Double => column.as_f64()?.map(Value::from),
                        DatumType::String => column.as_string()?.map(Value::from),
                        DatumType::Timestamp => column.as_timestamp()?.map(Value::from),
                        DatumType::Date => column.as_date()?.map(Value::from),
                        DatumType::Time => column.as_time()?.map(Value::from),
                        #[cfg(feature = "serde_json")]
                        DatumType::Json => column.as_json()?.map(Value::from),
                    };
                    value_row.push(value)
                } else {
                    return Ok(value_row);
                }
            }
        })
        .map(|v| v.and_then(|v| {
            // Verify that value types match schema
            debug_assert!(v.iter().map(|v| v.as_ref().map(|v| v.datum_type())).zip(self.schema()).all(|(v, s)| if let Some(v) = v { v == s.datum_type } else { true }));
            TryFromValueRow::try_from_row(v, self.schema()).map_err(|err| DataAccessError::FromRowError(Box::new(err)))
        }))
    }
}
