use error_context::prelude::*;
use log::{debug, log_enabled, trace};
use odbc::{ColumnDescriptor, DiagnosticRecord, Executed, Prepared, ResultSetState};
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;

use crate::query::{Handle, PreparedStatement};
use crate::row::{Settings, Configuration, ColumnType, DatumAccessError, Row, TryFromRow, UnsupportedSqlDataType};
use crate::OdbcError;
use crate::stats::QueryFetchingGuard;

/// Error crating ResultSet iterator.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
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
#[allow(clippy::large_enum_variant)]
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
/// Items of this iterator can be of any type that implements `TryFromRow` that includes common Rust types and tuples.
pub struct ResultSet<'h, 'c, V, S, C: Configuration> {
    statement: Option<ExecutedStatement<'c, S>>,
    schema: Vec<ColumnType>,
    columns: i16,
    settings: &'c Settings,
    configuration: C,
    phantom: PhantomData<&'h V>,
    _stats_guard: QueryFetchingGuard,
}

impl<'h, 'c, V, S, C: Configuration> fmt::Debug for ResultSet<'h, 'c, V, S, C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ResultSet")
            .field("schema", &self.schema)
            .field("columns", &self.columns)
            .field("settings", &self.settings)
            .field("configuration", &self.configuration)
            .finish()
    }
}

impl<'h, 'c, V, S, C: Configuration> Drop for ResultSet<'h, 'c, V, S, C> {
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

impl<'h, 'c: 'h, V, S, C: Configuration> ResultSet<'h, 'c, V, S, C>
where
    V: TryFromRow<C>,
{
    pub(crate) fn from_result(
        _handle: &'h Handle<'c, C>,
        result: ResultSetState<'c, '_, S>,
        stats_guard: QueryFetchingGuard,
        settings: &'c Settings,
        configuration: C,
    ) -> Result<ResultSet<'h, 'c, V, S, C>, ResultSetError> {
        let (odbc_schema, columns, statement) = match result {
            ResultSetState::Data(statement) => {
                let columns = statement
                    .num_result_cols()
                    .wrap_error_while("getting number of result columns")?;
                let odbc_schema = (1..=columns)
                    .map(|i| statement.describe_col(i as u16))
                    .collect::<Result<Vec<ColumnDescriptor>, _>>()
                    .wrap_error_while("getting column descriptiors")?;
                let statement = statement
                    .reset_parameters()
                    .wrap_error_while("reseting bound parameters on statement")?; // don't reference parameter data any more

                if log_enabled!(::log::Level::Debug) {
                    if odbc_schema.is_empty() {
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
            settings,
            configuration,
            _stats_guard: stats_guard,
        })
    }

    /// Information about column types.
    pub fn schema(&self) -> &[ColumnType] {
        self.schema.as_slice()
    }

    /// Get associated data access configuration object.
    pub fn configuration(&self) -> &C {
        &self.configuration
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

impl<'h, 'c: 'h, V, C: Configuration> ResultSet<'h, 'c, V, Prepared, C>
where
    V: TryFromRow<C>,
{
    /// Close the result set and discard any not consumed rows.
    pub fn close(mut self) -> Result<PreparedStatement<'c>, OdbcError> {
        match self.statement.take().unwrap() {
            ExecutedStatement::HasResult(statement) => Ok(PreparedStatement::from_statement(
                statement
                    .close_cursor()
                    .wrap_error_while("closing cursor on executed prepared statement")?,
            )),
            ExecutedStatement::NoResult(statement) => {
                Ok(PreparedStatement::from_statement(statement))
            }
        }
    }

    /// When available provides information on number of rows affected by query (e.g. "DELETE" statement).
    pub fn affected_rows(&self) -> Result<Option<i64>, OdbcError> {
        match &self.statement.as_ref().unwrap() {
            ExecutedStatement::HasResult(statement) => {
                let rows = statement.affected_row_count().wrap_error_while(
                    "getting affected row count from prepared statemnt with result",
                )?;
                Ok(if rows >= 0 { Some(rows as i64) } else { None })
            }
            ExecutedStatement::NoResult(_) => Ok(None),
        }
    }
}

impl<'h, 'c: 'h, V, C: Configuration> ResultSet<'h, 'c, V, Executed, C>
where
    V: TryFromRow<C>,
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
        Ok(if rows >= 0 { Some(rows as i64) } else { None })
    }
}

impl<'h, 'c: 'h, V, S, C: Configuration> Iterator for ResultSet<'h, 'c, V, S, C>
where
    V: TryFromRow<C>,
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

        let settings = self.settings;
        let configuration = &self.configuration;
        let schema = &self.schema;

        statement
            .fetch()
            .wrap_error_while("fetching row")
            .transpose()
            .map(|cursor| {
                let row = Row::new(cursor?, schema, settings, configuration);
                TryFromRow::try_from_row(row)
                    .map_err(|err| DataAccessError::FromRowError(Box::new(err)))
            })
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use crate::{Odbc, TryFromRow, Configuration, ValueRow, ColumnType, Value, DatumAccessError, Row};
    #[allow(unused_imports)]
    use assert_matches::assert_matches;

    #[derive(Debug)]
    struct Foo {
        val: i64,
    }

    impl<C: Configuration> TryFromRow<C> for Foo {
        type Error = DatumAccessError;
        fn try_from_row<'r, 's, 'c, S>(mut row: Row<'r, 's, 'c, S, C>) -> Result<Self, Self::Error> {
            Ok(Foo {
                val: row.shift_column().expect("column").into_i64()?.expect("value")
            })
        }
    }

    #[test]
    #[cfg(feature = "test-monetdb")]
    fn test_custom_type() {
        let mut db = crate::tests::connect_monetdb();

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
        let mut db = crate::tests::connect_monetdb();

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
        let mut db = crate::tests::connect_monetdb();

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
        let mut db = crate::tests::connect_monetdb();

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
        let mut db = crate::tests::connect_monetdb();

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
        let mut db = crate::tests::connect_monetdb();

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
        let mut db = crate::tests::connect_monetdb();

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
        let mut db = crate::tests::connect_monetdb();

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
    #[cfg(feature = "chrono")]
    #[cfg(feature = "test-monetdb")]
    fn test_single_date() {
        use chrono::Datelike;
        use chrono::NaiveDate;

        let mut db = crate::tests::connect_monetdb();

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
        let mut db = crate::tests::connect_monetdb();

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
    #[cfg(feature = "test-monetdb")]
    #[cfg(feature = "serde_json")]
    fn test_single_json() {
        let mut db = crate::tests::connect_monetdb();

        let value: serde_json::Value = db
            .handle()
            .query(r#"SELECT CAST('{ "foo": 42 }' AS JSON)"#)
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.pointer("/foo").unwrap().as_i64().unwrap(), 42);

        let value: Option<serde_json::Value> = db
            .handle()
            .query(r#"SELECT CAST('{ "foo": 42 }' AS JSON)"#)
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.unwrap().pointer("/foo").unwrap().as_i64().unwrap(), 42);

        let value: Option<serde_json::Value> = db
            .handle()
            .query("SELECT CAST(NULL AS JSON)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_none());
    }

    #[test]
    #[cfg(feature = "test-monetdb")]
    #[cfg(feature = "serde_json")]
    fn test_single_json_as_string() {
        let mut db = crate::tests::connect_monetdb();

        let value: String = db
            .handle()
            .query(r#"SELECT CAST('{ "foo": 42 }' AS JSON)"#)
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(&value, r#"{ "foo": 42 }"#);

        let value: Option<String> = db
            .handle()
            .query(r#"SELECT CAST('{ "foo": 42 }' AS JSON)"#)
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(&value.unwrap(), r#"{ "foo": 42 }"#);

        let value: Option<String> = db
            .handle()
            .query("SELECT CAST(NULL AS JSON)")
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert!(value.is_none());
    }
}
