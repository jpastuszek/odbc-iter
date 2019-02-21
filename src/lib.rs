use error_context::prelude::*;
use lazy_static::lazy_static;
use log::{debug, log_enabled, trace};
use odbc::{
    Allocated, Connection, DiagnosticRecord, DriverInfo, Environment, Executed, NoResult, OdbcType, Prepared,
    ResultSetState, SqlDate, SqlSsTime2, SqlTime, SqlTimestamp, Statement, Version3,
};
use regex::Regex;
use std::cell::{Ref, RefCell};
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::string::FromUtf16Error;

// Schema
pub use odbc::ffi::SqlDataType;
pub use odbc::ColumnDescriptor;

pub mod schema_access;
pub mod value;
pub use value::{Value, ValueRow};

/// TODO
/// * query/execute should take &mut ref so no other query can be run before RowIter is closed
/// * Looks like tests needs some global lock as I get spurious connection error/SEGV on SQL Server tests
/// * Prepared statement cache:
/// ** db.with_statement_cache() -> StatementCache
/// ** sc.query(str) - direct query
/// ** sc.query_prepared(impl ToString + Hash) - hash fist and look up in cache if found execute; .to_string otherwise and prepare + execute;
///    this is to avoid building query strings where we know hash e.g. from some other value than query string itself
/// ** sc.clear() - try close the statement and clear the cache
/// * Replace unit errors with never type when stable

// https://github.com/rust-lang/rust/issues/49431
pub trait Captures<'a> {}
impl<'a, T: ?Sized> Captures<'a> for T {}

pub trait Captures2<'a> {}
impl<'a, T: ?Sized> Captures2<'a> for T {}

/// General ODBC initialization and connection errors
#[derive(Debug)]
pub enum OdbcError {
    OdbcError(Option<DiagnosticRecord>, &'static str),
    NotConnectedError,
}

impl fmt::Display for OdbcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OdbcError::OdbcError(_, context) => write!(f, "ODBC call failed while {}", context),
            OdbcError::NotConnectedError => write!(f, "not connected to database"),
        }
    }
}

fn to_dyn(diag: &Option<DiagnosticRecord>) -> Option<&(dyn Error + 'static)> {
    diag.as_ref().map(|e| e as &(dyn Error + 'static))
}

impl Error for OdbcError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            OdbcError::OdbcError(diag, _) => to_dyn(diag),
            OdbcError::NotConnectedError => None,
        }
    }
}

impl From<ErrorContext<Option<DiagnosticRecord>, &'static str>> for OdbcError {
    fn from(err: ErrorContext<Option<DiagnosticRecord>, &'static str>) -> OdbcError {
        OdbcError::OdbcError(err.error, err.context)
    }
}

impl From<ErrorContext<DiagnosticRecord, &'static str>> for OdbcError {
    fn from(err: ErrorContext<DiagnosticRecord, &'static str>) -> OdbcError {
        OdbcError::OdbcError(Some(err.error), err.context)
    }
}

/// Errors related to query execution
#[derive(Debug)]
pub enum QueryError<R, S> {
    OdbcError(DiagnosticRecord, &'static str),
    BindError(DiagnosticRecord),
    FromSchemaError(S),
    MultipleQueriesError(SplitQueriesError),
    DataAccessError(DataAccessError<R>, &'static str),
}

impl<R, S> fmt::Display for QueryError<R, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryError::OdbcError(_, context) => write!(f, "ODBC call failed while {}", context),
            QueryError::BindError(_) => {
                write!(f, "ODBC call failed while binding parameter to statement")
            }
            QueryError::FromSchemaError(_) => {
                write!(f, "failed to convert table schema to target type")
            }
            QueryError::MultipleQueriesError(_) => write!(f, "failed to execute multiple queries"),
            QueryError::DataAccessError(_, context) => {
                write!(f, "failed to access result data while {}", context)
            }
        }
    }
}

impl<R, S> Error for QueryError<R, S>
where
    R: Error + 'static,
    S: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            QueryError::OdbcError(err, _) => Some(err),
            QueryError::BindError(err) => Some(err),
            QueryError::FromSchemaError(err) => Some(err),
            QueryError::MultipleQueriesError(err) => Some(err),
            QueryError::DataAccessError(err, _) => Some(err),
        }
    }
}

impl<R, S> From<ErrorContext<DiagnosticRecord, &'static str>> for QueryError<R, S> {
    fn from(err: ErrorContext<DiagnosticRecord, &'static str>) -> QueryError<R, S> {
        QueryError::OdbcError(err.error, err.context)
    }
}

impl<R, S> From<BindError> for QueryError<R, S> {
    fn from(err: BindError) -> QueryError<R, S> {
        QueryError::BindError(err.0)
    }
}

impl<R, S> From<SplitQueriesError> for QueryError<R, S> {
    fn from(err: SplitQueriesError) -> QueryError<R, S> {
        QueryError::MultipleQueriesError(err)
    }
}

impl<R, S> From<ErrorContext<DataAccessError<R>, &'static str>> for QueryError<R, S> {
    fn from(err: ErrorContext<DataAccessError<R>, &'static str>) -> QueryError<R, S> {
        QueryError::DataAccessError(err.error, err.context)
    }
}

/// Errors related to data access to query results
#[derive(Debug)]
pub enum DataAccessError<R> {
    OdbcError(DiagnosticRecord, &'static str),
    OdbcCursorError(DiagnosticRecord),
    FromRowError(R),
    FromUtf16Error(FromUtf16Error, &'static str),
}

impl<R> fmt::Display for DataAccessError<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataAccessError::OdbcError(_, context) => {
                write!(f, "ODBC call failed while {}", context)
            }
            DataAccessError::OdbcCursorError(_) => {
                write!(f, "failed to access data in ODBC cursor")
            }
            DataAccessError::FromRowError(_) => {
                write!(f, "failed to convert table row to target type")
            }
            DataAccessError::FromUtf16Error(_, context) => write!(
                f,
                "failed to create String from UTF-16 column data while {}",
                context
            ),
        }
    }
}

impl<R> Error for DataAccessError<R>
where
    R: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DataAccessError::OdbcError(err, _) => Some(err),
            DataAccessError::OdbcCursorError(err) => Some(err),
            DataAccessError::FromRowError(err) => Some(err),
            DataAccessError::FromUtf16Error(err, _) => Some(err),
        }
    }
}

// Note that we don't give context for cursor data access at this time
// TODO: better way to distinguish between general ODBC errors and cursor errors
impl<R> From<DiagnosticRecord> for DataAccessError<R> {
    fn from(err: DiagnosticRecord) -> DataAccessError<R> {
        DataAccessError::OdbcCursorError(err)
    }
}

impl<R> From<ErrorContext<DiagnosticRecord, &'static str>> for DataAccessError<R> {
    fn from(err: ErrorContext<DiagnosticRecord, &'static str>) -> DataAccessError<R> {
        DataAccessError::OdbcError(err.error, err.context)
    }
}

impl<R> From<ErrorContext<FromUtf16Error, &'static str>> for DataAccessError<R> {
    fn from(err: ErrorContext<FromUtf16Error, &'static str>) -> DataAccessError<R> {
        DataAccessError::FromUtf16Error(err.error, err.context)
    }
}

// Avoid leaking DiagnosticRecord as required public interface
/// Error returned by bind closure
#[derive(Debug)]
pub struct BindError(DiagnosticRecord);

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ODBC call failed while while binding parameter")
    }
}

impl Error for BindError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

impl From<DiagnosticRecord> for BindError {
    fn from(err: DiagnosticRecord) -> BindError {
        BindError(err)
    }
}

pub type EnvironmentV3 = Environment<Version3>;
pub type Schema = Vec<ColumnDescriptor>;

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

/// Iterate rows converting them to given value type
pub struct RowIter<'odbc, V, S>
where
    V: TryFromRow,
{
    statement: ExecutedStatement<'odbc, S>,
    odbc_schema: Vec<ColumnDescriptor>,
    schema: V::Schema,
    columns: i16,
    phantom: PhantomData<V>,
    utf_16_strings: bool,
}

enum ExecutedStatement<'odbc, S> {
    HasResult(odbc::Statement<'odbc, 'odbc, S, odbc::HasResult>),
    NoResult(odbc::Statement<'odbc, 'odbc, S, odbc::NoResult>),
}

impl<'odbc, V, S> RowIter<'odbc, V, S>
where
    V: TryFromRow,
{
    fn from_result<'t>(
        result: ResultSetState<'odbc, 't, S>,
        utf_16_strings: bool,
    ) -> Result<
        RowIter<'odbc, V, S>,
        QueryError<V::Error, <<V as TryFromRow>::Schema as TryFromSchema>::Error>,
    > {
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
        let schema =
            V::Schema::try_from_schema(&odbc_schema).map_err(QueryError::FromSchemaError)?;

        Ok(RowIter {
            statement,
            odbc_schema,
            schema,
            columns,
            phantom: PhantomData,
            utf_16_strings,
        })
    }

    pub fn schema(&self) -> &V::Schema {
        &self.schema
    }

    pub fn columns(&self) -> i16 {
        self.columns
    }
}

impl<'odbc, V> RowIter<'odbc, V, Prepared>
where
    V: TryFromRow,
{
    pub fn close(self) -> Result<PreparedStatement<'odbc>, OdbcError> {
        match self.statement {
            ExecutedStatement::HasResult(statement) => Ok(PreparedStatement(
                statement
                    .close_cursor()
                    .wrap_error_while("closing cursor on executed prepared statement")?,
            )),
            ExecutedStatement::NoResult(statement) => Ok(PreparedStatement(statement)),
        }
    }

    pub fn affected_rows(&self) -> Result<Option<i64>, OdbcError> {
        match &self.statement {
            ExecutedStatement::HasResult(statement) => {
                let rows = statement.affected_row_count().wrap_error_while(
                    "getting affected row count from prepared statemnt with result",
                )?;
                Ok(if rows >= 0 {
                    Some(rows)
                } else {
                    None
                })
            }
            ExecutedStatement::NoResult(_) => Ok(None),
        }
    }
}

impl<'odbc, V> RowIter<'odbc, V, Executed>
where
    V: TryFromRow,
{
    pub fn close(self) -> Result<(), OdbcError> {
        if let ExecutedStatement::HasResult(statement) = self.statement {
            statement
                .close_cursor()
                .wrap_error_while("closing cursor on executed statement")?;
        }
        Ok(())
    }

    pub fn affected_rows(&self) -> Result<Option<i64>, OdbcError> {
        let rows = match &self.statement {
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
        Ok(if rows >= 0 {
            Some(rows)
        } else {
            None
        })
    }
}

impl<'odbc, V, S> Iterator for RowIter<'odbc, V, S>
where
    V: TryFromRow,
{
    type Item = Result<V, DataAccessError<V::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        use odbc::ffi::SqlDataType::*;

        fn cursor_get_data<'i, S, T: odbc::OdbcType<'i>>(
            cursor: &'i mut odbc::Cursor<S>,
            index: u16,
        ) -> Result<Option<T>, DiagnosticRecord> {
            cursor.get_data::<T>(index + 1)
        }

        fn cursor_get_value<'i, S, T: odbc::OdbcType<'i> + Into<Value>>(
            cursor: &'i mut odbc::Cursor<S>,
            index: u16,
        ) -> Result<Option<Value>, DiagnosticRecord> {
            cursor_get_data::<S, T>(cursor, index).map(|value| value.map(Into::into))
        }

        let statement = match &mut self.statement {
            ExecutedStatement::HasResult(statement) => statement,
            ExecutedStatement::NoResult(_) => return None,
        };

        // Invalid cursor
        if self.columns == 0 {
            return None;
        }

        let utf_16_strings = self.utf_16_strings;

        // TODO: transpose?
        match statement.fetch().wrap_error_while("fetching row") {
            Err(err) => Some(Err(err.into())),
            Ok(Some(mut cursor)) => {
                Some(
                    self.odbc_schema
                        .iter()
                        .enumerate()
                        .map(|(index, column_descriptor)| {
                            trace!("Parsing column {}: {:?}", index, column_descriptor);
                            // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-2017
                            Ok(match column_descriptor.data_type {
                                SQL_EXT_TINYINT => {
                                    cursor_get_value::<S, i8>(&mut cursor, index as u16)?
                                }
                                SQL_SMALLINT => cursor_get_value::<S, i16>(&mut cursor, index as u16)?,
                                SQL_INTEGER => cursor_get_value::<S, i32>(&mut cursor, index as u16)?,
                                SQL_EXT_BIGINT => {
                                    cursor_get_value::<S, i64>(&mut cursor, index as u16)?
                                }
                                SQL_FLOAT => cursor_get_value::<S, f32>(&mut cursor, index as u16)?,
                                SQL_REAL => cursor_get_value::<S, f32>(&mut cursor, index as u16)?,
                                SQL_DOUBLE => cursor_get_value::<S, f64>(&mut cursor, index as u16)?,
                                SQL_CHAR | SQL_VARCHAR | SQL_EXT_LONGVARCHAR => {
                                    cursor_get_value::<S, String>(&mut cursor, index as u16)?
                                }
                                SQL_EXT_WCHAR | SQL_EXT_WVARCHAR | SQL_EXT_WLONGVARCHAR => {
                                    if utf_16_strings {
                                        if let Some(bytes) = cursor_get_data::<S, &[u16]>(&mut cursor, index as u16)? {
                                            Some(Value::String(String::from_utf16(bytes)
                                                .wrap_error_while("getting UTF-16 string (SQL_EXT_WCHAR | SQL_EXT_WVARCHAR | SQL_EXT_WLONGVARCHAR)")?))
                                        } else {
                                            None
                                        }
                                    } else {
                                        cursor_get_value::<S, String>(&mut cursor, index as u16)?
                                    }
                                }
                                SQL_TIMESTAMP => {
                                    if let Some(timestamp) = cursor_get_data::<S, SqlTimestamp>(&mut cursor, index as u16)? {
                                        trace!("{:?}", timestamp);
                                        Some(Value::String(format!(
                                            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
                                            timestamp.year,
                                            timestamp.month,
                                            timestamp.day,
                                            timestamp.hour,
                                            timestamp.minute,
                                            timestamp.second,
                                            timestamp.fraction / 1_000_000
                                        )))
                                    } else {
                                        None
                                    }
                                }
                                SQL_DATE => {
                                    cursor_get_value::<S, SqlDate>(&mut cursor, index as u16)?
                                },
                                SQL_TIME => {
                                    cursor_get_value::<S, SqlTime>(&mut cursor, index as u16)?
                                }
                                SQL_SS_TIME2 => {
                                    cursor_get_value::<S, SqlSsTime2>(&mut cursor, index as u16)?
                                }
                                SQL_EXT_BIT => {
                                    if let Some(byte) = cursor_get_data::<S, u8>(&mut cursor, index as u16)? {
                                        Some(Value::Bit(if byte == 0 { false } else { true }))
                                    } else {
                                        None
                                    }
                                }
                                _ => panic!(format!(
                                    "got unimplemented SQL data type: {:?}",
                                    column_descriptor.data_type
                                )),
                            })
                        })
                        .collect::<Result<Vec<Option<Value>>, _>>(),
                )
            }
            Ok(None) => None,
        }
        .map(|v| v.and_then(|v| TryFromRow::try_from_row(v, &self.schema).map_err(DataAccessError::FromRowError)))
    }
}

pub struct Binder<'odbc, 't, S> {
    statement: Statement<'odbc, 't, S, NoResult>,
    index: u16,
}

impl<'odbc, 't, S> Binder<'odbc, 't, S> {
    pub fn bind<'new_t, T>(self, value: &'new_t T) -> Result<Binder<'odbc, 'new_t, S>, BindError>
    where
        T: OdbcType<'new_t> + Debug,
        't: 'new_t,
    {
        let index = self.index + 1;
        if log_enabled!(::log::Level::Trace) {
            trace!("Parameter {}: {:?}", index, value);
        }
        let statement = self.statement.bind_parameter(index, value)?;

        Ok(Binder { statement, index })
    }

    fn into_inner(self) -> Statement<'odbc, 't, S, NoResult> {
        self.statement
    }
}

impl<'odbc, 't, S> From<Statement<'odbc, 'odbc, S, NoResult>> for Binder<'odbc, 'odbc, S> {
    fn from(statement: Statement<'odbc, 'odbc, S, NoResult>) -> Binder<'odbc, 'odbc, S> {
        Binder {
            statement,
            index: 0,
        }
    }
}

pub struct Odbc<'env> {
    connection: Connection<'env>,
    utf_16_strings: bool,
}

pub struct Options {
    utf_16_strings: bool,
}

/// Wrapper around ODBC prepared statement
pub struct PreparedStatement<'odbc>(Statement<'odbc, 'odbc, odbc::Prepared, odbc::NoResult>);

impl<'odbc> PreparedStatement<'odbc> {
    pub fn columns(&self) -> Result<i16, OdbcError> {
        Ok(self
            .0
            .num_result_cols()
            .wrap_error_while("getting number of columns in prepared statement")?)
    }

    pub fn schema(&self) -> Result<Schema, OdbcError> {
        Ok((1..self.columns()? + 1)
            .map(|i| self.0.describe_col(i as u16))
            .collect::<Result<Vec<ColumnDescriptor>, _>>()
            .wrap_error_while("getting column description")?)
    }
}

impl<'env> Odbc<'env> {
    pub fn env() -> Result<EnvironmentV3, OdbcError> {
        odbc::create_environment_v3()
            .wrap_error_while("creating v3 environment")
            .map_err(Into::into)
    }

    pub fn list_drivers(odbc: &mut EnvironmentV3) -> Result<Vec<DriverInfo>, OdbcError> {
        odbc.drivers()
            .wrap_error_while("listing drivers")
            .map_err(Into::into)
    }

    pub fn connect(
        env: &'env EnvironmentV3,
        connection_string: &str,
    ) -> Result<Odbc<'env>, OdbcError> {
        Self::connect_with_options(
            env,
            connection_string,
            Options {
                utf_16_strings: false,
            },
        )
    }

    pub fn connect_with_options(
        env: &'env EnvironmentV3,
        connection_string: &str,
        options: Options,
    ) -> Result<Odbc<'env>, OdbcError> {
        let connection = env
            .connect_with_connection_string(connection_string)
            .wrap_error_while("connecting to database")?;
        Ok(Odbc {
            connection,
            utf_16_strings: options.utf_16_strings,
        })
    }

    pub fn tables<V>(
        &self,
        catalog: &str,
        schema: Option<&str>,
        table: Option<&str>,
        table_type: Option<&str>,
    ) -> Result<
        RowIter<V, Executed>,
        QueryError<V::Error, <<V as TryFromRow>::Schema as TryFromSchema>::Error>,
    >
    where
        V: TryFromRow,
    {
        debug!("Getting ODBC tables");

        let statement = Statement::with_parent(&self.connection)
            .wrap_error_while("pairing statement with connection")?;
        
        RowIter::from_result(
            ResultSetState::Data(statement
                // .tables_str(catalog_name, "schema", "table", "type")
                .tables_str(catalog, schema.unwrap_or(""), table.unwrap_or(""), table_type.unwrap_or(""))
                .wrap_error_while("executing direct statement")?),
            self.utf_16_strings,
        )
    }

    pub fn prepare<'odbc>(&'odbc self, query: &str) -> Result<PreparedStatement<'odbc>, OdbcError> {
        debug!("Preparing ODBC query: {}", &query);

        let statement = Statement::with_parent(&self.connection)
            .wrap_error_while("pairing statement with connection")?
            .prepare(query)
            .wrap_error_while("preparing query")?;

        Ok(PreparedStatement(statement))
    }

    pub fn query<V>(
        &self,
        query: &str,
    ) -> Result<
        RowIter<V, Executed>,
        QueryError<V::Error, <<V as TryFromRow>::Schema as TryFromSchema>::Error>,
    >
    where
        V: TryFromRow,
    {
        self.query_with_parameters(query, |b| Ok(b))
    }

    pub fn query_with_parameters<'t, 'odbc: 't, V, F>(
        &'odbc self,
        query: &str,
        bind: F,
    ) -> Result<
        RowIter<V, Executed>,
        QueryError<V::Error, <<V as TryFromRow>::Schema as TryFromSchema>::Error>,
    >
    where
        V: TryFromRow,
        F: FnOnce(
            Binder<'odbc, 'odbc, Allocated>,
        ) -> Result<Binder<'odbc, 't, Allocated>, BindError>,
    {
        debug!("Direct ODBC query: {}", &query);

        let statement = Statement::with_parent(&self.connection)
            .wrap_error_while("pairing statement with connection")?;

        let statement: Statement<'odbc, 't, Allocated, NoResult> =
            bind(statement.into())?.into_inner();

        RowIter::from_result(
            statement
                .exec_direct(query)
                .wrap_error_while("executing direct statement")?,
            self.utf_16_strings,
        )
    }

    pub fn execute<'odbc, V>(
        &'odbc self,
        statement: PreparedStatement<'odbc>,
    ) -> Result<
        RowIter<'odbc, V, Prepared>,
        QueryError<V::Error, <<V as TryFromRow>::Schema as TryFromSchema>::Error>,
    >
    where
        V: TryFromRow,
    {
        self.execute_with_parameters(statement, |b| Ok(b))
    }

    pub fn execute_with_parameters<'t, 'odbc: 't, V, F>(
        &'odbc self,
        statement: PreparedStatement<'odbc>,
        bind: F,
    ) -> Result<
        RowIter<V, Prepared>,
        QueryError<V::Error, <<V as TryFromRow>::Schema as TryFromSchema>::Error>,
    >
    where
        V: TryFromRow,
        F: FnOnce(Binder<'odbc, 'odbc, Prepared>) -> Result<Binder<'odbc, 't, Prepared>, BindError>,
    {
        let statement: Statement<'odbc, 't, Prepared, NoResult> =
            bind(statement.0.into())?.into_inner();

        RowIter::from_result(
            statement
                .execute()
                .wrap_error_while("executing statement")?,
            self.utf_16_strings,
        )
    }

    pub fn query_multiple<'odbc, 'q, 't, V>(
        &'odbc self,
        queries: &'q str,
    ) -> impl Iterator<
        Item = Result<
            RowIter<V, Executed>,
            QueryError<V::Error, <<V as TryFromRow>::Schema as TryFromSchema>::Error>,
        >,
    > + Captures<'t>
                 + Captures<'env>
    where
        'env: 'odbc,
        'env: 't,
        'odbc: 't,
        'q: 't,
        V: TryFromRow,
    {
        split_queries(queries).map(move |query| {
            query
                .map_err(Into::into)
                .and_then(|query| self.query(query))
        })
    }
}

#[derive(Debug)]
pub struct SplitQueriesError;

impl fmt::Display for SplitQueriesError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to split queries")
    }
}

impl Error for SplitQueriesError {}

pub fn split_queries(queries: &str) -> impl Iterator<Item = Result<&str, SplitQueriesError>> {
    lazy_static! {
        // https://regex101.com/r/6YTuVG/4
        static ref RE: Regex = Regex::new(r#"(?:[\t \n]|--.*\n|!.*\n)*((?:[^;"']+(?:'(?:[^'\\]*(?:\\.)?)*')?(?:"(?:[^"\\]*(?:\\.)?)*")?)*;) *"#).unwrap();
    }
    RE.captures_iter(queries)
        .map(|c| c.get(1).ok_or(SplitQueriesError))
        .map(|r| r.map(|m| m.as_str()))
}

// Note: odbc-sys stuff is not Sent and therefore we need to create objects per thread
thread_local! {
    // Leaking ODBC handle per thread should be OK...ish assuming a thread pool is used?
    static ODBC: &'static EnvironmentV3 = Box::leak(Box::new(Odbc::env().expect("Failed to initialize ODBC")));
    static DB: RefCell<Result<Odbc<'static>, OdbcError>> = RefCell::new(Err(OdbcError::NotConnectedError));
}

/// Access to thread local connection
/// Connection will be established only once if successful or any time this function is called again after it failed to connect previously
pub fn thread_local_connection_with<O>(
    connection_string: &str,
    f: impl Fn(Ref<Result<Odbc<'static>, OdbcError>>) -> O,
) -> O {
    DB.with(|db| {
        {
            let mut db = db.borrow_mut();
            if db.is_err() {
                let id = std::thread::current().id();
                debug!("[{:?}] Connecting to database: {}", id, &connection_string);

                *db = ODBC.with(|odbc| Odbc::connect(odbc, &connection_string));
            }
        };

        f(db.borrow())
    })
}

#[cfg(test)]
mod query {
    use super::*;
    #[allow(unused_imports)]
    use assert_matches::assert_matches;

    // 600 chars
    #[cfg(any(
        feature = "test-sql-server",
        feature = "test-hive",
        feature = "test-monetdb"
    ))]
    const LONG_STRING: &'static str = "Lórem ipsum dołor sit amet, cońsectetur adipiścing elit. Fusce risus ipsum, ultricies ac odio ut, vestibulum hendrerit leo. Nunc cursus dapibus mattis. Donec quis est arcu. Sed a tortor sit amet erat euismod pulvinar. Etiam eu erat eget turpis semper finibus. Etiam lobortis egestas diam a consequat. Morbi iaculis lorem sed erat iaculis vehicula. Praesent at porttitor eros. Quisque tincidunt congue ornare. Donec sed nulla a ex sollicitudin lacinia. Fusce ut fermentum tellus, id pretium libero. Donec dapibus faucibus sapien at semper. In id felis sollicitudin, luctus doloź sit amet orci aliquam.";

    #[cfg(feature = "test-sql-server")]
    pub fn sql_server_connection_string() -> String {
        std::env::var("SQL_SERVER_ODBC_CONNECTION").expect("SQL_SERVER_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-hive")]
    pub fn hive_connection_string() -> String {
        std::env::var("HIVE_ODBC_CONNECTION").expect("HIVE_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-monetdb")]
    pub fn monetdb_connection_string() -> String {
        std::env::var("MONETDB_ODBC_CONNECTION").expect("HIVE_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_multiple_rows() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query::<ValueRow>("SELECT explode(x) AS n FROM (SELECT array(42, 24) AS x) d;")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Integer(number)) => assert_eq!(number, 42));
        assert_matches!(data[1][0], Some(Value::Integer(number)) => assert_eq!(number, 24));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_multiple_columns() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query::<ValueRow>("SELECT 42, 24;")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Integer(number)) => assert_eq!(number, 42));
        assert_matches!(data[0][1], Some(Value::Integer(number)) => assert_eq!(number, 24));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_integer() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive.query::<ValueRow>("SELECT cast(127 AS TINYINT), cast(32767 AS SMALLINT), cast(2147483647 AS INTEGER), cast(9223372036854775807 AS BIGINT);")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Tinyint(number)) => assert_eq!(number, 127));
        assert_matches!(data[0][1], Some(Value::Smallint(number)) => assert_eq!(number, 32767));
        assert_matches!(data[0][2], Some(Value::Integer(number)) => assert_eq!(number, 2147483647));
        assert_matches!(data[0][3], Some(Value::Bigint(number)) => assert_eq!(number, 9223372036854775807));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_boolean() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query::<ValueRow>("SELECT true, false, CAST(NULL AS BOOLEAN)")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Bit(true)));
        assert_matches!(data[0][1], Some(Value::Bit(false)));
        assert!(data[0][2].is_none());
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_string() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query::<ValueRow>("SELECT cast('foo' AS STRING), cast('bar' AS VARCHAR);")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
        assert_matches!(data[0][1], Some(Value::String(ref string)) => assert_eq!(string, "bar"));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_types_string() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, sql_server_connection_string().as_str()).expect("connect to Hive");
        let data = hive.query::<ValueRow>("SELECT 'foo', cast('bar' AS NVARCHAR), cast('baz' AS TEXT), cast('quix' AS NTEXT);")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
        assert_matches!(data[0][1], Some(Value::String(ref string)) => assert_eq!(string, "bar"));
        assert_matches!(data[0][2], Some(Value::String(ref string)) => assert_eq!(string, "baz"));
        assert_matches!(data[0][3], Some(Value::String(ref string)) => assert_eq!(string, "quix"));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_float() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query::<ValueRow>("SELECT cast(1.5 AS FLOAT), cast(2.5 AS DOUBLE);")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Float(number)) => assert!(number > 1.0 && number < 2.0));
        assert_matches!(data[0][1], Some(Value::Double(number)) => assert!(number > 2.0 && number < 3.0));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_null() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query::<ValueRow>("SELECT cast(NULL AS FLOAT), cast(NULL AS DOUBLE);")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert!(data[0][0].is_none());
        assert!(data[0][1].is_none());
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_tables() {
        let odbc = Odbc::env().expect("open ODBC");
        let sql_server =
            Odbc::connect(&odbc, sql_server_connection_string().as_str()).expect("connect to Hive");
        let data = sql_server
            .tables::<ValueRow>("master", Some("sys"), None, Some("view"))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");
        
        assert!(data.len() > 0);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_date() {
        let odbc = Odbc::env().expect("open ODBC");
        let sql_server =
            Odbc::connect(&odbc, sql_server_connection_string().as_str()).expect("connect to Hive");
        let data = sql_server
            .query::<ValueRow>("SELECT cast('2018-08-24' AS DATE) AS date")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Date(ref date)) => assert_eq!(&date.to_string(), "2018-08-24"));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_time() {
        let odbc = Odbc::env().expect("open ODBC");
        let sql_server = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");
        let data = sql_server
            .query::<ValueRow>("SELECT cast('10:22:33.7654321' AS TIME) AS date")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Time(ref time)) => assert_eq!(&time.to_string(), "10:22:33.765432100"));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_affected_rows_query() {
        let odbc = Odbc::env().expect("open ODBC");
        let sql_server = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let data = sql_server
            .query::<ValueRow>("SELECT 1 UNION SELECT 2")
            .expect("failed to run query");

        assert!(data.affected_rows().unwrap().is_none());
        // TODO: this should not be necessary - take &mut for queries and add Drop impl
        data.close().unwrap();

        let data = sql_server
            .query::<ValueRow>("SELECT foo INTO #bar FROM (SELECT 1 as foo UNION SELECT 2 as foo) a")
            .expect("failed to run insert query");

        assert_eq!(data.affected_rows().unwrap().unwrap(), 2);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_affected_rows_prepared() {
        let odbc = Odbc::env().expect("open ODBC");
        let sql_server = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let data = sql_server
            .query::<ValueRow>("SELECT 1 UNION SELECT 2")
            .expect("failed to run query");

        assert!(data.affected_rows().unwrap().is_none());
        // TODO: this should not be necessary - take &mut for queries and add Drop impl
        data.close().unwrap();

        let statement = sql_server
            .prepare("SELECT foo INTO #bar FROM (SELECT 1 as foo UNION SELECT 2 as foo) a")
            .expect("prepare statement");

        let _data = sql_server
            .execute::<ValueRow>(statement)
            .expect("failed to run insert query");

        //TODO: this returns None since Prepared, NoResult has not affected_row_count method
        // assert_eq!(data.affected_rows().unwrap().unwrap(), 2);
    }

    #[derive(Debug)]
    struct Foo {
        val: i32,
    }

    impl TryFromRow for Foo {
        type Schema = Schema;
        type Error = NoError;
        fn try_from_row(mut values: ValueRow, _schema: &Schema) -> Result<Self, NoError> {
            Ok(values
                .pop()
                .map(|val| Foo {
                    val: val
                        .and_then(|v| v.as_integer())
                        .expect("val to be an integer"),
                })
                .expect("value"))
        }
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_custom_type() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let foo = hive
            .query::<Foo>("SELECT 42 AS val;")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_eq!(foo[0].val, 42);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_query_with_parameters() {
        let odbc = Odbc::env().expect("open ODBC");
        let db = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let val = 42;

        let foo: Vec<Foo> = db
            .query_with_parameters("SELECT ? AS val;", |q| q.bind(&val))
            .expect("failed to run query")
            .collect::<Result<_, _>>()
            .expect("fetch data");

        assert_eq!(foo[0].val, 42);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_query_with_many_parameters() {
        let odbc = Odbc::env().expect("open ODBC");
        let db = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let val = [42, 24, 32, 666];

        let data: Vec<ValueRow> = db
            .query_with_parameters("SELECT ?, ?, ?, ? AS val;", |q| {
                val.iter().fold(Ok(q), |q, v| q.and_then(|q| q.bind(v)))
            })
            .expect("failed to run query")
            .collect::<Result<_, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Integer(ref number)) => assert_eq!(*number, 42));
        assert_matches!(data[0][1], Some(Value::Integer(ref number)) => assert_eq!(*number, 24));
        assert_matches!(data[0][2], Some(Value::Integer(ref number)) => assert_eq!(*number, 32));
        assert_matches!(data[0][3], Some(Value::Integer(ref number)) => assert_eq!(*number, 666));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_query_with_many_parameters_prepared() {
        let odbc = Odbc::env().expect("open ODBC");
        let db = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let val = [42, 24, 32, 666];

        let statement = db
            .prepare("SELECT ?, ?, ?, ? AS val;")
            .expect("prepare statement");

        let data: Vec<ValueRow> = db
            .execute_with_parameters(statement, |q| {
                val.iter().fold(Ok(q), |q, v| q.and_then(|q| q.bind(v)))
            })
            .expect("failed to run query")
            .collect::<Result<_, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Integer(ref number)) => assert_eq!(*number, 42));
        assert_matches!(data[0][1], Some(Value::Integer(ref number)) => assert_eq!(*number, 24));
        assert_matches!(data[0][2], Some(Value::Integer(ref number)) => assert_eq!(*number, 32));
        assert_matches!(data[0][3], Some(Value::Integer(ref number)) => assert_eq!(*number, 666));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_prepared_columns() {
        let odbc = Odbc::env().expect("open ODBC");
        let db = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let statement = db
            .prepare("SELECT ?, ?, ?, ? AS val;")
            .expect("prepare statement");
        assert_eq!(statement.columns().unwrap(), 4);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_prepared_schema() {
        let odbc = Odbc::env().expect("open ODBC");
        let db = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let statement = db
            .prepare("SELECT ?, CAST(? as INTEGER) as foo, ?, ? AS val;")
            .expect("prepare statement");
        let schema = statement.schema().unwrap();
        assert_eq!(schema.len(), 4);
        assert_eq!(schema[1].name, "foo");
        assert_eq!(schema[1].data_type, SqlDataType::SQL_INTEGER);
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_empty_data_set() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query::<ValueRow>("USE default;")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert!(data.is_empty());
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_long_string_fetch_utf_8() {
        let odbc = Odbc::env().expect("open ODBC");
        let sql_server = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .expect("connect to SQL Server");
        let data = sql_server
            .query::<ValueRow>(&format!("SELECT N'{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_long_string_fetch_utf_8() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query::<ValueRow>(&format!("SELECT '{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_moentdb_long_string_fetch_utf_8() {
        let odbc = Odbc::env().expect("open ODBC");
        let monetdb =
            Odbc::connect(&odbc, monetdb_connection_string().as_str()).expect("connect to MonetDB");
        let data = monetdb
            .query::<ValueRow>(&format!("SELECT '{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_long_string_fetch_utf_16_bind() {
        let odbc = Odbc::env().expect("open ODBC");
        let sql_server = Odbc::connect_with_options(
            &odbc,
            sql_server_connection_string().as_str(),
            Options {
                utf_16_strings: true,
            },
        )
        .expect("connect to SQL Server");

        let utf_16_string = LONG_STRING.encode_utf16().collect::<Vec<u16>>();

        let statement = sql_server
            .prepare("SELECT ? AS val;")
            .expect("prepare statement");

        let data: Vec<ValueRow> = sql_server
            .execute_with_parameters(statement, |q| q.bind(&utf_16_string))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_long_string_fetch_utf_16() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive = Odbc::connect_with_options(
            &odbc,
            hive_connection_string().as_str(),
            Options {
                utf_16_strings: true,
            },
        )
        .expect("connect to Hive");

        let data = hive
            .query::<ValueRow>(&format!("SELECT '{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_moentdb_long_string_fetch_utf_16() {
        let odbc = Odbc::env().expect("open ODBC");
        let monetdb = Odbc::connect_with_options(
            &odbc,
            monetdb_connection_string().as_str(),
            Options {
                utf_16_strings: true,
            },
        )
        .expect("connect to MonetDB");

        let data = monetdb
            .query::<ValueRow>(&format!("SELECT '{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }
    #[test]
    fn test_split_queries() {
        let queries = split_queries(
            r#"-- Foo
---
CREATE DATABASE IF NOT EXISTS daily_reports;
USE daily_reports;

SELECT *;"#,
        )
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse");
        assert_eq!(
            queries,
            [
                "CREATE DATABASE IF NOT EXISTS daily_reports;",
                "USE daily_reports;",
                "SELECT *;"
            ]
        );
    }

    #[test]
    fn test_split_queries_end_white() {
        let queries = split_queries(
            r#"USE daily_reports;
SELECT *;

"#,
        )
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse");
        assert_eq!(queries, ["USE daily_reports;", "SELECT *;"]);
    }

    #[test]
    fn test_split_queries_simple() {
        let queries = split_queries("SELECT 42;\nSELECT 24;\nSELECT 'foo';")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse");
        assert_eq!(queries, ["SELECT 42;", "SELECT 24;", "SELECT 'foo';"]);
    }

    #[test]
    fn test_split_queries_semicolon() {
        let queries = split_queries("SELECT 'foo; bar';\nSELECT 1;")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse");
        assert_eq!(queries, [r#"SELECT 'foo; bar';"#, "SELECT 1;"]);
    }

    #[test]
    fn test_split_queries_semicolon2() {
        let queries = split_queries(r#"foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad; foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad; select foo; foo "bar" baz 'quix; but' foo "bar" baz "quix; but" fsad; foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad; select foo;"#).collect::<Result<Vec<_>, _>>().expect("failed to parse");
        assert_eq!(
            queries,
            [
                r#"foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad;"#,
                r#"foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad;"#,
                r#"select foo;"#,
                r#"foo "bar" baz 'quix; but' foo "bar" baz "quix; but" fsad;"#,
                r#"foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad;"#,
                r#"select foo;"#,
            ]
        );
    }

    #[test]
    fn test_split_queries_escaped_quote() {
        let queries = split_queries("SELECT 'foo; b\\'ar';\nSELECT 1;")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse");
        assert_eq!(queries, [r#"SELECT 'foo; b\'ar';"#, "SELECT 1;"]);
    }

    #[test]
    fn test_split_queries_escaped_quote2() {
        let queries = split_queries("SELECT 'foo; b\\'ar';\nSELECT 'foo\\'bar';")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse");
        assert_eq!(
            queries,
            [r#"SELECT 'foo; b\'ar';"#, r#"SELECT 'foo\'bar';"#]
        );
    }

    #[test]
    fn test_split_queries_escaped_doublequote() {
        let queries = split_queries(r#"SELECT "foo; b\"ar";SELECT "foo\"bar";"#)
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse");
        assert_eq!(
            queries,
            [r#"SELECT "foo; b\"ar";"#, r#"SELECT "foo\"bar";"#]
        );
    }

    #[test]
    fn test_split_queries_comments() {
        let queries =
            split_queries("SELECT 1;\n-- SELECT x;\n---- SELECT x;\nSELECT 2;\nSELECT 3;")
                .collect::<Result<Vec<_>, _>>()
                .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[test]
    fn test_split_queries_comments2() {
        let queries = split_queries("-- TODO: add last_search_or_brochure_logentry_id\n-- TODO: DISTRIBUTE BY analytics_record_id SORT BY analytics_record_id ASC;\n-- TODO: check previous day for landing logentry detail\nSELECT '1' LEFT JOIN source_wcc.domain d ON regexp_extract(d.domain, '.*\\\\.([^\\.]+)$', 1) = c.domain AND d.snapshot_day = c.index;").collect::<Result<Vec<_>, _>>().expect("failed to parse");
        assert_eq!(queries, [r#"SELECT '1' LEFT JOIN source_wcc.domain d ON regexp_extract(d.domain, '.*\\.([^\.]+)$', 1) = c.domain AND d.snapshot_day = c.index;"#]);
    }

    #[test]
    fn test_split_queries_control() {
        let queries = split_queries(
            "!outputformat vertical\nSELECT 1;\n-- SELECT x;\n---- SELECT x;\nSELECT 2;\nSELECT 3;",
        )
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[test]
    fn test_split_queries_white() {
        let queries = split_queries(" \n  SELECT 1;\n  \nSELECT 2;\n \nSELECT 3;\n\n ")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[test]
    fn test_split_queries_white2() {
        let queries = split_queries("SELECT 1; \t \nSELECT 2; \n \nSELECT 3; ")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[test]
    fn test_split_queries_white_comment() {
        let queries = split_queries("SELECT 1; \t \nSELECT 2; -- foo bar\n \nSELECT 3; ")
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_multiple_queries() {
        let odbc = Odbc::env().expect("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).expect("connect to Hive");
        let data = hive
            .query_multiple::<ValueRow>("SELECT 42;\nSELECT 24;\nSELECT 'foo';")
            .flat_map(|i| i.expect("failed to run query"))
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Integer(ref number)) => assert_eq!(*number, 42));
        assert_matches!(data[1][0], Some(Value::Integer(ref number)) => assert_eq!(*number, 24));
        assert_matches!(data[2][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
    }
}
