use error_context::prelude::*;
use lazy_static::lazy_static;
use log::{debug, log_enabled, trace};
use odbc::{
    Allocated, Connection as OdbcConnection, DiagnosticRecord, DriverInfo, Environment,
    NoResult, ResultSetState, Statement, Version3,
};
use regex::Regex;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::string::FromUtf16Error;
use std::convert::{Infallible, TryFrom};

// Schema
pub use odbc::ColumnDescriptor;
// Allow for custom OdbcType impl for binning
pub use odbc::ffi;
pub use odbc::{OdbcType, SqlDate, SqlSsTime2, SqlTime, SqlTimestamp};
// Rows can be parametrized with this types
pub use odbc::{Executed, Prepared};

mod convert;
pub use convert::*;
pub mod schema_access;
pub mod value;
pub use value::{Value, ValueRow, NullableValue, AsNullable};
pub mod thread_local;
pub use thread_local::connection_with as thread_local_connection_with;
mod odbc_type;
pub use odbc_type::*;

/// TODO
/// * Reduce number of type parameters
/// ** by removing implicit conversion (TryFromSchema/Row/Value) and providing TryFrom/Into for
/// Value/Row types and let user deal with it; problem here is that some conversions require column
/// names which are not available with ValueRow and can't be since Item cannot reference the Iterator
/// ** or by using Box<dyn Error> for TryFromSchema/Row/Value error type so error types does not -
/// Schema.zip_with_rows(Rows) addaptor that references schema on stack and iterates rows with it
/// need parameters
/// ** schema is available on Rows object so implement conversion of Rows object to another type of result set e.g. AvroResultSet
/// *** Limit here is that you can only convert full ResultSet to Vec of given type if that type requires column names
/// ** use https://docs.rs/serde_db/0.8.2/serde_db/de/index.html to implement conversion to serde types - there are many problems with that, looks like Java dev designed it
/// ** separate ResultSet from Rows iterator so that each Row can have reference to schema stored in ResultSet that then can be used to convert single row into types requiring column names
/// *** problem is that returned Row cannot own the data as we cannot mutate ResultSet via Rows iterator - IntoIter cannot be implemented (https://gist.github.com/jpastuszek/d07391f617ac8a3656ecb41c4462ec97)
/// *** alternatively Rows cannot take ownership of ResultSet or it won't be able to put references to column names to each Row - would need to put column list behind Rc (https://gist.github.com/jpastuszek/49ec870810aba06a9795c65a03de2d71)
/// *** give up on Iterator impl and just provide next() method that gives out references to self
/// *** add ValueRowWithNames type that is short lived inside next() and used to do the conversion before ValueRow or T is emitted (https://gist.github.com/jpastuszek/6ab7fafb0042ea56dabfcd75e33a1ea2)
/// * Rename Rows to ResultSet - https://en.wikipedia.org/wiki/Result_set
/// * impl size_hint for Rows
/// * impl Debug on all structs
/// * Looks like tests needs some global lock as I get spurious connection error/SEGV on SQL Server tests
/// * Prepared statement cache:
/// ** db.with_statement_cache() -> StatementCache
/// ** sc.query(str) - direct query
/// ** sc.query_prepared(impl ToString + Hash) - hash fist and look up in cache if found execute; .to_string otherwise and prepare + execute;
///    this is to avoid building query strings where we know hash e.g. from some other value than query string itself
/// ** sc.clear() - try close the statement and clear the cache
/// * MultiConnection - special handle that does not require mutable reference to query but will automatically crate and manage connections if one is already busy
/// ** Connections behind RefCell, get Handle for each query
/// ** If connection RefCell is busy crate check next connection in the pool or add new one if all are busy
/// ** This will require statement cache per connection to support prepared statements as they have to be managed per connection

// https://github.com/rust-lang/rust/issues/49431
pub trait Captures<'a> {}
impl<'a, T: ?Sized> Captures<'a> for T {}

pub trait Captures2<'a> {}
impl<'a, T: ?Sized> Captures2<'a> for T {}

pub trait Captures3<'a> {}
impl<'a, T: ?Sized> Captures3<'a> for T {}

pub trait Captures4<'a> {}
impl<'a, T: ?Sized> Captures4<'a> for T {}

/// General ODBC initialization and connection errors
#[derive(Debug)]
pub struct OdbcError(Option<DiagnosticRecord>, &'static str);

impl OdbcError {
    pub fn into_query_error<R, S>(self) -> QueryError {
        QueryError::from(self)
    }
}

impl fmt::Display for OdbcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ODBC call failed while {}", self.1)
    }
}

fn to_dyn(diag: &Option<DiagnosticRecord>) -> Option<&(dyn Error + 'static)> {
    diag.as_ref().map(|e| e as &(dyn Error + 'static))
}

impl Error for OdbcError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        to_dyn(&self.0)
    }
}

impl From<ErrorContext<Option<DiagnosticRecord>, &'static str>> for OdbcError {
    fn from(err: ErrorContext<Option<DiagnosticRecord>, &'static str>) -> OdbcError {
        OdbcError(err.error, err.context)
    }
}

impl From<ErrorContext<DiagnosticRecord, &'static str>> for OdbcError {
    fn from(err: ErrorContext<DiagnosticRecord, &'static str>) -> OdbcError {
        OdbcError(Some(err.error), err.context)
    }
}

/// Errors related to query execution
#[derive(Debug)]
pub enum QueryError {
    OdbcError(OdbcError),
    BindError(DiagnosticRecord),
    MultipleQueriesError(SplitQueriesError),
    DataAccessError(DataAccessError),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryError::OdbcError(err) => write!(f, "{}", err),
            QueryError::BindError(_) => {
                write!(f, "ODBC call failed while binding parameter to statement")
            }
            QueryError::MultipleQueriesError(_) => write!(f, "failed to execute multiple queries"),
            QueryError::DataAccessError(_) => {
                write!(f, "failed to access result data")
            }
        }
    }
}

impl Error for QueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            QueryError::OdbcError(err) => err.source(),
            QueryError::BindError(err) => Some(err),
            QueryError::MultipleQueriesError(err) => Some(err),
            QueryError::DataAccessError(err) => Some(err),
        }
    }
}

impl From<ErrorContext<DiagnosticRecord, &'static str>> for QueryError {
    fn from(err: ErrorContext<DiagnosticRecord, &'static str>) -> QueryError {
        QueryError::OdbcError(err.into())
    }
}

impl From<BindError> for QueryError {
    fn from(err: BindError) -> QueryError {
        QueryError::BindError(err.0)
    }
}

impl From<SplitQueriesError> for QueryError {
    fn from(err: SplitQueriesError) -> QueryError {
        QueryError::MultipleQueriesError(err)
    }
}

impl From<OdbcError> for QueryError {
    fn from(err: OdbcError) -> QueryError {
        QueryError::OdbcError(err)
    }
}

impl From<DataAccessError> for QueryError {
    fn from(err: DataAccessError) -> QueryError {
        QueryError::DataAccessError(err)
    }
}

/// Errors related to data access to query results
/// Note: You can convert DataAccessError to QueryError with Into::into
#[derive(Debug)]
pub enum DataAccessError {
    OdbcError(DiagnosticRecord, &'static str),
    OdbcCursorError(DiagnosticRecord),
    /// We have to take by Box<dyn Error> since V::Error gives: error[E0212]: cannot extract an associated type from a higher-ranked trait bound in this context
    FromRowError(Box<dyn Error>),
    FromUtf16Error(FromUtf16Error, &'static str),
    UnexpectedNumberOfRows(&'static str),
    #[cfg(feature = "serde_json")]
    JsonError(serde_json::Error),
}

impl DataAccessError {
    pub fn into_query_error<S>(self) -> QueryError {
        QueryError::from(self)
    }
}

impl fmt::Display for DataAccessError {
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
            DataAccessError::UnexpectedNumberOfRows(context) => write!(
                f,
                "unexpected number of rows returned by query: {}",
                context
            ),
            #[cfg(feature = "serde_json")]
            DataAccessError::JsonError(_) => write!(f, "failed to convert data to JSON Value"),
        }
    }
}

impl Error for DataAccessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DataAccessError::OdbcError(err, _) => Some(err),
            DataAccessError::OdbcCursorError(err) => Some(err),
            DataAccessError::FromRowError(err) => Some(err.as_ref()),
            DataAccessError::FromUtf16Error(err, _) => Some(err),
            DataAccessError::UnexpectedNumberOfRows(_) => None,
            #[cfg(feature = "serde_json")]
            DataAccessError::JsonError(err) => Some(err),
        }
    }
}

// Note that we don't give context for cursor data access at this time
// TODO: better way to distinguish between general ODBC errors and cursor errors
impl From<DiagnosticRecord> for DataAccessError {
    fn from(err: DiagnosticRecord) -> DataAccessError {
        DataAccessError::OdbcCursorError(err)
    }
}

impl From<ErrorContext<DiagnosticRecord, &'static str>> for DataAccessError {
    fn from(err: ErrorContext<DiagnosticRecord, &'static str>) -> DataAccessError {
        DataAccessError::OdbcError(err.error, err.context)
    }
}

impl From<ErrorContext<FromUtf16Error, &'static str>> for DataAccessError {
    fn from(err: ErrorContext<FromUtf16Error, &'static str>) -> DataAccessError {
        DataAccessError::FromUtf16Error(err.error, err.context)
    }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Error> for DataAccessError {
    fn from(err: serde_json::Error) -> DataAccessError {
        DataAccessError::JsonError(err)
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

/// Iterate rows converting them to given value type
pub struct Rows<'h, 'c, V, S> {
    statement: Option<ExecutedStatement<'c, S>>,
    odbc_schema: Vec<ColumnDescriptor>,
    column_names: Vec<String>,
    columns: i16,
    utf_16_strings: bool,
    phantom: PhantomData<&'h V>,
}

impl<'h, 'c, V, S> Drop for Rows<'h, 'c, V, S> {
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

impl<'h, 'c: 'h, 'o: 'c, V, S> Rows<'h, 'c, V, S>
where
    V: for<'n> TryFrom<ValueRowWithNames<'n>>,
{
    fn from_result(
        _handle: &'h Handle<'c, 'o>,
        result: ResultSetState<'c, '_, S>,
        utf_16_strings: bool,
    ) -> Result<
        Rows<'h, 'c, V, S>,
        QueryError,
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
        let column_names = odbc_schema.iter().map(|s| s.name.clone()).collect::<Vec<_>>();

        Ok(Rows {
            statement: Some(statement),
            odbc_schema,
            column_names,
            columns,
            phantom: PhantomData,
            utf_16_strings,
        })
    }

    pub fn column_names(&self) -> &[String] {
        self.column_names.as_slice()
    }

    pub fn columns(&self) -> i16 {
        self.columns
    }

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

    pub fn first(mut self) -> Result<V, DataAccessError> {
        self.next().ok_or(DataAccessError::UnexpectedNumberOfRows(
            "expected at least one row but got no rows",
        ))?
    }

    pub fn no_result(mut self) -> Result<(), DataAccessError> {
        if self.next().is_some() {
            return Err(DataAccessError::UnexpectedNumberOfRows(
                "exepcted no rows but got at least one",
            ));
        }
        Ok(())
    }
}

impl<'h, 'c: 'h, 'o: 'c, V> Rows<'h, 'c, V, Prepared>
where
    V: for<'n> TryFrom<ValueRowWithNames<'n>>,
{
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

impl<'h, 'c: 'h, 'o: 'c, V> Rows<'h, 'c, V, Executed>
where
    V: for<'n> TryFrom<ValueRowWithNames<'n>>,
{
    pub fn close(mut self) -> Result<(), OdbcError> {
        if let ExecutedStatement::HasResult(statement) = self.statement.take().unwrap() {
            statement
                .close_cursor()
                .wrap_error_while("closing cursor on executed statement")?;
        }
        Ok(())
    }

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

impl<'h, 'c: 'h, 'o: 'c, V, S> Iterator for Rows<'h, 'c, V, S>
where
    V: for<'n> TryFrom<ValueRowWithNames<'n>>,
{
    type Item = Result<V, DataAccessError>;

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

        let statement = match self.statement.as_mut().unwrap() {
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
                                        //TODO: map + transpose
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
                                    cursor_get_value::<S, SqlTimestamp>(&mut cursor, index as u16)?
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
                                    cursor_get_data::<S, u8>(&mut cursor, index as u16)?.map(|byte| Value::Bit(if byte == 0 { false } else { true }))
                                }
                                SQL_UNKNOWN_TYPE => {
                                    // handle MonetDB JSON type for now
                                    match cursor_get_data::<S, String>(&mut cursor, index as u16) {
                                        Err(err) => Err(err).wrap_error_while("trying to interpret SQL_UNKNOWN_TYPE as SQL_CHAR"),
                                        Ok(Some(data)) => {
                                            #[cfg(feature = "serde_json")]
                                            {
                                                // MonetDB can only store arrays or objects as top level JSON values so check if data looks like JSON in case we are not talking to MonetDB
                                                if (data.starts_with("[") && data.ends_with("]")) || (data.starts_with("{") && data.ends_with("}")) {
                                                    let json = serde_json::from_str(&data)?;
                                                    Ok(Some(Value::Json(json)))
                                                } else {
                                                    Ok(Some(Value::String(data)))
                                                }
                                            }
                                            #[cfg(not(feature = "serde_json"))]
                                            {
                                                Ok(Some(Value::String(data)))
                                            }
                                        }
                                        Ok(None) => Ok(None)
                                    }?
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
        .map(|v| v.and_then(|v| ValueRowWithNames(v, self.column_names()).try_into().map_err(DataAccessError::FromRowError)))
    }
}

pub struct Binder<'h, 't, S> {
    statement: Statement<'h, 't, S, NoResult>,
    index: u16,
}

impl<'h, 't, S> Binder<'h, 't, S> {
    pub fn bind<'new_t, T>(self, value: &'new_t T) -> Result<Binder<'h, 'new_t, S>, BindError>
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

    fn into_inner(self) -> Statement<'h, 't, S, NoResult> {
        self.statement
    }
}

impl<'h, 't, S> From<Statement<'h, 'h, S, NoResult>> for Binder<'h, 'h, S> {
    fn from(statement: Statement<'h, 'h, S, NoResult>) -> Binder<'h, 'h, S> {
        Binder {
            statement,
            index: 0,
        }
    }
}

#[derive(Debug)]
pub struct Options {
    utf_16_strings: bool,
}

/// Wrapper around ODBC prepared statement
pub struct PreparedStatement<'h>(Statement<'h, 'h, odbc::Prepared, odbc::NoResult>);

impl<'h> PreparedStatement<'h> {
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

pub struct Odbc {
    environment: EnvironmentV3,
}

impl Odbc {
    pub fn new() -> Result<Odbc, OdbcError> {
        odbc::create_environment_v3()
            .wrap_error_while("creating v3 environment")
            .map_err(Into::into)
            .map(|environment| Odbc { environment })
    }
}

impl Odbc {
    pub fn list_drivers(&mut self) -> Result<Vec<DriverInfo>, OdbcError> {
        self.environment
            .drivers()
            .wrap_error_while("listing drivers")
            .map_err(Into::into)
    }

    pub fn connect(&self, connection_string: &str) -> Result<Connection, OdbcError> {
        self.connect_with_options(
            connection_string,
            Options {
                utf_16_strings: false,
            },
        )
    }

    pub fn connect_with_options(
        &self,
        connection_string: &str,
        options: Options,
    ) -> Result<Connection, OdbcError> {
        self.environment
            .connect_with_connection_string(connection_string)
            .wrap_error_while("connecting to database")
            .map_err(Into::into)
            .map(|connection| Connection {
                connection,
                utf_16_strings: options.utf_16_strings,
            })
    }
}

pub struct Connection<'o> {
    connection: OdbcConnection<'o>,
    utf_16_strings: bool,
}

/// Handle ensures that query results are consumed before next query can be performed
pub struct Handle<'c, 'o>(&'c Connection<'o>);

impl<'c, 'o: 'c> Connection<'o> {
    pub fn handle(&'c mut self) -> Handle<'c, 'o> {
        Handle(self)
    }
}

impl<'h, 'c: 'h, 'o: 'c> Handle<'c, 'o> {
    fn statement(&'h self) -> Result<Statement<'c, 'c, Allocated, NoResult>, OdbcError> {
        Statement::with_parent(&self.0.connection)
            .wrap_error_while("pairing statement with connection")
            .map_err(Into::into)
    }

    pub fn tables<'i, V>(
        &'h mut self,
        catalog: &'i str,
        schema: Option<&'i str>,
        table: Option<&'i str>,
        table_type: Option<&'i str>,
    ) -> Result<
        Rows<'h, 'c, V, Executed>,
        QueryError,
    >
    where
       V: for<'n> TryFrom<ValueRowWithNames<'n>>,
    {
        debug!("Getting ODBC tables");
        let statement = self.statement()?;
        let result_set: ResultSetState<'c, 'c, Allocated> = ResultSetState::Data(
            statement
                .tables_str(
                    catalog,
                    schema.unwrap_or(""),
                    table.unwrap_or(""),
                    table_type.unwrap_or(""),
                )
                .wrap_error_while("executing direct statement")?,
        );

        Rows::from_result(self, result_set, self.0.utf_16_strings)
    }

    pub fn prepare(&'h mut self, query: &str) -> Result<PreparedStatement<'c>, OdbcError> {
        debug!("Preparing ODBC query: {}", &query);

        let statement = self
            .statement()?
            .prepare(query)
            .wrap_error_while("preparing query")?;

        Ok(PreparedStatement(statement))
    }

    pub fn query<V>(
        &'h mut self,
        query: &str,
    ) -> Result<
        Rows<'h, 'c, V, Executed>,
        QueryError,
    >
    where
       V: for<'n> TryFrom<ValueRowWithNames<'n>>,
    {
        self.query_with_parameters(query, |b| Ok(b))
    }

    pub fn query_with_parameters<'t, V, F>(
        &'h mut self,
        query: &str,
        bind: F,
    ) -> Result<
        Rows<'h, 'c, V, Executed>,
        QueryError,
    >
    where
       V: for<'n> TryFrom<ValueRowWithNames<'n>>,
        F: FnOnce(Binder<'c, 'c, Allocated>) -> Result<Binder<'c, 't, Allocated>, BindError>,
    {
        debug!("Direct ODBC query: {}", &query);

        let statement = bind(self.statement()?.into())?.into_inner();

        Rows::from_result(
            self,
            statement
                .exec_direct(query)
                .wrap_error_while("executing direct statement")?,
            self.0.utf_16_strings,
        )
    }

    pub fn execute<V>(
        &'h mut self,
        statement: PreparedStatement<'c>,
    ) -> Result<
        Rows<'h, 'c, V, Prepared>,
        QueryError,
    >
    where
       V: for<'n> TryFrom<ValueRowWithNames<'n>>,
    {
        self.execute_with_parameters(statement, |b| Ok(b))
    }

    pub fn execute_with_parameters<'t, V, F>(
        &'h mut self,
        statement: PreparedStatement<'c>,
        bind: F,
    ) -> Result<
        Rows<'h, 'c, V, Prepared>,
        QueryError,
    >
    where
       V: for<'n> TryFrom<ValueRowWithNames<'n>>,
        F: FnOnce(Binder<'c, 'c, Prepared>) -> Result<Binder<'c, 't, Prepared>, BindError>,
    {
        let statement = bind(statement.0.into())?.into_inner();

        Rows::from_result(
            self,
            statement
                .execute()
                .wrap_error_while("executing statement")?,
            self.0.utf_16_strings,
        )
    }

    pub fn start_transaction(&mut self) -> Result<(), QueryError> {
        self.query::<()>("START TRANSACTION")?.no_result().unwrap();
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), QueryError> {
        self.query::<()>("COMMIT")?.no_result().unwrap();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), QueryError> {
        self.query::<()>("ROLLBACK")?.no_result().unwrap();
        Ok(())
    }

    /// Call function in transaction.
    /// If function returns Err the transaction will be rolled back otherwise committed.
    pub fn in_transaction<O, E>(&mut self, f: impl FnOnce(&mut Handle<'c, 'o>) -> Result<O, E>) -> Result<Result<O, E>, QueryError> {
        self.start_transaction()?;
        Ok(match f(self) {
            ok @ Ok(_) => {
                self.commit()?;
                ok
            }
            err @ Err(_) => {
                self.rollback()?;
                err
            }
        })
    }

    /// Commit current transaction, run function and start new one.
    /// This is useful when you need to do changes with auto-commit for example for schema while in transaction already.
    pub fn outside_of_transaction<O>(&mut self, f: impl FnOnce(&mut Handle<'c, 'o>) -> O) -> Result<O, QueryError> {
        self.commit()?;
        let ret = f(self);
        self.start_transaction()?;
        Ok(ret)
    }

    pub fn query_multiple<'q: 'h>(
        &'h mut self,
        queries: &'q str,
    ) -> impl Iterator<Item = Result<Vec<ValueRow>, QueryError<Infallible, Infallible>>>
                 + Captures<'q>
                 + Captures2<'h>
                 + Captures3<'c>
                 + Captures4<'o> {
        split_queries(queries).map(move |query| {
            query
                .map_err(Into::into)
                .and_then(|query| self.query(query))
                .and_then(|rows| {
                    rows.collect::<Result<Vec<_>, _>>()
                        .map_err(Into::into)
                })
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

#[cfg(test)]
pub mod tests {
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
        std::env::var("MONETDB_ODBC_CONNECTION").expect("MONETDB_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_multiple_rows() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive.handle()
            .query::<ValueRow>("SELECT cast(127 AS TINYINT), cast(32767 AS SMALLINT), cast(2147483647 AS INTEGER), cast(9223372036854775807 AS BIGINT);")
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive.handle()
            .query::<ValueRow>("SELECT 'foo', cast('bar' AS NVARCHAR), cast('baz' AS TEXT), cast('quix' AS NTEXT);")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
        assert_matches!(data[0][1], Some(Value::String(ref string)) => assert_eq!(string, "bar"));
        assert_matches!(data[0][2], Some(Value::String(ref string)) => assert_eq!(string, "baz"));
        assert_matches!(data[0][3], Some(Value::String(ref string)) => assert_eq!(string, "quix"));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_types_string_empty() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive.handle()
            .query::<ValueRow>("SELECT '', cast('' AS NVARCHAR), cast('' AS TEXT), cast('' AS NTEXT);")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, ""));
        assert_matches!(data[0][1], Some(Value::String(ref string)) => assert_eq!(string, ""));
        assert_matches!(data[0][2], Some(Value::String(ref string)) => assert_eq!(string, ""));
        assert_matches!(data[0][3], Some(Value::String(ref string)) => assert_eq!(string, ""));
    }

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_moentdb_string_empty() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut monetdb = odbc
            .connect(monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let data = monetdb
            .handle()
            .query::<ValueRow>("SELECT ''")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, ""));
    }

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_moentdb_json() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut monetdb = odbc
            .connect(monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let data = monetdb
            .handle()
            .query::<ValueRow>("SELECT CAST('[\"foo\"]' AS JSON)")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        #[cfg(feature = "serde_json")]
        {
            assert_matches!(data[0][0], Some(Value::Json(serde_json::Value::Array(ref arr))) => assert_eq!(arr, &[serde_json::Value::String("foo".to_owned())]));
        }
        #[cfg(not(feature = "serde_json"))]
        {
            assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "[\"foo\"]"));
        }
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_float() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut sql_server = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to Hive");

        let data = sql_server
            .handle()
            .tables::<ValueRow>("master", Some("sys"), None, Some("view"))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert!(data.len() > 0);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_date() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut sql_server = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to Hive");

        let data = sql_server
            .handle()
            .query::<ValueRow>("SELECT cast('2018-08-24' AS DATE) AS date")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(&data[0][0], Some(date @ Value::Date(_)) => assert_eq!(&date.to_naive_date().unwrap().to_string(), "2018-08-24"));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_time() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut sql_server = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let data = sql_server
            .handle()
            .query::<ValueRow>("SELECT cast('10:22:33.7654321' AS TIME) AS date")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(&data[0][0], Some(time @ Value::Time(_)) => assert_eq!(&time.to_naive_time().unwrap().to_string(), "10:22:33.765432100"));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_affected_rows_query() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut sql_server = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let mut db = sql_server.handle();

        let data = db
            .query::<ValueRow>("SELECT 1 UNION SELECT 2")
            .expect("failed to run query");

        assert!(data.affected_rows().unwrap().is_none());
        data.close().ok();

        let data = db
            .query::<ValueRow>(
                "SELECT foo INTO #bar FROM (SELECT 1 as foo UNION SELECT 2 as foo) a",
            )
            .expect("failed to run insert query");

        assert_eq!(data.affected_rows().unwrap().unwrap(), 2);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_affected_rows_prepared() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut sql_server = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let mut db = sql_server.handle();

        let data = db
            .query::<ValueRow>("SELECT 1 UNION SELECT 2")
            .expect("failed to run query");

        assert!(data.affected_rows().unwrap().is_none());
        data.close().ok();

        let statement = db
            .prepare("SELECT foo INTO #bar FROM (SELECT 1 as foo UNION SELECT 2 as foo) a")
            .expect("prepare statement");

        let _data = db
            .execute::<ValueRow>(statement)
            .expect("failed to run insert query");

        //TODO: this returns None since Prepared, NoResult has not affected_row_count method
        // assert_eq!(data.affected_rows().unwrap().unwrap(), 2);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_query_with_parameters() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let val = 42;

        let value: Value = db
            .handle()
            .query_with_parameters("SELECT ? AS val;", |q| q.bind(&val))
            .expect("failed to run query")
            .single()
            .expect("fetch data");

        assert_eq!(value.to_i32().unwrap(), 42);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_query_with_many_parameters() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let val = [42, 24, 32, 666];

        let data: Vec<ValueRow> = db
            .handle()
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let val = [42, 24, 32, 666];

        let mut handle = db.handle();

        let statement = handle
            .prepare("SELECT ?, ?, ?, ? AS val;")
            .expect("prepare statement");

        let data: Vec<ValueRow> = handle
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let statement = db
            .handle()
            .prepare("SELECT ?, ?, ?, ? AS val;")
            .expect("prepare statement");

        assert_eq!(statement.columns().unwrap(), 4);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_prepared_schema() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut db = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let statement = db
            .handle()
            .prepare("SELECT ?, CAST(? as INTEGER) as foo, ?, ? AS val;")
            .expect("prepare statement");

        let schema = statement.schema().unwrap();
        assert_eq!(schema.len(), 4);
        assert_eq!(schema[1].name, "foo");
        assert_eq!(schema[1].data_type, ffi::SqlDataType::SQL_INTEGER);
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_empty_data_set() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
            .query::<ValueRow>("USE default;")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert!(data.is_empty());
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_long_string_fetch_utf_8() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut sql_server = odbc
            .connect(sql_server_connection_string().as_str())
            .expect("connect to SQL Server");

        let data = sql_server
            .handle()
            .query::<ValueRow>(&format!("SELECT N'{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_long_string_fetch_utf_8() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
            .query::<ValueRow>(&format!("SELECT '{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_moentdb_long_string_fetch_utf_8() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut monetdb = odbc
            .connect(monetdb_connection_string().as_str())
            .expect("connect to MonetDB");

        let data = monetdb
            .handle()
            .query::<ValueRow>(&format!("SELECT '{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_long_string_fetch_utf_16_bind() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut sql_server = odbc
            .connect_with_options(
                sql_server_connection_string().as_str(),
                Options {
                    utf_16_strings: true,
                },
            )
            .expect("connect to SQL Server");

        let utf_16_string = LONG_STRING.encode_utf16().collect::<Vec<u16>>();

        let mut handle = sql_server.handle();

        let statement = handle
            .prepare("SELECT ? AS val;")
            .expect("prepare statement");

        let data: Vec<ValueRow> = handle
            .execute_with_parameters(statement, |q| q.bind(&utf_16_string))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_long_string_fetch_utf_16() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect_with_options(
                hive_connection_string().as_str(),
                Options {
                    utf_16_strings: true,
                },
            )
            .expect("connect to Hive");

        let data = hive
            .handle()
            .query::<ValueRow>(&format!("SELECT '{}'", LONG_STRING))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, LONG_STRING));
    }

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_moentdb_long_string_fetch_utf_16() {
        let odbc = Odbc::new().expect("open ODBC");
        let mut monetdb = odbc
            .connect_with_options(
                monetdb_connection_string().as_str(),
                Options {
                    utf_16_strings: true,
                },
            )
            .expect("connect to MonetDB");

        let data = monetdb
            .handle()
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
        let odbc = Odbc::new().expect("open ODBC");
        let mut hive = odbc
            .connect(hive_connection_string().as_str())
            .expect("connect to Hive");

        let data = hive
            .handle()
            .query_multiple("SELECT 42;\nSELECT 24;\nSELECT 'foo';")
            .flat_map(|i| i.expect("failed to run query"))
            .collect::<Vec<_>>();

        assert_matches!(data[0][0], Some(Value::Integer(ref number)) => assert_eq!(*number, 42));
        assert_matches!(data[1][0], Some(Value::Integer(ref number)) => assert_eq!(*number, 24));
        assert_matches!(data[2][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
    }
}
