use error_context::prelude::*;
use log::{debug, log_enabled, trace};
use odbc::{
    Allocated, ColumnDescriptor, Connection as OdbcConnection, DiagnosticRecord, Executed,
    NoResult, OdbcType, Prepared, ResultSetState, Statement,
};
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;

use crate::result_set::{DataAccessError, ResultSet, ResultSetError};
use crate::row::{ColumnType, UnsupportedSqlDataType};
use crate::value_row::TryFromValueRow;
use crate::{Odbc, OdbcError};

/// Errors related to execution of queries.
///
/// `OdbcError` and `DataAccessError` can be converted into `QueryError`.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum QueryError {
    OdbcError(OdbcError),
    BindError(DiagnosticRecord),
    UnsupportedSqlDataType(UnsupportedSqlDataType),
    ResultSetError(ResultSetError),
    DataAccessError(DataAccessError),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryError::OdbcError(err) => write!(f, "{}", err),
            QueryError::BindError(_) => {
                write!(f, "ODBC call failed while binding parameter to statement")
            }
            QueryError::UnsupportedSqlDataType(_) => {
                write!(f, "query schema has unsupported data type")
            }
            QueryError::ResultSetError(_) => write!(f, "failed to create result set for query"),
            QueryError::DataAccessError(_) => write!(f, "failed to access result data"),
        }
    }
}

impl Error for QueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            QueryError::OdbcError(err) => err.source(),
            QueryError::BindError(err) => Some(err),
            QueryError::UnsupportedSqlDataType(err) => Some(err),
            QueryError::ResultSetError(err) => Some(err),
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

impl From<OdbcError> for QueryError {
    fn from(err: OdbcError) -> QueryError {
        QueryError::OdbcError(err)
    }
}

impl From<UnsupportedSqlDataType> for QueryError {
    fn from(err: UnsupportedSqlDataType) -> QueryError {
        QueryError::UnsupportedSqlDataType(err)
    }
}

impl From<ResultSetError> for QueryError {
    fn from(err: ResultSetError) -> QueryError {
        QueryError::ResultSetError(err)
    }
}

impl From<DataAccessError> for QueryError {
    fn from(err: DataAccessError) -> QueryError {
        QueryError::DataAccessError(err)
    }
}

/// Error that can happen when binding values to parametrized queries.
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

/// Controls binding of parametrized query values.
pub struct Binder<'h, 't, S> {
    statement: Statement<'h, 't, S, NoResult>,
    index: u16,
}

impl<S> fmt::Debug for Binder<'_, '_, S> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Binder")
            .field("index", &self.index)
            .finish()
    }
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

/// Runtime configuration.
#[derive(Debug)]
pub struct Options {
    /// When `true` the `ResultSet` iterator will try to fetch strings as UTF-16 (wide) strings before converting them to Rust's UTF-8 `String`.
    pub utf_16_strings: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            utf_16_strings: false,
        }
    }
}

/// ODBC prepared statement.
pub struct PreparedStatement<'h>(Statement<'h, 'h, odbc::Prepared, odbc::NoResult>);

impl<'h> fmt::Debug for PreparedStatement<'h> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut d = f.debug_struct("PreparedStatement");

        let schema = (1..=self.0.num_result_cols().map_err(|_| std::fmt::Error)?)
            .map(|i| self.0.describe_col(i as u16))
            .collect::<Result<Vec<ColumnDescriptor>, _>>()
            .map_err(|_| std::fmt::Error)?;

        d.field("odbc_schema", &schema);
        d.finish()
    }
}

impl<'h> PreparedStatement<'h> {
    pub(crate) fn from_statement(
        statement: Statement<'h, 'h, odbc::Prepared, odbc::NoResult>,
    ) -> PreparedStatement<'h> {
        PreparedStatement(statement)
    }

    /// Query schema information deduced from prepared statement SQL text.
    pub fn schema(&self) -> Result<Vec<ColumnType>, QueryError> {
        (1..=self.columns()?)
            .map(|i| {
                self.0
                    .describe_col(i as u16)
                    .wrap_error_while("getting column description")
                    .map_err(QueryError::from)
                    .and_then(|cd| ColumnType::try_from(cd).map_err(Into::into))
            })
            .collect::<Result<_, _>>()
    }

    /// Query number of columns that would be returned by execution of this prepared statement.
    pub fn columns(&self) -> Result<i16, OdbcError> {
        Ok(self
            .0
            .num_result_cols()
            .wrap_error_while("getting number of columns in prepared statement")?)
    }
}

/// Database connection.
pub struct Connection {
    connection: OdbcConnection<'static>,
    utf_16_strings: bool,
}

/// Assuming drivers support sending Connection between threads.
unsafe impl Send for Connection {}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Connection")
            .field("utf_16_strings", &self.utf_16_strings)
            .finish()
    }
}

impl Connection {
    pub fn new(odbc: &'static Odbc, connection_string: &str) -> Result<Connection, OdbcError> {
        Self::with_options(odbc, connection_string, Options::default())
    }

    pub fn with_options(
        odbc: &'static Odbc,
        connection_string: &str,
        options: Options,
    ) -> Result<Connection, OdbcError> {
        odbc.environment
            .connect_with_connection_string(connection_string)
            .wrap_error_while("connecting to database")
            .map_err(Into::into)
            .map(|connection| Connection {
                connection,
                utf_16_strings: options.utf_16_strings,
            })
    }
}

/// Blocks access to `Connection` for duration of query.
///
/// Statically ensures that query result set is consumed before next query can be executed on this connection.
#[derive(Debug)]
pub struct Handle<'c>(&'c Connection);

impl<'c: 'c> Connection {
    pub fn handle(&'c mut self) -> Handle<'c> {
        Handle(self)
    }
}

impl<'h, 'c: 'h> Handle<'c> {
    fn statement(&'h self) -> Result<Statement<'c, 'c, Allocated, NoResult>, OdbcError> {
        Statement::with_parent(&self.0.connection)
            .wrap_error_while("pairing statement with connection")
            .map_err(Into::into)
    }

    /// Query list of tables from given catalog.
    /// Optionally result can be filtered by table name and table type.
    pub fn tables<'i, V>(
        &'h mut self,
        catalog: &'i str,
        schema: Option<&'i str>,
        table: Option<&'i str>,
        table_type: Option<&'i str>,
    ) -> Result<ResultSet<'h, 'c, V, Executed>, QueryError>
    where
        V: TryFromValueRow,
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

        Ok(ResultSet::from_result(
            self,
            result_set,
            self.0.utf_16_strings,
        )?)
    }

    /// Prepare statement for fast execution and parametrization.
    /// For one-off queries it is more efficient to use `query()` function.
    pub fn prepare(&'h mut self, query: &str) -> Result<PreparedStatement<'c>, OdbcError> {
        debug!("Preparing ODBC query: {}", &query);

        let statement = self
            .statement()?
            .prepare(query)
            .wrap_error_while("preparing query")?;

        Ok(PreparedStatement(statement))
    }

    /// Execute one-off query.
    pub fn query<V>(&'h mut self, query: &str) -> Result<ResultSet<'h, 'c, V, Executed>, QueryError>
    where
        V: TryFromValueRow,
    {
        self.query_with_parameters(query, Ok)
    }

    /// Execute one-off query with parameters.
    /// This creates prepared statement and binds values to it before execution.
    pub fn query_with_parameters<'t, V, F>(
        &'h mut self,
        query: &str,
        bind: F,
    ) -> Result<ResultSet<'h, 'c, V, Executed>, QueryError>
    where
        V: TryFromValueRow,
        F: FnOnce(Binder<'c, 'c, Allocated>) -> Result<Binder<'c, 't, Allocated>, BindError>,
    {
        debug!("Direct ODBC query: {}", &query);

        let statement = bind(self.statement()?.into())?.into_inner();

        Ok(ResultSet::from_result(
            self,
            statement
                .exec_direct(query)
                .wrap_error_while("executing direct statement")?,
            self.0.utf_16_strings,
        )?)
    }

    /// Execute prepared statement without parameters.
    pub fn execute<V>(
        &'h mut self,
        statement: PreparedStatement<'c>,
    ) -> Result<ResultSet<'h, 'c, V, Prepared>, QueryError>
    where
        V: TryFromValueRow,
    {
        self.execute_with_parameters(statement, Ok)
    }

    /// Bind parameters and execute prepared statement.
    pub fn execute_with_parameters<'t, V, F>(
        &'h mut self,
        statement: PreparedStatement<'c>,
        bind: F,
    ) -> Result<ResultSet<'h, 'c, V, Prepared>, QueryError>
    where
        V: TryFromValueRow,
        F: FnOnce(Binder<'c, 'c, Prepared>) -> Result<Binder<'c, 't, Prepared>, BindError>,
    {
        let statement = bind(statement.0.into())?.into_inner();

        Ok(ResultSet::from_result(
            self,
            statement
                .execute()
                .wrap_error_while("executing statement")?,
            self.0.utf_16_strings,
        )?)
    }

    /// Calls "START TRANSACTION"
    pub fn start_transaction(&mut self) -> Result<(), QueryError> {
        self.query::<()>("START TRANSACTION")?.no_result().unwrap();
        Ok(())
    }

    /// Calls "COMMIT"
    pub fn commit(&mut self) -> Result<(), QueryError> {
        self.query::<()>("COMMIT")?.no_result().unwrap();
        Ok(())
    }

    /// Calls "ROLLBACK"
    pub fn rollback(&mut self) -> Result<(), QueryError> {
        self.query::<()>("ROLLBACK")?.no_result().unwrap();
        Ok(())
    }

    /// Call function in transaction.
    /// If function returns Err the transaction will be rolled back otherwise committed.
    pub fn in_transaction<O, E>(
        &mut self,
        f: impl FnOnce(&mut Handle<'c>) -> Result<O, E>,
    ) -> Result<Result<O, E>, QueryError> {
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

    /// Commit current transaction, run function and start new transaction.
    /// This is useful when you need to do changes with auto-commit (for example change schema) while in open transaction already.
    pub fn outside_of_transaction<O>(
        &mut self,
        f: impl FnOnce(&mut Handle<'c>) -> O,
    ) -> Result<O, QueryError> {
        self.commit()?;
        let ret = f(self);
        self.start_transaction()?;
        Ok(ret)
    }
}
