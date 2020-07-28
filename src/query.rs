use error_context::prelude::*;
use log::{debug, log_enabled, trace};
use odbc::{
    Allocated, ColumnDescriptor, Connection as OdbcConnection, DiagnosticRecord, Executed,
    NoResult, OdbcType, Prepared, ResultSetState, Statement,
};
use lazy_static::lazy_static;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::sync::Mutex;

use crate::result_set::{DataAccessError, ResultSet, ResultSetError};
use crate::row::{Settings, Configuration, DefaultConfiguration, ColumnType, UnsupportedSqlDataType, TryFromRow};
use crate::{Odbc, OdbcError};
use crate::stats::{self, ConnectionOpenGuard};

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
    settings: Settings,
    _stats_guard: ConnectionOpenGuard,
}

/// Assuming drivers support sending Connection between threads.
unsafe impl Send for Connection {}

lazy_static! {
    static ref CONNECT_MUTEX: Mutex<()> = Mutex::new(());
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Connection")
            .field("settings", &self.settings)
            .finish()
    }
}

impl Connection {
    /// Connect to database using connection string with default configuration options.
    /// This implementation will synchronize driver connect calls.
    pub fn new(odbc: &'static Odbc, connection_string: &str) -> Result<Connection, OdbcError> {
        Self::with_settings(odbc, connection_string, Default::default())
    }

    /// Connect to database using connection string with default configuration options.
    /// Assume that driver connect call is thread safe.
    pub unsafe fn new_concurrent(odbc: &'static Odbc, connection_string: &str) -> Result<Connection, OdbcError> {
        Self::with_settings_concurrent(odbc, connection_string, Default::default())
    }

    /// Connect to database using connection string with configuration options.
    /// This implementation will synchronize driver connect calls.
    pub fn with_settings(
        odbc: &'static Odbc,
        connection_string: &str,
        settings: Settings,
    ) -> Result<Connection, OdbcError> {
        unsafe {
            let guard = CONNECT_MUTEX.lock().expect("Connection Mutex is poisoned!");
            let res = Self::with_settings_concurrent(odbc, connection_string, settings);
            drop(guard);
            res
        }
    }

    /// Connect to database using connection string with configuration options.
    /// Assume that driver connect call is thread safe.
    pub unsafe fn with_settings_concurrent(
        odbc: &'static Odbc,
        connection_string: &str,
        settings: Settings,
    ) -> Result<Connection, OdbcError> {
        odbc.environment
            .connect_with_connection_string(connection_string)
            .wrap_error_while("connecting to database")
            .map_err(Into::into)
            .map(|connection| {
                Connection {
                    connection,
                    settings,
                    _stats_guard: ConnectionOpenGuard::new(),
                }
            })
    }
}

/// Statically ensures that `Connection` can only be used after `ResultSet` was consumed to avoid runtime
/// errors.
///
/// Operations on `Connection` are only allowed after `ResultSet` was dropped.
/// Allocated `PreparedStatement` objects reference `Connection` directly so `Handle` can be still used to
/// query or allocate more `PreparedStatement` objects.
#[derive(Debug)]
pub struct Handle<'c, C: Configuration = DefaultConfiguration> {
    connection: &'c Connection,
    configuration: C,
}

impl<'c: 'c> Connection {
    pub fn handle(&'c mut self) -> Handle<'c, DefaultConfiguration> {
        Handle {
            connection: self,
            configuration: DefaultConfiguration,
        }
    }

    pub fn handle_with_configuration<C: Configuration>(&'c mut self, configuration: C) -> Handle<'c, C> {
        Handle {
            connection: self,
            configuration,
        }
    }
}

impl<'h, 'c: 'h, C: Configuration> Handle<'c, C> {
    pub fn with_configuration<CNew: Configuration>(&mut self, configuration: CNew) -> Handle<'c, CNew> {
        Handle {
            connection: self.connection,
            configuration,
        }
    }

    fn statement(&'h self) -> Result<Statement<'c, 'c, Allocated, NoResult>, OdbcError> {
        Statement::with_parent(&self.connection.connection)
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
    ) -> Result<ResultSet<'h, 'c, V, Executed, C>, QueryError>
    where
        V: TryFromRow<C>,
    {
        debug!("Getting ODBC tables");
        let statement = self.statement()?;

        let (result_set, stats_guard): (ResultSetState<'c, 'c, Allocated>, _) = stats::query_execution(move || {
            statement
                .tables_str(
                    catalog,
                    schema.unwrap_or(""),
                    table.unwrap_or(""),
                    table_type.unwrap_or(""),
                )
                .wrap_error_while("executing direct statement")
                .map(ResultSetState::Data)
        })?;

        Ok(ResultSet::from_result(
            self,
            result_set,
            stats_guard,
            &self.connection.settings,
            self.configuration.clone(),
        )?)
    }

    /// Prepare statement for fast execution and parametrization.
    /// For one-off queries it is more efficient to use `query()` function.
    pub fn prepare(&'h mut self, query: &str) -> Result<PreparedStatement<'c>, OdbcError> {
        debug!("Preparing ODBC query: {}", &query);

        let statement = stats::query_preparing(|| -> Result<_, OdbcError> {
            Ok(self
                .statement()?
                .prepare(query)
                .wrap_error_while("preparing query")?)
        })?;

        Ok(PreparedStatement(statement))
    }

    /// Execute one-off query.
    pub fn query<V>(&'h mut self, query: &str) -> Result<ResultSet<'h, 'c, V, Executed, C>, QueryError>
    where
        V: TryFromRow<C>,
    {
        self.query_with_parameters(query, Ok)
    }

    /// Execute one-off query with parameters.
    /// This creates prepared statement and binds values to it before execution.
    pub fn query_with_parameters<'t, V, F>(
        &'h mut self,
        query: &str,
        bind: F,
    ) -> Result<ResultSet<'h, 'c, V, Executed, C>, QueryError>
    where
        V: TryFromRow<C>,
        F: FnOnce(Binder<'c, 'c, Allocated>) -> Result<Binder<'c, 't, Allocated>, BindError>,
    {
        debug!("Direct ODBC query: {}", &query);

        let statement = stats::query_preparing(|| -> Result<_, QueryError> {
            //TODO: this take a long time potentially; can I reuse one for all direct queries?
            Ok(bind(self.statement()?.into())?.into_inner())
        })?;

        let (result_set, stats_guard) = stats::query_execution(move || {
            statement
                .exec_direct(query)
                .wrap_error_while("executing direct statement")
        })?;

        Ok(ResultSet::from_result(
            self,
            result_set,
            stats_guard,
            &self.connection.settings,
            self.configuration.clone(),
        )?)
    }

    /// Execute prepared statement without parameters.
    pub fn execute<V>(
        &'h mut self,
        statement: PreparedStatement<'c>,
    ) -> Result<ResultSet<'h, 'c, V, Prepared, C>, QueryError>
    where
        V: TryFromRow<C>,
    {
        self.execute_with_parameters(statement, Ok)
    }

    /// Bind parameters and execute prepared statement.
    pub fn execute_with_parameters<'t, V, F>(
        &'h mut self,
        statement: PreparedStatement<'c>,
        bind: F,
    ) -> Result<ResultSet<'h, 'c, V, Prepared, C>, QueryError>
    where
        V: TryFromRow<C>,
        F: FnOnce(Binder<'c, 'c, Prepared>) -> Result<Binder<'c, 't, Prepared>, BindError>,
    {
        let statement = stats::query_preparing(|| -> Result<_, QueryError> {
            Ok(bind(statement.0.into())?.into_inner())
        })?;

        let (result_set, stats_guard) = stats::query_execution(move || {
            statement
                .execute()
                .wrap_error_while("executing statement")
        })?;

        Ok(ResultSet::from_result(
            self,
            result_set,
            stats_guard,
            &self.connection.settings,
            self.configuration.clone(),
        )?)
    }

    /// Calls "START TRANSACTION"
    pub fn start_transaction(&mut self) -> Result<(), QueryError> {
        self.with_configuration(DefaultConfiguration).query::<()>("START TRANSACTION")?.no_result().unwrap();
        Ok(())
    }

    /// Calls "COMMIT"
    pub fn commit(&mut self) -> Result<(), QueryError> {
        self.with_configuration(DefaultConfiguration).query::<()>("COMMIT")?.no_result().unwrap();
        Ok(())
    }

    /// Calls "ROLLBACK"
    pub fn rollback(&mut self) -> Result<(), QueryError> {
        self.with_configuration(DefaultConfiguration).query::<()>("ROLLBACK")?.no_result().unwrap();
        Ok(())
    }

    /// Call function in transaction.
    /// If function returns Err the transaction will be rolled back otherwise committed.
    pub fn in_transaction<O, E>(
        &mut self,
        f: impl FnOnce(&mut Handle<'c, C>) -> Result<O, E>,
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
        f: impl FnOnce(&mut Handle<'c, C>) -> O,
    ) -> Result<O, QueryError> {
        self.commit()?;
        let ret = f(self);
        self.start_transaction()?;
        Ok(ret)
    }
}
