/*!
`odbc-iter` is Rust high level database access library based on `odbc` crate.

With this library you can:
* connect to any database supporting ODBC (e.g. via `unixodbc` library and ODBC database driver),
* run one-off, prepared or parametrized queries,
* iterate result set via standard `Iterator` interface with automatically converted rows into tuples of Rust standard types or custom types by implementing a trait,
* create thread local connections for multithreaded applications.

Example usage
=============

Connect and run one-off queries with row type conversion
-------------

```rust
use odbc_iter::{Odbc, ValueRow};

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING").expect("DB_CONNECTION_STRING environment not set");
let mut connection = Odbc::connect(&connection_string).expect("failed to connect to database");

// Handle statically guards access to connection and provides query functionality
let mut db = connection.handle();

// Get single row single column value
println!("{}", db.query::<String>("SELECT 'hello world'").expect("failed to run query").single().expect("failed to fetch row"));

// Iterate rows with single column
for row in db.query::<String>("SELECT 'hello world' UNION SELECT 'foo bar'").expect("failed to run query") {
    println!("{}", row.expect("failed to fetch row"))
}
// Prints:
// hello world
// foo bar

// Iterate rows multiple columns
for row in db.query::<(String, i8)>("SELECT 'hello world', CAST(24 AS TINYINT) UNION SELECT 'foo bar', CAST(32 AS TINYINT)").expect("failed to run query") {
    let (string, number) = row.expect("failed to fetch row");
    println!("{} {}", string, number);
}
// Prints:
// hello world 24
// foo bar 32

// Iterate rows dynamic `ValueRow` type that can represent any supported database row
for row in db.query::<ValueRow>("SELECT 'hello world', 24 UNION SELECT 'foo bar', 32").expect("failed to run query") {
    println!("{:?}", row.expect("failed to fetch row"))
}
// Prints:
// [Some(String("hello world")), Some(Tinyint(24))]
// [Some(String("foo bar")), Some(Tinyint(32))]
```

Using prepared statements and parametrized queries
-------------

```rust
use odbc_iter::{Odbc, ValueRow};

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING").expect("DB_CONNECTION_STRING environment not set");
let mut connection = Odbc::connect(&connection_string).expect("failed to connect to database");

// Handle statically guards access to connection and provides query functionality
let mut db = connection.handle();

let prepared_statement = db
    .prepare("SELECT 'hello world' AS foo, CAST(42 AS INTEGER) AS bar, CAST(10000000 AS BIGINT) AS baz")
    .expect("prepare prepared_statement");

let parametrized_query = db
    .prepare("SELECT ?, ?, ?")
    .expect("prepare parametrized_query");


// Database can infer schema of prepared statement
println!("{:?}", prepared_statement.schema());
// Prints:
// Ok([ColumnType { value_type: String, nullable: false, name: "foo" }, ColumnType { value_type: Integer, nullable: true, name: "bar" }, ColumnType { value_type: Bigint, nullable: true, name: "baz" }])


// Execute prepared statement without binding parameters
let result_set = db
    .execute::<ValueRow>(prepared_statement)
    .expect("failed to run query");

// Note that in this example prepared_statement will be dropped with the result_set iterator and cannot be reused
for row in result_set {
    println!("{:?}", row.expect("failed to fetch row"))
}
// Prints:
// [Some(String("hello world")), Some(Integer(42)), Some(Bigint(10000000))]


// Execute parametrized query by binding parameters to statement
let mut result_set = db
    .execute_with_parameters::<ValueRow, _>(parametrized_query, |q| {
        q
            .bind(&"hello world")?
            .bind(&43)?
            .bind(&1_000_000)
    })
    .expect("failed to run query");

// Passing &mut reference so we don't loose access to result_set
for row in &mut result_set {
    println!("{:?}", row.expect("failed to fetch row"))
}
// Prints:
// [Some(String("hello world")), Some(Integer(43)), Some(Bigint(1000000))]

// Get back the statement for later use
let parametrized_query = result_set.close().expect("failed to close result set");

// Bind new set of parameters to prepared statement
let mut result_set = db
    .execute_with_parameters::<ValueRow, _>(parametrized_query, |q| {
        q
            .bind(&"foo bar")?
            .bind(&99)?
            .bind(&2_000_000)
    })
    .expect("failed to run query");

for row in &mut result_set {
    println!("{:?}", row.expect("failed to fetch row"))
}
// Prints:
// [Some(String("foo bar")), Some(Integer(99)), Some(Bigint(2000000))]
```

Using thread local connection
-------------
```rust
use odbc_iter::{Odbc, ValueRow};

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING").expect("DB_CONNECTION_STRING environment not set");

// `connection_with` can be used to create one connection per thread (assuming thread pool is used)
let result = odbc_iter::thread_local::connection_with(&connection_string, |mut connection| {
    // Provided object contains result of the connection operation; in case of error calling
    // `connection_with` again will result in new connection attempt
    let mut connection = connection.expect("failed to connect");

    // Handle statically guards access to connection and provides query functionality
    let mut db = connection.handle();

    // Get single row single column value
    let result = db.query::<String>("SELECT 'hello world'").expect("failed to run query").single().expect("failed to fetch row");

    // Return connection back to thread local so it can be reused later on along with the result of
    // the query that will be returned by the `connection_with` call
    // Returning None connection is useful to force reconnect on the next call e.g. after some error
    (Some(connection), result)
});

println!("{}", result);
// Prints:
// hello world

```

Converting column values to `chrono` crate's date and time types (with "chrono" feature)
-------------
```rust
# #[cfg(feature = "chrono")]
# {
use odbc_iter::{Odbc, ValueRow};
use chrono::NaiveDateTime;

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING").expect("DB_CONNECTION_STRING environment not set");
let mut connection = Odbc::connect(&connection_string).expect("failed to connect to database");

// Handle statically guards access to connection and provides query functionality
let mut db = connection.handle();

// Get single row single column value
println!("{}", db.query::<NaiveDateTime>("SELECT CAST('2019-05-03 13:21:33.749' AS DATETIME)").expect("failed to run query").single().expect("failed to fetch row"));
// Prints:
// 2019-05-03 13:21:33.750
# }

```

Converting column values to `JSON` with MonetDB (with "serde_json" feature)
-------------

TBD

Serializing `Value` and `ValueRow` using `serde` (with "serde" feature)
-------------

TBD

!*/

use error_context::prelude::*;
use lazy_static::lazy_static;
use log::{debug, log_enabled, trace};
use odbc::{
    Allocated, ColumnDescriptor, Connection as OdbcConnection, DiagnosticRecord, DriverInfo,
    Environment, NoResult, ResultSetState, Statement, Version3, OdbcType, ffi
};
use regex::Regex;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::string::FromUtf16Error;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;

pub use odbc::{SqlDate, SqlSsTime2, SqlTime, SqlTimestamp};
// ResultSet can be parametrized with this types
pub use odbc::{Executed, Prepared};

mod value;
pub use value::{Value, ValueType, AsNullable, NullableValue, TryFromValue};
mod value_row;
pub use value_row::{ValueRow, ColumnType, TryFromValueRow, };
pub mod thread_local;
pub mod odbc_type;

// TODO
// * Prepared statement cache:
// ** db.with_statement_cache() -> StatementCache
// ** sc.query(str) - direct query
// ** sc.query_prepared(impl ToString + Hash) - hash fist and look up in cache if found execute; .to_string otherwise and prepare + execute;
//    this is to avoid building query strings where we know hash e.g. from some other value than query string itself
// ** sc.clear() - try close the statement and clear the cache
// * MultiConnection - special handle that does not require mutable reference to query but will automatically crate and manage connections if one is already busy
// ** Connections behind RefCell, get Handle for each query
// ** If connection RefCell is busy crate check next connection in the pool or add new one if all are busy
// ** This will require statement cache per connection to support prepared statements as they have to be managed per connection

/// ODBC library initialization and connection errors.
#[derive(Debug)]
pub struct OdbcError(Option<DiagnosticRecord>, &'static str);

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

/// Errors related to execution of queries.
///
/// `OdbcError` and `DataAccessError` can be converted into `QueryError`.
#[derive(Debug)]
pub enum QueryError {
    OdbcError(OdbcError),
    BindError(DiagnosticRecord),
    UnsupportedSqlDataType(UnsupportedSqlDataType),
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

impl From<UnsupportedSqlDataType> for QueryError {
    fn from(err: UnsupportedSqlDataType) -> QueryError {
        QueryError::UnsupportedSqlDataType(err)
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

/// Errors related to data access of query result set.
///
/// This error can happen when iterating rows of executed query result set.
/// For convenience this error can be converted into `QueryError`.
#[derive(Debug)]
pub enum DataAccessError {
    OdbcError(DiagnosticRecord, &'static str),
    OdbcCursorError(DiagnosticRecord),
    UnsupportedSqlDataType(UnsupportedSqlDataType),
    FromRowError(Box<dyn Error>),
    FromUtf16Error(FromUtf16Error, &'static str),
    UnexpectedNumberOfRows(&'static str),
    #[cfg(feature = "serde_json")]
    JsonError(serde_json::Error),
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
            DataAccessError::UnsupportedSqlDataType(_) => {
                write!(f, "failed to handle data type conversion")
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
            DataAccessError::UnsupportedSqlDataType(err) => Some(err),
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

impl From<UnsupportedSqlDataType> for DataAccessError {
    fn from(err: UnsupportedSqlDataType) -> DataAccessError {
        DataAccessError::UnsupportedSqlDataType(err)
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

/// This error can be returned if database provided column of type that currently cannot be mapped to `Value` type.
#[derive(Debug)]
pub struct UnsupportedSqlDataType(ffi::SqlDataType);

impl fmt::Display for UnsupportedSqlDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "unsupported SQL data type: {:?}", self.0)
    }
}

impl Error for UnsupportedSqlDataType {}

impl TryFrom<ColumnDescriptor> for ColumnType {
    type Error = UnsupportedSqlDataType;

    fn try_from(column_descriptor: ColumnDescriptor) -> Result<ColumnType, UnsupportedSqlDataType> {
        use odbc::ffi::SqlDataType::*;
        let value_type = match column_descriptor.data_type {
            SQL_EXT_BIT => ValueType::Bit,
            SQL_EXT_TINYINT => ValueType::Tinyint,
            SQL_SMALLINT => ValueType::Smallint,
            SQL_INTEGER => ValueType::Integer,
            SQL_EXT_BIGINT => ValueType::Bigint,
            SQL_FLOAT | SQL_REAL => ValueType::Float,
            SQL_DOUBLE => ValueType::Double,
            SQL_CHAR | SQL_VARCHAR | SQL_EXT_LONGVARCHAR | SQL_EXT_WCHAR | SQL_EXT_WVARCHAR
            | SQL_EXT_WLONGVARCHAR => ValueType::String,
            SQL_TIMESTAMP => ValueType::Timestamp,
            SQL_DATE => ValueType::Date,
            SQL_TIME | SQL_SS_TIME2 => ValueType::Time,
            SQL_UNKNOWN_TYPE => {
                #[cfg(feature = "serde_json")]
                {
                    ValueType::Json
                }
                #[cfg(not(feature = "serde_json"))]
                {
                    ValueType::String
                }
            }
            _ => return Err(UnsupportedSqlDataType(column_descriptor.data_type)),
        };

        Ok(ColumnType {
            value_type,
            nullable: column_descriptor.nullable.unwrap_or(true),
            name: column_descriptor.name,
        })
    }
}

/// Iterator over result set rows.
///
/// Items of this iterator can be of any type that implements `TryFromValueRow` that includes common Rust types and tuples.
pub struct ResultSet<'h, 'c, V, S> {
    statement: Option<ExecutedStatement<'c, S>>,
    odbc_schema: Vec<ColumnDescriptor>,
    schema: Vec<ColumnType>,
    columns: i16,
    utf_16_strings: bool,
    phantom: PhantomData<&'h V>,
}

impl<'h, 'c, V, S> fmt::Debug for ResultSet<'h, 'c, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ResultSet")
            .field("odbc_schema", &self.odbc_schema)
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
    fn from_result(
        _handle: &'h Handle<'c>,
        result: ResultSetState<'c, '_, S>,
        utf_16_strings: bool,
    ) -> Result<ResultSet<'h, 'c, V, S>, QueryError> {
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
            .iter()
            .map(|c| ColumnType::try_from(c.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResultSet {
            statement: Some(statement),
            odbc_schema,
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
                                _ => Err(UnsupportedSqlDataType(column_descriptor.data_type))?,
                            })
                        })
                        .collect::<Result<Vec<Option<Value>>, _>>(),
                )
            }
            Ok(None) => None,
        }
        .map(|v| v.and_then(|v| {
            // Verify that value types match schema
            debug_assert!(v.iter().map(|v| v.as_ref().map(|v| v.value_type())).zip(self.schema()).all(|(v, s)| if let Some(v) = v { v == s.value_type } else { true }));
            TryFromValueRow::try_from_row(v, self.schema()).map_err(|err| DataAccessError::FromRowError(Box::new(err)))
        }))
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

/// ODBC prepared statement.
pub struct PreparedStatement<'h>(Statement<'h, 'h, odbc::Prepared, odbc::NoResult>);

impl<'h> fmt::Debug for PreparedStatement<'h> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut d = f.debug_struct("PreparedStatement");

        let schema = (1..(self.0.num_result_cols().map_err(|_| std::fmt::Error)? + 1))
            .into_iter()
            .map(|i| self.0.describe_col(i as u16))
            .collect::<Result<Vec<ColumnDescriptor>, _>>()
            .map_err(|_| std::fmt::Error)?;

        d.field("odbc_schema", &schema);
        d.finish()
    }
}

impl<'h> PreparedStatement<'h> {
    /// Query schema information deduced from prepared statement SQL text.
    pub fn schema(&self) -> Result<Vec<ColumnType>, QueryError> {
        (1..self.columns()? + 1)
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

/// ODBC environment entry point.
///
/// There should be only one object of this type in your program.
/// It is stored as global static and accessed via associated static functions.
pub struct Odbc {
    environment: Environment<Version3>,
}

/// "The ODBC Specification indicates that an external application or process should use a single environment handle
/// that is shared by local threads. The threads share the environment handle by using it as a common resource
/// for allocating individual connection handles." (http://www.firstsql.com/ithread5.htm)
/// lazy_static will make sure only one environment is initialized.
unsafe impl Sync for Odbc {}

lazy_static! {
    static ref ODBC: Odbc = Odbc::new().expect("Failed to initialize ODBC environment");
}

/// We need to allow mutable environment to be used to list drivers but only one environment should exist at the same time.
static ODBC_INIT: AtomicBool = AtomicBool::new(false);

impl fmt::Debug for Odbc {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Odbc").field("version", &3).finish()
    }
}

impl Odbc {
    fn new() -> Result<Odbc, OdbcError> {
        if ODBC_INIT.compare_and_swap(false, true, atomic::Ordering::SeqCst) {
            panic!("ODBC environment already initialised");
        }

        let ret = odbc::create_environment_v3()
            .wrap_error_while("creating v3 environment")
            .map_err(Into::into)
            .map(|environment| Odbc { environment });

        ret
    }

    /// Initialize global static ODBC environment now.
    /// After this was called call to `list_drivers()` will panic.
    /// Connecting to a database will also initialize the environment.
    /// This function will panic if there was a problem crating ODBC environment.
    pub fn initialize() {
        lazy_static::initialize(&ODBC);
    }

    /// Provides list of `DriverInfo` structures describing available ODBC drivers.
    /// This will panic if ODBC was already initialized by `Odbc::connect()` or `Odbc::initialize()`.
    pub fn list_drivers() -> Result<Vec<DriverInfo>, OdbcError> {
        // we need mutable access to environment
        let mut odbc = Odbc::new()?;
        let ret = odbc
            .environment
            .drivers()
            .wrap_error_while("listing drivers")
            .map_err(Into::into);

        // Drop Odbc after providing list of drivers so we can allocate static singleton
        drop(odbc);
        ODBC_INIT.store(false, atomic::Ordering::SeqCst);

        ret
    }

    /// Connect to database using connection string with default configuration options.
    pub fn connect(connection_string: &str) -> Result<Connection, OdbcError> {
        Self::connect_with_options(
            connection_string,
            Options {
                utf_16_strings: false,
            },
        )
    }

    /// Connect to database using connection string with configuration options.
    pub fn connect_with_options(
        connection_string: &str,
        options: Options,
    ) -> Result<Connection, OdbcError> {
        ODBC.environment
            .connect_with_connection_string(connection_string)
            .wrap_error_while("connecting to database")
            .map_err(Into::into)
            .map(|connection| Connection {
                connection,
                utf_16_strings: options.utf_16_strings,
            })
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

        ResultSet::from_result(self, result_set, self.0.utf_16_strings)
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
        self.query_with_parameters(query, |b| Ok(b))
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

        ResultSet::from_result(
            self,
            statement
                .exec_direct(query)
                .wrap_error_while("executing direct statement")?,
            self.0.utf_16_strings,
        )
    }

    /// Execute prepared statement without parameters.
    pub fn execute<V>(
        &'h mut self,
        statement: PreparedStatement<'c>,
    ) -> Result<ResultSet<'h, 'c, V, Prepared>, QueryError>
    where
        V: TryFromValueRow,
    {
        self.execute_with_parameters(statement, |b| Ok(b))
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

        ResultSet::from_result(
            self,
            statement
                .execute()
                .wrap_error_while("executing statement")?,
            self.0.utf_16_strings,
        )
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

/// Error splitting SQL script into single queries.
#[derive(Debug)]
pub struct SplitQueriesError;

impl fmt::Display for SplitQueriesError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to split queries")
    }
}

impl Error for SplitQueriesError {}

/// Split SQL script into list of queries.
/// Each query needs to be terminated with semicolon (";").
/// Lines starting with two dashes ("--") are skipped.
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

    #[cfg(feature = "test-sql-server")]
    pub fn connect_sql_server() -> Connection {
        Odbc::connect(sql_server_connection_string().as_str()).expect("connect to SQL Server")
    }

    #[cfg(feature = "test-sql-server")]
    pub fn connect_sql_server_with_options(options: Options) -> Connection {
        Odbc::connect_with_options(sql_server_connection_string().as_str(), options)
            .expect("connect to SQL ServerMonetDB")
    }

    #[cfg(feature = "test-hive")]
    pub fn hive_connection_string() -> String {
        std::env::var("HIVE_ODBC_CONNECTION").expect("HIVE_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-hive")]
    pub fn connect_hive() -> Connection {
        Odbc::connect(hive_connection_string().as_str()).expect("connect to Hive")
    }

    #[cfg(feature = "test-hive")]
    pub fn connect_hive_with_options(options: Options) -> Connection {
        Odbc::connect_with_options(hive_connection_string().as_str(), options)
            .expect("connect to Hive")
    }

    #[cfg(feature = "test-monetdb")]
    pub fn monetdb_connection_string() -> String {
        std::env::var("MONETDB_ODBC_CONNECTION").expect("MONETDB_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-monetdb")]
    pub fn connect_monetdb() -> Connection {
        Odbc::connect(monetdb_connection_string().as_str()).expect("connect to MonetDB")
    }

    #[cfg(feature = "test-monetdb")]
    pub fn connect_monetdb_with_options(options: Options) -> Connection {
        Odbc::connect_with_options(monetdb_connection_string().as_str(), options)
            .expect("connect to MonetDB")
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_multiple_rows() {
        let mut hive = connect_hive();

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
        let mut hive = connect_hive();

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
        let mut hive = connect_hive();

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
        let mut hive = connect_hive();

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
        let mut hive = connect_hive();

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
        let mut connection = connect_sql_server();

        let data = connection.handle()
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
        let mut connection = connect_sql_server();

        let data = connection
            .handle()
            .query::<ValueRow>(
                "SELECT '', cast('' AS NVARCHAR), cast('' AS TEXT), cast('' AS NTEXT);",
            )
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
        let mut monetdb = crate::tests::connect_monetdb();;

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
        let mut monetdb = crate::tests::connect_monetdb();;

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
        let mut hive = connect_hive();

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
        let mut hive = connect_hive();

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
        let mut connection = connect_sql_server();

        let data = connection
            .handle()
            .tables::<ValueRow>("master", Some("sys"), None, Some("view"))
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert!(data.len() > 0);
    }

    #[cfg(feature = "chrono")]
    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_date() {
        let mut connection = connect_sql_server();

        let data = connection
            .handle()
            .query::<ValueRow>("SELECT cast('2018-08-24' AS DATE) AS date")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(&data[0][0], Some(date @ Value::Date(_)) => assert_eq!(&date.to_naive_date().unwrap().to_string(), "2018-08-24"));
    }

    #[cfg(feature = "chrono")]
    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_time() {
        let mut connection = connect_sql_server();

        let data = connection
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
        let mut connection = connect_sql_server();

        let mut db = connection.handle();

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
        let mut connection = connect_sql_server();

        let mut db = connection.handle();

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
        let mut connection = connect_sql_server();

        let val = 42;

        let value: Value = connection
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
        let mut connection = connect_sql_server();

        let val = [42, 24, 32, 666];

        let data: Vec<ValueRow> = connection
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
        let mut connection = connect_sql_server();

        let val = [42, 24, 32, 666];

        let mut handle = connection.handle();

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
        let mut connection = connect_sql_server();

        let statement = connection
            .handle()
            .prepare("SELECT ?, ?, ?, ? AS val;")
            .expect("prepare statement");

        assert_eq!(statement.columns().unwrap(), 4);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_prepared_schema() {
        let mut connection = connect_sql_server();

        let statement = connection
            .handle()
            .prepare("SELECT ?, CAST(? as INTEGER) as foo, ?, ? AS val;")
            .expect("prepare statement");

        let schema = statement.schema().unwrap();
        assert_eq!(schema.len(), 4);
        assert_eq!(schema[1].nullable, true);
        assert_eq!(schema[1].value_type, ValueType::Integer);
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_empty_data_set() {
        let mut hive = connect_hive();

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
        let mut connection = connect_sql_server();

        let data = connection
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
        let mut hive = connect_hive();

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
        let mut monetdb = crate::tests::connect_monetdb();

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
        let mut connection = connect_sql_server_with_options(Options {
            utf_16_strings: true,
        });

        let utf_16_string = LONG_STRING.encode_utf16().collect::<Vec<u16>>();

        let mut handle = connection.handle();

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
        let mut hive = connect_hive_with_options(Options {
            utf_16_strings: true,
        });

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
        let mut monetdb = connect_monetdb_with_options(Options {
            utf_16_strings: true,
        });

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

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_debug() {
        let mut connection = connect_sql_server_with_options(Options {
            utf_16_strings: true,
        });

        assert_eq!(
            format!("{:?}", connection),
            "Connection { utf_16_strings: true }"
        );

        let utf_16_string = LONG_STRING.encode_utf16().collect::<Vec<u16>>();

        let mut handle = connection.handle();
        assert_eq!(
            format!("{:?}", handle),
            "Handle(Connection { utf_16_strings: true })"
        );

        let statement = handle
            .prepare("SELECT ? AS foo, ? AS bar, ? AS baz;")
            .expect("prepare statement");

        assert_eq!(format!("{:?}", statement), "PreparedStatement { odbc_schema: [ColumnDescriptor { name: \"foo\", data_type: SQL_VARCHAR, column_size: Some(4000), decimal_digits: None, nullable: Some(true) }, ColumnDescriptor { name: \"bar\", data_type: SQL_VARCHAR, column_size: Some(4000), decimal_digits: None, nullable: Some(true) }, ColumnDescriptor { name: \"baz\", data_type: SQL_VARCHAR, column_size: Some(4000), decimal_digits: None, nullable: Some(true) }] }");

        let result_set = handle
            .execute_with_parameters::<ValueRow, _>(statement, |q| {
                let q = q.bind(&utf_16_string)?;
                assert_eq!(format!("{:?}", q), "Binder { index: 1 }");
                q.bind(&12)?.bind(&true)
            })
            .expect("failed to run query");

        assert_eq!(format!("{:?}", result_set), "ResultSet { odbc_schema: [ColumnDescriptor { name: \"foo\", data_type: SQL_EXT_WVARCHAR, column_size: Some(1200), decimal_digits: None, nullable: Some(true) }, ColumnDescriptor { name: \"bar\", data_type: SQL_INTEGER, column_size: Some(10), decimal_digits: None, nullable: Some(true) }, ColumnDescriptor { name: \"baz\", data_type: SQL_EXT_BIT, column_size: Some(1), decimal_digits: None, nullable: Some(true) }], schema: [ColumnType { value_type: String, nullable: true, name: \"foo\" }, ColumnType { value_type: Integer, nullable: true, name: \"bar\" }, ColumnType { value_type: Bit, nullable: true, name: \"baz\" }], columns: 3, utf_16_strings: true }");
    }
}
