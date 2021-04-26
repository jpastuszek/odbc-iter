/*!
`odbc-iter` is a Rust high level database access library based on `odbc` crate that uses native ODBC drivers to access a variety of databases.

With this library you can:
* connect to any database supporting ODBC standard (e.g. via `unixodbc` library and ODBC database driver),
* run one-off, prepared or parametrized queries,
* iterate result set via standard `Iterator` interface,
* automatically convert rows into:
    * tuples of Rust standard types,
    * custom type implementing a trait,
    * vector of dynamically typed values,
* create thread local connections for multithreaded applications.

Things still missing:
* support for `DECIMAL` types - currently `DECIMAL` columns need to be cast to `DOUBLE` on the query (PR welcome),
* rest of this list - please open issue in `GitHub` issue tracker for missing functionality, bugs, etc..

Example usage
=============

Connect and run one-off queries with row type conversion
-------------

```rust
use odbc_iter::{Odbc, ValueRow};

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING")
    .expect("DB_CONNECTION_STRING environment not set");
let mut connection = Odbc::connect(&connection_string)
    .expect("failed to connect to database");

// Handle statically guards access to connection and provides query functionality
let mut db = connection.handle();

// Get single row single column value
println!("{}", db.query::<String>("SELECT 'hello world'").expect("failed to run query")
    .single().expect("failed to fetch row"));

// Iterate rows with single column
for row in db.query::<String>("SELECT 'hello world' UNION SELECT 'foo bar'")
    .expect("failed to run query") {
    println!("{}", row.expect("failed to fetch row"))
}
// Prints:
// hello world
// foo bar

// Iterate rows with multiple columns
for row in db.query::<(String, i8)>(
    "SELECT 'hello world', CAST(24 AS TINYINT) UNION SELECT 'foo bar', CAST(32 AS TINYINT)")
    .expect("failed to run query") {
    let (string, number) = row.expect("failed to fetch row");
    println!("{} {}", string, number);
}
// Prints:
// hello world 24
// foo bar 32

// Iterate rows with dynamically typed values using `ValueRow` type that can represent
// any row
for row in db.query::<ValueRow>("SELECT 'hello world', 24 UNION SELECT 'foo bar', 32")
    .expect("failed to run query") {
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
let connection_string = std::env::var("DB_CONNECTION_STRING")
    .expect("DB_CONNECTION_STRING environment not set");
let mut connection = Odbc::connect(&connection_string)
    .expect("failed to connect to database");

// Handle statically guards access to connection and provides query functionality
let mut db = connection.handle();

// Allocate `PreparedStatement` on given connection
let prepared_statement = db
    .prepare("SELECT 'hello world' AS foo, CAST(42 AS INTEGER) AS bar, CAST(10000000 AS BIGINT) AS baz")
    .expect("prepare prepared_statement");

// Use `?` as placeholder for value
let parametrized_query = db
    .prepare("SELECT ?, ?, ?")
    .expect("prepare parametrized_query");

// Database can infer schema of prepared statement
println!("{:?}", prepared_statement.schema());
// Prints:
// Ok([ColumnType { datum_type: String, odbc_type: SQL_VARCHAR, nullable: false, name: "foo" },
// ColumnType { datum_type: Integer, odbc_type: SQL_INTEGER, nullable: true, name: "bar" },
// ColumnType { datum_type: Bigint, odbc_type: SQL_EXT_BIGINT, nullable: true, name: "baz" }])

// Execute prepared statement without binding parameters
let result_set = db
    .execute::<ValueRow>(prepared_statement)
    .expect("failed to run query");

// Note that in this example `prepared_statement` will be dropped with the `result_set`
// iterator and cannot be reused
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

// Passing `&mut` reference so we don't lose access to `result_set`
for row in &mut result_set {
    println!("{:?}", row.expect("failed to fetch row"))
}
// Prints:
// [Some(String("hello world")), Some(Integer(43)), Some(Bigint(1000000))]

// Get back the statement for later use dropping any unconsumed rows
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
let connection_string = std::env::var("DB_CONNECTION_STRING")
    .expect("DB_CONNECTION_STRING environment not set");

// `connection_with` can be used to create one connection per thread
let result = odbc_iter::thread_local::connection_with(&connection_string, |mut connection| {
    // Provided object contains result of the connection operation
    // in case of error calling `connection_with` again will result
    // in new connection attempt
    let mut connection = connection.expect("failed to connect");

    // Handle statically guards access to connection and provides query functionality
    let mut db = connection.handle();

    // Get single row single column value
    let result = db.query::<String>("SELECT 'hello world'")
        .expect("failed to run query").single().expect("failed to fetch row");

    // Return connection back to thread local so it can be reused later on along
    // with the result of the query that will be returned by the `connection_with` call
    // Returning `None` connection is useful to force new connection attempt on the
    // next call
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
let connection_string = std::env::var("DB_CONNECTION_STRING")
    .expect("DB_CONNECTION_STRING environment not set");
let mut connection = Odbc::connect(&connection_string)
    .expect("failed to connect to database");

// Handle statically guards access to connection and provides query functionality
let mut db = connection.handle();

// Get `chrono::NaiveDateTime` value
println!("{}", db.query::<NaiveDateTime>("SELECT CAST('2019-05-03 13:21:33.749' AS DATETIME2)")
    .expect("failed to run query").single().expect("failed to fetch row"));
// Prints:
// 2019-05-03 13:21:33.749
# }
```

Query JSON column from MonetDB (with "serde_json" feature)
-------------

```rust
# #[cfg(feature = "serde_json")]
# #[cfg(feature = "test-monetdb")]
# {
use odbc_iter::{Odbc, Value};

// Connect to database using connection string
let connection_string = std::env::var("MONETDB_ODBC_CONNECTION")
    .expect("MONETDB_ODBC_CONNECTION environment not set");
let mut connection = Odbc::connect(&connection_string)
    .expect("failed to connect to database");

// Handle statically guards access to connection and provides query functionality
let mut db = connection.handle();

// Get `Value::Json` variant containing `serde_json::Value` object
println!("{}", db.query::<Value>(r#"SELECT CAST('{ "foo": 42 }' AS JSON)"#)
    .expect("failed to run query").single().expect("failed to fetch row"));
// Prints:
// {"foo":42}
# }
```

Serializing `Value` and `ValueRow` using `serde` to JSON string (with "serde" feature)
-------------

```rust
# #[cfg(feature = "serde_json")]
# #[cfg(feature = "serde")]
# {
use odbc_iter::{Odbc, ValueRow};

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING")
    .expect("DB_CONNECTION_STRING environment not set");
let mut connection = Odbc::connect(&connection_string)
    .expect("failed to connect to database");

// Handle statically guards access to connection and provides query functionality
let mut db = connection.handle();

// Get `ValueRow` (or just single `Value`) that implements `serde::Serialize` trait
let row = db.query::<ValueRow>("SELECT 'hello world', CAST(42 AS INTEGER), CAST(10000000 AS BIGINT)")
    .expect("failed to run query").single().expect("failed to fetch row");

println!("{}", serde_json::to_string(&row).expect("failed to serialize"));
// Prints:
// ["hello world",42,10000000]
# }
```

UTF-16 databases (e.g. SQL Server)
=============

With SQL Server `NVARCHAR` data cannot be passed via query text (`N"foo"`) as query text itself is encoded as Rust String and hence UTF-8 and not UTF-16 as expected by SQL Server.

To correctly query `NVARCHAR` columns as `String` connection has to be configured like this:
```rust
use odbc_iter::{Odbc, Settings, ValueRow};

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING")
    .expect("DB_CONNECTION_STRING environment not set");

let mut connection = Odbc::connect_with_settings(&connection_string, Settings {
    utf_16_strings: true,
}).expect("failed to connect to database");
```

To correctly insert `NVARCHAR` column value, the `String` has to be cast to UTF-16 and bound as `&[u16]`:
```rust
use odbc_iter::{Odbc, Settings, ValueRow};

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING")
    .expect("DB_CONNECTION_STRING environment not set");

let mut connection = Odbc::connect_with_settings(&connection_string, Settings {
    utf_16_strings: true,
}).expect("failed to connect to database");

let mut db = connection.handle();

let utf_16_string = "Fóó".encode_utf16().collect::<Vec<u16>>();
let data: ValueRow = db.query_with_parameters("SELECT ? AS val", |q| q.bind(&utf_16_string))
    .expect("failed to run query")
    .single()
    .expect("fetch data");
```

Alternatively the provided `StringUtf16` type can be bound (implementes Deserialize and custom Debug):
```rust
use odbc_iter::{Odbc, Settings, ValueRow, StringUtf16};

// Connect to database using connection string
let connection_string = std::env::var("DB_CONNECTION_STRING")
    .expect("DB_CONNECTION_STRING environment not set");

let mut connection = Odbc::connect_with_settings(&connection_string, Settings {
    utf_16_strings: true,
}).expect("failed to connect to database");

let mut db = connection.handle();

let utf_16_string = StringUtf16::from("Fóó");
let data: ValueRow = db.query_with_parameters("SELECT ? AS val", |q| q.bind(&utf_16_string))
    .expect("failed to run query")
    .single()
    .expect("fetch data");
```

Runtime statistics (with "statistics" feature)
-------------

If enabled, function `odbc_iter::statistics()` will provide runtime statistics that can be `Display`ed.


`ODBC statistics: connections: open: 5, queries: preparing: 2, executing: 1, fetching: 2, done: 5, failed: 0`

Note that they are not strongly synchronised so things may be observed counted twice.

!*/

use error_context::prelude::*;
use lazy_static::lazy_static;
use odbc::{DiagnosticRecord, DriverInfo, Environment, Version3};
use regex::Regex;
use std::error::Error;
use std::fmt;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;

// Extra types that can be queried
pub use odbc::{SqlDate, SqlSsTime2, SqlTime, SqlTimestamp};
// ResultSet can be parametrized with this types
pub use odbc::{Executed, Prepared};

mod query;
pub use query::*;
mod result_set;
pub use result_set::*;
mod row;
pub use row::*;
mod value;
pub use value::*;
mod value_row;
pub use value_row::*;
mod stats;
#[cfg(feature = "statistics")]
pub use stats::statistics;

pub mod odbc_type;
pub mod thread_local;

pub use odbc_type::StringUtf16;

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
        if ODBC_INIT.compare_exchange(false, true, atomic::Ordering::SeqCst, atomic::Ordering::SeqCst).is_err() {
            panic!("ODBC environment already initialised");
        }

        odbc::create_environment_v3()
            .wrap_error_while("creating v3 environment")
            .map_err(Into::into)
            .map(|environment| Odbc { environment })
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
    /// This implementation will synchronize driver connect calls.
    pub fn connect(connection_string: &str) -> Result<Connection, OdbcError> {
        Connection::new(&ODBC, connection_string)
    }

    /// Connect to database using connection string with default configuration options.
    /// Assume that driver connect call is thread safe.
    pub unsafe fn connect_concurrent(connection_string: &str) -> Result<Connection, OdbcError> {
        Connection::new_concurrent(&ODBC, connection_string)
    }

    /// Connect to database using connection string with configuration options.
    /// This implementation will synchronize driver connect calls.
    pub fn connect_with_settings(
        connection_string: &str,
        settings: Settings,
    ) -> Result<Connection, OdbcError> {
        Connection::with_settings(&ODBC, connection_string, settings)
    }

    /// Connect to database using connection string with configuration options.
    /// Assume that driver connect call is thread safe.
    pub unsafe fn connect_with_settings_concurrent(
        connection_string: &str,
        settings: Settings,
    ) -> Result<Connection, OdbcError> {
        Connection::with_settings_concurrent(&ODBC, connection_string, settings)
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
    pub fn connect_sql_server_with_settings(settings: Settings) -> Connection {
        Odbc::connect_with_settings(sql_server_connection_string().as_str(), settings)
            .expect("connect to SQL ServerMonetDB")
    }

    #[cfg(feature = "test-hive")]
    pub fn hive_connection_string() -> String {
        std::env::var("HIVE_ODBC_CONNECTION").expect("HIVE_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-hive")]
    pub fn connect_hive() -> Connection {
        unsafe {
            Odbc::connect_concurrent(hive_connection_string().as_str()).expect("connect to Hive")
        }
    }

    #[cfg(feature = "test-hive")]
    pub fn connect_hive_with_settings(settings: Settings) -> Connection {
        unsafe {
            Odbc::connect_with_settings_concurrent(hive_connection_string().as_str(), settings)
                .expect("connect to Hive")
        }
    }

    #[cfg(feature = "test-monetdb")]
    pub fn monetdb_connection_string() -> String {
        std::env::var("MONETDB_ODBC_CONNECTION").expect("MONETDB_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-monetdb")]
    pub fn connect_monetdb() -> Connection {
        unsafe {
            Odbc::connect_concurrent(monetdb_connection_string().as_str()).expect("connect to MonetDB")
        }
    }

    #[cfg(feature = "test-monetdb")]
    pub fn connect_monetdb_with_settings(settings: Settings) -> Connection {
        unsafe {
            Odbc::connect_with_settings_concurrent(monetdb_connection_string().as_str(), settings)
                .expect("connect to MonetDB")
        }
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
        let mut monetdb = crate::tests::connect_monetdb();

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
        let mut monetdb = crate::tests::connect_monetdb();

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

    #[cfg(all(feature = "test-monetdb", feature = "rust_decimal"))]
    #[test]
    fn test_moentdb_types_decimal() {
        let mut monetdb = crate::tests::connect_monetdb();

        let data = monetdb
            .handle()
            .query::<ValueRow>("SELECT 10.9231213232423424324")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Decimal(ref dec)) => assert_eq!(dec.to_string(), "10.9231213232423424324"));
    }

    #[cfg(all(feature = "test-sql-server", feature = "rust_decimal"))]
    #[test]
    fn test_sql_server_types_decimal() {
        let mut connection = connect_sql_server();

        let data = connection
            .handle()
            .query::<ValueRow>("SELECT 10.9231213232423424324")
            .expect("failed to run query")
            .collect::<Result<Vec<_>, _>>()
            .expect("fetch data");

        assert_matches!(data[0][0], Some(Value::Decimal(ref dec)) => assert_eq!(dec.to_string(), "10.9231213232423424324"));
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
        assert_eq!(schema[1].datum_type, DatumType::Integer);
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
        let mut connection = connect_sql_server_with_settings(Settings {
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

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_long_string_fetch_utf_16_bind_string_utf_16() {
        let mut connection = connect_sql_server_with_settings(Settings {
            utf_16_strings: true,
        });

        let utf_16_string = StringUtf16::from(LONG_STRING);

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
        let mut hive = connect_hive_with_settings(Settings {
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
        let mut monetdb = connect_monetdb_with_settings(Settings {
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
        let mut connection = connect_sql_server_with_settings(Settings {
            utf_16_strings: true,
        });

        assert_eq!(
            format!("{:?}", connection),
            "Connection { settings: Settings { utf_16_strings: true } }"
        );

        let utf_16_string = LONG_STRING.encode_utf16().collect::<Vec<u16>>();

        let mut handle = connection.handle();
        assert_eq!(
            format!("{:?}", handle),
            "Handle { connection: Connection { settings: Settings { utf_16_strings: true } }, configuration: DefaultConfiguration }"
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

        assert_eq!(format!("{:?}", result_set), "ResultSet { schema: [ColumnType { datum_type: String, odbc_type: SQL_EXT_WVARCHAR, nullable: true, name: \"foo\" }, ColumnType { datum_type: Integer, odbc_type: SQL_INTEGER, nullable: true, name: \"bar\" }, ColumnType { datum_type: Bit, odbc_type: SQL_EXT_BIT, nullable: true, name: \"baz\" }], columns: 3, settings: Settings { utf_16_strings: true }, configuration: DefaultConfiguration }");
    }
}
