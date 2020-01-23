[![Latest Version]][crates.io] [![Documentation]][docs.rs] ![License]

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
* Full support for binary encoded NUMERICAL type (decimals) - current `Decimal` implementation depends on parsing string representation of the values (can be enabled with `rust_decimal` feature).
* Rest of this list - please open issue in `GitHub` issue tracker for missing functionality, bugs, etc..

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

[crates.io]: https://crates.io/crates/odbc-iter
[Latest Version]: https://img.shields.io/crates/v/odbc-iter.svg
[Documentation]: https://docs.rs/odbc-iter/badge.svg
[docs.rs]: https://docs.rs/odbc-iter
[License]: https://img.shields.io/crates/l/odbc-iter.svg
