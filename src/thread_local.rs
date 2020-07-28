use log::debug;
use std::cell::RefCell;

use crate::query::Connection;
use crate::{Odbc, OdbcError};

thread_local! {
    static DB: RefCell<Option<Connection>> = RefCell::new(None);
}

/// Access to thread local connection.
///
/// Provided closure will receive the `Connection` object and may return it for reuse by another call or drop it to force new connection to be established to database on next call.
///
/// If there was an error during connection it is provided to the closure. Next call will attempt to connect again and a new error may be provided.
///
/// `connection_string` is used only when making new `Connection` object initially, after error or after old `Connection` object was dropped.
pub fn connection_with<O>(
    connection_string: &str,
    f: impl Fn(Result<Connection, OdbcError>) -> (Option<Connection>, O),
) -> O {
    initialized_connection_with(connection_string, |_| Ok(()), f)
}

/// Access to thread local connection with connection initalization.
///
/// Like `connection_with` but also takes `init` closure that is executed once when new connection was
/// successfully established. This allows for execution of connection configuration queries.
///
/// If `init` returns and error it is passed to the second closure.
pub fn initialized_connection_with<O>(
    connection_string: &str,
    init: impl Fn(&mut Connection) -> Result<(), OdbcError>,
    f: impl Fn(Result<Connection, OdbcError>) -> (Option<Connection>, O),
) -> O {
    DB.with(|db| {
        let connection;

        let conn = db.borrow_mut().take();
        match conn {
            Some(conn) => connection = conn,
            None => {
                let id = std::thread::current().id();
                debug!("[{:?}] Connecting to database: {}", id, &connection_string);

                match Odbc::connect(&connection_string)
                    .and_then(|mut conn| init(&mut conn).map(|_| conn)) {
                    Ok(conn) => {
                        connection = conn;
                    }
                    Err(err) => return f(Err(err)).1,
                }
            }
        }

        let (connection, o) = f(Ok(connection));
        *db.borrow_mut() = connection;
        o
    })
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use crate::*;
    #[allow(unused_imports)]
    use assert_matches::assert_matches;

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_connection_with() {
        connection_with(
            crate::tests::monetdb_connection_string().as_str(),
            |result| {
                let mut monetdb = result.expect("connect to MonetDB");
                let data = monetdb
                    .handle()
                    .query::<ValueRow>("SELECT 'foo'")
                    .expect("failed to run query")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("fetch data");

                assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
                (Some(monetdb), ())
            },
        )
    }

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_connection_with_reconnect() {
        connection_with(
            crate::tests::monetdb_connection_string().as_str(),
            |result| {
                let mut monetdb = result.expect("connect to MonetDB");
                let data = monetdb
                    .handle()
                    .query::<ValueRow>("SELECT 'foo'")
                    .expect("failed to run query")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("fetch data");

                assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
                (None, ())
            },
        );

        connection_with(
            crate::tests::monetdb_connection_string().as_str(),
            |result| {
                let mut monetdb = result.expect("connect to MonetDB");
                let data = monetdb
                    .handle()
                    .query::<ValueRow>("SELECT 'foo'")
                    .expect("failed to run query")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("fetch data");

                assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
                (None, ())
            },
        )
    }

    #[cfg(feature = "test-monetdb")]
    #[test]
    fn test_connection_with_nested() {
        connection_with(
            crate::tests::monetdb_connection_string().as_str(),
            |result| {
                let mut monetdb = result.expect("connect to MonetDB");
                let data = monetdb
                    .handle()
                    .query::<ValueRow>("SELECT 'foo'")
                    .expect("failed to run query")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("fetch data");

                assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));

                connection_with(
                    crate::tests::monetdb_connection_string().as_str(),
                    |result| {
                        let mut monetdb = result.expect("connect to MonetDB");
                        let data = monetdb
                            .handle()
                            .query::<ValueRow>("SELECT 'foo'")
                            .expect("failed to run query")
                            .collect::<Result<Vec<_>, _>>()
                            .expect("fetch data");

                        assert_matches!(data[0][0], Some(Value::String(ref string)) => assert_eq!(string, "foo"));
                        (Some(monetdb), ())
                    },
                );

                (Some(monetdb), ())
            },
        )
    }
}
