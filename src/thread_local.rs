use crate::*;
use std::cell::RefCell;

thread_local! {
    static DB: RefCell<Option<Connection>> = RefCell::new(None);
}

/// Access to thread local connection
/// Connection will be established only once if successful or any time this function is called again after it failed to connect previously
pub fn connection_with<O>(
    connection_string: &str,
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

                match Odbc::connect(&connection_string) {
                    Ok(conn) => connection = conn,
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
