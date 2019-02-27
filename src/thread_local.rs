use crate::*;
use std::cell::RefCell;

// Note: odbc-sys stuff is not Sent and therefore we need to create objects per thread
thread_local! {
    // Leaking ODBC handle per thread should be OK...ish assuming a thread pool is used?
    static ODBC: &'static Odbc = Box::leak(Box::new(Odbc::new().expect("Failed to initialize ODBC")));
    static DB: RefCell<Option<Result<Connection<'static>, OdbcError>>> = RefCell::new(None);
}

/// Access to thread local connection
/// Connection will be established only once if successful or any time this function is called again after it failed to connect previously
pub fn connection_with<O>(
    connection_string: &str,
    f: impl Fn(Result<&mut Connection<'static>, OdbcError>) -> O,
) -> O {
    DB.with(|db| {
        {
            let mut db = db.borrow_mut();
            if db.is_none() {
                let id = std::thread::current().id();
                debug!("[{:?}] Connecting to database: {}", id, &connection_string);

                *db = Some(ODBC.with(|odbc| odbc.connect(&connection_string)));
            }
        };

        let (out, connection) = match db.borrow_mut().take().unwrap() {
            Ok(mut connection) => {
                let out = f(Ok(&mut connection));
                (out, Some(Ok(connection)))
            },
            Err(err) => {
                (f(Err(err)), None)
            }
        };

        *db.borrow_mut() = connection;
        out
    })
}
