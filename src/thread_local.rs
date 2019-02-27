use crate::*;
use std::cell::RefCell;

// Note: odbc-sys stuff is not Sent and therefore we need to create objects per thread
thread_local! {
    // Leaking ODBC handle per thread should be OK...ish assuming a thread pool is used?
    static ODBC: &'static Odbc = Box::leak(Box::new(Odbc::new().expect("Failed to initialize ODBC")));
    static DB: RefCell<Option<Connection<'static>>> = RefCell::new(None);
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

                match ODBC.with(|odbc| odbc.connect(&connection_string)) {
                    Ok(connection) => *db = Some(connection),
                    Err(err) => return f(Err(err)),
                }
            }
        };

        f(Ok(db.borrow_mut().as_mut().unwrap()))
    })
}
