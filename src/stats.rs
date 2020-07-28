#[cfg(feature = "statistics")]
mod inner {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::fmt;

    // Note: possible race where EXECUTING and FETCHING at the same time... but it is OK for the use case
    static OPEN_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
    static QUERIES_PREPARING: AtomicU64 = AtomicU64::new(0);
    static QUERIES_EXECUTING: AtomicU64 = AtomicU64::new(0);
    static QUERIES_FETCHING: AtomicU64 = AtomicU64::new(0);
    static QUERIES_DONE: AtomicU64 = AtomicU64::new(0);
    static QUERIES_FAILED: AtomicU64 = AtomicU64::new(0);

    pub(super) fn open_connections_inc () {
        OPEN_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn open_connections_dec() {
        assert!(OPEN_CONNECTIONS.fetch_sub(1, Ordering::Relaxed) > 0);
    }

    pub(super) fn queries_preparing_inc() {
        QUERIES_PREPARING.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn queries_preparing_dec() {
        assert!(QUERIES_PREPARING.fetch_sub(1, Ordering::Relaxed) > 0);
    }

    pub(super) fn queries_executing_inc() {
        QUERIES_EXECUTING.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn queries_executing_dec() {
        assert!(QUERIES_EXECUTING.fetch_sub(1, Ordering::Relaxed) > 0);
    }

    pub(super) fn queries_fetching_inc() {
        QUERIES_FETCHING.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn queries_fetching_dec() {
        assert!(QUERIES_FETCHING.fetch_sub(1, Ordering::Relaxed) > 0);
    }

    pub(super) fn queries_done_inc() {
        QUERIES_DONE.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn queries_failed_inc() {
        QUERIES_FAILED.fetch_add(1, Ordering::Relaxed);
    }

    #[derive(Debug)]
    pub struct Statistics {
        pub open_connections: u64,
        pub queries_preparing: u64,
        pub queries_executing: u64,
        pub queries_fetching: u64,
        pub queries_done: u64,
        pub queries_failed: u64,
    }

    impl fmt::Display for Statistics {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "ODBC statistics: connections: open: {open_connections}, queries: preparing: {queries_preparing}, executing: {queries_executing}, fetching: {queries_fetching}, done: {queries_done}, failed: {queries_failed}",
                open_connections = self.open_connections,
                queries_preparing = self.queries_preparing,
                queries_executing = self.queries_executing,
                queries_fetching = self.queries_fetching,
                queries_done = self.queries_done,
                queries_failed = self.queries_failed,
            )
        }
    }

    pub fn statistics() -> Statistics {
        Statistics {
            open_connections: OPEN_CONNECTIONS.load(Ordering::Relaxed),
            queries_preparing: QUERIES_PREPARING.load(Ordering::Relaxed),
            queries_executing: QUERIES_EXECUTING.load(Ordering::Relaxed),
            queries_fetching: QUERIES_FETCHING.load(Ordering::Relaxed),
            queries_done: QUERIES_DONE.load(Ordering::Relaxed),
            queries_failed: QUERIES_FAILED.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "statistics")]
pub use inner::statistics;

pub(crate) struct ConnectionOpenGuard;

impl ConnectionOpenGuard {
    pub(crate) fn new() -> ConnectionOpenGuard {
        #[cfg(feature = "statistics")]
        inner::open_connections_inc();
        ConnectionOpenGuard
    }
}

impl Drop for ConnectionOpenGuard {
    fn drop(&mut self) {
        #[cfg(feature = "statistics")]
        inner::open_connections_dec();
    }
}

pub(crate) struct QueryPreparingGuard;

impl QueryPreparingGuard {
    pub(crate) fn new() -> QueryPreparingGuard {
        #[cfg(feature = "statistics")]
        inner::queries_preparing_inc();
        QueryPreparingGuard
    }
}

impl Drop for QueryPreparingGuard {
    fn drop(&mut self) {
        #[cfg(feature = "statistics")]
        inner::queries_preparing_dec();
    }
}


struct QueryExecutingGuard;

impl QueryExecutingGuard {
    fn new() -> QueryExecutingGuard {
        #[cfg(feature = "statistics")]
        inner::queries_executing_inc();
        QueryExecutingGuard
    }

    fn failed(self) {
        #[cfg(feature = "statistics")]
        inner::queries_failed_inc();
    }

    fn fetching(self) -> QueryFetchingGuard {
        QueryFetchingGuard::new()
    }
}

#[cfg(feature = "statistics")]
impl Drop for QueryExecutingGuard {
    fn drop(&mut self) {
        #[cfg(feature = "statistics")]
        inner::queries_executing_dec();
    }
}

pub(crate) struct QueryFetchingGuard;

impl QueryFetchingGuard {
    fn new() -> QueryFetchingGuard {
        #[cfg(feature = "statistics")]
        inner::queries_fetching_inc();
        QueryFetchingGuard
    }
}

#[cfg(feature = "statistics")]
impl Drop for QueryFetchingGuard {
    fn drop(&mut self) {
        inner::queries_fetching_dec();
        inner::queries_done_inc();
    }
}

pub(crate) fn query_preparing<O, F>(f: F) -> O where F: FnOnce() -> O {
    let bind = QueryPreparingGuard::new();
    let ret = f();
    drop(bind);
    ret
}

pub(crate) fn query_execution<O, E, F>(f: F) -> Result<(O, QueryFetchingGuard), E> where F: FnOnce() -> Result<O, E> {
    let exec = QueryExecutingGuard::new();
    match f() {
        Ok(o) => {

            Ok((o, exec.fetching()))
        }
        Err(e) => {
            exec.failed();
            Err(e)
        }
    }
}
