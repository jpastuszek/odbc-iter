use std::sync::atomic::{AtomicU64, Ordering};
use std::fmt;

static OPEN_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static QUERIES_EXECUTING: AtomicU64 = AtomicU64::new(0);
static QUERIES_FETCHING: AtomicU64 = AtomicU64::new(0);
static QUERIES_DONE: AtomicU64 = AtomicU64::new(0);
static QUERIES_FAILED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct Stats {
    pub open_connections: u64,
    pub queries_executing: u64,
    pub queries_fetching: u64,
    pub queries_done: u64,
    pub queries_failed: u64,
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ODBC stats: connections: open: {open_connections}, queries: executing: {queries_executing}, fetching: {queries_fetching}, done: {queries_done}, failed: {queries_failed}",
            open_connections = self.open_connections,
            queries_executing = self.queries_executing,
            queries_fetching = self.queries_fetching,
            queries_done = self.queries_done,
            queries_failed = self.queries_failed,
        )
    }
}

pub fn stats() -> Stats {
    Stats {
        open_connections: OPEN_CONNECTIONS.load(Ordering::Relaxed),
        queries_executing: QUERIES_EXECUTING.load(Ordering::Relaxed),
        queries_fetching: QUERIES_FETCHING.load(Ordering::Relaxed),
        queries_done: QUERIES_DONE.load(Ordering::Relaxed),
        queries_failed: QUERIES_FAILED.load(Ordering::Relaxed),
    }
}

pub(crate) struct ConnectionOpenGuard;

impl ConnectionOpenGuard {
    pub(crate) fn new() -> ConnectionOpenGuard {
        OPEN_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
        ConnectionOpenGuard
    }
}

impl Drop for ConnectionOpenGuard {
    fn drop(&mut self) {
        assert!(OPEN_CONNECTIONS.fetch_sub(1, Ordering::Relaxed) > 0);
    }
}

struct QueryExecuting;

impl QueryExecuting {
    fn new() -> QueryExecuting {
        QUERIES_EXECUTING.fetch_add(1, Ordering::Relaxed);
        QueryExecuting
    }

    fn failed(self) {
        QUERIES_FAILED.fetch_add(1, Ordering::Relaxed);
    }

    fn fetching(self) -> QueryFetchingGuard {
        QueryFetchingGuard::new()
    }
}

impl Drop for QueryExecuting {
    fn drop(&mut self) {
        // Note: possible race where EXECUTING and FETCHING at the same time... but it is OK for
        // the use case
        assert!(QUERIES_EXECUTING.fetch_sub(1, Ordering::Relaxed) > 0);
    }
}

pub(crate) struct QueryFetchingGuard;

impl QueryFetchingGuard {
    fn new() -> QueryFetchingGuard {
        QUERIES_FETCHING.fetch_add(1, Ordering::Relaxed);
        QueryFetchingGuard
    }
}

impl Drop for QueryFetchingGuard {
    fn drop(&mut self) {
        assert!(QUERIES_FETCHING.fetch_sub(1, Ordering::Relaxed) > 0);
        QUERIES_DONE.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn query_execution<O, E, F>(f: F) -> Result<(O, QueryFetchingGuard), E> where F: FnOnce() -> Result<O, E> {
    let exec = QueryExecuting::new();
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
