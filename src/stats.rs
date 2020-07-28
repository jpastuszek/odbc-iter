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

struct QueryExecuting;

impl QueryExecuting {
    fn new() -> QueryExecuting {
        QUERIES_EXECUTING.fetch_add(1, Ordering::Relaxed);
        QueryExecuting
    }

    fn failed(self) {
        QUERIES_FAILED.fetch_add(1, Ordering::Relaxed);
    }

    fn fetching(self) {
        QUERIES_FETCHING.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for QueryExecuting {
    fn drop(&mut self) {
        // Note: possible race where EXECUTING and FETCHING at the same time... but it is OK for
        // the use case
        assert!(QUERIES_EXECUTING.fetch_sub(1, Ordering::Relaxed) > 0);
    }
}

pub(crate) fn connection_opened() {
    OPEN_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn connection_closed() {
    assert!(OPEN_CONNECTIONS.fetch_sub(1, Ordering::Relaxed) > 0);
}

pub(crate) fn query_done() {
    assert!(QUERIES_FETCHING.fetch_sub(1, Ordering::Relaxed) > 0);
    QUERIES_DONE.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn query_execution<F, O, E>(f: F) -> Result<O, E> where F: FnOnce() -> Result<O, E> {
    let exec = QueryExecuting::new();
    match f() {
        Ok(o) => {
            exec.fetching();
            Ok(o)
        }
        Err(e) => {
            exec.failed();
            Err(e)
        }
    }
}
