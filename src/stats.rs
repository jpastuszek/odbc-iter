use std::sync::atomic::{AtomicU64, Ordering};
use std::fmt;

static OPEN_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static QUERIES_PENDING: AtomicU64 = AtomicU64::new(0);
static QUERIES_FETCHING: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct Stats {
    pub open_connections: u64,
    pub queries_pending: u64,
    pub queries_fetching: u64,
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ODBC stats: connections: open: {open_connections}, queries: pending: {queries_pending}, fetching: {queries_fetching}",
            open_connections = self.open_connections,
            queries_pending = self.queries_pending,
            queries_fetching = self.queries_fetching,
        )
    }
}

pub fn stats() -> Stats {
    Stats {
        open_connections: OPEN_CONNECTIONS.load(Ordering::Relaxed),
        queries_pending: QUERIES_PENDING.load(Ordering::Relaxed),
        queries_fetching: QUERIES_FETCHING.load(Ordering::Relaxed),
    }
}

pub(crate) fn connection_opened() {
    OPEN_CONNECTIONS.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn connection_closed() {
    assert!(OPEN_CONNECTIONS.fetch_sub(1, Ordering::SeqCst) > 0);
}

pub(crate) fn query_pending() {
    QUERIES_PENDING.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn query_fetching() {
    assert!(QUERIES_PENDING.fetch_sub(1, Ordering::SeqCst) > 0);
    QUERIES_FETCHING.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn query_done() {
    assert!(QUERIES_FETCHING.fetch_sub(1, Ordering::SeqCst) > 0);
}

pub(crate) fn query_execution<F, O, E>(f: F) -> Result<O, E> where F: FnOnce() -> Result<O, E> {
    query_pending();
    match f() {
        Ok(o) => {
            query_fetching();
            Ok(o)
        }
        Err(e) => {
            query_done();
            Err(e)
        }
    }
}
