//! Implementation of OdbcType for types used to represent data from database so they can be bound as parameters

pub use odbc::OdbcType;
pub use odbc::ffi;

// Need a way to convert NaiveTime/Date to something that can be bound (impl OdbcType)
// IntoSqlValue trait?