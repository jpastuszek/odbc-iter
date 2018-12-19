use cotton::prelude::*;

use lazy_static::lazy_static;
use odbc::{
    self, ColumnDescriptor, Connection, DriverInfo, Environment, NoResult, OdbcType, Prepared,
    ResultSetState, SqlDate, SqlSsTime2, SqlTime, SqlTimestamp, Statement, Version3,
};
use regex::Regex;
pub use serde_json::value::Value;
use std::cell::{Ref, RefCell};
use std::marker::PhantomData;

/// TODO
/// * Use poroper error type
/// * Use custom Value type but provide From traits for JSON behind feature
/// * Make tests somehow runable?

// https://github.com/rust-lang/rust/issues/49431
pub trait Captures<'a> {}
impl<'a, T: ?Sized> Captures<'a> for T {}

pub trait Captures2<'a> {}
impl<'a, T: ?Sized> Captures2<'a> for T {}

pub type EnvironmentV3 = Environment<Version3>;
pub type Values = Vec<Value>;
pub type OdbcSchema = Vec<ColumnDescriptor>;

pub struct SchemaAccess<'v> {
    value: Vec<Value>,
    schema: &'v OdbcSchema,
}

pub trait WithSchemaAccess {
    fn with_schema_access<'i>(self, schema: &'i OdbcSchema) -> SchemaAccess<'i>;
}

impl WithSchemaAccess for Values {
    fn with_schema_access<'i>(self, schema: &'i OdbcSchema) -> SchemaAccess<'i> {
        SchemaAccess {
            value: self,
            schema,
        }
    }
}

pub trait SchemaIndex {
    fn column_index(self, name: &str) -> Result<usize, Problem>;
}

impl<'i> SchemaIndex for &'i OdbcSchema {
    fn column_index(self, name: &str) -> Result<usize, Problem> {
        self.iter()
            .position(|desc| desc.name == name)
            .ok_or_problem("column not found")
            .problem_while_with(|| {
                format!("accessing column {} in data with schema: {:?}", name, self)
            })
    }
}

impl<'i> SchemaAccess<'i> {
    pub fn get(&self, column_name: &str) -> Result<&Value, Problem> {
        let index = self.schema.column_index(column_name)?;
        Ok(self
            .value
            .get(index)
            .expect("index out of range while getting value by column name"))
    }

    pub fn take(&mut self, column_name: &str) -> Result<Value, Problem> {
        let index = self.schema.column_index(column_name)?;
        Ok(self
            .value
            .get_mut(index)
            .expect("index out of range while taking value by column name")
            .take())
    }
}

/// Convert from ODBC schema to other type of schema
pub trait TryFromOdbcSchema: Sized {
    fn try_from_odbc_schema(schema: &OdbcSchema) -> Result<Self, Problem>;
}

impl TryFromOdbcSchema for () {
    fn try_from_odbc_schema(_schema: &OdbcSchema) -> Result<Self, Problem> {
        Ok(())
    }
}

impl TryFromOdbcSchema for OdbcSchema {
    fn try_from_odbc_schema(schema: &OdbcSchema) -> Result<Self, Problem> {
        Ok(schema.clone())
    }
}

/// Convert from ODBC row to other type of value
pub trait TryFromRow: Sized {
    /// Type of shema for the target value
    type Schema: TryFromOdbcSchema;
    fn try_from_row(values: Values, schema: &Self::Schema) -> Result<Self, Problem>;
}

impl TryFromRow for Values {
    type Schema = OdbcSchema;
    fn try_from_row(values: Values, _schema: &Self::Schema) -> Result<Self, Problem> {
        Ok(values)
    }
}

impl TryFromRow for Value {
    type Schema = OdbcSchema;
    fn try_from_row(values: Values, _schema: &Self::Schema) -> Result<Self, Problem> {
        Ok(values.into())
    }
}

/// Iterate rows converting them to given value type
pub struct RowIter<'odbc, V>
where
    V: TryFromRow,
{
    statement: Option<odbc::Statement<'odbc, 'odbc, odbc::Prepared, odbc::HasResult>>,
    odbc_schema: Vec<ColumnDescriptor>,
    schema: V::Schema,
    phantom: PhantomData<V>,
}

impl<'odbc, V> RowIter<'odbc, V>
where
    V: TryFromRow,
{
    pub fn schema(&self) -> &V::Schema {
        &self.schema
    }

    pub fn close(self) -> Result<(), Problem> {
        if let Some(statement) = self.statement {
            return statement
                .close_cursor()
                .map(|_s| ())
                .problem_while("closing cursor");
        }
        Ok(())
    }
}

impl<'odbc, V> Iterator for RowIter<'odbc, V>
where
    V: TryFromRow,
{
    type Item = Result<V, Problem>;

    fn next(&mut self) -> Option<Self::Item> {
        use odbc_sys::SqlDataType::*;

        fn cursor_get_data<'i, T: odbc::OdbcType<'i>>(
            cursor: &'i mut odbc::Cursor<odbc::Prepared>,
            index: u16,
        ) -> Result<Option<T>, Problem> {
            cursor.get_data::<T>(index + 1).map_problem()
        }

        fn into_value<T: Into<Value>>(value: Option<T>) -> Value {
            value.map(Into::into).unwrap_or(Value::Null)
        }

        fn cursor_get_value<'i, T: odbc::OdbcType<'i> + Into<Value>>(
            cursor: &'i mut odbc::Cursor<odbc::Prepared>,
            index: u16,
        ) -> Result<Value, Problem> {
            cursor_get_data::<T>(cursor, index).map(into_value)
        }

        if self.statement.is_none() {
            return None;
        }

        match self.statement.as_mut().unwrap().fetch() {
            Err(err) => Some(Err(err.to_problem())),
            Ok(Some(mut cursor)) => {
                Some(
                    self.odbc_schema
                        .iter()
                        .enumerate()
                        .map(|(index, column_descriptor)| {
                            trace!("Parsing column {}: {:?}", index, column_descriptor);
                            // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-2017
                            match column_descriptor.data_type {
                                SQL_EXT_TINYINT => {
                                    cursor_get_value::<i8>(&mut cursor, index as u16)
                                }
                                SQL_SMALLINT => cursor_get_value::<i16>(&mut cursor, index as u16),
                                SQL_INTEGER => cursor_get_value::<i32>(&mut cursor, index as u16),
                                SQL_EXT_BIGINT => {
                                    cursor_get_value::<i64>(&mut cursor, index as u16)
                                }
                                SQL_FLOAT => cursor_get_value::<f32>(&mut cursor, index as u16),
                                SQL_REAL => cursor_get_value::<f32>(&mut cursor, index as u16),
                                SQL_DOUBLE => cursor_get_value::<f64>(&mut cursor, index as u16),
                                SQL_CHAR | SQL_VARCHAR | SQL_EXT_LONGVARCHAR => {
                                    cursor_get_value::<String>(&mut cursor, index as u16)
                                }
                                SQL_EXT_WCHAR | SQL_EXT_WVARCHAR | SQL_EXT_WLONGVARCHAR => {
                                    cursor_get_data::<&[u16]>(&mut cursor, index as u16).and_then(
                                        |value| {
                                            if let Some(bytes) = value {
                                                String::from_utf16(bytes)
                                                    .map_problem()
                                                    .map(Value::String)
                                            } else {
                                                Ok(Value::Null)
                                            }
                                        },
                                    )
                                }
                                SQL_TIMESTAMP => {
                                    cursor_get_data::<SqlTimestamp>(&mut cursor, index as u16)
                                        .and_then(|value| {
                                            if let Some(timestamp) = value {
                                                trace!("{:?}", timestamp);
                                                Ok(Value::String(format!(
                                                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
                                                    timestamp.year,
                                                    timestamp.month,
                                                    timestamp.day,
                                                    timestamp.hour,
                                                    timestamp.minute,
                                                    timestamp.second,
                                                    timestamp.fraction / 1_000_000
                                                )))
                                            } else {
                                                Ok(Value::Null)
                                            }
                                        })
                                }
                                SQL_DATE => cursor_get_data::<SqlDate>(&mut cursor, index as u16)
                                    .and_then(|value| {
                                        if let Some(date) = value {
                                            trace!("{:?}", date);
                                            Ok(Value::String(format!(
                                                "{:04}-{:02}-{:02}",
                                                date.year, date.month, date.day
                                            )))
                                        } else {
                                            Ok(Value::Null)
                                        }
                                    }),
                                SQL_TIME => cursor_get_data::<SqlTime>(&mut cursor, index as u16)
                                    .and_then(|value| {
                                        if let Some(time) = value {
                                            trace!("{:?}", time);
                                            Ok(Value::String(format!(
                                                "{:02}:{:02}:{:02}",
                                                time.hour, time.minute, time.second
                                            )))
                                        } else {
                                            Ok(Value::Null)
                                        }
                                    }),
                                SQL_SS_TIME2 => {
                                    cursor_get_data::<SqlSsTime2>(&mut cursor, index as u16)
                                        .and_then(|value| {
                                            if let Some(time) = value {
                                                trace!("{:?}", time);
                                                Ok(Value::String(format!(
                                                    "{:02}:{:02}:{:02}.{:07}",
                                                    time.hour,
                                                    time.minute,
                                                    time.second,
                                                    time.fraction / 100
                                                )))
                                            } else {
                                                Ok(Value::Null)
                                            }
                                        })
                                }
                                SQL_EXT_BIT => cursor_get_data::<u8>(&mut cursor, index as u16)
                                    .map(|byte| {
                                        if let Some(byte) = byte {
                                            Value::Bool(if byte == 0 { false } else { true })
                                        } else {
                                            Value::Null
                                        }
                                    }),
                                _ => panic!(format!(
                                    "got unimplemented SQL data type: {:?}",
                                    column_descriptor.data_type
                                )),
                            }
                            .problem_while("getting value from cursor")
                        })
                        .collect::<Result<Vec<Value>, Problem>>(),
                )
            }
            Ok(None) => None,
        }
        .map(|v| v.and_then(|v| TryFromRow::try_from_row(v, &self.schema)))
    }
}

pub struct Binder<'odbc, 't> {
    statement: Statement<'odbc, 't, Prepared, NoResult>,
    index: u16,
}

impl<'odbc, 't> Binder<'odbc, 't> {
    pub fn bind<'new_t, T>(self, value: &'new_t T) -> Result<Binder<'odbc, 'new_t>, Problem>
    where
        T: OdbcType<'new_t> + Display,
        't: 'new_t,
    {
        let index = self.index + 1;
        debug!("Parameter {}: {}", index, value);
        let statement = self.statement.bind_parameter(index, value).map_problem()?;

        Ok(Binder { statement, index })
    }

    fn into_inner(self) -> Statement<'odbc, 't, Prepared, NoResult> {
        self.statement
    }
}

impl<'odbc, 't> From<Statement<'odbc, 'odbc, Prepared, NoResult>> for Binder<'odbc, 'odbc> {
    fn from(statement: Statement<'odbc, 'odbc, Prepared, NoResult>) -> Binder<'odbc, 'odbc> {
        Binder {
            statement,
            index: 0,
        }
    }
}

pub struct Odbc<'env> {
    connection: Connection<'env>,
}

impl<'env> Odbc<'env> {
    pub fn env() -> Result<EnvironmentV3, Problem> {
        odbc::create_environment_v3().map_problem()
    }

    pub fn list_drivers(odbc: &mut Environment<Version3>) -> Result<Vec<DriverInfo>, Problem> {
        odbc.drivers().map_problem()
    }

    pub fn connect(
        env: &'env Environment<Version3>,
        connection_string: &str,
    ) -> Result<Odbc<'env>, Problem> {
        let connection = env
            .connect_with_connection_string(connection_string)
            .map_problem()?;
        Ok(Odbc { connection })
    }

    pub fn statement<'odbc>(
        &'odbc self,
    ) -> Result<Statement<'odbc, 'odbc, odbc::Allocated, odbc::NoResult>, Problem> {
        Statement::with_parent(&self.connection).map_problem()
    }

    pub fn query<V>(&self, query: &str) -> Result<RowIter<V>, Problem>
    where
        V: TryFromRow,
    {
        self.query_with_parameters(query, |b| Ok(b))
    }

    pub fn query_with_parameters<'t, 'odbc: 't, V, F>(
        &'odbc self,
        query: &str,
        bind: F,
    ) -> Result<RowIter<V>, Problem>
    where
        V: TryFromRow,
        F: FnOnce(Binder<'odbc, 'odbc>) -> Result<Binder<'odbc, 't>, Problem>,
    {
        debug!("Running ODBC query: {}", &query);
        let statement = self.statement()?;
        let statement = statement
            .prepare(query)
            .problem_while_with(|| format!("preparing query: '{}'", query))?;

        let statement: Statement<'odbc, 't, Prepared, NoResult> = bind(statement.into())
            .problem_while("binding parameters")?
            .into_inner();

        statement.execute().map_problem().and_then(|res| {
            let (odbc_schema, statement) = match res {
                ResultSetState::Data(statement) => {
                    let num_cols = statement.num_result_cols().map_problem()?;
                    let odbc_schema = (1..num_cols + 1)
                            .map(|i| statement.describe_col(i as u16))
                            .collect::<Result<Vec<ColumnDescriptor>, _>>().map_problem()?;
                    let statement = statement.reset_parameters().map_problem()?; // don't refrence parameter data any more

                    if log_enabled!(log::Level::Debug) {
                        if odbc_schema.len() == 0 {
                            debug!("Got empty data set");
                        } else {
                            debug!("Got data with columns: {}", odbc_schema.iter().map(|cd| format!("{} [{:?}]", cd.name, cd.data_type)).collect::<Vec<String>>().join(", "));
                        }
                    }

                    if num_cols == 0 {
                        // Invalid cursor state.
                        (odbc_schema, None)
                    } else {
                        (odbc_schema, Some(statement))
                    }
                }
                ResultSetState::NoData(statement) => {
                    debug!("No data");
                    let num_cols = statement.num_result_cols().map_problem()?;
                    let odbc_schema = (1..num_cols + 1)
                            .map(|i| statement.describe_col(i as u16))
                            .collect::<Result<Vec<ColumnDescriptor>, _>>().map_problem()?;
                    (odbc_schema, None)
                }
            };

            if log_enabled!(log::Level::Trace) {
                for cd in &odbc_schema {
                    trace!("ODBC query result schema: {} [{:?}] size: {:?} nullable: {:?} decimal_digits: {:?}", cd.name, cd.data_type, cd.column_size, cd.nullable, cd.decimal_digits);
                }
            }

            let schema = V::Schema::try_from_odbc_schema(&odbc_schema)?;

            Ok(RowIter {
                statement: statement,
                odbc_schema,
                schema,
                phantom: PhantomData
            })
        }).problem_while_with(|| format!("executing query: '{}'", query))
    }

    pub fn query_multiple<'odbc, 'q, 't, V>(
        &'odbc self,
        queries: &'q str,
    ) -> impl Iterator<Item = Result<RowIter<V>, Problem>> + Captures<'t> + Captures<'env>
    where
        'env: 'odbc,
        'env: 't,
        'odbc: 't,
        'q: 't,
        V: TryFromRow,
    {
        split_queries(queries).map(move |query| query.and_then(|query| self.query(query)))
    }
}

pub fn split_queries(queries: &str) -> impl Iterator<Item = Result<&str, Problem>> {
    lazy_static! {
        // https://regex101.com/r/6YTuVG/4
        static ref RE: Regex = Regex::new(r#"(?:[\t \n]|--.*\n|!.*\n)*((?:[^;"']+(?:'(?:[^'\\]*(?:\\.)?)*')?(?:"(?:[^"\\]*(?:\\.)?)*")?)*;) *"#).unwrap();
    }
    RE.captures_iter(queries)
        .map(|c| c.get(1).ok_or_problem("filed to match query"))
        .map(|r| r.map(|m| m.as_str()))
}

// Note: odbc-sys stuff is not Sent and therfore we need to create objects per thread
thread_local! {
    // Leaking ODBC handle per thread should be OK...ish assuming a thread pool is used?
    static ODBC: &'static EnvironmentV3 = Box::leak(Box::new(Odbc::env().or_failed_to("Initialize ODBC")));
    static DB: RefCell<Result<Odbc<'static>, Problem>> = RefCell::new(Err(Problem::cause("Not connected")));
}

/// Access to thread local connection
/// Connection will be astablished only once if successful or any time this function is called again after it failed to connect prevously
pub fn thread_local_connection_with<O>(
    connection_string: &str,
    f: impl Fn(Ref<Result<Odbc<'static>, Problem>>) -> O,
) -> O {
    DB.with(|db| {
        {
            let mut db = db.borrow_mut();
            if db.is_err() {
                let id = std::thread::current().id();
                debug!("[{:?}] Connecting to database: {}", id, &connection_string);

                *db = ODBC.with(|odbc| Odbc::connect(odbc, &connection_string));
            }
        };

        f(db.borrow())
    })
}

#[cfg(test)]
mod query {
    use super::*;
    #[allow(unused_imports)]
    use assert_matches::assert_matches;

    #[cfg(feature = "test-sql-server")]
    pub fn sql_server_connection_string() -> String {
        std::env::var("SQL_SERVER_ODBC_CONNECTION")
            .or_failed_to("SQL_SERVER_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-hive")]
    pub fn hive_connection_string() -> String {
        std::env::var("HIVE_ODBC_CONNECTION").or_failed_to("HIVE_ODBC_CONNECTION not set")
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_multiple_rows() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("SELECT explode(x) AS n FROM (SELECT array(42, 24) AS x) d;")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(42)));
        assert_matches!(data[1][0], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(24)));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_multiple_columns() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("SELECT 42, 24;")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(42)));
        assert_matches!(data[0][1], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(24)));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_integer() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive.query::<Value>("SELECT cast(127 AS TINYINT), cast(32767 AS SMALLINT), cast(2147483647 AS INTEGER), cast(9223372036854775807 AS BIGINT);")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(127)));
        assert_matches!(data[0][1], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(32767)));
        assert_matches!(data[0][2], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(2147483647)));
        assert_matches!(data[0][3], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(9223372036854775807)));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_boolean() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("SELECT true, false, CAST(NULL AS BOOLEAN)")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_eq!(data[0][0], Value::Bool(true));
        assert_eq!(data[0][1], Value::Bool(false));
        assert_eq!(data[0][2], Value::Null);
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_string() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("SELECT cast('foo' AS STRING), cast('bar' AS VARCHAR);")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::String(ref string) => assert_eq!(string.as_str(), "foo"));
        assert_matches!(data[0][1], Value::String(ref string) => assert_eq!(string.as_str(), "bar"));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_types_string() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .or_failed_to("connect to Hive");
        let data = hive.query::<Value>("SELECT 'foo', cast('bar' AS NVARCHAR), cast('baz' AS TEXT), cast('quix' AS NTEXT);")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::String(ref string) => assert_eq!(string.as_str(), "foo"));
        assert_matches!(data[0][1], Value::String(ref string) => assert_eq!(string.as_str(), "bar"));
        assert_matches!(data[0][2], Value::String(ref string) => assert_eq!(string.as_str(), "baz"));
        assert_matches!(data[0][3], Value::String(ref string) => assert_eq!(string.as_str(), "quix"));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_float() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("SELECT cast(1.5 AS FLOAT), cast(2.5 AS DOUBLE);")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::Number(ref number) => assert!(number.as_f64().unwrap() > 1.0 && number.as_f64().unwrap() < 2.0));
        assert_matches!(data[0][1], Value::Number(ref number) => assert!(number.as_f64().unwrap() > 2.0 && number.as_f64().unwrap() < 3.0));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_types_null() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("SELECT cast(NULL AS FLOAT), cast(NULL AS DOUBLE);")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert!(data[0][0].is_null());
        assert!(data[0][1].is_null());
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_date() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("SELECT cast('2018-08-24' AS DATE) AS date")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::String(ref string) => assert_eq!(string.as_str(), "2018-08-24"));
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_time() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("SELECT cast('10:22:33.7654321' AS TIME) AS date")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::String(ref string) => assert_eq!(string.as_str(), "10:22:33.7654321"));
    }

    #[derive(Debug)]
    struct Foo {
        val: i64,
    }

    impl TryFromRow for Foo {
        type Schema = OdbcSchema;
        fn try_from_row(values: Values, schema: &OdbcSchema) -> Result<Self, Problem> {
            values
                .with_schema_access(schema)
                .take("val")
                .map(|val| Foo {
                    val: val.as_i64().expect("val to be a number"),
                })
        }
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_custom_type() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let foo = hive
            .query::<Foo>("SELECT 42 AS val;")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_eq!(foo[0].val, 42);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_query_with_parameters() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .or_failed_to("connect to Hive");

        let val = 42;

        let foo: Vec<Foo> = hive
            .query_with_parameters("SELECT ? AS val;", |q| q.bind(&val))
            .or_failed_to("failed to run query")
            .collect::<Result<_, Problem>>()
            .or_failed_to("fetch data");

        assert_eq!(foo[0].val, 42);
    }

    #[cfg(feature = "test-sql-server")]
    #[test]
    fn test_sql_server_query_with_many_parameters() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive = Odbc::connect(&odbc, sql_server_connection_string().as_str())
            .or_failed_to("connect to Hive");

        let val = [42, 24, 32, 666];

        let data: Vec<Value> = hive
            .query_with_parameters("SELECT ?, ?, ?, ? AS val;", |q| {
                val.iter().fold(Ok(q), |q, v| q.and_then(|q| q.bind(v)))
            })
            .or_failed_to("failed to run query")
            .collect::<Result<_, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(42)));
        assert_matches!(data[0][1], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(24)));
        assert_matches!(data[0][2], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(32)));
        assert_matches!(data[0][3], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(666)));
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_empty_data_set() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive
            .query::<Value>("USE default;")
            .or_failed_to("failed to run query")
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert!(data.is_empty());
    }

    #[test]
    fn test_split_queries() {
        let queries = split_queries(
            r#"-- Foo
---
CREATE DATABASE IF NOT EXISTS daily_reports;
USE daily_reports;

SELECT *;"#,
        )
        .collect::<Result<Vec<_>, Problem>>()
        .expect("failed to parse");
        assert_eq!(
            queries,
            [
                "CREATE DATABASE IF NOT EXISTS daily_reports;",
                "USE daily_reports;",
                "SELECT *;"
            ]
        );
    }

    #[test]
    fn test_split_queries_end_white() {
        let queries = split_queries(
            r#"USE daily_reports;
SELECT *;

"#,
        )
        .collect::<Result<Vec<_>, Problem>>()
        .expect("failed to parse");
        assert_eq!(queries, ["USE daily_reports;", "SELECT *;"]);
    }

    #[test]
    fn test_split_queries_simple() {
        let queries = split_queries("SELECT 42;\nSELECT 24;\nSELECT 'foo';")
            .collect::<Result<Vec<_>, Problem>>()
            .expect("failed to parse");
        assert_eq!(queries, ["SELECT 42;", "SELECT 24;", "SELECT 'foo';"]);
    }

    #[test]
    fn test_split_queries_semicolon() {
        let queries = split_queries("SELECT 'foo; bar';\nSELECT 1;")
            .collect::<Result<Vec<_>, Problem>>()
            .expect("failed to parse");
        assert_eq!(queries, [r#"SELECT 'foo; bar';"#, "SELECT 1;"]);
    }

    #[test]
    fn test_split_queries_semicolon2() {
        let queries = split_queries(r#"foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad; foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad; select foo; foo "bar" baz 'quix; but' foo "bar" baz "quix; but" fsad; foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad; select foo;"#).collect::<Result<Vec<_>, Problem>>().expect("failed to parse");
        assert_eq!(
            queries,
            [
                r#"foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad;"#,
                r#"foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad;"#,
                r#"select foo;"#,
                r#"foo "bar" baz 'quix; but' foo "bar" baz "quix; but" fsad;"#,
                r#"foo "bar" baz "quix; but" foo "bar" baz "quix; but" fsad;"#,
                r#"select foo;"#,
            ]
        );
    }

    #[test]
    fn test_split_queries_escaped_quote() {
        let queries = split_queries("SELECT 'foo; b\\'ar';\nSELECT 1;")
            .collect::<Result<Vec<_>, Problem>>()
            .expect("failed to parse");
        assert_eq!(queries, [r#"SELECT 'foo; b\'ar';"#, "SELECT 1;"]);
    }

    #[test]
    fn test_split_queries_escaped_quote2() {
        let queries = split_queries("SELECT 'foo; b\\'ar';\nSELECT 'foo\\'bar';")
            .collect::<Result<Vec<_>, Problem>>()
            .expect("failed to parse");
        assert_eq!(
            queries,
            [r#"SELECT 'foo; b\'ar';"#, r#"SELECT 'foo\'bar';"#]
        );
    }

    #[test]
    fn test_split_queries_escaped_doublequote() {
        let queries = split_queries(r#"SELECT "foo; b\"ar";SELECT "foo\"bar";"#)
            .collect::<Result<Vec<_>, Problem>>()
            .expect("failed to parse");
        assert_eq!(
            queries,
            [r#"SELECT "foo; b\"ar";"#, r#"SELECT "foo\"bar";"#]
        );
    }

    #[test]
    fn test_split_queries_comments() {
        let queries =
            split_queries("SELECT 1;\n-- SELECT x;\n---- SELECT x;\nSELECT 2;\nSELECT 3;")
                .collect::<Result<Vec<_>, Problem>>()
                .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[test]
    fn test_split_queries_comments2() {
        let queries = split_queries("-- TODO: add last_search_or_brochure_logentry_id\n-- TODO: DISTRIBUTE BY analytics_record_id SORT BY analytics_record_id ASC;\n-- TODO: check previous day for landing logentry detail\nSELECT '1' LEFT JOIN source_wcc.domain d ON regexp_extract(d.domain, '.*\\\\.([^\\.]+)$', 1) = c.domain AND d.snapshot_day = c.index;").collect::<Result<Vec<_>, Problem>>().expect("failed to parse");
        assert_eq!(queries, [r#"SELECT '1' LEFT JOIN source_wcc.domain d ON regexp_extract(d.domain, '.*\\.([^\.]+)$', 1) = c.domain AND d.snapshot_day = c.index;"#]);
    }

    #[test]
    fn test_split_queries_control() {
        let queries = split_queries(
            "!outputformat vertical\nSELECT 1;\n-- SELECT x;\n---- SELECT x;\nSELECT 2;\nSELECT 3;",
        )
        .collect::<Result<Vec<_>, Problem>>()
        .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[test]
    fn test_split_queries_white() {
        let queries = split_queries(" \n  SELECT 1;\n  \nSELECT 2;\n \nSELECT 3;\n\n ")
            .collect::<Result<Vec<_>, Problem>>()
            .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[test]
    fn test_split_queries_white2() {
        let queries = split_queries("SELECT 1; \t \nSELECT 2; \n \nSELECT 3; ")
            .collect::<Result<Vec<_>, Problem>>()
            .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[test]
    fn test_split_queries_white_comment() {
        let queries = split_queries("SELECT 1; \t \nSELECT 2; -- foo bar\n \nSELECT 3; ")
            .collect::<Result<Vec<_>, Problem>>()
            .expect("failed to parse");
        assert_eq!(queries, ["SELECT 1;", "SELECT 2;", "SELECT 3;"]);
    }

    #[cfg(feature = "test-hive")]
    #[test]
    fn test_hive_multiple_queries() {
        let odbc = Odbc::env().or_failed_to("open ODBC");
        let hive =
            Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
        let data = hive
            .query_multiple::<Value>("SELECT 42;\nSELECT 24;\nSELECT 'foo';")
            .or_failed_to("failed to run query")
            .flat_map(|i| i)
            .collect::<Result<Vec<_>, Problem>>()
            .or_failed_to("fetch data");

        assert_matches!(data[0][0], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(42)));
        assert_matches!(data[1][0], Value::Number(ref number) => assert_eq!(number.as_i64(), Some(24)));
        assert_matches!(data[2][0], Value::String(ref string) => assert_eq!(string, "foo"));
    }
}
