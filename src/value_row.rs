use crate::row::{Row, TryFromColumn, DefaultConfiguration, TryFromRow, RowConvertError};
use crate::value::{TryFromValue, Value};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;

/// Row of dynamic nullable column values.
///
/// This objects are constructed from row data returned by ODBC library and can be further converted to types implementing `TryFromValueRow`/`TryFromValue` traits.
pub type ValueRow = Vec<Option<Value>>;

impl TryFromRow<DefaultConfiguration> for ValueRow {
    type Error = RowConvertError;

    fn try_from_row<'r, 's, 'c, S>(mut row: Row<'r, 's, 'c, S, DefaultConfiguration>) -> Result<Self, Self::Error> {
        let mut value_row = Vec::with_capacity(row.columns() as usize);

        loop {
            if let Some(column) = row.shift_column() {
                let value: Option<Value> = TryFromColumn::try_from_column(column)?;
                value_row.push(value)
            } else {
                return Ok(value_row);
            }
        }
    }
}

// Note: TryFrom/TryInto cannot be implemented since we need to own the trait

/// This traits allow for conversion of `ValueRow` type used internally by `ResultSet` iterator to any
/// other type returned as `Item` that implements it.
///
/// This trait is implemented for Rust tuple type enabling conversion of rows to tuples of types implementing `TryFromValue`.
/// Also this trait implementation allows to convert single column rows to types implementing `TryFromValue`.
///
/// This trait can be implemented for custom objects. This will enable them to be queried directly from database as `Item` of `ResultSet` iterator.
pub trait TryFromValueRow: Sized {
    type Error: Error + 'static;
    /// Convert from `ValueRow` to other type of value representing table row.
    fn try_from_value_row(values: ValueRow) -> Result<Self, Self::Error>;
}

/// Errors that may happen during conversion of `ValueRow` to given type.
#[derive(Debug)]
pub enum ValueRowConvertError {
    UnexpectedNullValue(&'static str),
    UnexpectedValue,
    UnexpectedNumberOfColumns { expected: u16, got: usize },
    ValueConvertError(Box<dyn Error>),
}

impl fmt::Display for ValueRowConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueRowConvertError::UnexpectedNullValue(t) => {
                write!(f, "expecting value of type {} but got NULL", t)
            }
            ValueRowConvertError::UnexpectedValue => write!(f, "expecting no data (unit) but got a row"),
            ValueRowConvertError::UnexpectedNumberOfColumns { expected, got } => write!(
                f,
                "unexpected number of columns: expected {} but got {}",
                expected, got
            ),
            ValueRowConvertError::ValueConvertError(_) => {
                write!(f, "failed to convert column value to target type")
            }
        }
    }
}

impl Error for ValueRowConvertError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ValueRowConvertError::UnexpectedNullValue(_)
            | ValueRowConvertError::UnexpectedValue
            | ValueRowConvertError::UnexpectedNumberOfColumns { .. } => None,
            ValueRowConvertError::ValueConvertError(err) => Some(err.as_ref()),
        }
    }
}

/// Allow to retrieve unconverted `ValueRow` as item of `ResultSet` iterator.
impl TryFromValueRow for ValueRow {
    type Error = Infallible;
    fn try_from_value_row(values: ValueRow) -> Result<Self, Self::Error> {
        Ok(values)
    }
}

/// Unit can be used to signal that no rows of data should be produced.
impl TryFromValueRow for () {
    type Error = ValueRowConvertError;
    fn try_from_value_row(_values: ValueRow) -> Result<Self, Self::Error> {
        Err(ValueRowConvertError::UnexpectedValue)
    }
}

/// Convert row with single column to any type implementing `TryFromValue`.
impl<T> TryFromValueRow for T
where
    T: TryFromValue,
{
    type Error = ValueRowConvertError;
    fn try_from_value_row(mut values: ValueRow) -> Result<Self, Self::Error> {
        if values.len() != 1 {
            return Err(ValueRowConvertError::UnexpectedNumberOfColumns {
                expected: 1,
                got: values.len(),
            });
        }
        values
            .pop()
            .ok_or_else(|| ValueRowConvertError::UnexpectedNullValue("Value"))
            .and_then(|v| {
                TryFromValue::try_from_value(v)
                    .map_err(|e| ValueRowConvertError::ValueConvertError(Box::new(e)))
            })
    }
}

/// Errors that my arise when converting rows to tuples.
#[derive(Debug)]
pub enum ValueRowConvertTupleError {
    UnexpectedNumberOfColumns { expected: u16, tuple: &'static str },
    ValueConvertError(Box<dyn Error>),
}

impl fmt::Display for ValueRowConvertTupleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueRowConvertTupleError::UnexpectedNumberOfColumns { expected, tuple } => write!(
                f,
                "failed to convert row with {} columns to tuple {}",
                expected, tuple
            ),
            ValueRowConvertTupleError::ValueConvertError(_) => {
                write!(f, "failed to convert column value to target type")
            }
        }
    }
}

impl Error for ValueRowConvertTupleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ValueRowConvertTupleError::UnexpectedNumberOfColumns { .. } => None,
            ValueRowConvertTupleError::ValueConvertError(err) => Some(err.as_ref()),
        }
    }
}

macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! try_from_tuple {
    ($(
        $Tuple:ident {
            $(($idx:tt) -> $T:ident)+
        }
    )+) => {
        $(
            impl<$($T: TryFromValue),+> TryFromValueRow for ($($T,)+) {
                type Error = ValueRowConvertTupleError;
                fn try_from_value_row(values: ValueRow) -> Result<($($T,)+), Self::Error> {
                    if values.len() != count!($($T)+) {
                        return Err(ValueRowConvertTupleError::UnexpectedNumberOfColumns { expected: values.len() as u16, tuple: stringify![($($T,)+)] })
                    }
                    let mut values = values.into_iter();
                    Ok(($({ let x: $T = $T::try_from_value(values.next().unwrap()).map_err(|err| ValueRowConvertTupleError::ValueConvertError(Box::new(err)))?; x},)+))
                }
            }
        )+
    }
}

try_from_tuple! {
    Tuple1 {
        (0) -> A
    }
    Tuple2 {
        (0) -> A
        (1) -> B
    }
    Tuple3 {
        (0) -> A
        (1) -> B
        (2) -> C
    }
    Tuple4 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
    }
    Tuple5 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
    }
    Tuple6 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
    }
    Tuple7 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
    }
    Tuple8 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
    }
    Tuple9 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
    }
    Tuple10 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
    }
    Tuple11 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
    }
    Tuple12 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
    }
    Tuple13 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
    }
    Tuple14 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
    }
    Tuple15 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
    }
    Tuple16 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
    }
    Tuple17 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
        (17) -> Q
    }
    Tuple18 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
        (17) -> Q
        (18) -> R
    }
    Tuple19 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
        (17) -> Q
        (18) -> R
        (19) -> S
    }
    Tuple20 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
        (17) -> Q
        (18) -> R
        (19) -> S
        (20) -> T
    }
    Tuple21 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
        (17) -> Q
        (18) -> R
        (19) -> S
        (20) -> T
        (21) -> U
    }
    Tuple22 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
        (17) -> Q
        (18) -> R
        (19) -> S
        (20) -> T
        (21) -> U
        (22) -> V
    }
    Tuple23 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
        (17) -> Q
        (18) -> R
        (19) -> S
        (20) -> T
        (21) -> U
        (22) -> V
        (23) -> W
    }
    Tuple24 {
        (0) -> A
        (1) -> B
        (2) -> C
        (3) -> D
        (4) -> E
        (5) -> F
        (6) -> G
        (7) -> H
        (8) -> I
        (9) -> J
        (10) -> K
        (11) -> L
        (13) -> M
        (14) -> N
        (15) -> O
        (16) -> P
        (17) -> Q
        (18) -> R
        (19) -> S
        (20) -> T
        (21) -> U
        (22) -> V
        (23) -> W
        (24) -> Y
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use crate::Odbc;
    #[allow(unused_imports)]
    use assert_matches::assert_matches;

    #[derive(Debug)]
    struct Foo {
        val: i64,
    }

    impl TryFromValueRow for Foo {
        type Error = Infallible;
        fn try_from_value_row(mut values: ValueRow) -> Result<Self, Self::Error> {
            Ok(values
                .pop()
                .map(|val| Foo {
                    val: val.and_then(|v| v.to_i64()).expect("val to be an bigint"),
                })
                .expect("value"))
        }
    }

    use crate::value::Value;

    #[test]
    fn test_custom_type() {
        let test_row: ValueRow = vec![Some(Value::Bigint(42))];
        let foo: Foo = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(foo.val, 42);
    }

    #[test]
    fn test_single_value() {
        let test_row: ValueRow = vec![Some(Value::Bigint(42))];
        let value: Value = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value.to_i64().unwrap(), 42);
    }

    #[test]
    fn test_single_nullable_value() {
        let test_row: ValueRow = vec![Some(Value::Bigint(42))];
        let value: Option<Value> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert!(value.is_some());
        assert_eq!(value.unwrap().to_i64().unwrap(), 42);

        let test_row: ValueRow = vec![None];
        let value: Option<Value> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert!(value.is_none());
    }

    #[test]
    fn test_value_row() {
        let test_row: ValueRow = vec![Some(Value::Bigint(42)), Some(Value::Integer(22))];
        let value: ValueRow = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value.len(), 2);
        assert_eq!(value[0].as_ref().unwrap().to_i64().unwrap(), 42);
        assert_eq!(value[1].as_ref().unwrap().to_i32().unwrap(), 22);
    }

    #[test]
    fn test_single_copy() {
        let test_row: ValueRow = vec![Some(Value::Bit(true))];
        let value: bool = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value, true);

        let test_row: ValueRow = vec![Some(Value::Bit(true))];
        let value: Option<bool> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value.unwrap(), true);

        let test_row: ValueRow = vec![None];
        let value: Option<bool> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert!(value.is_none());

        let test_row: ValueRow = vec![Some(Value::Bigint(42))];
        let value: i64 = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value, 42);

        let test_row: ValueRow = vec![Some(Value::Bigint(42))];
        let value: Option<i64> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value.unwrap(), 42i64);

        let test_row: ValueRow = vec![None];
        let value: Option<i64> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert!(value.is_none());
    }

    #[test]
    fn test_single_unsigned() {
        let test_row: ValueRow = vec![Some(Value::Bigint(42))];
        let value: Option<u64> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value.unwrap(), 42u64);
    }

    #[test]
    #[should_panic(expected = "ValueOutOfRange")]
    fn test_single_unsigned_err() {
        let test_row: ValueRow = vec![Some(Value::Bigint(-666))];
        let _value: Option<u64> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");
    }

    #[test]
    fn test_single_string() {
        let test_row: ValueRow = vec![Some(Value::String("foo".into()))];
        let value: String = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(&value, "foo");

        let test_row: ValueRow = vec![Some(Value::String("foo".into()))];
        let value: Option<String> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(&value.unwrap(), "foo");

        let test_row: ValueRow = vec![None];
        let value: Option<String> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert!(value.is_none());
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_single_date() {
        use chrono::Datelike;
        use chrono::NaiveDate;

        let test_row: ValueRow = vec![Some(Value::Date(odbc::SqlDate { year: 2019, month: 4, day: 2 }))];
        let value: NaiveDate = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value.year(), 2019);
        assert_eq!(value.month(), 4);
        assert_eq!(value.day(), 2);

        let test_row: ValueRow = vec![Some(Value::Date(odbc::SqlDate { year: 2019, month: 4, day: 2 }))];
        let value: Option<NaiveDate> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(value.unwrap().year(), 2019);
        assert_eq!(value.unwrap().month(), 4);
        assert_eq!(value.unwrap().day(), 2);

        let test_row: ValueRow = vec![None];
        let value: Option<NaiveDate> = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert!(value.is_none());
    }

    #[test]
    fn test_tuple_value() {
        let test_row: ValueRow = vec![Some(Value::String("foo".into())), Some(Value::Bigint(42)), Some(Value::Bit(true))];
        let value: (String, i64, bool) = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(&value.0, "foo");
        assert_eq!(value.1, 42);
        assert_eq!(value.2, true);

        let test_row: ValueRow = vec![Some(Value::String("foo".into())), Some(Value::Bigint(42)), Some(Value::Bit(true))];
        let value: (Option<String>, i64, Option<bool>) = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert_eq!(&value.0.unwrap(), "foo");
        assert_eq!(value.1, 42);
        assert_eq!(value.2.unwrap(), true);

        let test_row: ValueRow = vec![None, Some(Value::Bigint(42)), None];
        let value: (Option<String>, i64, Option<bool>) = TryFromValueRow::try_from_value_row(test_row).expect("failed to convert");

        assert!(&value.0.is_none());
        assert_eq!(value.1, 42);
        assert!(value.2.is_none());
    }

    #[test]
    fn test_value_row_conversions() {
        let test_row: ValueRow = vec![Some(Value::Bit(true)), Some(Value::Integer(42)), None];
        type Test = (Option<bool>, Option<u32>, Option<String>);

        //let (b, u, s): (Option<bool>, Option<u32>, Option<String>) = test_row.try_into().unwrap();
        let (b, u, s) = Test::try_from_value_row(test_row).unwrap();
        assert_eq!(b, Some(true));
        assert_eq!(u, Some(42));
        assert_eq!(s, None);
    }
}
