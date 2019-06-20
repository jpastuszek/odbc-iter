use crate::*;

#[derive(Debug)]
pub struct SchemaAccess<'v> {
    value: ValueRow,
    schema: &'v Schema,
}

pub trait WithSchemaAccess {
    fn with_schema_access<'i>(self, schema: &'i Schema) -> SchemaAccess<'i>;
}

impl WithSchemaAccess for ValueRow {
    fn with_schema_access<'i>(self, schema: &'i Schema) -> SchemaAccess<'i> {
        SchemaAccess {
            value: self,
            schema,
        }
    }
}

pub trait SchemaIndex {
    fn column_index(self, name: &str) -> Option<usize>;
}

impl<'i> SchemaIndex for &'i Schema {
    fn column_index(self, name: &str) -> Option<usize> {
        self.iter().position(|desc| desc.name == name)
    }
}

impl<'i> SchemaAccess<'i> {
    pub fn get(&self, column_name: &str) -> Option<Option<&Value>> {
        self.schema
            .column_index(column_name)
            .and_then(|index| self.value.get(index).map(Into::into))
    }

    pub fn take(&mut self, column_name: &str) -> Option<Option<Value>> {
        self.schema
            .column_index(column_name)
            .and_then(|index| self.value.get_mut(index))
            .map(|value| value.take())
    }
}

#[cfg(test)]
mod query {
    use super::*;
    use odbc::ffi::SqlDataType;

    #[test]
    fn test_get() {
        let values = vec![
            Some(Value::String("foo".to_owned())),
            Some(Value::String("bar".to_owned())),
        ];
        let schema = vec![
            ColumnDescriptor {
                name: "quix".to_owned(),
                data_type: SqlDataType::SQL_EXT_LONGVARCHAR,
                column_size: None,
                decimal_digits: None,
                nullable: None,
            },
            ColumnDescriptor {
                name: "baz".to_owned(),
                data_type: SqlDataType::SQL_EXT_LONGVARCHAR,
                column_size: None,
                decimal_digits: None,
                nullable: None,
            },
        ];

        let values = values.with_schema_access(&schema);

        assert!(values.get("foo").is_none());

        assert!(values.get("quix").is_some());
        assert_eq!(
            values.get("quix").unwrap().unwrap().as_str().unwrap(),
            "foo"
        );

        assert!(values.get("baz").is_some());
        assert_eq!(values.get("baz").unwrap().unwrap().as_str().unwrap(), "bar");
    }

    #[test]
    fn test_take() {
        let values = vec![
            Some(Value::String("foo".to_owned())),
            Some(Value::String("bar".to_owned())),
        ];
        let schema = vec![
            ColumnDescriptor {
                name: "quix".to_owned(),
                data_type: SqlDataType::SQL_EXT_LONGVARCHAR,
                column_size: None,
                decimal_digits: None,
                nullable: None,
            },
            ColumnDescriptor {
                name: "baz".to_owned(),
                data_type: SqlDataType::SQL_EXT_LONGVARCHAR,
                column_size: None,
                decimal_digits: None,
                nullable: None,
            },
        ];

        let mut values = values.with_schema_access(&schema);

        assert_eq!(
            values.take("quix").unwrap().unwrap().as_str().unwrap(),
            "foo"
        );
        assert!(values.take("quix").unwrap().is_none());

        assert_eq!(
            values.take("baz").unwrap().unwrap().as_str().unwrap(),
            "bar"
        );
        assert!(values.take("baz").unwrap().is_none());
    }
}
