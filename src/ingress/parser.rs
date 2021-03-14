use arrow::array::{ArrayRef, StringArray, StringBuilder};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use regex::bytes;
use regex::Regex;
use std::sync::Arc;

pub struct Parser {
    schema: SchemaRef,
    regex: Regex,
}

impl Parser {
    fn new(pattern: &str, schema: SchemaRef) -> Parser {
        Parser {
            schema,
            regex: Regex::new(pattern).unwrap(),
        }
    }

    fn parse(&self, lines: Vec<&str>) -> RecordBatch {
        // Create builders for each column
        let fields = self.schema.fields();
        let cols = fields.len();
        let mut string_builders = Vec::with_capacity(cols);
        for _ in 0..cols {
            string_builders.push(StringBuilder::new(lines.len()));
        }

        // Write columns to each builder
        for line in lines {
            let caps = self.regex.captures(line).unwrap();
            for i in 0..cols {
                match caps.name(fields[i].name()) {
                    Some(x) => {
                        string_builders[i].append_value(x.as_str()).unwrap();
                    }
                    None => {
                        string_builders[i].append_null().unwrap();
                    }
                }
            }
        }

        // Collect builder to form array
        let mut arrays = Vec::with_capacity(cols);
        for i in 0..cols {
            let array = StringArray::from(string_builders[i].finish());
            let array_ref = Arc::new(array) as ArrayRef;
            let typed_array_ref = cast(&array_ref, fields[i].data_type()).unwrap();
            arrays.push(typed_array_ref);
        }

        RecordBatch::try_new(self.schema.clone(), arrays).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use log::debug;
    use std::sync::Arc;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn parse_basic() {
        init();
        let parser = Parser::new(
            "f=(?P<f>\\w+),b=(?P<b>\\w+)?",
            Arc::from(Schema::new(vec![
                Field::new("f", DataType::Utf8, false),
                Field::new("b", DataType::Utf8, false),
            ])),
        );

        let record_batch = parser.parse(vec!["f=o1,b=ar", "f=o2,b=99", "f=o3,b="]);
        assert_eq!(3, record_batch.num_rows());
        assert_eq!(2, record_batch.num_columns());
        assert_eq!(
            format!("{:#?}", record_batch),
            "RecordBatch {
    schema: Schema {
        fields: [
            Field {
                name: \"f\",
                data_type: Utf8,
                nullable: false,
                dict_id: 0,
                dict_is_ordered: false,
                metadata: None,
            },
            Field {
                name: \"b\",
                data_type: Utf8,
                nullable: false,
                dict_id: 0,
                dict_is_ordered: false,
                metadata: None,
            },
        ],
        metadata: {},
    },
    columns: [
        StringArray
        [
          \"o1\",
          \"o2\",
          \"o3\",
        ],
        StringArray
        [
          \"ar\",
          \"99\",
          null,
        ],
    ],
}"
        );
    }

    #[test]
    fn parse_column_by_name() {
        init();
        // Notice that f goes first in the pattern but last in the schema.
        let parser = Parser::new(
            "f=(?P<f>\\w+),b=(?P<b>\\w+)?",
            Arc::from(Schema::new(vec![
                Field::new("b", DataType::Utf8, false),
                Field::new("f", DataType::Utf8, false),
            ])),
        );

        let record_batch = parser.parse(vec!["f=oo,b=ar"]);
        assert_eq!(1, record_batch.num_rows());
        debug!("{:#?}", record_batch);

        let col_b = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(StringArray::from(vec!["ar"]), *col_b);

        let col_f = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(StringArray::from(vec!["oo"]), *col_f);
    }
}
