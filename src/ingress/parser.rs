use arrow::array::{ArrayRef, StringBuilder};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use log::debug;
use regex::Regex;
use std::sync::Arc;
use bytes::Bytes;

pub struct Parser {
    schema: SchemaRef,
    regex: Regex,
}

impl Parser {
    pub fn new(pattern: &str, schema: SchemaRef) -> Parser {
        Parser {
            schema,
            regex: Regex::new(pattern).unwrap(),
        }
    }

    pub fn parse(&self, bytes: Bytes) -> RecordBatch {
        // TODO: split by log type, e.g. NEW_LINE vs START_WITH etc.
        let utf8 = String::from_utf8(bytes.to_vec()).unwrap();
        let lines = utf8.split('\n').collect();
        self.parse_lines(lines)
    }

    fn parse_lines(&self, lines: Vec<&str>) -> RecordBatch {
        // Create builders for each column
        let fields = self.schema.fields();
        let cols = fields.len();
        let mut string_builders = Vec::with_capacity(cols);
        for _ in 0..cols {
            string_builders.push(StringBuilder::new(lines.len()));
        }

        // Write columns to each builder
        for line in lines {
            debug!("Parsing line: {}", line);
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
            let array_ref = Arc::new(string_builders[i].finish()) as ArrayRef;
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

        let record_batch = parser.parse("f=o1,b=ar\nf=o2,b=99\nf=o3,b=".into());
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

        let record_batch = parser.parse("f=oo,b=ar".into());
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
