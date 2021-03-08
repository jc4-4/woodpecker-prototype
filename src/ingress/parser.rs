use regex::bytes;
use regex::Regex;

use arrow::array::{
    ArrayRef, StringArray, StringBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
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

    // TODO: arrow to parquet: https://github.com/apache/arrow/blob/master/rust/parquet/src/arrow/arrow_writer.rs
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
            for j in 0..cols {
                // index 0 refers to the entire match
                match caps.get(j + 1) {
                    Some(x) => {
                        string_builders[j].append_value(x.as_str()).unwrap();
                    }
                    None => {
                        string_builders[j].append_null().unwrap();
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

#[test]
fn parser() {
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
fn example_group() {
    let re = Regex::new("f=(?P<f>\\w+),b=(?P<b>\\w+)").unwrap();
    let text = "f=oo,b=ar";
    let caps = re.captures(text).unwrap();
    assert_eq!("oo", &caps["f"]);
    assert_eq!("ar", &caps["b"]);

    assert_eq!("oo", caps.name("f").unwrap().as_str());
    assert_eq!("ar", caps.name("b").unwrap().as_str());
    assert_eq!(None, caps.name("nothing"));

    assert_eq!("f=oo,b=ar", caps.get(0).unwrap().as_str());
    assert_eq!("oo", caps.get(1).unwrap().as_str());
    assert_eq!("ar", caps.get(2).unwrap().as_str());
    assert_eq!(None, caps.get(3));
}

#[test]
fn example_bytes() {
    let re = bytes::Regex::new("f=(?P<f>\\w+),b=(?P<b>\\w+)").unwrap();
    let bytes = b"f=oo,b=ar";
    let caps = re.captures(bytes).unwrap();

    assert_eq!("oo", std::str::from_utf8(&caps["f"]).unwrap());
    assert_eq!(b"ar", &caps["b"]);
}
