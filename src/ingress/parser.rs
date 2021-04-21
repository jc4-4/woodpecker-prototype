use arrow::array::{ArrayRef, StringBuilder};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use log::debug;
use regex::bytes::Regex as BytesRegex;
use regex::Regex;
use std::str;
use std::str::from_utf8;
use std::sync::Arc;

pub struct Parser {
    schema: SchemaRef,
    regex: Regex,
}

impl Parser {
    // TODO: Refactor to try_new for regex errors.
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

    // TODO: refactor to return Result<RecordBatch>
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
            // TODO: handle when line does not match regex
            // TODO: add system fields like timestamp and raw
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

/// Parser splits log by line into events, then parse each event to fields with regex.
pub struct RegexParser {
    regex: BytesRegex,
    schema: SchemaRef,
}

impl RegexParser {
    pub fn new(regex_str: &str) -> Self {
        let regex = BytesRegex::new(regex_str).unwrap();
        let fields = regex
            .capture_names()
            .flatten()
            .map(|name| Field::new(name, DataType::Utf8, false))
            .collect();
        Self {
            regex,
            schema: Arc::new(Schema::new(fields)),
        }
    }

    pub fn parse(&self, bytes: Bytes) -> RecordBatch {
        let cols = self.columns();
        let mut builders: Vec<StringBuilder> = Vec::with_capacity(cols);
        for _ in 0..cols {
            builders.push(StringBuilder::new(10));
        }
        for line in bytes.split(|&char| char == b'\n') {
            if line.is_empty() {
                continue;
            }
            self.parse_event(line, &mut builders);
        }

        let mut arrays = Vec::with_capacity(cols);
        for i in 0..cols {
            let array_ref = Arc::new(builders[i].finish()) as ArrayRef;
            arrays.push(array_ref);
        }

        RecordBatch::try_new(self.schema.clone(), arrays).unwrap()
    }

    fn parse_event(&self, event: &[u8], builders: &mut Vec<StringBuilder>) {
        let caps = self.regex.captures(event).expect("Regex matches event");
        for i in 0..builders.len() {
            if let Some(m) = caps.name(self.schema.field(i).name()) {
                builders[i]
                    .append_value(std::str::from_utf8(m.as_bytes()).expect("Wellformed Utf8"))
                    .expect("Append String");
            }
        }
    }

    fn columns(&self) -> usize {
        self.schema.fields().len()
    }
}

/// Parser splits log by line into events, then parse each event into whitespace-separated fields.
pub struct WhitespaceParser {
    schema: SchemaRef,
}

impl WhitespaceParser {
    pub fn new(names: Vec<&str>) -> Self {
        let fields = names
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, false))
            .collect();
        Self {
            schema: Arc::new(Schema::new(fields)),
        }
    }

    pub fn parse(&self, bytes: Bytes) -> RecordBatch {
        let cols = self.columns();
        let mut builders: Vec<StringBuilder> = Vec::with_capacity(cols);
        for _ in 0..cols {
            builders.push(StringBuilder::new(10));
        }
        for line in bytes.split(|&char| char == b'\n') {
            if line.is_empty() {
                continue;
            }
            self.parse_event(line, &mut builders);
        }

        let mut arrays = Vec::with_capacity(cols);
        for i in 0..cols {
            let array_ref = Arc::new(builders[i].finish()) as ArrayRef;
            arrays.push(array_ref);
        }

        RecordBatch::try_new(self.schema.clone(), arrays).unwrap()
    }

    fn parse_event(&self, event: &[u8], builders: &mut Vec<StringBuilder>) {
        let mut rem = event;
        let mut i = 0;
        while i < builders.len() - 1 {
            let groups: Vec<&[u8]> = rem.splitn(2, |&char| char == b' ').collect();
            // ignore consecutive whitespace
            if !groups[0].is_empty() {
                builders[i]
                    .append_value(from_utf8(groups[0]).expect("Well-formed Utf8"))
                    .expect("Append String: ");
                i += 1;
            }
            rem = groups[1]
        }
        builders[i]
            .append_value(from_utf8(rem).expect("Well-formed Utf8"))
            .expect("Append String: ");
    }

    fn columns(&self) -> usize {
        self.schema.fields().len()
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;
    use crate::error::Result;
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

        let col_b = to_string_array(&record_batch, 0);
        assert_eq!(StringArray::from(vec!["ar"]), *col_b);

        let col_f = to_string_array(&record_batch, 1);
        assert_eq!(StringArray::from(vec!["oo"]), *col_f);
    }

    fn to_string_array(record_batch: &RecordBatch, col: usize) -> &StringArray {
        record_batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    #[test]
    fn rust_log() -> Result<()> {
        init();
        let line = "[2021-04-07T05:33:41Z DEBUG log_gen]    Its fleece was white as snow,";
        // RFC3339 regex: https://gist.github.com/marcelotmelo/b67f58a08bee6c2468f8
        let parser = Parser::new(
            "\\[(?P<timestamp>([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\\.[0-9]+)?(([Zz])|([\\+|\\-]([01][0-9]|2[0-3]):[0-5][0-9]))) (?P<level>\\w+) (?P<class>\\w+)\\](?P<content>.*)",
            Arc::from(Schema::new(vec![
                Field::new("timestamp", DataType::Utf8, false),
                Field::new("level", DataType::Utf8, false),
                Field::new("class", DataType::Utf8, false),
                Field::new("content", DataType::Utf8, false),
            ])),
        );

        let record_batch = parser.parse(line.into());
        assert_eq!(1, record_batch.num_rows());
        debug!("{:#?}", record_batch);

        assert_eq!(
            StringArray::from(vec!["2021-04-07T05:33:41Z"]),
            *to_string_array(&record_batch, 0)
        );
        assert_eq!(
            StringArray::from(vec!["DEBUG"]),
            *to_string_array(&record_batch, 1)
        );
        assert_eq!(
            StringArray::from(vec!["log_gen"]),
            *to_string_array(&record_batch, 2)
        );
        assert_eq!(
            StringArray::from(vec!["    Its fleece was white as snow,"]),
            *to_string_array(&record_batch, 3)
        );

        Ok(())
    }
}
