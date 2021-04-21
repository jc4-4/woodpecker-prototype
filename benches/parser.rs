use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use log::{debug, error};
use regex::bytes::Regex;
use std::fs::File;
use std::io::Read;
use std::str;
use std::str::from_utf8;
use std::sync::Arc;

/// Parser splits log by line into events, then parse each event to fields with regex.
pub struct RegexParser {
    regex: Regex,
    schema: SchemaRef,
}

impl RegexParser {
    pub fn new(regex_str: &str) -> Self {
        let regex = Regex::new(regex_str).unwrap();
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

fn read_testinput(file: &str) -> Bytes {
    let file = File::open(format!("{}{}", "./testinput/", file)).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let mut buf = String::new();
    reader.read_to_string(&mut buf).unwrap();
    Bytes::from(buf)
}

fn regex_parser_benchmark(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut group = c.benchmark_group("regex_parser_benchmark");
    let parser = RegexParser::new(
        r"\[(?P<timestamp>\S+)\s+(?P<level>\S+)\s+(?P<class>\S+)]\s+(?P<content>.*)",
    );
    for file in ["small.log", "medium.log", "large.log"].iter() {
        let bytes = read_testinput(file);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(&file), &bytes, |b, bytes| {
            b.iter(|| {
                let _events = parser.parse(bytes.clone());
            });
        });
    }
    group.finish();
}

fn whitespace_parser_benchmark(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut group = c.benchmark_group("whitespace_parser_benchmark");
    let parser = WhitespaceParser::new(vec!["timestamp", "level", "class", "content"]);
    for file in ["small.log", "medium.log", "large.log"].iter() {
        let bytes = read_testinput(file);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(&file), &bytes, |b, bytes| {
            b.iter(|| {
                let _events = parser.parse(bytes.clone());
            });
        });
    }
    group.finish();
}

criterion_group!(benches, regex_parser_benchmark, whitespace_parser_benchmark);
criterion_main!(benches);
