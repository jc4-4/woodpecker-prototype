use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use log::debug;
use prototype::ingress::parser::Parser;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

fn read_file(name: &str) -> Bytes {
    let file = File::open(format!("{}{}", "./src/bin/", name)).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let mut buf = String::new();
    reader.read_to_string(&mut buf).unwrap();
    debug!("{}", buf);
    buf.into()
}

fn parse(parser: &Parser, bytes: Bytes) {
    let record_batch = parser.parse(bytes);
    assert_eq!(26, record_batch.num_rows());
}

fn criterion_benchmark(c: &mut Criterion) {
    env_logger::init();
    let bytes = read_file("mary.log");
    // Compile regex only once
    let parser = Parser::new(
        "\\[(?P<timestamp>([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\\.[0-9]+)?(([Zz])|([\\+|\\-]([01][0-9]|2[0-3]):[0-5][0-9]))) (?P<level>\\w+) (?P<class>\\w+)\\](?P<content>.*)",
        Arc::from(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("level", DataType::Utf8, false),
            Field::new("class", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
        ])),
    );
    c.bench_function("mary.log", |b| {
        b.iter(|| parse(black_box(&parser), black_box(bytes.clone())))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
