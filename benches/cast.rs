use arrow::array::{Array, ArrayRef, PrimitiveBuilder, StringArray, TimestampNanosecondArray};
use arrow::datatypes::TimestampNanosecondType;
use bytes::Bytes;
use chrono::prelude::*;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use log::debug;
use prototype::ingress::parser::RegexParser;
use std::fs::File;
use std::io::Read;
use std::str;

fn cast_string_to_timestamp(array: &ArrayRef) -> TimestampNanosecondArray {
    let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
    let mut builder = PrimitiveBuilder::<TimestampNanosecondType>::new(string_array.len());
    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            builder.append_null().unwrap();
        } else {
            match DateTime::parse_from_rfc3339(string_array.value(i)) {
                Ok(nanos) => builder.append_value(nanos.timestamp_nanos()).unwrap(),
                Err(_) => builder.append_null().unwrap(), // not a valid date
            }
        }
    }
    builder.finish()
}

fn read_testinput(file: &str) -> Bytes {
    let file = File::open(format!("{}{}", "./testinput/", file)).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let mut buf = String::new();
    reader.read_to_string(&mut buf).unwrap();
    Bytes::from(buf)
}

fn cast_benchmark(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut group = c.benchmark_group("cast_benchmark");
    let parser = RegexParser::new(
        r"\[(?P<timestamp>\S+)\s+(?P<level>\S+)\s+(?P<class>\S+)]\s+(?P<content>.*)",
    );
    for file in ["small.log", "medium.log", "large.log"].iter() {
        let bytes = read_testinput(file);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        let batch = parser.parse(bytes);
        group.bench_with_input(BenchmarkId::from_parameter(&file), &batch, |b, batch| {
            b.iter(|| {
                let array_ref = batch.column(0);
                debug!("{:#?}", array_ref.slice(15, 5));
                let typed_array_ref = cast_string_to_timestamp(array_ref);
                debug!("{:#?}", typed_array_ref.slice(15, 5));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, cast_benchmark);
criterion_main!(benches);
