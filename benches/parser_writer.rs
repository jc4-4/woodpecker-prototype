use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prototype::ingress::parser::{RegexParser, WhitespaceParser};
use std::fs::File;
use std::io::Read;
use prototype::ingress::writer::Writer;

fn read_testinput(file: &str) -> Bytes {
    let file = File::open(format!("{}{}", "./testinput/", file)).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let mut buf = String::new();
    reader.read_to_string(&mut buf).unwrap();
    Bytes::from(buf)
}

fn whitespace_parser_writer_benchmark(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut group = c.benchmark_group("whitespace_parser_writer_benchmark");
    let parser = WhitespaceParser::new(vec!["timestamp", "level", "class", "content"]);
    for file in ["small.log", "medium.log", "large.log"].iter() {
        let bytes = read_testinput(file);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        let writer = Writer::new(parser.schema());
        group.bench_with_input(BenchmarkId::from_parameter(&file), &bytes, |b, _batch| {
            b.iter(|| {
                let batch = parser.parse(bytes.clone());
                let _file = writer.write(batch);
            });
        });
    }
    group.finish();
}

fn regex_parser_writer_benchmark(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut group = c.benchmark_group("regex_parser_writer_benchmark");
    let parser = RegexParser::new(
        r"\[(?P<timestamp>\S+)\s+(?P<level>\S+)\s+(?P<class>\S+)]\s+(?P<content>.*)",
    );
    for file in ["small.log", "medium.log", "large.log"].iter() {
        let bytes = read_testinput(file);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        let writer = Writer::new(parser.schema());
        group.bench_with_input(BenchmarkId::from_parameter(&file), &bytes, |b, _batch| {
            b.iter(|| {
                let batch = parser.parse(bytes.clone());
                let _file = writer.write(batch);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, regex_parser_writer_benchmark, whitespace_parser_writer_benchmark);
criterion_main!(benches);
