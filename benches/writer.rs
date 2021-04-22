use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prototype::ingress::parser::RegexParser;
use prototype::ingress::writer::Writer;
use std::fs::File;
use std::io::Read;

fn read_testinput(file: &str) -> Bytes {
    let file = File::open(format!("{}{}", "./testinput/", file)).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let mut buf = String::new();
    reader.read_to_string(&mut buf).unwrap();
    Bytes::from(buf)
}

fn writer_benchmark(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut group = c.benchmark_group("writer_benchmark");
    let parser = RegexParser::new(
        r"\[(?P<timestamp>\S+)\s+(?P<level>\S+)\s+(?P<class>\S+)]\s+(?P<content>.*)",
    );
    for file in ["small.log", "medium.log", "large.log"].iter() {
        let bytes = read_testinput(file);
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        let batch = parser.parse(bytes);
        let writer = Writer::new(parser.schema());
        group.bench_with_input(BenchmarkId::from_parameter(&file), &batch, |b, _batch| {
            b.iter(|| {
                let _file = writer.write(batch.clone());
            });
        });
    }
    group.finish();
}

criterion_group!(benches, writer_benchmark);
criterion_main!(benches);
