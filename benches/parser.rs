use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use log::{debug, error};
use regex::bytes::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

/// Parser splits log by line into events, then parse each event to fields with regex.
pub struct RegexParser {
    regex: Regex,
}

impl RegexParser {
    pub fn new(regex: &str) -> Self {
        Self {
            regex: Regex::new(regex).unwrap(),
        }
    }

    pub fn parse(&self, bytes: Bytes) -> Vec<HashMap<String, String>> {
        let mut result = vec![];
        for line in bytes.split(|&char| char == b'\n') {
            if line.is_empty() {
                continue;
            }
            result.push(self.parse_event(line));
        }
        result
    }

    fn parse_event(&self, event: &[u8]) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for caps in self.regex.captures_iter(event) {
            for name in self.regex.capture_names() {
                if let Some(name) = name {
                    let cap = caps.name(name).unwrap();
                    map.insert(
                        name.to_string(),
                        String::from_utf8_lossy(cap.as_bytes()).to_string(),
                    );
                }
            }
        }
        if map.is_empty() {
            panic!(
                "Event does not match regex: {}",
                String::from_utf8_lossy(event)
            );
        }
        map
    }
}

/// Parser splits log by line into events, then parse each event into whitespace-separated fields.
pub struct WhitespaceParser {
    fields: Vec<String>,
}

impl WhitespaceParser {
    pub fn new(fields: Vec<&str>) -> Self {
        let mut strings = Vec::with_capacity(fields.len());
        for s in fields {
            strings.push(s.to_string());
        }
        Self { fields: strings }
    }

    pub fn parse(&self, bytes: Bytes) -> Vec<HashMap<String, String>> {
        let mut result = vec![];
        for line in bytes.split(|&char| char == b'\n') {
            if line.is_empty() {
                continue;
            }
            result.push(self.parse_event(line));
        }
        result
    }

    fn parse_event(&self, event: &[u8]) -> HashMap<String, String> {
        let mut map = HashMap::new();
        let mut rem = event;
        let mut i = 0;
        while i < self.fields.len() - 1 {
            let groups: Vec<&[u8]> = rem.splitn(2, |&char| char == b' ').collect();
            // if groups.len() != 2 {
            //     error!("{} => {:#?}", String::from_utf8_lossy(event), String::from_utf8_lossy(groups[0]));
            // }

            // ignore consecutive whitespace
            if !groups[0].is_empty() {
                map.insert(
                    self.fields[i].clone(),
                    String::from_utf8_lossy(groups[0]).to_string(),
                );
                i += 1;
            }
            rem = groups[1]
        }
        map.insert(
            self.fields[i].clone(),
            String::from_utf8_lossy(rem).to_string(),
        );
        // if map.len() != 4 {
        //     error!("{} => {:#?}", String::from_utf8_lossy(event), map);
        // }
        map
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
