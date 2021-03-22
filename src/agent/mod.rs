pub mod key_repository;

pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/woodpecker.protobuf.rs"));
}

pub mod client;
pub mod server;
pub mod tailer;
