pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/woodpecker.protobuf.rs"));
}

pub mod client;
mod integration_test;
pub mod server;
