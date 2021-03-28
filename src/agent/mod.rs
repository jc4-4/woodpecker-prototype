pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/woodpecker.protobuf.rs"));
}

pub mod client;
pub mod presigned_url;
pub mod server;
pub mod tailer;
pub mod uploader;
