use std::{
    error::Error,
    fmt::{Display, Formatter},
    io, result,
};

use arrow::error::ArrowError;

pub type Result<T> = result::Result<T, WoodpeckerError>;

/// Woodpecker error
#[derive(Debug)]
pub enum WoodpeckerError {
    ArrowError(ArrowError),
    General(String),
    GrpcError(tonic::Status),
    Internal(String),
    IoError(io::Error),
    SerdeJsonError(serde_json::Error),
    NotImplemented(String),
    RusotoError(String), // Use String to workaround type parameter in RusotoError.
    SerdeDdbError(serde_dynamodb::Error),
    TokioError(tokio::task::JoinError),
    TonicError(tonic::transport::Error),
}

impl<T> From<WoodpeckerError> for Result<T> {
    fn from(e: WoodpeckerError) -> Self {
        Err(e)
    }
}

impl From<WoodpeckerError> for tonic::Status {
    fn from(e: WoodpeckerError) -> Self {
        tonic::Status::new(tonic::Code::Internal, e.to_string())
    }
}

pub fn woodpecker_error(message: &str) -> WoodpeckerError {
    WoodpeckerError::General(message.to_owned())
}

impl From<ArrowError> for WoodpeckerError {
    fn from(e: ArrowError) -> Self {
        WoodpeckerError::ArrowError(e)
    }
}

impl From<serde_json::Error> for WoodpeckerError {
    fn from(e: serde_json::Error) -> Self {
        WoodpeckerError::SerdeJsonError(e)
    }
}

impl From<String> for WoodpeckerError {
    fn from(e: String) -> Self {
        WoodpeckerError::General(e)
    }
}

impl From<tonic::Status> for WoodpeckerError {
    fn from(e: tonic::Status) -> Self {
        WoodpeckerError::GrpcError(e)
    }
}

impl From<io::Error> for WoodpeckerError {
    fn from(e: io::Error) -> Self {
        WoodpeckerError::IoError(e)
    }
}

impl<E: Error + 'static> From<rusoto_core::RusotoError<E>> for WoodpeckerError {
    fn from(e: rusoto_core::RusotoError<E>) -> Self {
        WoodpeckerError::RusotoError(e.to_string())
    }
}

impl From<serde_dynamodb::Error> for WoodpeckerError {
    fn from(e: serde_dynamodb::Error) -> Self {
        WoodpeckerError::SerdeDdbError(e)
    }
}

impl From<tokio::task::JoinError> for WoodpeckerError {
    fn from(e: tokio::task::JoinError) -> Self {
        WoodpeckerError::TokioError(e)
    }
}

impl From<tonic::transport::Error> for WoodpeckerError {
    fn from(e: tonic::transport::Error) -> Self {
        WoodpeckerError::TonicError(e)
    }
}

impl Display for WoodpeckerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WoodpeckerError::ArrowError(ref desc) => write!(f, "Arrow error: {}", desc),
            WoodpeckerError::General(ref desc) => write!(f, "General error: {}", desc),
            WoodpeckerError::GrpcError(desc) => write!(f, "Grpc error: {}", desc),
            WoodpeckerError::Internal(desc) => write!(f, "Internal error: {}", desc),
            WoodpeckerError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            WoodpeckerError::NotImplemented(ref desc) => write!(f, "Not implemented: {}", desc),
            WoodpeckerError::RusotoError(ref desc) => write!(f, "Rusoto error: {}", desc),
            WoodpeckerError::SerdeDdbError(ref desc) => write!(f, "Serde error: {}", desc),
            WoodpeckerError::SerdeJsonError(ref desc) => write!(f, "Serde error: {}", desc),
            WoodpeckerError::TokioError(desc) => write!(f, "Tokio join error: {}", desc),
            WoodpeckerError::TonicError(desc) => write!(f, "Tonic error: {}", desc),
        }
    }
}

impl Error for WoodpeckerError {}
