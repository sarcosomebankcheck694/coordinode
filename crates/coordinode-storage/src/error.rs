//! Storage engine error types.

/// Errors from the storage engine layer.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Underlying storage error.
    #[error("storage engine error: {0}")]
    Engine(#[from] lsm_tree::Error),

    /// Partition not found.
    #[error("partition not found: {name}")]
    PartitionNotFound { name: String },

    /// Invalid configuration.
    #[error("invalid storage config: {0}")]
    InvalidConfig(String),

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Transaction conflict (OCC retry needed).
    #[error("transaction conflict, retry")]
    Conflict,

    /// I/O error (file read/write, directory operations).
    #[error("I/O error: {0}")]
    Io(String),

    /// CRC32 checksum mismatch — data corruption detected.
    #[error("checksum mismatch in {context}: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        expected: u32,
        actual: u32,
        context: String,
    },
}

/// Result type alias for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;
