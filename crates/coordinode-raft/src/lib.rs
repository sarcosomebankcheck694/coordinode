pub mod cluster;
pub mod proposal;
pub mod snapshot;
pub mod storage;

/// Generated protobuf types for Raft inter-node gRPC protocol.
pub mod proto {
    pub mod replication {
        tonic::include_proto!("coordinode.v1.replication");
    }
}
