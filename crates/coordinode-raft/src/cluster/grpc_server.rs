//! gRPC server handler for Raft inter-node RPCs.
//!
//! Implements `RaftService` tonic trait. Dispatches incoming RPCs to the
//! local openraft instance. Uses msgpack for type serialization.

use std::pin::Pin;
use std::sync::{Arc, Weak};

use futures_util::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use coordinode_storage::engine::core::StorageEngine;

use crate::proto::replication::raft_service_server::RaftService;
use crate::proto::replication::{RaftEmpty, RaftPayload};
use crate::storage::{CoordinodeStateMachine, TypeConfig};

type RaftInstance = openraft::Raft<TypeConfig, CoordinodeStateMachine>;

/// gRPC server handler for Raft consensus RPCs.
pub struct RaftGrpcHandler {
    raft: Arc<RaftInstance>,
    /// Weak ref to engine for incremental snapshots.
    /// Weak avoids preventing engine drop during node shutdown.
    engine: Weak<StorageEngine>,
}

impl RaftGrpcHandler {
    pub fn new(raft: Arc<RaftInstance>, engine: Arc<StorageEngine>) -> Self {
        Self {
            raft,
            engine: Arc::downgrade(&engine),
        }
    }
}

fn ser<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    rmp_serde::to_vec(value).map_err(|e| Status::internal(format!("msgpack serialize: {e}")))
}

fn de<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, Status> {
    rmp_serde::from_slice(data)
        .map_err(|e| Status::invalid_argument(format!("msgpack deserialize: {e}")))
}

#[tonic::async_trait]
impl RaftService for RaftGrpcHandler {
    async fn vote(&self, request: Request<RaftPayload>) -> Result<Response<RaftPayload>, Status> {
        let vote_req: openraft::raft::VoteRequest<TypeConfig> = de(&request.into_inner().data)?;

        let vote_resp = self
            .raft
            .vote(vote_req)
            .await
            .map_err(|e| Status::internal(format!("vote: {e}")))?;

        Ok(Response::new(RaftPayload {
            data: ser(&vote_resp)?,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let req: openraft::raft::AppendEntriesRequest<TypeConfig> = de(&request.into_inner().data)?;

        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("append_entries: {e}")))?;

        Ok(Response::new(RaftPayload { data: ser(&resp)? }))
    }

    type StreamAppendStream = Pin<Box<dyn Stream<Item = Result<RaftPayload, Status>> + Send>>;

    async fn stream_append(
        &self,
        request: Request<Streaming<RaftPayload>>,
    ) -> Result<Response<Self::StreamAppendStream>, Status> {
        let input = request.into_inner();

        // Deserialize incoming RaftPayload stream → AppendEntriesRequest stream
        let input_stream = input.filter_map(|result| async move {
            match result {
                Ok(payload) => match rmp_serde::from_slice(&payload.data) {
                    Ok(req) => Some(req),
                    Err(e) => {
                        tracing::warn!("stream_append deserialize error: {e}");
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!("stream_append receive error: {e}");
                    None
                }
            }
        });

        // Feed to openraft's stream_append — it handles everything
        let output = self.raft.stream_append(input_stream);

        // Serialize output stream: StreamAppendResult → RaftPayload
        let output_stream = output.map(|result| match result {
            Ok(stream_result) => {
                let data = rmp_serde::to_vec(&stream_result)
                    .map_err(|e| Status::internal(format!("serialize: {e}")))?;
                Ok(RaftPayload { data })
            }
            Err(fatal) => Err(Status::internal(format!("fatal: {fatal}"))),
        });

        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn snapshot(
        &self,
        request: Request<Streaming<RaftPayload>>,
    ) -> Result<Response<RaftPayload>, Status> {
        let mut stream = request.into_inner();

        // Collect all chunks into a single buffer.
        // Currently snapshots are sent as a single message, but this
        // handles chunked streaming for future large snapshots.
        let mut all_data = Vec::new();
        while let Some(result) = stream.next().await {
            let payload = result
                .map_err(|e| Status::internal(format!("snapshot stream receive error: {e}")))?;
            all_data.extend_from_slice(&payload.data);
        }

        // Deserialize the snapshot transfer message
        let transfer: crate::snapshot::SnapshotTransfer = rmp_serde::from_slice(&all_data)
            .map_err(|e| Status::invalid_argument(format!("snapshot transfer deserialize: {e}")))?;

        let is_incremental = transfer.since_ts.is_some();

        tracing::info!(
            snapshot_id = %transfer.meta.snapshot_id,
            data_bytes = transfer.data.len(),
            last_log_index = transfer.meta.last_log_id.map(|id| id.index),
            incremental = is_incremental,
            since_ts = ?transfer.since_ts,
            "received snapshot from leader"
        );

        // For incremental snapshots: apply delta data directly to storage,
        // then pass empty data to openraft for Raft state update only.
        // For full snapshots: pass data through to openraft as before.
        let raft_data = if is_incremental {
            let engine = self.engine.upgrade().ok_or_else(|| {
                Status::unavailable("engine dropped during incremental snapshot install")
            })?;
            crate::snapshot::install_incremental_snapshot(&engine, &transfer.data)
                .map_err(|e| Status::internal(format!("incremental snapshot install: {e}")))?;
            // Empty data — state machine's install_snapshot will skip data apply
            Vec::new()
        } else {
            transfer.data
        };

        let snapshot_cursor = std::io::Cursor::new(raft_data);
        let snapshot = openraft::storage::Snapshot {
            meta: transfer.meta.clone(),
            snapshot: snapshot_cursor,
        };

        // install_full_snapshot returns SnapshotResponse with the
        // follower's current vote — NOT the leader's vote from the transfer.
        // This is important: the leader uses the response vote to detect
        // if the follower has seen a higher term (split-brain prevention).
        let response = self
            .raft
            .install_full_snapshot(transfer.vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("install_snapshot: {e}")))?;

        let resp_bytes = ser(&response)?;

        tracing::info!("snapshot installation complete");
        Ok(Response::new(RaftPayload { data: resp_bytes }))
    }

    async fn transfer_leader(
        &self,
        request: Request<RaftPayload>,
    ) -> Result<Response<RaftEmpty>, Status> {
        let req: openraft::raft::TransferLeaderRequest<TypeConfig> =
            de(&request.into_inner().data)?;

        self.raft
            .handle_transfer_leader(req)
            .await
            .map_err(|e| Status::internal(format!("transfer_leader: {e}")))?;

        Ok(Response::new(RaftEmpty {}))
    }
}
