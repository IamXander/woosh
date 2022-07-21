use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::Stream;
use log::trace;
use tonic::Status;

use crate::{
    memory_store::MemoryStore,
    proto::build::bazel::remote::execution::v2::{
        content_addressable_storage_server::ContentAddressableStorage, BatchReadBlobsRequest,
        BatchReadBlobsResponse, BatchUpdateBlobsRequest, BatchUpdateBlobsResponse,
        FindMissingBlobsRequest, FindMissingBlobsResponse, GetTreeRequest, GetTreeResponse,
    },
};

#[derive(Default)]
pub struct ContentAddressableStorageServer {
    memory_store: Arc<Mutex<MemoryStore>>,
}

impl ContentAddressableStorageServer {
    pub fn new(memory_store: Arc<Mutex<MemoryStore>>) -> ContentAddressableStorageServer {
        ContentAddressableStorageServer {
            memory_store: memory_store,
        }
    }
}
type GetTreeResponseStream = Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send>>;

#[tonic::async_trait]
impl ContentAddressableStorage for ContentAddressableStorageServer {
    type GetTreeStream = GetTreeResponseStream;

    async fn find_missing_blobs(
        &self,
        find_missing_blobs_req: tonic::Request<FindMissingBlobsRequest>,
    ) -> Result<tonic::Response<FindMissingBlobsResponse>, Status> {
        let find_missing_blobs_req = find_missing_blobs_req.into_inner();
        // trace!("FB:\n{:?}", find_missing_blobs_req);
        let mut missing_blob_digests = vec![];
        for blob_digest in find_missing_blobs_req.blob_digests {
            let resource_id = blob_digest.clone().into();
            if !self.memory_store.lock().unwrap().in_cache(&resource_id) {
                missing_blob_digests.push(blob_digest);
            }
        }
        trace!("FB MISSING:\n{:?}", missing_blob_digests);
        Ok(tonic::Response::new(FindMissingBlobsResponse {
            missing_blob_digests: missing_blob_digests,
        }))
    }

    async fn batch_update_blobs(
        &self,
        batch_update_blobs_req: tonic::Request<BatchUpdateBlobsRequest>,
    ) -> Result<tonic::Response<BatchUpdateBlobsResponse>, Status> {
        trace!("BUB:\n{:?}", batch_update_blobs_req);
        todo!()
    }

    async fn batch_read_blobs(
        &self,
        batch_read_blobs_req: tonic::Request<BatchReadBlobsRequest>,
    ) -> Result<tonic::Response<BatchReadBlobsResponse>, Status> {
        trace!("BRB:\n{:?}", batch_read_blobs_req);
        todo!()
    }

    async fn get_tree(
        &self,
        get_tree_req: tonic::Request<GetTreeRequest>,
    ) -> Result<tonic::Response<Self::GetTreeStream>, tonic::Status> {
        trace!("GT:\n{:?}", get_tree_req);
        todo!()
    }
}
