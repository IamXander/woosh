use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::Stream;
use tonic::Status;

use crate::{
    memory_store::MemoryStore,
    proto::google::bytestream::{
        byte_stream_server::ByteStream, QueryWriteStatusRequest, QueryWriteStatusResponse,
        ReadRequest, ReadResponse, WriteRequest, WriteResponse,
    },
};

#[derive(Default)]
pub struct ByteStreamServer {
    memory_store: Arc<Mutex<MemoryStore>>,
}

type ReadResponseStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send>>;

impl ByteStreamServer {
    pub fn new(memory_store: Arc<Mutex<MemoryStore>>) -> ByteStreamServer {
        ByteStreamServer {
            memory_store: memory_store,
        }
    }
}

#[tonic::async_trait]
impl ByteStream for ByteStreamServer {
    type ReadStream = ReadResponseStream;
    // type BidirectionalStreamingWriteStream = WriteResponseStream;

    // async fn find_missing_blobs(&self, find_missing_blobs_req: tonic::Request<FindMissingBlobsRequest>) -> Result<Response<FindMissingBlobsResponse>, Status> {
    //     println!("FB:\n{:?}", find_missing_blobs_req);
    //     Ok(tonic::Response::new(FindMissingBlobsResponse { missing_blob_digests: find_missing_blobs_req.into_inner().blob_digests }))
    //     // todo!()
    // }

    // async fn batch_update_blobs(&self, batch_update_blobs_req: tonic::Request<BatchUpdateBlobsRequest>) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
    //     println!("BUB:\n{:?}", batch_update_blobs_req);
    //     todo!()
    // }

    // async fn batch_read_blobs(&self, batch_read_blobs_req: tonic::Request<BatchReadBlobsRequest>) -> Result<Response<BatchReadBlobsResponse>, Status> {
    //     println!("BRB:\n{:?}", batch_read_blobs_req);
    //     todo!()
    // }

    // async fn bidirectional_streaming_echo(
    //         &self,
    //         req: Request<Streaming<EchoRequest>>,
    //     ) -> EchoResult<Self::BidirectionalStreamingEchoStream> {

    // async fn write(
    //     &self,
    //     req: Request<tonic::Streaming<WriteRequest> >,
    // ) -> Result<tonic::Response<Self::BidirectionalStreamingWriteStream>, tonic::Status> {
    //     todo!()
    // }

    async fn write(
        &self,
        write_requests: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let mut messages = write_requests.into_inner();
        let mut comitted_len = None;
        let mut resource_name: String = "".to_string();
        loop {
            let write_request = messages.message().await;
            if write_request.is_err() {
                println!("Write request was an error");
                println!("{:?}", write_request);
                break;
            }
            let write_request = write_request.unwrap();
            if write_request.is_none() {
                break;
            }
            let write_request = write_request.unwrap();
            if resource_name == "" {
                resource_name = write_request.resource_name.clone()
            }
            println!("WRITE:\n{:?}", write_request);
            comitted_len = self.memory_store.lock().unwrap().append_data(
                &resource_name,
                write_request.data,
                write_request.write_offset.try_into().unwrap(),
                write_request.finish_write,
            );
            if comitted_len.is_none() {
                println!("There was an error writing data");
                return Err(Status::new(
                    tonic::Code::Unknown,
                    "There was an error writing data",
                ));
            }

            // message
        }
        if comitted_len.is_none() {
            println!("No data was ever comitted.");
            return Err(Status::new(
                tonic::Code::Unknown,
                "No data was ever comitted.",
            ));
        }
        let comitted_len = comitted_len.unwrap();
        // if let Some(next_message) = write_requests.into_inner().message().await {
        //     println!("{:?}", next_message);
        // }
        // for write_request in write_requests.into_inner() {

        // }
        // println!("WRITE:\n{:?}", write_request);
        // let (length, complete) = self
        //     .memory_store
        //     .lock()
        //     .unwrap()
        //     .get_write_status(query_write_status_req.into_inner().resource_name);
        Ok(tonic::Response::new(WriteResponse {
            committed_size: comitted_len.try_into().unwrap(),
        }))
    }

    async fn read(
        &self,
        read_request: tonic::Request<ReadRequest>,
    ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
        println!("READ:\n{:?}", read_request);
        todo!()
    }

    async fn query_write_status(
        &self,
        query_write_status_req: tonic::Request<QueryWriteStatusRequest>,
    ) -> Result<tonic::Response<QueryWriteStatusResponse>, Status> {
        println!("QWS:\n{:?}", query_write_status_req);
        let (length, complete) = self
            .memory_store
            .lock()
            .unwrap()
            .get_write_status(query_write_status_req.into_inner().resource_name);
        Ok(tonic::Response::new(QueryWriteStatusResponse {
            committed_size: length.try_into().unwrap(),
            complete: complete,
        }))
    }
}
