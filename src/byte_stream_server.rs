use std::{
    cmp,
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
    resource_id::ResourceId,
};

static MAX_MESSAGE_SIZE: u64 = 16384;

#[derive(Default)]
pub struct ReadResponseStream {
    memory_store: Arc<Mutex<MemoryStore>>,
    resource_id: ResourceId,
    read_offset: u64,
    read_limit: Option<u64>,
}

// resource_name: "blobs/8f9cf8177c72cbdc437b5be8e336361ae1b578dfaade8174bcc196890ef6871e/38397", read_offset: 0, read_limit: 0
impl ReadResponseStream {
    pub fn new(
        memory_store: Arc<Mutex<MemoryStore>>,
        resource_id: ResourceId,
        read_offset: u64,
        read_limit: Option<u64>,
    ) -> ReadResponseStream {
        assert!(read_limit.is_none() || read_limit.unwrap() != 0);
        assert!(read_limit.is_none() || read_limit.unwrap() >= read_offset);
        println!(
            "Created response stream for {:?} from {:?} to {:?}",
            resource_id, read_offset, read_limit
        );
        ReadResponseStream {
            memory_store: memory_store,
            resource_id: resource_id,
            read_offset: read_offset,
            read_limit: read_limit,
        }
    }
}

impl Stream for ReadResponseStream {
    type Item = Result<ReadResponse, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let chunked_read_distance = cmp::min(
            MAX_MESSAGE_SIZE,
            self.read_limit.unwrap_or(u64::MAX) - self.read_offset,
        );
        let data = self.memory_store.lock().unwrap().get_data(
            &self.resource_id,
            self.read_offset,
            self.read_offset + chunked_read_distance,
        );

        if data.is_none() {
            println!(
                "THERE WAS AN ERROR WRITING DATA for {:?} from {:?} with a length of {:?}",
                self.resource_id, self.read_offset, chunked_read_distance
            );
            return std::task::Poll::Ready(Some(Err(Status::new(
                tonic::Code::NotFound,
                "There was an error writing data",
            ))));
        }
        let data = data.unwrap();
        println!(
            "Sending data for {:?} from {:?} with a length of {:?} which is {:?} bytes",
            self.resource_id,
            self.read_offset,
            chunked_read_distance,
            data.len()
        );
        self.read_offset += chunked_read_distance;
        if data.is_empty() {
            // There is no more data left to send back
            return std::task::Poll::Ready(None);
        }
        return std::task::Poll::Ready(Some(Ok(ReadResponse { data: data })));
    }
}

#[derive(Default)]
pub struct ByteStreamServer {
    memory_store: Arc<Mutex<MemoryStore>>,
}

// type ReadResponseStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send>>;

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
        Ok(tonic::Response::new(WriteResponse {
            committed_size: comitted_len.try_into().unwrap(),
        }))
    }

    async fn read(
        &self,
        read_request: tonic::Request<ReadRequest>,
    ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
        // Ok(tonic::Streaming::())
        println!("READ:\n{:?}", read_request);
        let read_request = read_request.into_inner();
        let read_limit: Option<u64> = if read_request.read_limit == 0 {
            None
        } else {
            Some(read_request.read_limit.try_into().unwrap())
        };
        return Ok(tonic::Response::new(ReadResponseStream::new(
            self.memory_store.clone(),
            read_request.resource_name.try_into().unwrap(),
            read_request.read_offset.try_into().unwrap(),
            read_limit,
        )));
        // todo!()
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
