mod action_cache_server;
mod byte_stream_server;
mod memory_store;
mod proto;
mod resource_id;

use proto::build::bazel::remote::execution::v2::{GetTreeRequest, WaitExecutionRequest};
use proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use tonic::{transport::Server, Request, Response, Status};

use futures::Stream;
// use woosh::memory_store::MemoryStore;
use crate::memory_store::MemoryStore;
use crate::proto::build::bazel::remote::execution::v2::action_cache_server::{
    ActionCache, ActionCacheServer,
};
use crate::proto::build::bazel::remote::execution::v2::capabilities_server::{
    Capabilities, CapabilitiesServer,
};
use crate::proto::build::bazel::remote::execution::v2::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use crate::proto::build::bazel::remote::execution::v2::execution_server::{
    Execution, ExecutionServer,
};
use crate::proto::build::bazel::remote::execution::v2::{
    digest_function, symlink_absolute_path_strategy, ActionCacheUpdateCapabilities, ActionResult,
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, CacheCapabilities, ExecuteRequest, FindMissingBlobsRequest,
    FindMissingBlobsResponse, GetActionResultRequest, GetCapabilitiesRequest, GetTreeResponse,
    PriorityCapabilities, ServerCapabilities, UpdateActionResultRequest,
};
use crate::proto::build::bazel::semver::SemVer;
use crate::proto::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use std::sync::{Arc, Mutex};
use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};

use crate::proto::build::bazel::remote::execution::v2::compressor;

// use hello_world::greeter_server::{Greeter, GreeterServer};
// use hello_world::{HelloReply, HelloRequest};

// # build.bazel.remote.execution.v2
// pub mod build {
//     pub mod bazel {
//         pub mod remote {
//             pub mod execution {
//                 pub mod v2 {
//                     tonic::include_proto!("build.bazel.remote.execution.v2");
//                 }
//             }
//         }
//         pub mod semver {
//             tonic::include_proto!("build.bazel.semver");
//         }
//     }
// }

// pub mod google {
//     pub mod rpc {
//         tonic::include_proto!("google.rpc");
//     }
//     pub mod longrunning {
//         tonic::include_proto!("google.longrunning");
//     }
// }

// third_party/remote-apis/build/bazel/semver/semver.proto

// pub mod google {
//     pub mod longrunning {
//         pub mod Operation {
//             tonic::include_proto!(".bazel.remote.execution.v2build");
//         }
//     }
// }
type ReadResponseStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send>>;
type GetTreeResponseStream = Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send>>;
type ExecuteResponseStream = Pin<
    Box<dyn Stream<Item = Result<crate::proto::google::longrunning::Operation, Status>> + Send>,
>;
type WaitExecutionResponseStream = Pin<
    Box<dyn Stream<Item = Result<crate::proto::google::longrunning::Operation, Status>> + Send>,
>;

#[derive(Debug, Default)]
pub struct WooshServer {}

// #[tonic::async_trait]
// impl ByteStream for WooshByteStreamServer {
//     type ReadStream = ReadResponseStream;
//     // type BidirectionalStreamingWriteStream = WriteResponseStream;

//     // async fn find_missing_blobs(&self, find_missing_blobs_req: tonic::Request<FindMissingBlobsRequest>) -> Result<Response<FindMissingBlobsResponse>, Status> {
//     //     println!("FB:\n{:?}", find_missing_blobs_req);
//     //     Ok(tonic::Response::new(FindMissingBlobsResponse { missing_blob_digests: find_missing_blobs_req.into_inner().blob_digests }))
//     //     // todo!()
//     // }

//     // async fn batch_update_blobs(&self, batch_update_blobs_req: tonic::Request<BatchUpdateBlobsRequest>) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
//     //     println!("BUB:\n{:?}", batch_update_blobs_req);
//     //     todo!()
//     // }

//     // async fn batch_read_blobs(&self, batch_read_blobs_req: tonic::Request<BatchReadBlobsRequest>) -> Result<Response<BatchReadBlobsResponse>, Status> {
//     //     println!("BRB:\n{:?}", batch_read_blobs_req);
//     //     todo!()
//     // }

//     // async fn bidirectional_streaming_echo(
//     //         &self,
//     //         req: Request<Streaming<EchoRequest>>,
//     //     ) -> EchoResult<Self::BidirectionalStreamingEchoStream> {

//     // async fn write(
//     //     &self,
//     //     req: Request<tonic::Streaming<WriteRequest> >,
//     // ) -> Result<tonic::Response<Self::BidirectionalStreamingWriteStream>, tonic::Status> {
//     //     todo!()
//     // }

//     async fn write(
//         &self,
//         write_requests: tonic::Request<tonic::Streaming<WriteRequest>>,
//     ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
//         let mut messages = write_requests.into_inner();
//         loop {
//             let message_res = messages.message().await;
//             if message_res.is_err() {
//                 break;
//             }
//             let mut message_opt = message_res.unwrap();
//             if message_opt.is_none() {
//                 break;
//             }
//             let message = message_opt.unwrap();
//             println!("WRITE:\n{:?}", message);
//             // message
//         }
//         // if let Some(next_message) = write_requests.into_inner().message().await {
//         //     println!("{:?}", next_message);
//         // }
//         // for write_request in write_requests.into_inner() {

//         // }
//         // println!("WRITE:\n{:?}", write_request);
//         todo!()
//     }

//     async fn read(
//         &self,
//         read_request: Request<ReadRequest>,
//     ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
//         println!("READ:\n{:?}", read_request);
//         todo!()
//     }

//     async fn query_write_status(
//         &self,
//         query_write_status_req: tonic::Request<QueryWriteStatusRequest>,
//     ) -> Result<Response<QueryWriteStatusResponse>, Status> {
//         println!("QWS:\n{:?}", query_write_status_req);
//         todo!()
//     }
// }

#[tonic::async_trait]
impl ContentAddressableStorage for WooshServer {
    type GetTreeStream = GetTreeResponseStream;

    async fn find_missing_blobs(
        &self,
        find_missing_blobs_req: tonic::Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        println!("FB:\n{:?}", find_missing_blobs_req);
        Ok(tonic::Response::new(FindMissingBlobsResponse {
            missing_blob_digests: find_missing_blobs_req.into_inner().blob_digests,
        }))
        // todo!()
    }

    async fn batch_update_blobs(
        &self,
        batch_update_blobs_req: tonic::Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        println!("BUB:\n{:?}", batch_update_blobs_req);
        todo!()
    }

    async fn batch_read_blobs(
        &self,
        batch_read_blobs_req: tonic::Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        println!("BRB:\n{:?}", batch_read_blobs_req);
        todo!()
    }

    async fn get_tree(
        &self,
        req: Request<GetTreeRequest>,
    ) -> Result<tonic::Response<Self::GetTreeStream>, tonic::Status> {
        todo!()
    }
    // type WaitExecutionStream = WaitExecutionResponseStream;

    // async fn execute(
    //     &self,
    //     req: Request<crate::build::bazel::remote::execution::v2::ExecuteRequest>,
    // ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
    //     todo!()
    // }

    // async fn wait_execution(
    //     &self,
    //     req: Request<crate::build::bazel::remote::execution::v2::WaitExecutionRequest>,
    // ) -> Result<tonic::Response<Self::WaitExecutionStream>, tonic::Status> {
    //     todo!()
    // }
}

#[tonic::async_trait]
impl Execution for WooshServer {
    type ExecuteStream = ExecuteResponseStream;
    type WaitExecutionStream = WaitExecutionResponseStream;

    async fn execute(
        &self,
        req: Request<ExecuteRequest>,
    ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
        todo!()
    }

    async fn wait_execution(
        &self,
        req: Request<WaitExecutionRequest>,
    ) -> Result<tonic::Response<Self::WaitExecutionStream>, tonic::Status> {
        todo!()
    }
}
// #[derive(Debug)]
// impl std::fmt::Debug for GetActionResultRequest {

// }

#[tonic::async_trait]
impl Capabilities for WooshServer {
    async fn get_capabilities(
        &self,
        get_capabilities_request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        println!("CAP:\n{:?}", get_capabilities_request);

        Ok(tonic::Response::new(ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                digest_functions: vec![digest_function::Value::Sha256.into()],
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities {
                    update_enabled: true,
                }),
                cache_priority_capabilities: Some(PriorityCapabilities { priorities: vec![] }),
                max_batch_total_size_bytes: 0,
                symlink_absolute_path_strategy: symlink_absolute_path_strategy::Value::Disallowed
                    .into(),
                supported_compressors: vec![compressor::Value::Identity.into()],
                supported_batch_update_compressors: vec![compressor::Value::Identity.into()],
            }),
            execution_capabilities: None,
            deprecated_api_version: None,
            low_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: "".to_string(),
            }),
            high_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: "".to_string(),
            }),
        }))
        // todo!()
        // Err(Status::unimplemented("not implemented"))
    }

    // type ExecuteStream = ExecuteResponseStream;
    // type WaitExecutionStream = WaitExecutionResponseStream;

    // async fn execute(
    //     &self,
    //     req: Request<crate::build::bazel::remote::execution::v2::ExecuteRequest>,
    // ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
    //     todo!()
    // }

    // async fn wait_execution(
    //     &self,
    //     req: Request<crate::build::bazel::remote::execution::v2::WaitExecutionRequest>,
    // ) -> Result<tonic::Response<Self::WaitExecutionStream>, tonic::Status> {
    //     todo!()
    // }
}

// use futures::Stream;
// use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};
// use tokio::sync::mpsc;
// use tokio_stream::{wrappers::ReceiverStream, StreamExt};
// use tonic::{transport::Server, Request, Response, Status, Streaming};

// use pb::{EchoRequest, EchoResponse};

// type EchoResult<T> = Result<Response<T>, Status>;
// type ResponseStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

// fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
//     let mut err: &(dyn Error + 'static) = err_status;

//     loop {
//         if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
//             return Some(io_err);
//         }

//         // h2::Error do not expose std::io::Error with `source()`
//         // https://github.com/hyperium/h2/pull/462
//         if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
//             if let Some(io_err) = h2_err.get_io() {
//                 return Some(io_err);
//             }
//         }

//         err = match err.source() {
//             Some(err) => err,
//             None => return None,
//         };
//     }
// }

// #[derive(Debug)]
// pub struct EchoServer {}

// #[tonic::async_trait]
// impl pb::echo_server::Echo for EchoServer {
//     async fn unary_echo(&self, _: Request<EchoRequest>) -> EchoResult<EchoResponse> {
//         Err(Status::unimplemented("not implemented"))
//     }

//     type ServerStreamingEchoStream = ResponseStream;

//     async fn server_streaming_echo(
//         &self,
//         req: Request<EchoRequest>,
//     ) -> EchoResult<Self::ServerStreamingEchoStream> {
//         println!("EchoServer::server_streaming_echo");
//         println!("\tclient connected from: {:?}", req.remote_addr());

//         // creating infinite stream with requested message
//         let repeat = std::iter::repeat(EchoResponse {
//             message: req.into_inner().message,
//         });
//         let mut stream = Box::pin(tokio_stream::iter(repeat).throttle(Duration::from_millis(200)));

//         // spawn and channel are required if you want handle "disconnect" functionality
//         // the `out_stream` will not be polled after client disconnect
//         let (tx, rx) = mpsc::channel(128);
//         tokio::spawn(async move {
//             while let Some(item) = stream.next().await {
//                 match tx.send(Result::<_, Status>::Ok(item)).await {
//                     Ok(_) => {
//                         // item (server response) was queued to be send to client
//                     }
//                     Err(_item) => {
//                         // output_stream was build from rx and both are dropped
//                         break;
//                     }
//                 }
//             }
//             println!("\tclient disconnected");
//         });

//         let output_stream = ReceiverStream::new(rx);
//         Ok(Response::new(
//             Box::pin(output_stream) as Self::ServerStreamingEchoStream
//         ))
//     }

//     async fn client_streaming_echo(
//         &self,
//         _: Request<Streaming<EchoRequest>>,
//     ) -> EchoResult<EchoResponse> {
//         Err(Status::unimplemented("not implemented"))
//     }

//     type BidirectionalStreamingEchoStream = ResponseStream;

//     async fn bidirectional_streaming_echo(
//         &self,
//         req: Request<Streaming<EchoRequest>>,
//     ) -> EchoResult<Self::BidirectionalStreamingEchoStream> {
//         println!("EchoServer::bidirectional_streaming_echo");

//         let mut in_stream = req.into_inner();
//         let (tx, rx) = mpsc::channel(128);

//         // this spawn here is required if you want to handle connection error.
//         // If we just map `in_stream` and write it back as `out_stream` the `out_stream`
//         // will be drooped when connection error occurs and error will never be propagated
//         // to mapped version of `in_stream`.
//         tokio::spawn(async move {
//             while let Some(result) = in_stream.next().await {
//                 match result {
//                     Ok(v) => tx
//                         .send(Ok(EchoResponse { message: v.message }))
//                         .await
//                         .expect("working rx"),
//                     Err(err) => {
//                         if let Some(io_err) = match_for_io_error(&err) {
//                             if io_err.kind() == ErrorKind::BrokenPipe {
//                                 // here you can handle special case when client
//                                 // disconnected in unexpected way
//                                 eprintln!("\tclient disconnected: broken pipe");
//                                 break;
//                             }
//                         }

//                         match tx.send(Err(err)).await {
//                             Ok(_) => (),
//                             Err(_err) => break, // response was droped
//                         }
//                     }
//                 }
//             }
//             println!("\tstream ended");
//         });

//         // echo just write the same data that was received
//         let out_stream = ReceiverStream::new(rx);

//         Ok(Response::new(
//             Box::pin(out_stream) as Self::BidirectionalStreamingEchoStream
//         ))
//     }
// }

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let server = EchoServer {};
//     Server::builder()
//         .add_service(pb::echo_server::EchoServer::new(server))
//         .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
//         .await
//         .unwrap();

//     Ok(())
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let memory_store = Arc::new(Mutex::new(MemoryStore::new()));

    let server1 = WooshServer {};
    let server2 = WooshServer {};
    let server4 = WooshServer {};
    // build::bazel::remote::execution::v2::execution_server::ExecutionServer::new(server)
    Server::builder()
        .accept_http1(true)
        .add_service(CapabilitiesServer::new(server1))
        .add_service(ExecutionServer::new(server2))
        .add_service(crate::action_cache_server::ActionCacheServer::new(
            memory_store,
        ))
        .add_service(ContentAddressableStorageServer::new(server4))
        .add_service(ByteStreamServer::new(
            crate::byte_stream_server::ByteStreamServer::new(memory_store),
        ))
        .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();

    Ok(())
}
