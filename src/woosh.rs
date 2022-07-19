mod action_cache_server;
mod byte_stream_server;
mod capabilities_server;
mod content_addressable_storage_server;
mod execution_server;
mod memory_store;
mod proto;
mod resource_id;

use env_logger::Env;
use tonic::transport::Server;

use crate::memory_store::MemoryStore;
use crate::proto::build::bazel::remote::execution::v2::action_cache_server::ActionCacheServer;
use crate::proto::build::bazel::remote::execution::v2::capabilities_server::CapabilitiesServer;
use crate::proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorageServer;
use crate::proto::build::bazel::remote::execution::v2::execution_server::ExecutionServer;

use crate::proto::google::bytestream::byte_stream_server::ByteStreamServer;

use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};

// #[derive(Debug)]
// impl std::fmt::Debug for GetActionResultRequest {

// }

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
    let logging_env = Env::default()
        .filter_or("LOG_LEVEL", "info,woosh_server=trace")
        .write_style_or("LOG_LEVEL", "always");

    env_logger::init_from_env(logging_env);

    let memory_store = Arc::new(Mutex::new(MemoryStore::new()));

    Server::builder()
        .accept_http1(true)
        .add_service(CapabilitiesServer::new(
            crate::capabilities_server::CapabilitiesServer::new(),
        ))
        .add_service(ExecutionServer::new(
            crate::execution_server::ExecutionServer::new(memory_store.clone()),
        ))
        .add_service(ActionCacheServer::new(
            crate::action_cache_server::ActionCacheServer::new(memory_store.clone()),
        ))
        .add_service(ContentAddressableStorageServer::new(
            crate::content_addressable_storage_server::ContentAddressableStorageServer::new(
                memory_store.clone(),
            ),
        ))
        .add_service(ByteStreamServer::new(
            crate::byte_stream_server::ByteStreamServer::new(memory_store),
        ))
        .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();

    Ok(())
}
