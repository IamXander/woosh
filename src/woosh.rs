mod lib;

use tonic::{transport::Server, Request, Response, Status};

use lib::build::bazel::remote::execution::v2::execution_server::{Execution, ExecutionServer};
use lib::build::bazel::remote::execution::v2::ExecuteRequest;
use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};
use futures::Stream;

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

type ExecuteResponseStream = Pin<Box<dyn Stream<Item = Result<lib::google::longrunning::Operation, Status>> + Send>>;
type WaitExecutionResponseStream = Pin<Box<dyn Stream<Item = Result<lib::google::longrunning::Operation, Status>> + Send>>;

#[derive(Debug, Default)]
pub struct WooshServer {}

#[tonic::async_trait]
impl lib::build::bazel::remote::execution::v2::execution_server::Execution for WooshServer {

    type ExecuteStream = ExecuteResponseStream;
    type WaitExecutionStream = WaitExecutionResponseStream;

    async fn execute(
        &self,
        req: Request<lib::build::bazel::remote::execution::v2::ExecuteRequest>,
    ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
        todo!()
    }

    async fn wait_execution(
        &self,
        req: Request<lib::build::bazel::remote::execution::v2::WaitExecutionRequest>,
    ) -> Result<tonic::Response<Self::WaitExecutionStream>, tonic::Status> {
        todo!()
    }
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
    let server = WooshServer {};
    // build::bazel::remote::execution::v2::execution_server::ExecutionServer::new(server)
    Server::builder()
        .add_service(lib::build::bazel::remote::execution::v2::execution_server::ExecutionServer::new(server))
        .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();

    Ok(())
}