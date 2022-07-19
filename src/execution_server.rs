use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::Stream;
use log::trace;

use crate::{
    memory_store::MemoryStore,
    proto::build::bazel::remote::execution::v2::{
        execution_server::Execution, ExecuteRequest, WaitExecutionRequest,
    },
};

#[derive(Default)]
pub struct ExecutionServer {
    #[allow(dead_code)]
    memory_store: Arc<Mutex<MemoryStore>>,
}

impl ExecutionServer {
    pub fn new(memory_store: Arc<Mutex<MemoryStore>>) -> ExecutionServer {
        ExecutionServer {
            memory_store: memory_store,
        }
    }
}

type ExecuteResponseStream = Pin<
    Box<
        dyn Stream<Item = Result<crate::proto::google::longrunning::Operation, tonic::Status>>
            + Send,
    >,
>;
type WaitExecutionResponseStream = Pin<
    Box<
        dyn Stream<Item = Result<crate::proto::google::longrunning::Operation, tonic::Status>>
            + Send,
    >,
>;

#[tonic::async_trait]
impl Execution for ExecutionServer {
    type ExecuteStream = ExecuteResponseStream;
    type WaitExecutionStream = WaitExecutionResponseStream;

    async fn execute(
        &self,
        execute_req: tonic::Request<ExecuteRequest>,
    ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
        trace!("ER:\n{:?}", execute_req);
        todo!()
    }

    async fn wait_execution(
        &self,
        wait_execute_req: tonic::Request<WaitExecutionRequest>,
    ) -> Result<tonic::Response<Self::WaitExecutionStream>, tonic::Status> {
        trace!("WER:\n{:?}", wait_execute_req);
        todo!()
    }
}
