use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::Stream;
use log::{error, trace};
use prost::Message;

use crate::{
    memory_store::MemoryStore,
    proto::build::bazel::remote::execution::v2::{
        execution_server::Execution, Action, Command, Directory, ExecuteRequest,
        WaitExecutionRequest,
    },
};

#[derive(Default)]
pub struct ExecuteResponseStream {
    // memory_store: Arc<Mutex<MemoryStore>>,
    // resource_id: ResourceId,
    // read_offset: u64,
    // read_limit: Option<u64>,
}

impl ExecuteResponseStream {
    pub fn new(// memory_store: Arc<Mutex<MemoryStore>>,
        // resource_id: ResourceId,
        // read_offset: u64,
        // read_limit: Option<u64>,
    ) -> Self {
        Self {
            // memory_store: memory_store,
            // resource_id: resource_id,
            // read_offset: read_offset,
            // read_limit: read_limit,
        }
    }
}

impl Stream for ExecuteResponseStream {
    type Item = Result<crate::proto::google::longrunning::Operation, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
        // let chunked_read_distance = cmp::min(
        //     MAX_MESSAGE_SIZE,
        //     self.read_limit.unwrap_or(u64::MAX) - self.read_offset,
        // );
        // let data = self.memory_store.lock().unwrap().get_data(
        //     &self.resource_id,
        //     self.read_offset,
        //     self.read_offset + chunked_read_distance,
        // );

        // if data.is_none() {
        //     error!(
        //         "THERE WAS AN ERROR WRITING DATA for {:?} from {:?} with a length of {:?}",
        //         self.resource_id, self.read_offset, chunked_read_distance
        //     );
        //     return std::task::Poll::Ready(Some(Err(Status::new(
        //         tonic::Code::NotFound,
        //         "There was an error writing data",
        //     ))));
        // }
        // let data = data.unwrap();
        // trace!(
        //     "Sending data for {} from {:?} with a length of {:?} which is {:?} bytes",
        //     self.resource_id,
        //     self.read_offset,
        //     chunked_read_distance,
        //     data.len()
        // );
        // self.read_offset += chunked_read_distance;
        // if data.is_empty() {
        //     // There is no more data left to send back
        //     return std::task::Poll::Ready(None);
        // }
        // return std::task::Poll::Ready(Some(Ok(ReadResponse { data: data })));
    }
}

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
        let execute_req = execute_req.into_inner();
        assert_eq!(execute_req.execution_policy, None);
        assert_eq!(execute_req.results_cache_policy, None);
        assert_eq!(execute_req.skip_cache_lookup, false);
        if execute_req.action_digest.is_none() {
            error!("Execute Request action digest was none");
            return Err(tonic::Status::new(
                tonic::Code::Unknown,
                "Execute Request action digest was none",
            ));
        }
        let action_digest_resource_id = execute_req.action_digest.unwrap().into();
        trace!("ACTION DIGEST {}", action_digest_resource_id);
        trace!("{}", self.memory_store.lock().unwrap());
        let action: Option<Action> = self
            .memory_store
            .lock()
            .unwrap()
            .get_typed_data(&action_digest_resource_id);
        if action.is_none() {
            error!("There was an error finding the action {:?}", action);
            return Err(tonic::Status::new(
                tonic::Code::NotFound,
                "There was an error finding the action",
            ));
        }
        let action = action.unwrap();
        trace!("{:?}", action);
        let command_digest = action.command_digest;
        let input_root_digest = action.input_root_digest;
        if command_digest.is_none() || input_root_digest.is_none() {
            error!("Command digest or input root digest not set");
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                "Command digest or input root digest not set",
            ));
        }
        let command_resource_id = command_digest.unwrap().into();
        let input_root_resource_id = input_root_digest.unwrap().into();

        trace!(
            "COMMAND DIGEST {} INPUT ROOT DIGEST {}",
            command_resource_id,
            input_root_resource_id
        );
        let command: Option<Command> = self
            .memory_store
            .lock()
            .unwrap()
            .get_typed_data(&command_resource_id);

        let input_root: Option<Directory> = self
            .memory_store
            .lock()
            .unwrap()
            .get_typed_data(&input_root_resource_id);
        if command.is_none() || input_root.is_none() {
            error!("Could not find the command or input root");
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                "Could not find the command or input root",
            ));
        }
        let command = command.unwrap();
        let input_root = input_root.unwrap();
        trace!("COMMAND {:?} INPUT_ROOT {:?}", command, input_root);

        // Message
        // Action::try_from(action_data);
        // Action::from(action_data);

        // unsafe {
        //     let t = &action_data[0..68];
        //     let action_data = std::str::from_utf8_unchecked(t);
        //     trace!("{:?}", action_data);
        // }
        // let read_limit: Option<u64> = if read_request.read_limit == 0 {
        //     None
        // } else {
        //     Some(read_request.read_limit.try_into().unwrap())
        // };
        // return Ok(tonic::Response::new(ReadResponseStream::new(
        //     self.memory_store.clone(),
        //     read_request.resource_name.try_into().unwrap(),
        //     read_request.read_offset.try_into().unwrap(),
        //     read_limit,
        // )));
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
