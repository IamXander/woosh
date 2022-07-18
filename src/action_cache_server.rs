use std::sync::{Arc, Mutex};

use tonic::Status;

use crate::{
    memory_store::MemoryStore,
    proto::build::bazel::remote::execution::v2::{
        action_cache_server::ActionCache, ActionResult, GetActionResultRequest,
        UpdateActionResultRequest,
    },
};

#[derive(Default)]
pub struct ActionCacheServer {
    memory_store: Arc<Mutex<MemoryStore>>,
}

#[tonic::async_trait]
impl ActionCache for ActionCacheServer {
    async fn get_action_result(
        &self,
        get_action_result_request: tonic::Request<GetActionResultRequest>,
    ) -> Result<tonic::Response<ActionResult>, Status> {
        let get_action_result_request = get_action_result_request.into_inner();
        if get_action_result_request.action_digest.is_none() {
            println!(
                "Action result request does not have an action digest, {:?}",
                get_action_result_request
            );
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "Action result request does not have an action digest",
            ));
        }
        let action_digest = get_action_result_request.action_digest.unwrap();

        // if get_action_result_request.is
        // self.memory_store.lock().unwrap().get_action_cache(sha)
        // get_action_result_request.into_inner().inline_stderr
        // println!("GET:\n{:?}", get_action_result_request);
        // crate::proto::google::rpc::Code::NotFound.into()
        Err(Status::new(tonic::Code::NotFound, "Couldn't find it"))
        // Err(Status {
        //     code: crate::proto::google::rpc::Code::NotFound.into(),
        //      message: "Couldn't find it".to_string(),
        //      details: todo!(),
        //      metadata: todo!(),
        //       source: todo!()
        //      })
        // ActionResult {}
        // let abc = ActionResult {
        //     output_files: todo!(),
        //     output_file_symlinks: todo!(),
        //     output_symlinks: todo!(),
        //     output_directories: todo!(),
        //     output_directory_symlinks: todo!(),
        //     exit_code: todo!(),
        //     stdout_raw: todo!(),
        //     stdout_digest: todo!(),
        //     stderr_raw: todo!(),
        //     stderr_digest: todo!(),
        //     execution_metadata: todo!()
        // };
        // todo!()
        // None
    }

    async fn update_action_result(
        &self,
        update_action_result_request: tonic::Request<UpdateActionResultRequest>,
    ) -> Result<tonic::Response<ActionResult>, Status> {
        // update_action_result_request.into_inner().action_result.unwrap().
        println!("UPDATE:\n{:?}", update_action_result_request);
        todo!()
    }
    // type ExecuteStream = ExecuteResponseStream;
    // type WaitExecutionStream = WaitExecutionResponseStream;

    // async fn execute(
    //     &self,
    //     req: Request<ExecuteRequest>,
    // ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status> {
    //     todo!()
    // }

    // async fn wait_execution(
    //     &self,
    //     req: Request<WaitExecutionRequest>,
    // ) -> Result<tonic::Response<Self::WaitExecutionStream>, tonic::Status> {
    //     todo!()
    // }
}
