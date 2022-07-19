use std::sync::{Arc, Mutex};

use log::{trace, warn};
use tonic::Status;

use crate::{
    memory_store::MemoryStore,
    proto::build::bazel::remote::execution::v2::{
        action_cache_server::ActionCache, ActionResult, GetActionResultRequest,
        UpdateActionResultRequest,
    },
    resource_id::ResourceId,
};

#[derive(Default)]
pub struct ActionCacheServer {
    memory_store: Arc<Mutex<MemoryStore>>,
}

impl ActionCacheServer {
    pub fn new(memory_store: Arc<Mutex<MemoryStore>>) -> ActionCacheServer {
        ActionCacheServer {
            memory_store: memory_store,
        }
    }
}

#[tonic::async_trait]
impl ActionCache for ActionCacheServer {
    async fn get_action_result(
        &self,
        get_action_result_request: tonic::Request<GetActionResultRequest>,
    ) -> Result<tonic::Response<ActionResult>, Status> {
        let get_action_result_request = get_action_result_request.into_inner();
        if get_action_result_request.action_digest.is_none() {
            warn!(
                "Action result request does not have an action digest, {:?}",
                get_action_result_request
            );
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "Action result request does not have an action digest",
            ));
        }
        let action_digest = get_action_result_request.action_digest.unwrap();
        let res_id: ResourceId = action_digest.into();
        let action_result = self.memory_store.lock().unwrap().get_action_cache(&res_id);
        if action_result.is_none() {
            return Err(Status::new(
                tonic::Code::NotFound,
                "Did not find action cache item with provided digest",
            ));
        }

        return Ok(tonic::Response::new(action_result.unwrap().clone()));
    }

    async fn update_action_result(
        &self,
        update_action_result_request: tonic::Request<UpdateActionResultRequest>,
    ) -> Result<tonic::Response<ActionResult>, Status> {
        let update_action_result_request = update_action_result_request.into_inner();
        if update_action_result_request.action_digest.is_none()
            || update_action_result_request.action_result.is_none()
        {
            warn!(
                "Update action result request does not have an action digest, {:?}",
                update_action_result_request
            );
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "Update action result request does not have an action digest",
            ));
        }
        trace!("UPDATE:\n{:?}", update_action_result_request);
        let action_result = update_action_result_request.action_result.unwrap();
        self.memory_store.lock().unwrap().set_action_cache(
            update_action_result_request.action_digest.unwrap().into(),
            action_result.clone(),
        );

        return Ok(tonic::Response::new(action_result.clone()));
    }
}
