use log::trace;

use crate::proto::build::bazel::{
    remote::execution::v2::{
        capabilities_server::Capabilities, compressor, digest_function,
        symlink_absolute_path_strategy, ActionCacheUpdateCapabilities, CacheCapabilities,
        GetCapabilitiesRequest, PriorityCapabilities, ServerCapabilities,
    },
    semver::SemVer,
};

#[derive(Default)]
pub struct CapabilitiesServer {}

impl CapabilitiesServer {
    pub fn new() -> CapabilitiesServer {
        CapabilitiesServer {}
    }
}

#[tonic::async_trait]
impl Capabilities for CapabilitiesServer {
    async fn get_capabilities(
        &self,
        get_capabilities_request: tonic::Request<GetCapabilitiesRequest>,
    ) -> Result<tonic::Response<ServerCapabilities>, tonic::Status> {
        trace!("CAP:\n{:?}", get_capabilities_request);

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
    }
}
