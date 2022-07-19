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
