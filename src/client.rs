mod proto;
mod resource_id;

use crate::proto::build::bazel::remote::execution::v2;
use crate::proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use crate::proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use crate::proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, FindMissingBlobsRequest, GetActionResultRequest,
    UpdateActionResultRequest,
};
use crate::proto::google::bytestream::byte_stream_client::ByteStreamClient;
use crate::proto::google::bytestream::WriteRequest;
use crate::resource_id::ResourceId;
use clap::Parser;
use env_logger::Env;
use futures::Stream;
use log::{error, warn, info};
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fmt::Write;
use std::fs;
use std::io::ErrorKind;
use tonic::transport::Channel;
use uuid::Uuid;

static MAX_MESSAGE_SIZE: usize = 16384;

#[derive(Parser)] // requires `derive` feature
#[clap(author, version, about, long_about = None)]
enum CLI {
    Check(Check),
    Pull(Pull),
    Push(Push),
}

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
struct Check {
    #[clap(long, value_parser)]
    input: std::path::PathBuf,
}

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
struct Pull {
    #[clap(long, value_parser)]
    input: std::path::PathBuf,
}

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
struct Push {
    #[clap(long, value_parser)]
    input: std::path::PathBuf,
    #[clap(long, value_parser)]
    output: std::path::PathBuf,
}

#[derive(Deserialize, Debug)]
struct TestInput {
    input_files: Vec<String>,
    expected_files: Vec<String>,
    command: Vec<String>,
    // TODOL Properties
    // TODO: Enviorment variables
}

#[derive(Deserialize, Debug)]
struct TestResults {
    files: Vec<String>,
    exit_code: i32,
}

struct ByteStreamClientWriter {
    resource_name: String,
    data: Vec<u8>,
    write_offset: usize,
}

impl ByteStreamClientWriter {
    fn new(resource_id: ResourceId, data: Vec<u8>) -> Self {
        // resource_name: "uploads/e57a73ff-744e-4fa3-b9af-b2b384589149/blobs/639b6acdc55bebb727fb0aa19230fc3d548b62e9fc9a0eb9deda55f4c352de32/1146"
        let mut resource_name = String::new();
        write!(
            &mut resource_name,
            "uploads/{}/blobs/{}/{}",
            Uuid::new_v4(),
            resource_id.hash,
            resource_id.length
        )
        .unwrap();
        Self {
            resource_name: resource_name,
            data: data,
            write_offset: 0,
        }
    }
}

impl Stream for ByteStreamClientWriter {
    type Item = WriteRequest;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // self.write_offset != 0 - we need to send at least one "good" before we kill the stream
        if self.write_offset >= self.data.len() && self.write_offset != 0 {
            // We are done
            return std::task::Poll::Ready(None);
        } else if self.write_offset + MAX_MESSAGE_SIZE >= self.data.len() {
            //Rest of data can be sent
            let wr = WriteRequest {
                resource_name: self.resource_name.clone(),
                write_offset: self.write_offset.try_into().unwrap(),
                finish_write: true,
                data: self.data[self.write_offset..].to_vec(),
            };
            // Add an additional in the case of an empty file
            self.write_offset = self.data.len() + 1;
            return std::task::Poll::Ready(Some(wr));
        } else {
            let wr = WriteRequest {
                resource_name: self.resource_name.clone(),
                write_offset: self.write_offset.try_into().unwrap(),
                finish_write: false,
                data: self.data[self.write_offset..self.write_offset + MAX_MESSAGE_SIZE].to_vec(),
            };
            self.write_offset += MAX_MESSAGE_SIZE;
            return std::task::Poll::Ready(Some(wr));
        }
    }
}

fn get_resource_id(message_enc: Vec<u8>) -> ResourceId {
    let message_len: u64 = message_enc.len().try_into().unwrap();
    let mut hasher = Sha256::new();
    hasher.update(message_enc);
    let command_digest = hasher.finalize();
    ResourceId {
        length: message_len,
        hash: resource_id::SHA256 {
            hash: command_digest.try_into().unwrap(),
        },
    }
}

async fn find_missing_blobs(
    blobs: &Vec<(ResourceId, Vec<u8>)>,
    cas_client: &mut ContentAddressableStorageClient<Channel>,
) -> Vec<ResourceId> {
    let missing_blobs = cas_client
        .find_missing_blobs(FindMissingBlobsRequest {
            instance_name: "".to_string(),
            blob_digests: blobs
                .iter()
                .map(|(res, _)| v2::Digest::from(res.clone()))
                .collect(),
        })
        .await
        .unwrap();

    missing_blobs
        .into_inner()
        .missing_blob_digests
        .iter()
        .map(|res| ResourceId::from(res.clone()))
        .collect()
}

async fn upload_blobs(
    blobs: Vec<(ResourceId, Vec<u8>)>,
    byte_stream_client: &mut ByteStreamClient<Channel>,
    cas_client: &mut ContentAddressableStorageClient<Channel>,
) -> bool {
    let missing_blobs: HashSet<ResourceId> = find_missing_blobs(&blobs, cas_client)
        .await
        .iter()
        .map(|res| ResourceId::from(res.clone()))
        .collect();
    // There are some missing blobs
    for (res, blob) in blobs {
        if !missing_blobs.contains(&res) {
            continue;
        }
        info!("Uploading {}", res);
        // TODO: run this async
        if !upload_data(res.clone(), blob, byte_stream_client).await {
            error!("Failed to upload to cache");
            return false;
        }
    }
    return true;
}

async fn upload_data(
    resource_id: ResourceId,
    data: Vec<u8>,
    byte_stream_client: &mut ByteStreamClient<Channel>,
) -> bool {
    let committed_len: i64 = data.len().try_into().unwrap();
    let res = byte_stream_client
        .write(ByteStreamClientWriter::new(resource_id, data))
        .await;
    assert_eq!(res.unwrap().into_inner().committed_size, committed_len);
    return true;
}

fn build_file(path: &String) -> (ResourceId, Vec<u8>) {
    let file_data = fs::read(path);
    if file_data.is_err() {
        panic!(
            "Could not open file {} with error {}",
            path,
            file_data.unwrap_err()
        );
    }
    let file_data = file_data.unwrap();
    let resource_id = get_resource_id(file_data.clone());
    return (resource_id, file_data);
}

fn build_command(command_args: Vec<String>, expected_files: Vec<String>) -> (ResourceId, Command) {
    let command = Command {
        arguments: command_args,
        environment_variables: vec![], // TODO
        output_files: vec![],
        output_directories: vec![],
        output_paths: expected_files,
        platform: None,
        working_directory: "".to_string(),
        output_node_properties: vec![],
    };
    let resource_id = get_resource_id(command.encode_to_vec());
    return (resource_id, command);
}

// TODO: mega hack, we are going to just hash all the files and skip the directory
fn build_action(
    input_files: &Vec<(ResourceId, Vec<u8>)>,
    command_resource_id: ResourceId,
) -> (ResourceId, Action) {
    let mut files_hashs_len: u64 = 0;
    let mut hasher = Sha256::new();

    for (resource_id, _) in input_files {
        let digest: v2::Digest = resource_id.clone().into();
        let digest_vec = digest.encode_to_vec();
        files_hashs_len += u64::try_from(digest_vec.len()).unwrap();
        hasher.update(digest_vec);
    }
    let command_digest = hasher.finalize();
    let input_root_digest = ResourceId {
        length: files_hashs_len.try_into().unwrap(),
        hash: resource_id::SHA256 {
            hash: command_digest.try_into().unwrap(),
        },
    };
    let command = Action {
        command_digest: Some(command_resource_id.into()),
        input_root_digest: Some(input_root_digest.into()),
        timeout: None,
        do_not_cache: false,
        salt: vec![],
        platform: None,
    };
    let resource_id = get_resource_id(command.encode_to_vec());
    return (resource_id, command);
}

fn build_action_result(output_files: Vec<String>, exit_code: i32) -> (ResourceId, ActionResult) {
    assert_eq!(output_files.len(), 0);
    let action_result = ActionResult {
        output_files: vec![],
        output_file_symlinks: vec![],
        output_symlinks: vec![],
        output_directories: vec![],
        output_directory_symlinks: vec![],
        exit_code: exit_code,
        stdout_raw: vec![],
        stdout_digest: None,
        stderr_raw: vec![],
        stderr_digest: None,
        execution_metadata: None,
    };
    let resource_id = get_resource_id(action_result.encode_to_vec());
    return (resource_id, action_result);
}

async fn client_check(
    check: Check,
    byte_stream_client: &mut ByteStreamClient<Channel>,
    action_cache_client: &mut ActionCacheClient<Channel>,
    cas_client: &mut ContentAddressableStorageClient<Channel>,
) -> Result<i32, Box<dyn std::error::Error>> {
    let pull_res = client_pull(
        Pull { input: check.input },
        byte_stream_client,
        action_cache_client,
        cas_client,
    )
    .await;
    if pull_res.is_ok() {
        return Ok(0);
    }
    return Ok(1);
}

async fn client_pull(
    pull: Pull,
    _byte_stream_client: &mut ByteStreamClient<Channel>,
    action_cache_client: &mut ActionCacheClient<Channel>,
    cas_client: &mut ContentAddressableStorageClient<Channel>,
) -> Result<i32, Box<dyn std::error::Error>> {
    let json_input_str = fs::read_to_string(pull.input)?;
    let test_input: TestInput = serde_json::from_str(&json_input_str)?;
    let (command_resource_id, command, mut input_files, action_resource_id, _action) =
        build_input_objects(test_input).await;
    input_files.push((command_resource_id, command.encode_to_vec()));
    let missing_blobs = find_missing_blobs(&input_files, cas_client).await;
    if !missing_blobs.is_empty() {
        warn!("Some data you asked for is not in the cache");
        return Err(Box::new(std::io::Error::new(ErrorKind::Other, "oh no!")));
    }
    let action_result = action_cache_client
        .get_action_result(GetActionResultRequest {
            instance_name: "".to_string(),
            action_digest: Some(action_resource_id.into()),
            inline_stdout: false,
            inline_stderr: false,
            inline_output_files: vec![],
        })
        .await?;
    Ok(action_result.into_inner().exit_code)
}

async fn build_input_objects(
    test_input: TestInput,
) -> (
    ResourceId,
    Command,
    Vec<(ResourceId, Vec<u8>)>,
    ResourceId,
    Action,
) {
    let (command_resource_id, command) =
        build_command(test_input.command, test_input.expected_files);
    let mut hashing_funcs = Vec::new();
    let mut input_files = Vec::new();
    for input_file in test_input.input_files.clone() {
        hashing_funcs.push(tokio::task::spawn_blocking(move || build_file(&input_file)));
    }

    for hashing_func in hashing_funcs {
        input_files.push(hashing_func.await.unwrap())
    }
    let (action_resource_id, action) = build_action(&input_files, command_resource_id.clone());
    return (
        command_resource_id,
        command,
        input_files,
        action_resource_id,
        action,
    );
}

async fn client_push(
    push: Push,
    byte_stream_client: &mut ByteStreamClient<Channel>,
    action_cache_client: &mut ActionCacheClient<Channel>,
    cas_client: &mut ContentAddressableStorageClient<Channel>,
) -> Result<i32, Box<dyn std::error::Error>> {
    let json_input_str = fs::read_to_string(push.input)?;
    let json_output_str = fs::read_to_string(push.output)?;
    let test_input: TestInput = serde_json::from_str(&json_input_str)?;
    let test_ouput: TestResults = serde_json::from_str(&json_output_str)?;
    let (command_resource_id, command, mut input_files, action_resource_id, _action) =
        build_input_objects(test_input).await;
    let (_action_result_resource_id, action_result) =
        build_action_result(test_ouput.files, test_ouput.exit_code);
    input_files.push((command_resource_id, command.encode_to_vec()));
    let upload_res = upload_blobs(input_files, byte_stream_client, cas_client).await;
    assert!(upload_res);
    action_cache_client
        .update_action_result(UpdateActionResultRequest {
            instance_name: "".to_string(),
            action_digest: Some(action_resource_id.into()),
            action_result: Some(action_result),
            results_cache_policy: None,
        })
        .await?;
    return Ok(0);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let logging_env = Env::default()
        .filter_or("LOG_LEVEL", "info,woosh_server=trace")
        .write_style_or("LOG_LEVEL", "always");

    env_logger::init_from_env(logging_env);

    let mut byte_stream_client = ByteStreamClient::connect("http://[::1]:50051")
        .await
        .unwrap();
    let mut action_cache_client = ActionCacheClient::connect("http://[::1]:50051")
        .await
        .unwrap();
    let mut cas_client = ContentAddressableStorageClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    let cli_args = CLI::parse();
    let ret_code = match cli_args {
        CLI::Check(check) => {
            client_check(
                check,
                &mut byte_stream_client,
                &mut action_cache_client,
                &mut cas_client,
            )
            .await
        }
        CLI::Pull(pull) => {
            client_pull(
                pull,
                &mut byte_stream_client,
                &mut action_cache_client,
                &mut cas_client,
            )
            .await
        }
        CLI::Push(push) => {
            client_push(
                push,
                &mut byte_stream_client,
                &mut action_cache_client,
                &mut cas_client,
            )
            .await
        }
    }?;

    std::process::exit(ret_code);
}
