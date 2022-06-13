// google::rpc::Status

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        // .extern_path(".build.bazel.remote.execution.v2", "crate::build::bazel::remote::execution::v2")
        // .extern_path(
        //     ".google.rpc.Status",
        //     "crate::google::bazel::remote::execution::v2",
        // )
        // .extern_path(
        //     ".build.bazel.semver.semver",
        //     "crate::google::bazel::remote::execution::v2",
        // )
        // .extern_path(
        //     ".third_party.googleapis",
        //     "()",
        // ).extern_path(
        //     ".third_party.remote-apis",
        //     "()",
        // )
        .compile(
            &[
                "third_party/remote-apis/build/bazel/remote/execution/v2/remote_execution.proto",
                "third_party/remote-apis/build/bazel/semver/semver.proto",
                "third_party/googleapis/google/rpc/status.proto",
                "third_party/googleapis/google/longrunning/operations.proto",
            ],
            &["third_party/remote-apis", "third_party/googleapis"],
        )?;
    Ok(())
}
