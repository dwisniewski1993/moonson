//! Build script: compile the gRPC `.proto` definitions into Rust code before the
//! crate itself is built. The generated code lands in OUT_DIR and is pulled in
//! via `tonic::include_proto!` in main.rs.

fn main() {
    // Use a bundled protoc binary so contributors do not need to install one
    // (no `brew install protobuf` required).
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("bundled protoc available");
    std::env::set_var("PROTOC", protoc);

    tonic_build::compile_protos("../../proto/echo.proto").expect("failed to compile echo.proto");
    println!("cargo:rerun-if-changed=../../proto/echo.proto");
}
