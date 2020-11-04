extern crate protoc_rust_grpc;

fn main() {
  protoc_rust_grpc::Codegen::new()
    .out_dir("src/")
    .inputs(&["proto/service.proto"])
    .includes(&["../proto", "proto"])
    .rust_protobuf(true)
    .run()
    .expect("Running protoc failed.");
}
