extern crate bitrust;
extern crate example_service;
extern crate futures;
extern crate grpc;

use futures::executor;

use example_service::service::{
  GetRequest, GetResponse, PutRequest, PutResponse,
};
use example_service::service_grpc::BitRustServiceClient;
use grpc::ClientStubExt;

fn put(
  client: &mut BitRustServiceClient,
  key: Vec<u8>,
  val: Vec<u8>,
) -> PutResponse {
  let mut req = PutRequest::new();
  req.set_key(key);
  req.set_value(val);
  let fut = client
    .put(grpc::RequestOptions::new(), req)
    .join_metadata_result();
  let (_, r, _) = executor::block_on(fut).expect("Put RPC");
  r
}

fn get(client: &mut BitRustServiceClient, key: Vec<u8>) -> GetResponse {
  let mut req = GetRequest::new();
  req.set_key(key);
  let fut = client
    .get(grpc::RequestOptions::new(), req)
    .join_metadata_result();
  let (_, r, _) = executor::block_on(fut).expect("Get RPC");
  r
}

fn main() {
  let mut client =
    BitRustServiceClient::new_plain("::1", 50051, Default::default())
      .expect("client construct");
  let args = std::env::args().collect::<Vec<_>>();
  let cmd = &args[1];
  match cmd.as_str() {
    "get" => {
      if args.len() != 3 {
        println!("get takes exactly one arg");
        return;
      }
      println!("{:?}", get(&mut client, args[2].as_bytes().to_vec()));
    }
    "put" => {
      if args.len() != 4 {
        println!("put takes exactly two args");
        return;
      }
      println!(
        "{:?}",
        put(
          &mut client,
          args[2].as_bytes().to_vec(),
          args[3].as_bytes().to_vec()
        )
      );
    }
    _ => {
      println!("Bad args {:?}", args);
    }
  }
}
