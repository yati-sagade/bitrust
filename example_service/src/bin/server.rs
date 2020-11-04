extern crate bitrust;
extern crate ctrlc;
extern crate dirs;
extern crate example_service;
extern crate grpc;

use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc,
};

use bitrust::config::{MergeConfig, DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES};
use example_service::service::{
  GetRequest, GetResponse, PutRequest, PutResponse,
  Status_StatusCode as StatusCode,
};
use example_service::service_grpc::{BitRustService, BitRustServiceServer};

struct BitRustServerImpl {
  br: bitrust::BitRust<bitrust::util::SerialLogicalClock>,
}

impl BitRustService for BitRustServerImpl {
  fn get(
    &self,
    _: grpc::ServerHandlerContext,
    req: grpc::ServerRequestSingle<GetRequest>,
    resp: grpc::ServerResponseUnarySink<GetResponse>,
  ) -> grpc::Result<()> {
    let mut r = GetResponse::new();
    match self.br.get(req.message.get_key()) {
      Ok(Some(val)) => {
        r.set_value(val);
        r.mut_status().set_code(StatusCode::OK);
      }
      Ok(None) => {
        r.mut_status().set_code(StatusCode::NOT_FOUND);
      }
      Err(e) => {
        r.mut_status().set_code(StatusCode::INTERNAL);
        r.mut_status().set_message(format!("{:?}", e));
      }
    }
    resp.finish(r)
  }

  fn put(
    &self,
    _: grpc::ServerHandlerContext,
    mut req: grpc::ServerRequestSingle<PutRequest>,
    resp: grpc::ServerResponseUnarySink<PutResponse>,
  ) -> grpc::Result<()> {
    let mut r = PutResponse::new();
    match self
      .br
      .put(req.message.take_key(), req.message.take_value())
    {
      Ok(_) => {
        r.mut_status().set_code(StatusCode::OK);
      }
      Err(e) => {
        r.mut_status().set_code(StatusCode::INTERNAL);
        r.mut_status().set_message(format!("{:?}", e));
      }
    }
    resp.finish(r)
  }
}

fn main() {
  let main_thread = std::thread::current();
  let done = Arc::new(AtomicBool::new(false));
  let done_clone = done.clone();
  ctrlc::set_handler(move || {
    done_clone.store(true, Ordering::Release);
    main_thread.unpark();
  })
  .expect("set SIGINT handler");

  let br = bitrust::BitRust::open(
    bitrust::Config {
      datadir: dirs::home_dir()
        .expect("Could not resolve $HOME, please provide -d")
        .join("bitrust_data"),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    },
    bitrust::util::SerialLogicalClock::new(0),
  )
  .expect("bitrust open");

  let srv_impl = BitRustServerImpl { br };
  let mut bld = grpc::ServerBuilder::new_plain();
  bld.http.set_port(50051);
  bld.add_service(BitRustServiceServer::new_service_def(srv_impl));
  let _server = bld.build().expect("Build server");
  println!("Server started on port 50051");

  while !done.load(Ordering::Acquire) {
    std::thread::park();
  }
}
