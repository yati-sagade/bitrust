// Approach lifted from:
// https://github.com/Zondax/ledger-rs/pull/45/commits/43908c4d033b8fe53b1cde76bf5c3db70b72fa7c

extern crate protoc_rust;
use std::{env, fs, path::PathBuf};
fn main() {
  protoc_rust::Codegen::new()
    .out_dir(outdir())
    .inputs(&["proto/bitrust_pb.proto"])
    .include("proto")
    .run()
    .expect("Running protoc failed.");
  generate_mod_rs();
}

fn outdir() -> PathBuf {
  PathBuf::from(env::var_os("OUT_DIR").unwrap())
}

fn generate_mod_rs() {
  let outdir = outdir();

  let mods = glob::glob(&outdir.join("*.rs").to_string_lossy())
    .expect("glob")
    .filter_map(|p| {
      p.ok().map(|p| {
        format!("pub mod {};", p.file_stem().unwrap().to_string_lossy())
      })
    })
    .collect::<Vec<_>>()
    .join("\n");

  let mod_rs = outdir.join("proto_mod.rs");
  fs::write(&mod_rs, format!("{}\n", mods)).expect("write");

  println!("cargo:rustc-env=PROTO_MOD_RS={}", mod_rs.to_string_lossy());
}
