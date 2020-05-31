extern crate bitrust;
extern crate simplelog;
extern crate tempfile;
#[macro_use]
extern crate log;

use bitrust::test_utils::dump_all_datafiles;
use bitrust::util::{rand_str, SerialLogicalClock};
use bitrust::{BitRust, BitRustState, ConfigBuilder};
use simplelog::{CombinedLogger, LevelFilter, TermLogger};
use std::collections::HashMap;

#[allow(dead_code)]
fn setup_logging() {
  CombinedLogger::init(vec![TermLogger::new(
    LevelFilter::Debug,
    simplelog::Config::default(),
  )
  .unwrap()])
  .expect("Logging setup");
}

#[test]
fn test_model_based_load_store() {
  let data_dir = tempfile::tempdir().unwrap();
  let cfg = ConfigBuilder::new(&data_dir).build();
  let mut br = BitRustState::new(cfg, SerialLogicalClock::new(0)).unwrap();
  let mut model = HashMap::new();

  for _ in 0..1000 {
    let key = rand_str().as_bytes().to_vec();
    let val = rand_str().as_bytes().to_vec();
    model.insert(key.clone(), val.clone());
    br.put(key.clone(), val.clone()).unwrap();
    assert!(&br.get(&key).unwrap().unwrap() == model.get(&key).unwrap());
  }

  for (key, val) in model {
    assert!(br.get(&key).unwrap().unwrap() == val);
  }
}

#[test]
fn test_model_based_load_store_with_restarts() {
  let data_dir = tempfile::tempdir().unwrap();
  let keys = (0..1000)
    .map(|k| format!("key_{}", k))
    .collect::<Vec<String>>();
  for i in 0..5 {
    debug!("Start generation {}", i);
    {
      debug!("Generation {}: Opening bitrust", i);
      let cfg = ConfigBuilder::new(&data_dir)
        .max_file_fize_bytes(1000)
        .build();
      let mut br = BitRust::open(cfg, SerialLogicalClock::new(0)).unwrap();
      debug!("Generation {}: Putting keys", i);
      for key in &keys {
        let val = format!("{}_{}", key, i);
        debug!("put {:?} => {:?}", key, &val);
        br.put(key.as_bytes().to_vec(), val.as_bytes().to_vec())
          .expect(&format!("Put {:?} => {:?} in generation {}", key, val, i,));
      }
    }
    debug!("Generation {}: Opening bitrust for reading", i);
    let cfg = ConfigBuilder::new(&data_dir)
      .max_file_fize_bytes(1000)
      .build();
    let mut br = BitRust::open(cfg, SerialLogicalClock::new(0)).unwrap();
    debug!(">>>>>");
    dump_all_datafiles(&br.state).expect("Dump state");
    debug!("<<<<<");
    for (key_idx, key) in keys.iter().enumerate() {
      let expected = Some(format!("{}_{}", key, i));
      let got = br.get(key.as_bytes()).expect(&format!(
        "get {} (index {}) for generation {}",
        key, key_idx, i
      ));
      assert!(
        got == expected.as_ref().map(|v| v.as_bytes().to_vec()),
        "Expected {:?}, got {:?}",
        expected,
        got.map(|v| String::from_utf8(v.clone()).expect("valid string"))
      );
    }
    debug!("Generation {} complete", i);
  }
}
