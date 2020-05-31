extern crate bitrust;
extern crate simplelog;
extern crate tempfile;
#[macro_use]
extern crate log;

use bitrust::test_utils::{dump_all_datafiles, setup_logging};
use bitrust::util::{rand_str, SerialLogicalClock};
use bitrust::{BitRust, BitRustState, ConfigBuilder};
use std::collections::HashMap;

#[test]
fn test_model_based_load_store() {
  setup_logging();
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
  setup_logging();
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

#[test]
fn test_model_based_load_store_with_restarts_and_merges() {
  setup_logging();
  let data_dir = tempfile::tempdir().unwrap();
  let expected_value = {
    let mut br = BitRust::open(
      ConfigBuilder::new(&data_dir)
        // Every write rotates the active file since our records are at least 16
        // bytes each.
        .max_file_fize_bytes(30)
        .build(),
      SerialLogicalClock::new(0),
    )
    .expect("Bitrust open for putting");
    let mut i = 0;
    while br.state.active_file_id() != 2 {
      br.put(b"foo".to_vec(), format!("foo_{}", i).as_bytes().to_vec())
        .expect(&format!("put foo => foo_{}", i));
      i += 1;
    }
    debug!(
      "Total {} records written. Current active file is {}.",
      br.state.active_file_id(),
      i
    );
    debug!("Before merge");
    dump_all_datafiles(&br.state).expect("dump state");
    debug!("Merging now");
    br.merge().expect("merge");
    dump_all_datafiles(&br.state).expect("dump state");
    debug!("Writing another record foo => _");
    br.put(b"foo".to_vec(), b"_".to_vec())
      .expect("put foo => _");
    dump_all_datafiles(&br.state).expect("dump state");
    "_"
  };
  debug!("Reopening for reading");
  let mut br = BitRust::open(
    ConfigBuilder::new(&data_dir)
      .max_file_fize_bytes(1000)
      .build(),
    SerialLogicalClock::new(0),
  )
  .expect("Bitrust open for putting");
  let read_value = br.get(b"foo").expect("Get foo");
  let read_value_str =
    String::from_utf8(read_value.expect("Some value")).expect("Valid string");
  assert!(
    read_value_str == expected_value,
    "Expected {}, read {}",
    expected_value,
    read_value_str
  );
}
