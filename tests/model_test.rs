extern crate bitrust;
extern crate tempfile;

use bitrust::util::{rand_str, LogicalClock, SerialLogicalClock};
use bitrust::BitRustState;
use bitrust::ConfigBuilder;
use std::collections::HashMap;

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
