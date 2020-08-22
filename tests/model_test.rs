extern crate bitrust;
extern crate simplelog;
extern crate tempfile;
#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bitrust::config::{
  AutoMergeConfig, Config, MergeConfig, DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
};
use bitrust::test_utils::{dump_all_datafiles, setup_logging};
use bitrust::util::{rand_str_with_rand_size, SerialLogicalClock};
use bitrust::{BitRust, BitRustState};

#[test]
fn test_model_based_load_store() {
  setup_logging();
  let data_dir = tempfile::tempdir().unwrap();
  let cfg = Config {
    datadir: data_dir.as_ref().to_path_buf(),
    merge_config: MergeConfig::default(),
    file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
  };
  let br = BitRustState::new(cfg, SerialLogicalClock::new(0)).unwrap();
  let mut model = HashMap::new();

  for _ in 0..1000 {
    let key = rand_str_with_rand_size().as_bytes().to_vec();
    let val = rand_str_with_rand_size().as_bytes().to_vec();
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
      let cfg = Config {
        datadir: data_dir.as_ref().to_path_buf(),
        merge_config: MergeConfig::default(),
        file_size_soft_limit_bytes: 1000,
      };
      let br = BitRust::open(cfg, SerialLogicalClock::new(0)).unwrap();
      debug!("Generation {}: Putting keys", i);
      for key in &keys {
        let val = format!("{}_{}", key, i);
        debug!("put {:?} => {:?}", key, &val);
        br.put(key.as_bytes().to_vec(), val.as_bytes().to_vec())
          .expect(&format!("Put {:?} => {:?} in generation {}", key, val, i,));
      }
    }
    debug!("Generation {}: Opening bitrust for reading", i);
    let cfg = Config {
      datadir: data_dir.as_ref().to_path_buf(),
      merge_config: MergeConfig::default(),
      file_size_soft_limit_bytes: 1000,
    };
    let br = BitRust::open(cfg, SerialLogicalClock::new(0)).unwrap();
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
  let mut expected_values = HashMap::<String, String>::new();
  {
    let br = BitRust::open(
      Config {
        datadir: data_dir.as_ref().to_path_buf(),
        merge_config: MergeConfig::default(),
        // We write records of the form foo => foo_[0-9], which makes the entry
        // size 30 bytes. This option makes the active file rotate after each
        // put.
        file_size_soft_limit_bytes: 30,
      },
      SerialLogicalClock::new(0),
    )
    .expect("Bitrust open for putting");
    let mut i = 0;
    while br.state.active_file_id() != 2 {
      br.put(b"foo".to_vec(), format!("foo_{}", i).as_bytes().to_vec())
        .expect(&format!("put foo => foo_{}", i));
      br.put(b"bar".to_vec(), format!("bar_{}", i).as_bytes().to_vec())
        .expect(&format!("put foo => bar_{}", i));
      i += 1;
    }
    debug!(
      "Total {} records written. Current active file is {}.",
      i * 2,
      br.state.active_file_id(),
    );
    br.merge().expect("merge");
    br.put(b"foo".to_vec(), b"_".to_vec())
      .expect("put foo => _");
    dump_all_datafiles(&br.state).expect("dump state");
    expected_values.insert("foo".to_string(), "_".to_string());
    expected_values.insert("bar".to_string(), format!("bar_{}", i - 1));
  }
  debug!("Reopening for reading");
  let br = BitRust::open(
    Config {
      datadir: data_dir.as_ref().to_path_buf(),
      merge_config: MergeConfig::default(),
      file_size_soft_limit_bytes: 1000,
    },
    SerialLogicalClock::new(0),
  )
  .expect("Bitrust open for putting");
  debug!(
    "Opened bitrust with active file {}",
    br.state.active_file_id()
  );
  for (key, val) in &expected_values {
    let v = String::from_utf8(
      br.get(key.as_bytes())
        .expect(&format!("Get {} with non-error value", key))
        .expect(&format!("Get {} with some value", key)),
    )
    .expect("Valid utf8 string");
    assert!(
      v == *val,
      "Expected {} => {}, found {} => {}",
      key,
      val,
      key,
      v
    );
  }
  assert!(
    br.state.active_file_id() == 2,
    "Expected file id 2 to be the active file, but active file id is {}",
    br.state.active_file_id()
  );
}

#[test]
fn test_model_based_load_store_with_auto_merge() {
  setup_logging();
  let data_dir = tempfile::tempdir().unwrap();
  let br = BitRust::open(
    Config {
      datadir: data_dir.as_ref().to_path_buf(),
      merge_config: MergeConfig {
        require_hint_file_write_success: true,
        auto_merge_config: Some(AutoMergeConfig {
          check_interval_secs: 1,
          min_inactive_files: 1,
        }),
      },
      // We write records of the form foo => foo_[0-9], which makes the entry
      // size 30 bytes. This option makes the active file rotate after each
      // put.
      file_size_soft_limit_bytes: 10000,
    },
    SerialLogicalClock::new(0),
  )
  .expect("Bitrust open");
  let mut i = 0;
  let start = Instant::now();
  let keystr = "bar";
  let key = keystr.as_bytes().to_vec();
  while Instant::now() - start <= Duration::from_secs(3) {
    let val = format!("bar_{}", i).as_bytes().to_vec();
    br.put(key.clone(), val.clone())
      .expect(&format!("put foo => bar_{}", i));
    assert_eq!(
      br.get(&key)
        .expect(&format!("get {}", &keystr))
        .expect(&format!("key {} should exist", &keystr)),
      &val[..],
    );
    i += 1;
  }
  debug!(
    "Total {} records written. Current active file is {}.",
    i * 2,
    br.state.active_file_id(),
  );
  br.put(key.clone(), b"_".to_vec())
    .expect(&format!("last put to {}", &keystr));
  assert_eq!(
    br.get(&key)
      .expect(&format!("get {}", &keystr))
      .expect(&format!("key {} should exist", &keystr)),
    b"_".to_vec(),
  );
}
