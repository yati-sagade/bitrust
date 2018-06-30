extern crate bitrust;
extern crate tempfile;

use std::collections::HashMap;
use bitrust::BitRustState;
use bitrust::util::rand_str;


#[test]
fn test_model_based_load_store() {
    let data_dir = tempfile::tempdir().unwrap();
    let mut br = BitRustState::new(data_dir.path()).unwrap();
    let mut model = HashMap::new();

    for _ in 0..1000 {
        let key = rand_str();
        let val = rand_str();
        model.insert(key.clone(), val.clone());
        br.put(key.clone(), val).unwrap();
        assert!(&br.get(&key).unwrap().unwrap() == model.get(&key).unwrap());
    }

    for (key, val) in model {
        assert!(br.get(&key).unwrap().unwrap() == val);
    }

}
