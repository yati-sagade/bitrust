extern crate bitrust;
extern crate tempfile;
extern crate rand;

use std::collections::HashMap;
use rand::Rng;
use bitrust::BitRust;

fn rand_str() -> String {
    let size: usize = rand::thread_rng().gen_range(4, 1024);
    rand::thread_rng().gen_iter::<char>().take(size).collect()
}

#[test]
fn test_model_based_load_store() {
    let data_dir = tempfile::tempdir().unwrap();
    let mut br = BitRust::new(data_dir.path()).unwrap();
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
