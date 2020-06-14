#[macro_use]
extern crate criterion;
extern crate bitrust;
extern crate tempfile;

#[macro_use]
extern crate log;

use bitrust::config::{
  Config, MergeConfig, DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
};
use bitrust::BitRust;
use criterion::{criterion_group, BatchSize, Criterion, Throughput};

pub fn put_throughput_benchmark(c: &mut Criterion) {
  let data_dir = tempfile::tempdir().unwrap();
  let mut br = BitRust::open(
    Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    },
    bitrust::util::SerialLogicalClock::new(0),
  )
  .expect("Opening bitrust");
  let mut vals = Vec::new();
  let szs = vec![100, 1000];
  for sz in &szs {
    vals.push(vec![0u8; *sz as usize]);
  }
  let mut group = c.benchmark_group("put throughput");
  for (i, val) in vals.iter().enumerate() {
    group.throughput(Throughput::Bytes(szs[i] as u64));
    group.bench_with_input(format!("Put {} bytes", szs[i]), val, |b, val| {
      b.iter(|| {
        br.put(
          format!("bench_key_of_reasonable_size_{}", i)
            .as_bytes()
            .to_vec(),
          val.clone(),
        )
        .expect("Put");
      });
    });
  }
}

/// Benchmark to measure time taken to put 100k randomly generated records.
/// Key size: 16 bytes
/// Value size: 100 bytes
pub fn fillrandom(c: &mut Criterion) {
  let mut kv = Vec::new();
  for _ in 0..1_00_000 {
    kv.push((
      bitrust::util::rand_bytes(16),
      bitrust::util::rand_bytes(100),
    ));
  }
  c.bench_function("fillrandom", move |b| {
    b.iter_batched(
      || {
        let data_dir = tempfile::tempdir().unwrap();
        let br = BitRust::open(
          Config {
            datadir: data_dir.as_ref().to_path_buf(),
            file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
            merge_config: MergeConfig::default(),
          },
          bitrust::util::SerialLogicalClock::new(0),
        )
        .expect("Opening bitrust");
        (br, data_dir, kv.clone())
      },
      |(mut br, data_dir, kv)| {
        for (key, val) in kv {
          br.put(key, val).expect("Put");
        }
        (br, data_dir)
      },
      BatchSize::LargeInput,
    );
  });
}

pub fn overwrite(c: &mut Criterion) {
  let data_dir = tempfile::tempdir().unwrap();
  let mut br = BitRust::open(
    Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    },
    bitrust::util::SerialLogicalClock::new(0),
  )
  .expect("Opening bitrust");
  let mut keys = Vec::new();
  let nrecs = 1000;
  for _ in 0..nrecs {
    let key = bitrust::util::rand_bytes(16);
    keys.push(key.clone());
    br.put(key, bitrust::util::rand_bytes(100)).expect("Put");
  }
  c.bench_function("overwrite", move |b| {
    b.iter_batched(
      || {
        let mut vals = Vec::new();
        for i in 0..nrecs {
          vals.push((keys[i].clone(), bitrust::util::rand_bytes(100)));
        }
        vals
      },
      |vals| {
        for (key, val) in vals {
          br.put(key, val).expect("Put");
        }
      },
      BatchSize::SmallInput,
    );
  });
}

pub fn getrandom_no_merge(c: &mut Criterion) {
  bitrust::test_utils::setup_logging();
  let data_dir = tempfile::tempdir().unwrap();
  let mut br = BitRust::open(
    Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    },
    bitrust::util::SerialLogicalClock::new(0),
  )
  .expect("Opening bitrust");
  let mut keys = Vec::new();
  let nrecs = 1_000_000;
  for _ in 0..nrecs {
    let key = bitrust::util::rand_bytes(16);
    keys.push(key.clone());
    br.put(key, bitrust::util::rand_bytes(100)).expect("Put");
  }
  debug!("Total {} keys", keys.len());
  let mut i = 0;
  c.bench_function("getrandom_no_merge", |b| {
    b.iter_batched(
      || {
        let key = keys[i % keys.len()].clone();
        i += 1;
        key
      },
      |key| {
        let ret = br.get(&key);
        (key, ret)
      },
      BatchSize::SmallInput,
    )
  });
}

pub fn putrandom_no_merge(c: &mut Criterion) {
  let data_dir = tempfile::tempdir().unwrap();
  let mut br = BitRust::open(
    Config {
      datadir: data_dir.as_ref().to_path_buf(),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    },
    bitrust::util::SerialLogicalClock::new(0),
  )
  .expect("Opening bitrust");
  c.bench_function("putrandom_no_merge", |b| {
    b.iter_batched(
      || {
        (
          bitrust::util::rand_bytes(16),
          bitrust::util::rand_bytes(100),
        )
      },
      |(k, v)| br.put(k, v).expect("Put"),
      BatchSize::SmallInput,
    );
  });
}

criterion_group!(benches, getrandom_no_merge, putrandom_no_merge);
criterion_main!(benches);
