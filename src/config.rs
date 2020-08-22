use serde::{Deserialize, Serialize};

use std::path::PathBuf;

pub const DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES: i64 = 10 << 20; // 10MiB

fn default_file_size_soft_limit_bytes() -> i64 {
  DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES
}

fn default_require_hint_file_write_success() -> bool {
  false
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct AutoMergeConfig {
  /// How often to check if a merge can be performed.
  pub check_interval_secs: u64,
  /// Minimum number of inactive datafiles for a merge to be triggered.
  pub min_inactive_files: i32,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct MergeConfig {
  /// Whether failure writing hintfiles should fail merges. Hintfiles speed up
  /// initial startup, but beyond that are not necessary for correctness.
  #[serde(default = "default_require_hint_file_write_success")]
  pub require_hint_file_write_success: bool,
  #[serde(default)]
  pub auto_merge_config: Option<AutoMergeConfig>,
}

impl Default for MergeConfig {
  fn default() -> MergeConfig {
    MergeConfig {
      require_hint_file_write_success: false,
      auto_merge_config: None,
    }
  }
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Config {
  /// Main data directory where BitRust datafiles are kept.
  pub datadir: PathBuf,
  #[serde(default = "default_file_size_soft_limit_bytes")]
  pub file_size_soft_limit_bytes: i64,
  #[serde(default)]
  pub merge_config: MergeConfig,
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn test_read_minimal_config() {
    let cfg: Config = toml::from_str(
      r#"
    datadir = "/home/ysagade/bitrust_data"
    "#,
    )
    .unwrap();
    println!("Parsed: {:?}", cfg);
  }
}
