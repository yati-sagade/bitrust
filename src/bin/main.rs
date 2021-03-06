extern crate bitrust;
#[macro_use]
extern crate log;
extern crate ctrlc;
extern crate dirs;
extern crate getopts;
extern crate isatty;
extern crate simplelog;
extern crate tempfile;

use std::env;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;
use std::process;

use bitrust::config::{
  Config, MergeConfig, DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
};
use bitrust::util;
use bitrust::BitRust;
use simplelog::{CombinedLogger, LevelFilter, TermLogger, WriteLogger};

use bitrust::{Result, ResultExt};

fn main() -> Result<()> {
  let args: Vec<String> = env::args().collect();
  let (matches, opts) = parse_opts(&args[1..]);
  let program = &args[0];

  if matches.opt_present("h") {
    print_usage(program, opts);
    process::exit(0);
  }

  let config = build_config(&matches);
  println!("Config {:?}", &config);
  println!(
    "{}",
    toml::ser::to_string(&config).expect("config serialize")
  );
  let datadir = config.datadir.to_path_buf();

  setup_logging(&datadir, log_level_filter(&matches))?;

  let mut br = BitRust::open(config, util::SerialLogicalClock::new(0))
    .chain_err(|| format!("Failed to open bitrust at {:?}", &datadir))?;

  let ret = cmd_loop(&mut br);
  ret
}

fn build_config(matches: &getopts::Matches) -> Config {
  if let Some(path) = matches.opt_str("c") {
    let config_str = std::fs::read_to_string(path).expect("Read config file");
    toml::from_str(&config_str).expect("Parse configuration")
  } else {
    Config {
      datadir: dirs::home_dir()
        .expect("Could not resolve $HOME, please provide -d")
        .join("bitrust_data"),
      file_size_soft_limit_bytes: DEFAULT_FILE_SIZE_SOFT_LIMIT_BYTES,
      merge_config: MergeConfig::default(),
    }
  }
}

fn prompt() -> Result<()> {
  print!("> ");
  io::stdout()
    .flush()
    .chain_err(|| "Error displaying input prompt")?;
  Ok(())
}

fn get_usage(
  cmd_usages: &[(&'static str, &'static str)],
  cmd_name: &str,
) -> Option<&'static str> {
  for (name, usage) in cmd_usages {
    if *name == cmd_name {
      return Some(usage);
    }
  }
  None
}

fn cmd_loop<ClockT: util::LogicalClock + Send + Sync + 'static>(
  br: &mut BitRust<ClockT>,
) -> Result<()> {
  ctrlc::set_handler(move || {
    println!("Type exit to quit");
    prompt().unwrap();
  })
  .expect("Error setting handler");

  let cmd_usages = vec![
    ("put", "put KEY VAL\n  Store VAL into given KEY"),
    ("get", "get KEY\n  Get value for given KEY"),
    ("del", "del KEY\n  Delete given KEY"),
    ("lst", "lst\n  List all keys"),
    ("exit", "exit/quit\n  Exit this shell"),
    ("_merge", "merge data files (blocking)"),
    ("help", "help/?\n  Show this message"),
  ];

  let isatty = isatty::stdin_isatty();

  loop {
    let mut cmd = String::new();

    if isatty {
      prompt()?;
    }

    io::stdin()
      .read_line(&mut cmd)
      .chain_err(|| "Could not read next line of input")?;

    if cmd.len() == 0 {
      break;
    }

    let cmd = cmd.trim().split_whitespace().collect::<Vec<_>>();

    if cmd.len() == 0 {
      continue;
    }

    if cmd[0] == "help" || cmd[0] == "?" {
      println!("Commands:");
      for (_, usage) in &cmd_usages {
        println!("{}", usage);
      }
    } else if cmd[0] == "put" {
      if cmd.len() != 3 {
        println!("{}", get_usage(&cmd_usages, "put").unwrap());
        if isatty {
          continue;
        } else {
          break;
        }
      }
      let key = cmd[1];
      let val = cmd[2];

      br.put(key.as_bytes().to_vec(), val.as_bytes().to_vec())
        .chain_err(|| "put failed")?;
    } else if cmd[0] == "get" {
      if cmd.len() != 2 {
        println!("{}", get_usage(&cmd_usages, "get").unwrap());
        if isatty {
          continue;
        } else {
          break;
        }
      }
      let key = cmd[1];

      match br.get(key.as_bytes()) {
        e @ Err(_) => println!("{:?}", e),
        v @ Ok(None) => println!("{:?}", v),
        Ok(Some(val)) => {
          if let Ok(val_str) = String::from_utf8(val.clone()) {
            let v: Result<Option<String>> = Ok(Some(val_str));
            println!("{:?}", v);
          } else {
            let v: Result<Option<Vec<u8>>> = Ok(Some(val));
            println!("{:?}", v);
          }
        }
      }
    } else if cmd[0] == "lst" {
      for key in br.keys() {
        if let Ok(key_str) = String::from_utf8(key.to_vec()) {
          println!("{}", key_str);
        } else {
          println!("{:x?}", &key);
        }
      }
    } else if cmd[0] == "del" {
      if cmd.len() != 2 {
        println!("{}", get_usage(&cmd_usages, "del").unwrap());
        if isatty {
          continue;
        } else {
          break;
        }
      }
      println!("{:?}", br.delete(cmd[1].as_bytes()));
    } else if cmd[0] == "_merge" {
      br.merge()?;
    } else if cmd[0] == "exit" || cmd[0] == "quit" {
      break;
    } else {
      if !isatty {
        println!("Invalid command {}, quitting", cmd[0]);
        break;
      } else {
        println!("Invalid command {}, try typing help", cmd[0]);
      }
    }
  }
  info!("Exit");
  #[allow(unreachable_code)]
  Ok(())
}

fn log_level_filter(matches: &getopts::Matches) -> LevelFilter {
  let loglevel = matches.opt_str("l").unwrap_or_else(|| String::from("info"));
  match loglevel.as_ref() {
    "info" => LevelFilter::Info,
    "debug" => LevelFilter::Debug,
    "warn" => LevelFilter::Warn,
    "error" => LevelFilter::Error,
    "trace" => LevelFilter::Trace,
    _ => panic!("Invalid loglevel"),
  }
}

fn setup_logging<P: AsRef<Path>>(
  datadir: P,
  level_filter: LevelFilter,
) -> Result<()> {
  let log_file_path = datadir.as_ref().join("bitrust.log");

  let log_file = OpenOptions::new()
    .create(true)
    .append(true)
    .open(&log_file_path)
    .chain_err(|| format!("Could not open logfile {:?}", &log_file_path))?;

  CombinedLogger::init(vec![
    TermLogger::new(LevelFilter::Warn, simplelog::Config::default()).unwrap(),
    WriteLogger::new(level_filter, simplelog::Config::default(), log_file),
  ])
  .chain_err(|| "Error setting up logging")
}

// Returns the matches, options and the program name on the command line
fn parse_opts(args: &[String]) -> (getopts::Matches, getopts::Options) {
  let mut opts = getopts::Options::new();
  opts.optflag("h", "help", "Print this help message and quit");
  opts.optopt("c", "configfile", "Path to config file", "CONFIG_FILE");
  opts.optopt("l", "loglevel", "Log level", "LEVEL");
  opts.optopt(
    "b",
    "bench",
    "Print benchmark stats for an operation and exit",
    "KIND",
  );
  let matches = opts.parse(args).expect("Error parsing options");
  (matches, opts)
}

fn print_usage(program: &str, opts: getopts::Options) {
  let brief = format!("Usage: {} [options]", program);
  print!("{}", opts.usage(&brief));
}
