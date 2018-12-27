extern crate bitrust;
#[macro_use]
extern crate log;
extern crate simplelog;
extern crate getopts;
extern crate ctrlc;
extern crate tempfile;
extern crate isatty;

use std::time::Instant;
use std::io::{self, Write};
use std::env;
use std::process;
use std::path::{PathBuf, Path};
use std::fs::OpenOptions;

use simplelog::{CombinedLogger, TermLogger, WriteLogger, LevelFilter};
use bitrust::{BitRust, ConfigBuilder, Config};
use bitrust::util;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let (matches, opts) = parse_opts(&args[1..]);
    let program = &args[0];

    if matches.opt_present("h") {
        print_usage(program, opts);
        process::exit(0);
    }

    let config = build_config(&matches);
    let datadir = config.datadir().to_path_buf();
    setup_logging(&datadir, log_level_filter(&matches))?;
    let mut br = match BitRust::open(config) {
        Ok(br) => br,
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
            eprintln!(
                "Lock file {:?} exists, and is already held by pid {}",
                datadir.join(".lock"),
                0
            );
            process::exit(1);
        }
        Err(e) => {
            return Err(e);
        }
    };


    if let Some(cmd) = matches.opt_str("b") {
        match cmd.as_str() {
            "put" => bench_put(),
            _ => panic!("Invalid input to -b"),
        }
    } else {
        cmd_loop(&mut br)
    }
}

fn build_config(matches: &getopts::Matches) -> Config {
    let data_dir = datadir(&matches);
    let mut config = ConfigBuilder::new(data_dir);
    if let Some(sz) = matches.opt_str("s") {
        let max_file_fize_bytes = sz.parse::<u64>().unwrap_or_else(|e| {
            panic!("Invalid value {} for -s: {:?}", sz, e);
        });
        config.max_file_fize_bytes(max_file_fize_bytes);
    }
    config.build()
}

fn prompt() -> io::Result<()> {
    print!("> ");
    io::stdout().flush()?;
    Ok(())
}

fn get_usage(cmd_usages: &[(&'static str, &'static str)], cmd_name: &str) -> Option<&'static str> {
    for (name, usage) in cmd_usages {
        if *name == cmd_name {
            return Some(usage);
        }
    }
    None
}

fn cmd_loop(br: &mut BitRust) -> io::Result<()> {

    ctrlc::set_handler(move || {
        println!("Type exit to quit");
        prompt().unwrap();
    }).expect("Error setting handler");

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

        io::stdin().read_line(&mut cmd)?;

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
            br.put(key.to_string(), val.to_string())?;
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
            println!("{:?}", br.get(key));
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
            println!("{:?}", br.delete(cmd[1]));
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
    #[allow(unreachable_code)] Ok(())
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

fn datadir(matches: &getopts::Matches) -> PathBuf {
    matches
    .opt_str("d")
    .map(Into::into) // convert to PathBuf
    .unwrap_or_else(|| {
        env::home_dir()
              .expect("Could not resolve $HOME, please provide -d")
              .join("bitrust_data")
    })
}

fn setup_logging<P: AsRef<Path>>(datadir: P, level_filter: LevelFilter) -> io::Result<()> {
    let log_file_path = datadir.as_ref().join("bitrust.log");
    let log_file = OpenOptions::new().create(true).append(true).open(
        &log_file_path,
    )?;
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Warn,
            simplelog::Config::default()
        ).unwrap(),
        WriteLogger::new(
            level_filter,
            simplelog::Config::default(),
            log_file
        ),
    ]).expect("Error setting up logging");

    Ok(())
}

// Returns the matches, options and the program name on the command line
fn parse_opts(args: &[String]) -> (getopts::Matches, getopts::Options) {
    let mut opts = getopts::Options::new();
    opts.optflag("h", "help", "Print this help message and quit");
    opts.optopt("l", "loglevel", "Log level", "LEVEL");
    opts.optopt("d", "datadir", "Data directory", "DATADIR");
    opts.optopt(
        "b",
        "bench",
        "Print benchmark stats for an operation and exit",
        "KIND",
    );
    opts.optopt(
        "s",
        "max_file_size",
        "Max data file size in bytes",
        "MAX_FILE_SIZE",
    );
    let matches = opts.parse(args).expect("Error parsing options");
    (matches, opts)
}

fn bench_put() -> io::Result<()> {
    let data_dir = tempfile::tempdir().unwrap();
    let config = ConfigBuilder::new(&data_dir).build();
    let mut br = BitRust::open(config).unwrap();

    let kvs = (0..10000)
        .map(|_| (util::rand_str(), util::rand_str()))
        .collect::<Vec<_>>();

    let total_writes = kvs.len() as f64;
    let mut durs = Vec::new();
    let mut dur_sum = 0f64;
    for (key, val) in kvs {

        let begin = Instant::now();
        br.put(key, val)?;
        let end = Instant::now();

        let dur = end - begin;
        let ns = dur.as_secs() * 1_000_000_000 + dur.subsec_nanos() as u64;
        durs.push(ns);
        dur_sum += ns as f64;
    }

    println!(
        "puts per second in this run: {}",
        total_writes / dur_sum * 1_000_000_000f64
    );
    Ok(())
}

fn _mean_std(vals: &[u64]) -> (f64, f64) {
    let mut sum = 0f64;
    let mut sq_sum = 0f64;

    for val in vals.iter().cloned() {
        let val = val as f64;
        sum += val;
        sq_sum += val * val;
    }

    let mean = sum / vals.len() as f64;
    let mean_sq = sq_sum / vals.len() as f64;
    let var = mean_sq - mean * mean;
    (mean, var.sqrt())
}

fn print_usage(program: &str, opts: getopts::Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}
