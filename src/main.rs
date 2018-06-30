extern crate bitrust;
#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate getopts;
extern crate ctrlc;

use std::io::{self, Write};
use std::env;
use std::process;
use std::path::PathBuf;

use log::LogLevel;
use bitrust::BitRust;

fn main() -> io::Result<()> {
    let matches = parse_opts();
    setup_logging(loglevel(&matches));
    let data_dir = datadir(&matches);
    info!("data_dir: {:?}", &data_dir);
    let mut br = match BitRust::open(&data_dir) {
        Ok(br) => br,
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
            eprintln!(
                "Lock file {:?} exists, and is already held by pid {}",
                data_dir.join(".lock"),
                0
            );
            process::exit(1);
        }
        Err(e) => {
            return Err(e);
        }
    };

    cmd_loop(&mut br)
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
        ("help", "help/?\n  Show this message"),
    ];


    loop {
        let mut cmd = String::new();
        prompt()?;
        io::stdin().read_line(&mut cmd)?;
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
            }
            let key = cmd[1];
            let val = cmd[2];
            br.put(key.to_string(), val.to_string())?;
        } else if cmd[0] == "get" {
            if cmd.len() != 2 {
                println!("{}", get_usage(&cmd_usages, "get").unwrap());
            }
            let key = cmd[1];
            println!("{:?}", br.get(key));
        } else if cmd[0] == "lst" {
            for key in br.keys() {
                println!("{}", key);
            }
        } else if cmd[0] == "del" {
            if cmd.len() != 2 {
                println!("{}", get_usage(&cmd_usages, "del").unwrap());
            }
            println!("{:?}", br.delete(cmd[1]));
        } else if cmd[0] == "exit" || cmd[0] == "quit" {
            break;
        } else {
            println!("Invalid command {}, try typing help", cmd[0]);
        }
    }
    info!("Exit");
    #[allow(unreachable_code)] Ok(())
}

fn loglevel(matches: &getopts::Matches) -> LogLevel {
    let loglevel = matches.opt_str("l").unwrap_or_else(|| String::from("info"));
    match loglevel.as_ref() {
        "info" => LogLevel::Info,
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
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

fn setup_logging(loglevel: LogLevel) {
    simple_logger::init_with_level(loglevel).expect("Could not initialize logger");
}

fn parse_opts() -> getopts::Matches {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();
    opts.optopt("l", "loglevel", "Log level", "LEVEL");
    opts.optopt("d", "datadir", "Data directory", "DATADIR");
    opts.parse(&args[1..]).expect("Error parsing options")
}
