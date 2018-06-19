extern crate bitrust;
extern crate log;
extern crate simple_logger;
extern crate getopts;

use std::io::{self};
use std::env;
use std::path::Path;

use log::LogLevel;
use bitrust::BitRust;

fn main() -> io::Result<()> {
    let matches = parse_opts();

    let loglevel = matches.opt_str("l").unwrap_or(String::from("info"));

    let loglevel = match loglevel.as_ref() {
        "info" => LogLevel::Info,
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
        _ => panic!("Invalid loglevel"),
    };

    simple_logger::init_with_level(loglevel)
        .expect("Could not initialize logger");
    let mut br = BitRust::new(Path::new("/home/ys/data"))?;
    loop {
        let mut cmd = String::new();
        io::stdin().read_line(&mut cmd)?;
        let cmd = cmd.trim().split_whitespace().collect::<Vec<_>>();
        if cmd[0] == "put" {
            let key = cmd[1];
            let val = cmd[2];
            br.put(key.to_string(), val.to_string())?;
        } else if cmd[0] == "get" {
            let key = cmd[1];
            println!("{:?}", br.get(key));
        }
    }
    #[allow(unreachable_code)]
    Ok(())
}

fn parse_opts() -> getopts::Matches {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();
    opts.optopt("l", "loglevel", "Log level", "LEVEL");
    opts.parse(&args[1..]).expect("Error parsing options")
}
