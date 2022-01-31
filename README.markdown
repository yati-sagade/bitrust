# Bitrust

 This is a Rust implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf), a persistent key-value store for
 when the keyspace fits in memory. This is not a distributed datastore, but
 can be used as a building block for one.
 
 This crate hasn't been stress-tested. While it should be good enough for experimentation, we make no guarantees around data durability.
 
 ## Running

```shell
$ mkdir /tmp/bitrust_data
$ cargo run -- --configfile ./example_configs/no-automerge.toml --loglevel debug
> put foo bar
> get foo
Ok(Some("bar"))
> put baz spam
> put lala baba
> put baz egg
> lst
lala
foo
baz
> get baz
Ok(Some("egg"))
> quit 
$ less /tmp/bitrust_data/bitrust.log
```

You can view the logs in a separate window by tailing `bitrust.log` in the
datadir.

```
$ tail -f /tmp/bitrust_data/bitrust.log
```

## Data directory contents

- Data files are stored as `N.data` where `N` is an integer starting at 0.
- `.activefile` contains the name of the current "active" log file (i.e., the
one taking writes). All other datafiles are immutable.
- `bitrust.log` contains logs. Level of logging is controlled by the `-l/--loglevel` switch.
