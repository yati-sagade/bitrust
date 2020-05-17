# Bitrust

 This is a Rust implmentation of Bitcask: http://basho.com/wp-content/uploads/2015/05/bitcask-intro.pdf

 ** This project is under development. Please do not use it seriously **

## Running

```shell
$ mkdir /tmp/datadir
$ cargo run -- --datadir /tmp/datadir --loglevel debug
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
$ less /tmp/datadir/bitrust.log
```

## Data directory contents

- Data files are stored as `N.data` where `N` is an integer starting at 0.
- `.activefile` contains the name of the current "active" log file (i.e., the
one taking writes). All other datafiles are immutable.
- `bitrust.log` contains logs. Level of logging is controlled by the `-l/--loglevel` switch.
