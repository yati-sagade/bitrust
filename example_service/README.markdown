### example_service

This contains gRPC server and client programs.

#### Start server

The server opens a datadir at ~/bitrust_data. 

Run from this directory:

```shell
$ mkdir ~/bitrust_data
$ cargo run --bin server
```

#### Use client

```shell
$ cargo build --bin client
$ target/debug/client put foo bar
status {code: OK}
$ target/debug/client get foo
value: "bar" status {code: OK}
...
```
