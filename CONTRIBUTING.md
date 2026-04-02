# Contributing to Event Winnower

Thank you for your interest in contributing to the project.

## Setting up the project

Clone the repository and build the project using Cargo:

```console
$ cargo build
```

To build the release binary:

```console
$ cargo build --release
```

The binary will be available at `target/release/eventwinnower`.

## Writing code

### Code style

This project follows the standard conventions for Rust projects that are imposed by 
[`rustfmt`](https://github.com/rust-lang/rustfmt). `rustfmt` is exposed via the 
`cargo fmt` sub-command.

```console
$ cargo fmt
```

To assist with writing idiomatic code, you should also regularly apply the `clippy`
code linter. This can also be invoked by `cargo`:

```console
$ cargo clippy
```

### Dependencies

To add dependencies, update `Cargo.toml` with the crate name and version. Dependencies are pulled from crates.io.
