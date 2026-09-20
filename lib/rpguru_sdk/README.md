# `rpguru_sdk`

Shared crate holding the generated gRPC/protobuf types for the whole workspace.

## What it does

- Holds a `build.rs` (add one here) that compiles the `.proto` files in the
  workspace [`proto/`](../../proto) directory with `tonic-prost-build`.
- Re-exports each generated package as a Rust module via
  `tonic::include_proto!`.
- Hosts conversions between protobuf types and domain types (`time`, `uuid`,
  …) so every consumer shares one implementation.

Both the [`guru-master`](../../bin/guru-master) binary and the business modules
under [`modules/`](../../modules) depend on this crate.

## Adding a service

1. Write your `.proto` under `proto/` — group by module, e.g.
   `proto/base/base.proto`, with a package name like `app.base`.
2. Register it in `build.rs` and compile with `tonic-prost-build`
   (`build_server(true)` / `build_client(true)`).
3. Re-export the generated package in `src/lib.rs`:

   ```rust,ignore
   pub mod base {
       tonic::include_proto!("app.base");
   }
   ```

4. Implement the generated service trait in the owning module's `rpc/` layer.

## Why a single crate

Generating all protobuf code in one place means:

- protobuf types are compiled once, not per module;
- cross-cutting conversions (dates, intervals, `Empty`, …) live in one spot;
- both server and client stubs are available everywhere they are needed.
