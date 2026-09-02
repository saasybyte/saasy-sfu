# saasy-sfu

## Commands
- `make run` — run dev server
- `make build` / `make release` — debug / release build
- `make check` — fast compilation check (no codegen)
- `make test` — run tests
- `make clippy` / `make clippy-strict` — lint / lint with `-D warnings`
- `make fmt` — format code

## Conventions
- **Error types**: per-module enums with `thiserror::Error` (e.g., `SessionError`, `WorkerManagerError`, `CodecError`). No `anyhow`.
- **Logging**: `tracing` crate (`info!`, `warn!`, `error!`, `debug!`). Not `log` or `println!`.
- **Entry point**: `#[tokio::main]`, not `#[actix_web::main]`. Actix is only used for health check HTTP server, spawned as a background task.
- **Shared state**: `SfuCore` wrapped in `Arc<tokio::sync::Mutex<_>>` — single lock guards all session state.
- **Proto types**: from `saasy-proto-rust` (git dep): `saasy_proto_rust::{sfu, shared}`.
- **gRPC service pattern**: `SfuHandler` implements `SfuService` via `#[tonic::async_trait]`. Each method extracts data from `SfuRequestEnvelope`, calls `SfuCore`, wraps result in `SfuResponseEnvelope` with `type` string + data variant.
- **Event handler pattern**: core methods return `PendingEventSetup` variants. The handler layer wires mediasoup callbacks via `setup_event_handlers` — event wiring does not happen inside `SfuCore`.
- **Consumers start paused**: always created with `paused = true`, must call `ResumeConsumer` to unpause.

## Service Boundaries
- **Serves saasy-signal** (gRPC): media resource management. Signal translates client WebSocket requests into SFU gRPC calls.
- **Proto types from saasy-proto-rust** (git dep): do not define proto types locally.
- **Does not own**: signaling (saasy-signal), proto schema (saasy-proto-rust).
