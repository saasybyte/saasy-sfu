# saasy-sfu

Media-plane WebRTC Selective Forwarding Unit for [SaasyByte](https://github.com/saasybyte/saasybyte), an open-source real-time AI voice platform.

Built on [mediasoup](https://mediasoup.org/), the SFU owns all media routing: it allocates transports, producers, and consumers on behalf of the signaling server, and forwards RTP audio between the web client and the AI media engines. It exposes no client-facing API; the signaling server drives it over an internal gRPC boundary using mediasoup's ORTC-style primitives (no SDP).

## How It Fits

- **Serves saasy-signal** (gRPC): media resource management. Signal translates client WebSocket requests into SFU gRPC calls.
- **Forwards RTP** between the web client and the C++ media engines (saasy-media-engine), which connect as first-class WebRTC peers.
- **Proto types** come from [saasy-proto-rust](https://github.com/saasybyte/saasy-proto-rust) (git dependency).

See the [platform overview](https://github.com/saasybyte/saasybyte) for the full architecture.

## Build & Run

Requirements: stable Rust toolchain, `protoc` (protobuf compiler). The mediasoup crate builds its own C++ worker on first compile.

```bash
make run            # run dev server
make build          # debug build
make release        # release build
make test           # run tests
make clippy-strict  # lint, fail on warnings
```

Configuration lives in `config/default.toml` with environment variable overrides. The one setting every deployment must change is `announced_ip_addr`: the IP announced to WebRTC clients in ICE candidates (the host's public IP in production; `127.0.0.1` works for an all-local stack). Ports: 9091 (HTTP health), 50051 (gRPC), plus UDP ranges for RTP.

A `Dockerfile` is included; `docker build .` needs no credentials.

## License

Apache-2.0, see [LICENSE](LICENSE).
