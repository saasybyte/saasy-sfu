# ------------------------------
# Build stage
# ------------------------------
FROM rust:bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y git python3-pip protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Install rustfmt (required by mediasoup-sys build)
RUN rustup component add rustfmt

# Copy dependency manifests
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo

# Copy source code
COPY src ./src

# Build the application
RUN cargo build --release

# ------------------------------
# Runtime stage
# ------------------------------
FROM debian:bookworm-slim

WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/saasy-sfu ./saasy-sfu

# Expose HTTP (health) and gRPC ports
EXPOSE 9091 50051

# Run the application
CMD ["./saasy-sfu"]
