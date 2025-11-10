FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release -p soulbase-gateway

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/soulbase-gateway /usr/local/bin/soulbase-gateway
COPY config/gateway.local.toml /etc/soulbase/gateway.toml
ENV GATEWAY_CONFIG_FILE=/etc/soulbase/gateway.toml
ENV RUST_LOG=info
EXPOSE 8080
CMD ["soulbase-gateway"]
