FROM rust:1.72-slim-bookworm AS base
RUN apt update \
    && apt install -yf libssl-dev libsasl2-dev pkgconf gcc g++ python3 make \
    && apt autoremove

WORKDIR /usr/src/app
COPY . .
RUN cargo install --path .

FROM debian:12-slim
RUN apt update && apt install -yf libssl3 libsasl2-2
WORKDIR /app
COPY --from=base /usr/src/app/target/release/rust-redpanda .
ENTRYPOINT ["/app/rust-redpanda"]
