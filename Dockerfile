FROM rust:1.72-slim-bookworm

RUN apt update \
    && apt install -yf libssl-dev libsasl2-dev pkgconf gcc g++ python3 make \
    && apt autoremove

WORKDIR /usr/src/app
COPY . .
RUN cargo install --path .
ENTRYPOINT ["rust-redpanda"]
