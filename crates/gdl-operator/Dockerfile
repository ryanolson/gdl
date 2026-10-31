# The gdl operator's image, built from the repository's root:
#
#   docker build -f crates/gdl-operator/Dockerfile -t <image> .
#
# The build stage is the toolchain that rust-toolchain.toml pins, on Debian
# 12, the release the base image is built from, so that the binary finds
# the glibc it was linked against. RUSTUP_TOOLCHAIN keeps rustup from
# installing the toml's extra components, which the build does not use.
FROM rust:1.93.1-slim-bookworm AS build
ENV RUSTUP_TOOLCHAIN=1.93.1 CARGO_TARGET_DIR=/build
WORKDIR /app
COPY . /app
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build \
    cargo build --release --locked -p gdl-operator \
    && cp /build/release/gdl-operator /gdl-operator
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /gdl-operator /usr/local/bin/gdl-operator
ENTRYPOINT ["/usr/local/bin/gdl-operator"]
