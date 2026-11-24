# Builds the Sail CLI from a pinned commit of https://github.com/lakehq/sail.
# Adapted from Sail's own docker/release/Dockerfile. Differences:
#   * checks out an exact commit SHA instead of a release tag
#   * uses thin LTO (CARGO_PROFILE_RELEASE_LTO=thin) so the build fits a
#     4-vCPU / 16 GB GitHub-hosted runner; upstream release builds use fat LTO
ARG PYTHON_VERSION=3.14
FROM python:${PYTHON_VERSION}-slim AS builder
ARG SAIL_SHA
ARG RUST_VERSION=1.99.1
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates gcc g++ libc6-dev protobuf-compiler libprotobuf-dev curl git pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup-init && \
    chmod a+x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}" \
    CARGO_PROFILE_RELEASE_LTO=thin \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
    CARGO_NET_GIT_FETCH_WITH_CLI=true
WORKDIR /app
RUN git init -q . && git remote add origin https://github.com/lakehq/sail.git && \
    git fetch -q --depth 1 origin "${SAIL_SHA}" && git checkout -q FETCH_HEAD && \
    git rev-parse HEAD > /SAIL_SHA
RUN cargo build -p sail-cli --release --bins && cp target/release/sail /usr/local/bin/sail && \
    rm -rf target

FROM python:${PYTHON_VERSION}-slim
ARG PYSPARK_VERSION=4.2.1
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl procps \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install --no-cache-dir "pyspark-client==${PYSPARK_VERSION}"
COPY --from=builder /usr/local/bin/sail /usr/local/bin/sail
COPY --from=builder /SAIL_SHA /SAIL_SHA
ENTRYPOINT ["/usr/local/bin/sail"]
