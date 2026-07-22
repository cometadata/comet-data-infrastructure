# ----------------------------------
# COMET Docker Image
# Two-stage build: compile arxiv-tex-extract, then copy to lightweight runtime
# ----------------------------------

ARG UV_VERSION
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

# ----------------------------------
# Stage 1: Build arxiv-tex-extract
# ----------------------------------
FROM amazonlinux:2023 AS builder
ARG RUST_TARGET_CPU

RUN dnf groupinstall "Development Tools" -y

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy local arxiv-tex-extract source
COPY --from=arxiv-tex-extract . /arxiv-tex-extract
WORKDIR /arxiv-tex-extract
RUN RUSTFLAGS="-C target-cpu=${RUST_TARGET_CPU}" cargo build --release

# ----------------------------------
# Stage 2: Lightweight runtime image
# ----------------------------------
FROM amazonlinux:2023
ARG S5CMD_VERSION
ARG DUCKDB_VERSION
ARG PYTHON_VERSION

ENV S5CMD_VERSION=${S5CMD_VERSION}
ENV DUCKDB_VERSION=${DUCKDB_VERSION}
ENV PYTHON_VERSION=${PYTHON_VERSION}

# Pull uv into image
COPY --from=uv /uv /uvx /bin/

# Cache and site-packages are on separate filesystems; copy instead of hardlinking.
ENV UV_LINK_MODE=copy

RUN dnf install -y \
    aws-cli \
    git \
    tar \
    gzip \
    wget \
    nano \
    unzip \
    python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-pip

ENV TMPDIR=/data/tmp
RUN mkdir -p ${TMPDIR}

WORKDIR /app

# Install s5cmd
RUN wget https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}/s5cmd_${S5CMD_VERSION}_Linux-64bit.tar.gz && \
    tar -xzf s5cmd_${S5CMD_VERSION}_Linux-64bit.tar.gz s5cmd && \
    chmod +x s5cmd && \
    mv s5cmd /usr/local/bin/ && \
    rm s5cmd_${S5CMD_VERSION}_Linux-64bit.tar.gz

# Install duckdb
RUN wget https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip && \
    unzip duckdb_cli-linux-amd64.zip && \
    chmod +x duckdb && \
    mv duckdb /usr/local/bin/ && \
    rm duckdb_cli-linux-amd64.zip

# Copy built binary from builder stage
COPY --from=builder /arxiv-tex-extract/target/release/latex-extract /usr/local/bin/

# Sync Python dependencies
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev --python ${PYTHON_VERSION}

ENV PATH="/app/.venv/bin:$PATH"

# Install comet package
COPY src/ src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --python ${PYTHON_VERSION}
