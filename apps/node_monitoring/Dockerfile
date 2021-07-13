ARG BASE_IMAGE=tezedge/tezedge-libs:latest

FROM tezedge/tezos-opam-builder:debian10 as build-env

USER root
RUN apt-get update && apt-get install -y libssl-dev

# Checkout and compile tezedge source code
ARG tezedge_git="https://github.com/tezedge/tezedge.git"
ARG rust_toolchain="nightly-2020-12-31"
ARG SOURCE_BRANCH
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain ${rust_toolchain} -y
ENV PATH=/home/appuser/.cargo/bin:$PATH
ENV RUST_BACKTRACE=1
ENV SODIUM_USE_PKG_CONFIG=1
ENV OCAML_BUILD_CHAIN=remote
ARG RUSTFLAGS=""
ENV OCAML_WHERE_PATH=/tmp/ocaml-includes
RUN wget https://gitlab.com/tezedge/tezos/uploads/1bf4d1d130e75129be82a148149b04b3/libtezos-ffi-headers.tar.gz
RUN mkdir -p $OCAML_WHERE_PATH; tar xvzf libtezos-ffi-headers.tar.gz -C $OCAML_WHERE_PATH
RUN cd /home/appuser && git clone ${tezedge_git} --branch ${SOURCE_BRANCH} && cd tezedge && RUSTFLAGS=${RUSTFLAGS} cargo build --bin node-monitoring --release

WORKDIR /home/appuser/tezedge

FROM ${BASE_IMAGE} as light-node

# Copy binaries
COPY --from=build-env /home/appuser/tezedge/target/release/node-monitoring /

# Copy shared libraries
COPY --from=build-env /usr/lib/x86_64-linux-gnu/libssl.so.1.1 /usr/lib/x86_64-linux-gnu/libssl.so.1.1
COPY --from=build-env /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1 /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1

# Default entry point runs node with default config + several default values, which can be overriden by CMD
ENTRYPOINT [ "/node-monitoring" ]
