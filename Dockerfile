# Stage 1: compile CAPIO from source. Building inside the image (rather than
# copying host-built binaries) keeps the result independent from whatever
# happens to be installed on the machine doing the build.
FROM debian:bookworm AS capio-builder

# ForceIPv4: some networks advertise IPv6 without a working route, and apt then
# hangs/fails on deb.debian.org instead of falling back. Harmless where IPv6 works.
RUN apt-get -o Acquire::ForceIPv4=true update \
    && apt-get -o Acquire::ForceIPv4=true install -y --no-install-recommends \
    build-essential cmake git ca-certificates \
    libopenmpi-dev openmpi-bin \
    && rm -rf /var/lib/apt/lists/*

# Pinned to the commit validated during development.
ARG CAPIO_COMMIT=5a6097d34f4d7e4b764a89e293b220df630e46de
RUN git clone https://github.com/High-Performance-IO/capio.git /src/capio \
    && cd /src/capio && git checkout ${CAPIO_COMMIT}

# Local-only fixes, never upstreamed and never pushed to the CAPIO repository - applied
# only to this build's copy of the source. See docs/ for the full writeup of each:
#   - capio-concurrent-read-race: race on the shared file buffer when several readers of the
#     same on_close file are woken together (docs/capio-concurrent-read-race.md)
#   - capio-mpi-rank-hostname: rank_to_hostname never populated for remote ranks, which
#     misroutes every remote read reply (docs/multinode-segfault-kubernetes-repro.md)
#   - capio-remote-read-eof: remote read replies used the raw requested count instead of the
#     already-computed, clamped bytes_read, so reads never saw EOF (same doc as above)
COPY patches/ /tmp/patches/
RUN cd /src/capio \
    && git apply /tmp/patches/capio-concurrent-read-race.patch \
    && git apply /tmp/patches/capio-mpi-rank-hostname.patch \
    && git apply /tmp/patches/capio-remote-read-eof.patch

WORKDIR /src/capio/build
RUN cmake -DCMAKE_BUILD_TYPE=Release .. \
    && cmake --build . -j"$(nproc)"

# Stage 2: compile the driver itself.
FROM golang:1.25-bookworm AS driver-builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 go build -o /out/capio-csi-driver ./cmd/capio-csi-driver

# Stage 3: runtime image. Ships capio_server (needs OpenMPI even with the
# "none" backend), the driver binary, and libcapio_posix.so with its
# syscall_intercept dependency. The library is not used by the driver
# itself, but the same image doubles as the base for producer/consumer test
# pods, which need it for LD_PRELOAD.
#
# The same image also serves as launcher and worker for MPIJobs (Kubeflow MPI
# Operator), which is how multi-node capio_server instances get started as MPI
# ranks: openmpi-bin provides mpirun for the launcher role, openssh-server the
# sshd the launcher connects to on each worker. In the CSI driver role the
# ENTRYPOINT below is unchanged and sshd is simply never started.
FROM debian:bookworm-slim

RUN apt-get -o Acquire::ForceIPv4=true update \
    && apt-get -o Acquire::ForceIPv4=true install -y --no-install-recommends \
    libopenmpi3 openmpi-bin ca-certificates \
    openssh-server openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY docker/sshd_config /etc/ssh/sshd_config
COPY docker/ssh_config /etc/ssh/ssh_config

# sshd refuses to start without its privilege separation directory.
RUN mkdir -p /run/sshd

COPY --from=capio-builder /src/capio/build/capio/server/capio_server /usr/local/bin/capio_server
COPY --from=capio-builder /src/capio/build/capio/posix/libcapio_posix.so /usr/local/lib/libcapio_posix.so
COPY --from=capio-builder /src/capio/build/capio/posix/syscall_intercept/lib/libsyscall_intercept.so.0 /usr/local/lib/libsyscall_intercept.so.0
COPY --from=driver-builder /out/capio-csi-driver /usr/local/bin/capio-csi-driver

RUN ldconfig

ENTRYPOINT ["/usr/local/bin/capio-csi-driver"]
