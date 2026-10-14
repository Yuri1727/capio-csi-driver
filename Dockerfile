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

# Pinned to upstream master of 2026-10-11 ("Fix unbounded
# files_created_by_producer growth", #252). It already contains the fixes
# for the MPI rank-to-hostname routing and for the length of remote read
# replies (upstream #223), so only one local patch is still needed.
ARG CAPIO_COMMIT=c25790caff6c1d3268ee918924c4f1213b1dfea4
RUN git clone https://github.com/High-Performance-IO/capio.git /src/capio \
    && cd /src/capio && git checkout ${CAPIO_COMMIT}

# Local-only fix, never upstreamed and never pushed to the CAPIO repository - applied only
# to this build's copy of the source:
#   - capio-concurrent-read-race: concurrent readers of the same file are served by
#     copying the requested slice of the shared file buffer under the file's lock, instead
#     of through the raw getBuffer() pointer, which a concurrent expandBuffer() can free.
COPY patches/capio-concurrent-read-race.patch /tmp/patches/
RUN cd /src/capio \
    && git apply /tmp/patches/capio-concurrent-read-race.patch

WORKDIR /src/capio/build
RUN cmake -DCMAKE_BUILD_TYPE=Release .. \
    && cmake --build . -j"$(nproc)"

# libcapio_posix.so and libsyscall_intercept.so.0 are symlinks in the build tree. Docker's
# COPY follows them, but Kaniko (used to build this image in-cluster) copies the symlink
# itself, leaving a dangling link in the runtime image. Resolve them here (cp -L) and copy
# the real files from this directory instead.
RUN mkdir -p /out/lib \
    && cp -L capio/posix/libcapio_posix.so /out/lib/libcapio_posix.so \
    && cp -L capio/posix/syscall_intercept/lib/libsyscall_intercept.so.0 /out/lib/libsyscall_intercept.so.0

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
# Installing openssh-server also generates host keys under /etc/ssh; they are
# never used (sshd_config points HostKey at the key mounted by the MPI
# Operator) and would be the same in every copy of the published image, so
# they are removed.
RUN mkdir -p /run/sshd \
    && rm -f /etc/ssh/ssh_host_*

COPY --from=capio-builder /src/capio/build/capio/server/capio_server /usr/local/bin/capio_server
COPY --from=capio-builder /out/lib/libcapio_posix.so /usr/local/lib/libcapio_posix.so
COPY --from=capio-builder /out/lib/libsyscall_intercept.so.0 /usr/local/lib/libsyscall_intercept.so.0
COPY --from=driver-builder /out/capio-csi-driver /usr/local/bin/capio-csi-driver

RUN ldconfig

ENTRYPOINT ["/usr/local/bin/capio-csi-driver"]
