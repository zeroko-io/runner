# syntax=docker/dockerfile:1
# The Zeroko runner: the Go binary and workerd, on an image with nothing else
# but the C library both link against and CA certificates.
# Every stage runs on the build machine and cross-compiles for the target
# (amd64 or arm64), so no stage needs emulation.

FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build
ARG TARGETARCH
RUN set -eux; \
    case "$TARGETARCH" in amd64) t=x86-64;; arm64) t=aarch64;; *) echo "unsupported: $TARGETARCH"; exit 1;; esac; \
    apt-get update && apt-get install -y --no-install-recommends "gcc-$t-linux-gnu" "libc6-dev-$TARGETARCH-cross" \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
ARG VERSION=dev
RUN case "$TARGETARCH" in amd64) export CC=x86_64-linux-gnu-gcc;; arm64) export CC=aarch64-linux-gnu-gcc;; esac; \
    CGO_ENABLED=1 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X 'main.version=${VERSION}'" -o /out/runner ./cmd/runner

# The target's Debian, only to copy its C library from: nothing runs in it.
FROM debian:bookworm-slim AS target

FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS rootfs
ARG TARGETARCH
ARG WORKERD_VERSION=1.20261021.1
RUN set -eux; \
    case "$TARGETARCH" in amd64) t=x86-64;; arm64) t=aarch64;; esac; \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates curl "binutils-$t-linux-gnu" \
 && rm -rf /var/lib/apt/lists/*
COPY --from=target / /target/
COPY --from=build /out/runner /rootfs/usr/bin/runner
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) t=x86_64-linux-gnu; pkg=workerd-linux-64;; \
      arm64) t=aarch64-linux-gnu; pkg=workerd-linux-arm64;; \
    esac; \
    curl -fsSL "https://registry.npmjs.org/@cloudflare/$pkg/-/$pkg-$WORKERD_VERSION.tgz" | tar xz -C /tmp; \
    $t-strip -o /rootfs/usr/bin/workerd /tmp/package/bin/workerd; \
    # The shared libraries both binaries load, and those they load, and
    # nothing else, from the target's Debian.
    mkdir -p /rootfs/lib/$t; \
    todo="/rootfs/usr/bin/workerd /rootfs/usr/bin/runner"; seen=" "; \
    while [ -n "$todo" ]; do \
      set -- $todo; f=$1; shift; todo="$*"; \
      for lib in $($t-readelf -d "$f" | sed -n 's/.*Shared library: \[\(.*\)\]/\1/p'); do \
        case "$seen" in *" $lib "*) continue;; esac; seen="$seen$lib "; \
        cp -L "/target/usr/lib/$t/$lib" "/rootfs/lib/$t/$lib"; \
        todo="$todo /rootfs/lib/$t/$lib"; \
      done; \
    done; \
    interp=$($t-readelf -l /rootfs/usr/bin/runner | sed -n 's/.*interpreter: \(.*\)]/\1/p'); \
    mkdir -p "/rootfs$(dirname "$interp")"; \
    cp -L "/target/usr/lib/$t/$(basename "$interp")" "/rootfs$interp"; \
    mkdir -p /rootfs/etc/ssl/certs /rootfs/tmp /rootfs/data; \
    cp /etc/ssl/certs/ca-certificates.crt /rootfs/etc/ssl/certs/; \
    chmod 1777 /rootfs/tmp; \
    chown 1000:1000 /rootfs/data

# Zeroko starts as root only to give /data to ZEROKO_UID and ZEROKO_GID, and
# runs as them.
FROM scratch
COPY --from=rootfs /rootfs /
VOLUME /data
EXPOSE 8080 8081
ENTRYPOINT ["/usr/bin/runner"]
