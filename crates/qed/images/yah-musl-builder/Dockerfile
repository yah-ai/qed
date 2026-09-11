# syntax=docker/dockerfile:1.7
#
# yah-musl-builder — the builder image for `yah-cli-release`'s
# x86_64-unknown-linux-musl leg.
#
# WHY IT EXISTS. That leg used to run on the camp Mac through
# `scripts/cross-build-guarded.sh`'s zigbuild branch, serially, behind the two
# darwin legs it does not depend on. It is now a `native = true` offload step on
# the arch-matched build-worker fleet (us-west-003), and an offloaded qed step
# runs as a CONTAINER via kamaji/containerd — so the fleet needs a builder
# image, for the same reason `mesofact-musl` needs `mesofact-musl-builder`.
#
# WHY NOT REUSE mesofact-musl-builder. Three independent disqualifications, and
# the third is the one that would have wasted a fleet build to discover:
#
#   1. It pins Rust 1.97.0 to match oss/mesofact/rust-toolchain.toml. The yah
#      root workspace has NO rust-toolchain.toml — it builds on whatever the
#      camp's default toolchain is, `stable` = 1.98.0 as of 2026-09-09, which is
#      also what us-west-003 is pinned to so the two can share sccache entries.
#   2. It bakes a ~152 MB musl librusty_v8.a plus RUSTY_V8_ARCHIVE /
#      RUSTY_V8_SRC_BINDING_PATH. `-p yah` has no v8 in its graph; that is pure
#      weight shipped to a worker for nothing.
#   3. IT HAS NO nasm. `-p yah --features all-image` needs one for two separate
#      crates (see the apk list below), and the failure lands in a build script
#      partway through a cold release build rather than at image-pull time.
#
# NO CROSS-COMPILE ANYWHERE. rustup publishes a musl HOST toolchain for both
# arches, so on an x86_64 Alpine worker `--target x86_64-unknown-linux-musl` is
# a native build: host cc IS the musl linker, and there is no sysroot to get
# wrong. This is the same choice mesofact-musl-builder makes and for the same
# reason.
#
# ── BUILD + PUBLISH ─────────────────────────────────────────────────────────
# One variant today (amd64) because one leg offloads today. The aarch64 musl
# leg still builds on the Mac: the arm workers keep containerd's store on a
# 2.5 GB rootfs, which fits neither this image nor a target dir, and relocating
# it to /data is a live-node filesystem change on a dev-raft voter — operator's
# call, the same one blocking mesofact-musl's arm legs.
#
#   docker buildx build --platform linux/amd64 \
#     -t yah-musl-builder:rust1.98.0-amd64 \
#     -o type=oci,dest=/tmp/yah-musl-builder-amd64.tar \
#     -f oss/qed/crates/qed/images/yah-musl-builder/Dockerfile \
#     oss/qed/crates/qed/images/yah-musl-builder
#   yah cloud cr push <layout-dir> --name yah-musl-builder --tag rust1.98.0-amd64
#
# The tag names the one thing that forces a rebuild — the Rust channel — NOT
# the yah version. A release that does not move the toolchain reuses the image.
#
# ⚠ PRE-PULL IS REQUIRED ON THE WORKER. kamaji does not auto-pull; a cold node
# refuses the deploy in ~55 ms with BackendRefused "image not found in
# containerd — pre-pull required". Namespace `yah`, on us-west-003:
#   sudo ctr -n yah images pull cr.yah.dev/yah-musl-builder:<tag>@sha256:<d>
#
# Catalog spec: oss/qed/crates/qed/images/catalog.toml + W148.
# Consumer: .yah/qed/yah-cli-release.toml, step build-x86_64-unknown-linux-musl.

# alpine:3.22 by index digest — the same bytes on both arches, and the same
# digest mesofact-musl-builder pins, so the two images share a base layer on any
# worker that holds both.
FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

# The camp's default toolchain (`rustup show`: stable = 1.98.0, 2026-09-09).
# Baked so an offloaded build never reaches static.rust-lang.org mid-run.
#
# THIS DOES NOT TRACK. The Mac is on `stable` and this is a literal, exactly as
# us-west-003's own rustc pin is (recorded in .yah/infra/machines/us-west-003.toml).
# When the Mac's stable moves, the darwin legs move with it and this one does
# not — rebuild and repin, or the four triples of one release ship compiled by
# two different compilers.
ARG RUST_CHANNEL=1.98.0

# build-base/musl-dev/linux-headers — the cc-crate floor: ring, zstd-sys,
#     libdeflate-sys (oxipng -> libdeflater) and the bundled libsqlite3-sys all
#     compile C here.
# nasm — REQUIRED, not defensive, and it is why this image exists rather than
#     mesofact's. Two consumers in `-p yah --features all-image`: mozjpeg-sys
#     assembles libjpeg-turbo's SIMD kernels with it (the same `brew install
#     nasm` the recipe header tells a Mac operator to run), and rav1e's default
#     `asm` feature does the same for AV1 encode via nasm-rs. Both are in
#     Cargo.lock; neither is optional at this feature set.
# cmake/perl/pkgconf — the -sys build-script floor. Cheap, and the class of
#     failure they prevent costs a whole cold build to observe.
# clang/clang-dev — libclang for any bindgen-backed build script. musl is a
#     target bindgen has no pregenerated bindings for.
# gawk — scripts/package-cli-tarball.sh parses the workspace version with an
#     awk program; busybox awk handles it, but the packaging script is
#     install.sh's contract and is not a place to discover a dialect difference.
# git — app/yah/cli/build.rs shells out to git. It finds no checkout here (the
#     source arrives as a tar extract), which is exactly why the pipeline passes
#     YAH_BUILD_ID; git is installed so the probe fails cleanly rather than as a
#     "command not found" in the middle of a build script.
RUN apk add --no-cache \
        bash coreutils findutils grep gawk tar xz gzip curl git ca-certificates \
        build-base musl-dev linux-headers \
        nasm \
        cmake make perl pkgconf \
        clang clang-dev

# rustup with a musl HOST toolchain — `uname -m` picks the right rustup-init, so
# each platform variant installs its own native compiler.
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN set -eux; \
    arch="$(uname -m)"; \
    triple="${arch}-unknown-linux-musl"; \
    curl -fsSL "https://static.rust-lang.org/rustup/dist/${triple}/rustup-init" \
        -o /tmp/rustup-init; \
    chmod +x /tmp/rustup-init; \
    /tmp/rustup-init -y --no-modify-path --profile minimal \
        --default-toolchain "${RUST_CHANNEL}"; \
    rm -f /tmp/rustup-init; \
    chmod -R a+w "$RUSTUP_HOME" "$CARGO_HOME"; \
    rustc -vV; \
    cargo --version

# ── NEUTRALISING THE CAMP'S ROOT .cargo/config.toml ─────────────────────────
# The step extracts the camp tree at /work and builds from inside it, so cargo
# walks upward and picks up /work/.cargo/config.toml. Two of its keys are wrong
# in here, and each fails differently:
#
#  1. `build.rustc-wrapper = "cargo-orphan-gc"` (R748) — a yah workspace tool,
#     not installed here. An EMPTY RUSTC_WRAPPER is cargo's documented "no
#     wrapper", which is why this is a disable rather than a redirect. (Same
#     line, same reason, as mesofact-musl-builder; it is a property of the camp
#     config, not of either pipeline.)
#  2. `[target.x86_64-unknown-linux-musl] linker = "x86_64-linux-musl-gcc"`
#     (R330-S26) — a musl-CROSS linker name. This image is musl-NATIVE, where
#     the linker is plain `cc`, so the config's name does not exist and the link
#     would fail after a full compile. Both triples are set; the one that does
#     not match the image's arch is inert.
#
# THE THIRD KEY IS DELIBERATELY LEFT ALONE, and this is where this image
# diverges from mesofact-musl-builder on purpose. That one sets a global
# RUSTFLAGS (it has to — it carries link-args for V8's C++ runtime). Cargo picks
# exactly ONE rustflags source and the env var outranks every config table, so
# setting RUSTFLAGS here would SILENTLY DROP
# `[target.<triple>].rustflags = ["-C","link-arg=-Wl,-z,stack-size=8388608"]`.
#
# That flag is not decoration. musl's default pthread stack is ~128 KiB against
# glibc's 8 MiB, and R755-T4 found the flag had been a no-op typo for every musl
# release cut through `cross` — so every musl `yah` ever shipped went out
# without the backstop. Dropping it again here would reproduce that bug in a
# form no test catches: the build succeeds, the binary stack-overflows on a
# user's machine. Leaving RUSTFLAGS unset means this image asks for exactly what
# a local musl build asks for. If a link-arg ever IS needed here, fold that
# stack-size flag into it rather than replacing it.
ENV RUSTC_WRAPPER="" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=cc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=cc

# Fail at image-build time rather than 40 minutes into a fleet build.
RUN set -eux; \
    triple="$(uname -m)-unknown-linux-musl"; \
    bash --version >/dev/null; \
    rustc -vV | grep -q "host: ${triple}"; \
    rustc -vV | grep -q "release: ${RUST_CHANNEL}"; \
    cargo --version >/dev/null; \
    cc --version >/dev/null; \
    nasm -v >/dev/null; \
    cmake --version >/dev/null; \
    gawk --version >/dev/null; \
    tar --version | grep -q GNU; \
    test -z "${RUSTFLAGS:-}"

# The pipeline packs its whole invocation into ONE argv string and relies on the
# entrypoint being `bash -c` — a multi-element argv would drop everything past
# [0]. Same constraint as mesofact-musl-builder and rusty-v8-musl-builder.
ENTRYPOINT ["bash", "-c"]
