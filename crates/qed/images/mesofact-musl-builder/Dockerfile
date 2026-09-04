# syntax=docker/dockerfile:1.7
#
# mesofact-musl-builder — the builder image for the `mesofact-musl` qed
# pipeline (R560-T7).
#
# WHY IT EXISTS. The two `*-unknown-linux-musl` mesofact legs are NOT built in
# GitHub Actions (see .yah/qed/mesofact-musl.toml and R560-T5's superseded-
# approach note). They run as `native = true` offload steps on the arch-matched
# build-worker fleet, and an offloaded qed step runs as a CONTAINER via
# kamaji/containerd — so the fleet needs a builder image, for the same reason
# `rusty-v8-musl` needs `rusty-v8-musl-builder`.
#
# WHY NOT REUSE rusty-v8-musl-builder. That image is a V8 TOOLBOX, not a Rust
# builder: it installs Alpine's apk `rust` (1.96, no rustup) and bakes
# V8_FROM_SOURCE=1 + GN/NINJA/CLANG_BASE_PATH as ENV. Both are disqualifying
# here — oss/mesofact/rust-toolchain.toml pins channel 1.97.0 (R560-T2:
# oxc_transformer needs `if let` match-arm guards, stabilized in 1.97.0), and
# inheriting V8_FROM_SOURCE makes v8's build.rs IGNORE RUSTY_V8_ARCHIVE and
# shell out to gn/ninja, which then dies on a missing icudtl.dat without ever
# mentioning the archive (R546-T9 burned a debug cycle on exactly that). This
# image therefore sets NONE of those four variables. Do not add them.
#
# NO CROSS-COMPILE ANYWHERE. rustup publishes host toolchains for both musl
# triples, so on Alpine the compiler is musl-native and `cargo build --target
# <the host's own triple>` is a native build. That matters for more than tidiness:
# the baked librusty_v8 archive was built `use_custom_libcxx=false` against
# Alpine's GNU libstdc++, so `-lstdc++` has to resolve to that same libstdc++.
# A glibc-host → musl-target cross would need a musl-cross sysroot carrying its
# own libstdc++ and is a strictly worse way to reach the same place.
# VERIFIED 2026-08-11 in `alpine:3.22` on both arches (`rustc -vV` reports
# `host: {aarch64,x86_64}-unknown-linux-musl`, release 1.97.0). `libgcc` is a
# hard requirement of that toolchain, not a nicety — without it every rustup
# binary dies with `Error loading shared library libgcc_s.so.1`.
#
# NATIVE PER-ARCH (no multi-arch cross-build): build and push BOTH platform
# variants so each native fleet leg finds a matching manifest, exactly as
# rusty-v8-musl-builder does. `uname -m` at image-build time selects the arch's
# archive, so each variant is self-consistent.
#
#   docker buildx build --platform linux/arm64 \
#     -t mesofact-musl-builder:v0.8.22-arm64 \
#     -o type=oci,dest=/tmp/mesofact-musl-builder-arm64.tar \
#     -f oss/qed/crates/qed/images/mesofact-musl-builder/Dockerfile \
#     oss/qed/crates/qed/images/mesofact-musl-builder
#   yah cloud cr push <layout-dir> --name mesofact-musl-builder --tag v0.8.22-arm64
#
# ── PUBLISHED 2026-08-11 (R560-T7) ─────────────────────────────────────────
# Both variants are live on cr.yah.dev and served unauthenticated by digest.
# These are the refs `.yah/qed/mesofact-musl.toml` (R560-T8) must spell:
#
#   cr.yah.dev/mesofact-musl-builder:v149.4.0-rust1.97-arm64
#     @sha256:0b88b0616570d5c8194a57752bb87584312d7afe963f719c9d09df7abdbdc3c8
#   cr.yah.dev/mesofact-musl-builder:v149.4.0-rust1.97-amd64
#     @sha256:0f87b99416ee530c0841b24367af38f988acb30a2c1bda15db16a3e8a3e51fa3
#
# The tag names the two things that actually force a rebuild — the rusty_v8
# version of the baked archive and the Rust channel — NOT the mesofact version.
# A mesofact release that moves neither can reuse the same image.
#
# PROVENANCE, stated exactly. The arm64 variant was built AND fully exercised
# natively on an arm64 Mac: a real `build-mesofact.sh aarch64-unknown-linux-musl`
# run against the camp tree produced both binaries, both statically linked, and
# they run on a bare `alpine:3.22` with zero libstdc++ packages installed. The
# amd64 variant was built under QEMU on the same host, and NO mesofact build has
# been run through it. That is a smaller gap than it sounds: every byte in this
# image is a *download* (apk packages, the rustup tarballs, the v8 archive) with
# no compilation step emulation could corrupt, and the build-time smoke test
# passed. But the x86 leg's first real `yah qed run` is still its first real
# proof — treat it as unverified until then.
#
# The published arm64 image was pulled back from cr.yah.dev unauthenticated and
# its baked build-mesofact.sh re-hashed to
#   sha256 a0f5c11dd2bb22d2d12d26ffe51e14a88bcd39f8ef5d36a1e9f481b1213d9ac9
# equal to the working-tree file beside this Dockerfile. Stated as a HASH on
# purpose: rusty-v8-musl-amd64.toml records how a prose "byte-identical
# (verified)" claim against a mutable working-tree file silently expired and
# cost a whole publish cycle.
#
# To publish a new one, paste the returned `:tag@sha256` into
# `.yah/qed/mesofact-musl.toml`'s `image =` line. A bare catalog name would resolve through
# velveteen_exec::default_image::catalog_image to `ghcr.io/yah-ai/<name>:latest`
# and silently pull the wrong registry (R590-B5) — the pipeline must spell the
# FULL digest-pinned ref, and app/yah/cli/tests/camp_qed_image_pins.rs fails the
# build if it does not.
#
# ⚠ PRE-PULL IS REQUIRED ON THE WORKER. kamaji does not auto-pull; a cold node
# refuses the deploy in ~55ms with BackendRefused "image not found in
# containerd — pre-pull required". Namespace `yah`:
#   sudo ctr -n yah images pull cr.yah.dev/mesofact-musl-builder:<tag>@sha256:<d>
#
# ⚠ AND THE PRE-PULL DOES NOT FIT TODAY ON THE arm WORKERS. Measured 2026-08-11
# over ssh on us-west-011/013/014: each has a 2.5 GB rootfs with 0.7-1.3 GB free
# and a 234 GB NVMe at /data with 214 GB free, and containerd declares no `root`,
# so its store sits on the small rootfs. This image is ~2.5 GB unpacked and a
# mesofact build needs another ~3.4 GB of target dir. Relocating containerd to
# /data is one line per box, but it is a live-node filesystem change on a
# dev-raft voter — operator's call. Tracked on R560-T8.
#
# Catalog spec: oss/qed/crates/qed/images/catalog.toml + W148.

# alpine:3.22 by index digest — the same bytes on both arches.
FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

# Matches oss/mesofact/rust-toolchain.toml. Baking the channel here means the
# offloaded build never has to reach static.rust-lang.org: rustup resolves the
# tree's rust-toolchain.toml pin against a toolchain that is already installed.
ARG RUST_CHANNEL=1.97.0

# rusty_v8 version the baked archive was built for. Must equal the `v8` crate
# version in oss/mesofact/Cargo.lock (149.4.0 as of 2026-08-11) — a mismatch is
# silent ABI skew, not a build error.
ARG RUSTY_V8_VERSION=v149.4.0

# sha256 of the two published archives. The workload.toml that owns them
# (.yah/services/yah-cloud/components/rusty-v8-musl/) records BLAKE3, which
# busybox cannot check; these are the SAME bytes, re-hashed with sha256 so the
# pin is verifiable with what Alpine ships. Confirmed 2026-08-11 by downloading
# both and reproducing the recorded BLAKE3 (x86_64 567e8f9c…, aarch64 b0044840…)
# alongside the sha256 below.
ARG RUSTY_V8_SHA256_X86_64=7e184718863eb3b980f27c030c304fafdee3c657dd48e7651ce1bdb35b8e1d69
ARG RUSTY_V8_SHA256_AARCH64=1b0a1bec9ad9bd943ed43b46940524602a0e6b8b48880968ddc27e27e719882a

# libgcc     — required by the rustup musl host toolchain itself (see header).
# libstdc++  — V8's C++ runtime; the archive is use_custom_libcxx=false, so
#              `-lstdc++` must find Alpine's GNU libstdc++. libstdc++-dev
#              carries the per-arch bits/c++config.h that build-base omits.
# libatomic  — aarch64 __atomic_* from V8; harmless on x86_64.
# cmake/perl — aws-lc-sys builds through cmake. NOT speculative: aws-lc-rs is
#              rustls' default provider in mesofact's graph, and a real build
#              (2026-08-11) compiled aws-lc-sys, cmake and ring.
# clang-dev  — libclang, for aws-lc-sys' bindgen path. musl is one of the
#              targets it has no pregenerated bindings for, so this is required,
#              not defensive: `clang-sys` and `bindgen` both appear in the same
#              real build. `llvm-dev` is deliberately NOT installed — measured
#              2026-08-11, `clang-dev` alone symlinks /usr/lib/libclang.so, and
#              llvm-dev is several hundred MB this image would carry to a Pi for
#              nothing.
RUN apk add --no-cache \
        bash coreutils findutils grep tar xz gzip curl git ca-certificates \
        build-base musl-dev linux-headers \
        libgcc libstdc++ libstdc++-dev libatomic \
        cmake make perl pkgconf \
        clang clang-dev \
        python3 \
        openssl-dev zlib-dev

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

# The prebuilt musl librusty_v8 (R546/R546-B11). Baked rather than fetched per
# run: it is ~30 MB compressed / ~152 MB unpacked and does not change within a
# rusty_v8 version, so a per-run fetch would buy nothing and add a network
# dependency to every fleet build. The `.a` is what makes this image large; that
# is the trade, and it is the same one rusty-v8-musl-builder makes with its
# toolchain.
RUN set -eux; \
    arch="$(uname -m)"; \
    case "$arch" in \
      x86_64)  triple=x86_64-unknown-linux-musl;  want="$RUSTY_V8_SHA256_X86_64" ;; \
      aarch64) triple=aarch64-unknown-linux-musl; want="$RUSTY_V8_SHA256_AARCH64" ;; \
      *) echo "mesofact-musl-builder: unsupported arch $arch" >&2; exit 1 ;; \
    esac; \
    mkdir -p /opt/rusty-v8; \
    url="https://cdn.yah.dev/yah-cloud/rusty-v8/${RUSTY_V8_VERSION}/rusty-v8-${triple}.tar.gz"; \
    curl -fsSL "$url" -o /tmp/rusty-v8.tar.gz; \
    got="$(sha256sum /tmp/rusty-v8.tar.gz | cut -d' ' -f1)"; \
    [ "$got" = "$want" ] || { echo "mesofact-musl-builder: $url sha256 $got != $want" >&2; exit 1; }; \
    tar -xzf /tmp/rusty-v8.tar.gz -C /opt/rusty-v8; \
    rm -f /tmp/rusty-v8.tar.gz; \
    test -f "/opt/rusty-v8/librusty_v8_simdutf_release_${triple}.a"; \
    test -f "/opt/rusty-v8/src_binding_simdutf_release_${triple}.rs"; \
    printf '%s\n' "$triple" > /opt/rusty-v8/TRIPLE; \
    ln -sf "librusty_v8_simdutf_release_${triple}.a"  /opt/rusty-v8/librusty_v8.a; \
    ln -sf "src_binding_simdutf_release_${triple}.rs" /opt/rusty-v8/src_binding.rs; \
    ls -la /opt/rusty-v8

# The R546 CONSUMER CONTRACT, baked so a caller cannot forget half of it.
#
# The two paths point at the arch-independent SYMLINKS created above, not at the
# arch-suffixed real names — a Dockerfile `ENV` cannot shell out, so an
# arch-dependent value here would have to be patched by a later `RUN`, which
# does not update the environment the container actually starts with. The
# symlink moves the arch-dependence into the filesystem, where the earlier layer
# already knew the answer.
#
# The `_simdutf_` infix behind those symlinks is load-bearing: deno_core 0.404.0
# hard-enables `v8/simdutf`, cargo features are additive, and pairing a plain
# binding with a simdutf archive is silent ABI skew. That mismatch is what
# R546-T4 shipped.
#
# RUSTFLAGS carries FIVE link args and every one of them was earned:
#
#   -Wl,-Bstatic
#             MEASURED 2026-08-11, and the subtlest of the set. rustc emits
#             `-static -no-pie` for a musl target but ALSO emits `-Wl,-Bdynamic`
#             before the trailing user link args, so the three `-l` flags below
#             were resolving to `libstdc++.so.6` / `libatomic.so.1`. That
#             produced a binary that was dynamic despite `-static`, and — because
#             ld picks the emulation's default interpreter once a dynamic section
#             exists — one requesting `/lib/ld-linux-aarch64.so.1`, the GLIBC
#             loader, on a musl system. It failed to start inside its own builder
#             image with `cannot execute: required file not found`, which reads
#             like a missing binary and is actually a missing interpreter.
#             `-Bstatic` re-pins the search to the `.a` archives (libstdc++.a,
#             libatomic.a and libgcc.a are all present via build-base +
#             libstdc++-dev). Nothing dynamic follows it, so it does not need a
#             matching `-Bdynamic`.
#   -Wl,--start-group … -lc … -Wl,--end-group
#             The second half of that same fix, and it does not become visible
#             until the first half works. Static archives resolve in ORDER, and
#             rustc puts these link args at the very END — after its own `-lc`.
#             So `libstdc++.a`'s own libc references had nothing left to bind
#             against: the aarch64 link failed on 110 undefined symbols across 27
#             names, every one of them libc (wmemcpy, wcscoll, wcsxfrm, strxfrm,
#             strftime, wcsftime, pthread_cond_wait, get_nprocs, …). Re-listing
#             `-lc` inside a group makes the loop between libstdc++, libgcc and
#             libc resolve in one pass, whatever order the members are pulled in.
#             Do not "simplify" this to a bare trailing `-lc` — the group is what
#             makes it order-independent, and the failure it prevents costs a
#             ~30-minute rebuild to observe.
#             The R546 workload.toml anticipated exactly this fork — "runtime
#             image must carry libstdc++ + libatomic (or link them static)" —
#             and static is the correct arm here: install.sh's alpine path wants
#             a self-contained binary, and a bare Alpine has neither .so.
#   -lstdc++  V8's C++ runtime. The archive is use_custom_libcxx=false, so this
#             must resolve to the same GNU libstdc++ it was built against —
#             which is Alpine's, which is why this image is Alpine.
#   -latomic  aarch64's __atomic_*, from V8.
#   -lgcc     __clear_cache. MEASURED 2026-08-11: without it the aarch64 link of
#             `mesofact` fails with
#               libv8…rlib(cpu-arm64.o): undefined reference to symbol
#               '__clear_cache@@GCC_3.0'
#               /usr/lib/libgcc_s.so.1: error adding symbols: DSO missing from
#               command line
#             This does NOT show up in R546's verify-consumer.sh, and the
#             difference is worth knowing: that script builds with no --target,
#             so it produces a DYNAMIC musl binary (it reports an
#             `/lib/ld-musl-aarch64.so.1` interpreter) and picks libgcc_s up
#             implicitly. This build passes --target, which turns on crt-static,
#             and `-nodefaultlibs` then means nothing puts libgcc on the line.
#             Static is the point — install.sh's alpine path wants one file —
#             so the fix is to name libgcc, not to drop --target.
#   -z stacksize=8388608
#             R330-S26's musl pthread-stack backstop, copied from the camp's
#             .cargo/config.toml because the env var below outranks that table
#             (see the block after this one). HONEST MEASUREMENT: Alpine's ld
#             does not implement it — every link prints
#             `ld: warning: -z stacksize=8388608 ignored`. It is kept anyway so
#             this image asks for exactly what a local musl build asks for, and
#             so nobody re-derives the flag later believing it is missing. The
#             per-spawn-site `.stack_size()` calls are the real fix, exactly as
#             that config file already says.
#
# The first two exist because v8's build.rs emits NEITHER on the
# prebuilt-archive path.
#
# ⚠ THIS ENV DOES NOT REACH BUILD SCRIPTS OR PROC MACROS. Corrected 2026-09-03
# (R556-F6) — this paragraph previously said the opposite, in as many words:
# "Setting all of it as ENV means it also applies to build scripts and proc
# macros, which here compile for the same musl host triple — that is correct,
# not incidental." That was never true and cost a fleet build to disprove.
#
# Cargo applies RUSTFLAGS to TARGET units only once `--target` is passed, and it
# does so EVEN WHEN the host triple equals the target triple — which is exactly
# the case here, and exactly why the old sentence read as reasonable. Host units
# (build scripts, proc macros) are built without these flags. `target-applies-to-
# host` / `[host] rustflags` exist to change that and are nightly-gated, so there
# is no stable knob to reach for. `build-mesofact.sh` always passes `--target`
# and its header defends that at length, so this is not incidental either.
#
# HOW IT SURFACED, so the next person recognises it in one look. MFT-R823 gave
# `mesofact-ssr` a build.rs taking deno_core + four deno extension crates as
# BUILD-dependencies. The build-script binary therefore linked librusty_v8.a as a
# host unit, never saw `-lstdc++`, and died with ~30 undefined references to
# libstdc++ symbols (`std::__throw_length_error`) out of V8 objects — while every
# TARGET crate in the same run, on this same image, linked fine. Target-vs-host
# was the only variable. MFT-R823's fix was to remove V8 from the build-script
# closure entirely (`cargo tree -p mesofact-ssr -e build` is now just serde_json)
# rather than to patch link flags into the host side, on the grounds that a
# link-flag patch has to be independently right on darwin, debian-glibc and
# alpine-musl while only alpine-musl makes being wrong visible.
#
# So: A BUILD SCRIPT IN THIS TREE MUST NOT NEED LINK FLAGS. If one ever does,
# that is the constraint to design around, not a flag to add here.
#
# NOTE for any consumer: a workspace `build.rustflags` in .cargo/config.toml
# would be OVERRIDDEN by this env var. oss/mesofact has no .cargo/config.toml
# today (checked 2026-08-11); if one ever appears carrying rustflags, this ENV
# has to merge with it rather than shadow it.
ENV RUSTY_V8_ARCHIVE=/opt/rusty-v8/librusty_v8.a \
    RUSTY_V8_SRC_BINDING_PATH=/opt/rusty-v8/src_binding.rs \
    RUSTFLAGS="-C link-arg=-Wl,-Bstatic -C link-arg=-Wl,--start-group -C link-arg=-lstdc++ -C link-arg=-latomic -C link-arg=-lgcc -C link-arg=-lc -C link-arg=-Wl,--end-group -C link-arg=-Wl,-z,stacksize=8388608"

# ── NEUTRALISING THE CAMP'S ROOT .cargo/config.toml ─────────────────────────
# MEASURED 2026-08-11, not anticipated: the first real build inside this image
# died at `[1/4]` with
#   error: could not execute process `cargo-orphan-gc … rustc -vV` (never executed)
#
# oss/mesofact is an independent workspace but it is NESTED under the camp root,
# and cargo walks CWD upward for config — so a build with the camp mounted at
# /work picks up /work/.cargo/config.toml. Three of its keys are wrong inside
# this image, and each fails differently:
#
#  1. `build.rustc-wrapper = "cargo-orphan-gc"` (R748) — that binary is a yah
#     workspace tool, not installed here. This is the one that actually fired.
#     An EMPTY RUSTC_WRAPPER is cargo's documented "no wrapper", which is why
#     this is a disable and not a redirect.
#  2. `[target.<arch>-unknown-linux-musl] linker = "<arch>-linux-musl-gcc"`
#     (R330-S26) — that is a musl-CROSS linker name. This image is musl-NATIVE,
#     where the linker is plain `cc`, so the config's name does not exist and
#     the link would fail after a full compile. Both triples are pinned below;
#     the one that does not match the image's arch is inert.
#  3. `[target.<…>-musl] rustflags = ["-C","link-arg=-Wl,-z,stacksize=8388608"]`
#     — cargo picks exactly ONE rustflags source, and the RUSTFLAGS env var
#     above outranks any config table. Setting RUSTFLAGS here would therefore
#     have SILENTLY DROPPED that flag. It is folded into the RUSTFLAGS line
#     above instead, so this image asks for exactly what a local musl build
#     asks for — keep the two in sync. (Alpine's ld then ignores it and says so;
#     see the RUSTFLAGS block. That is a fact about the flag, not about this
#     image, and it is the reason the flag is a backstop rather than the fix.)
#
# None of this is mesofact-specific — any qed builder image that mounts the camp
# root inherits the same three. Left as ENV rather than buried in the build
# script so an interactive `docker run … bash` debugging session behaves the
# same way the pipeline does.
ENV RUSTC_WRAPPER="" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=cc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=cc

COPY build-mesofact.sh /usr/local/bin/build-mesofact.sh
RUN chmod +x /usr/local/bin/build-mesofact.sh

# Fail at image-build time rather than 20 minutes into a fleet build.
RUN set -eux; \
    triple="$(cat /opt/rusty-v8/TRIPLE)"; \
    bash --version >/dev/null; \
    rustc -vV | grep -q "host: ${triple}"; \
    cargo --version >/dev/null; \
    cmake --version >/dev/null; \
    cc --version >/dev/null; \
    test -f "/opt/rusty-v8/librusty_v8_simdutf_release_${triple}.a"; \
    test -z "${V8_FROM_SOURCE:-}${GN:-}${NINJA:-}${CLANG_BASE_PATH:-}"

# The pipeline packs its whole invocation into ONE argv string and relies on the
# entrypoint being `bash -c` — a multi-element argv would drop everything past
# [0]. Same constraint as rusty-v8-musl-builder and whisper-quantize.toml.
ENTRYPOINT ["bash", "-c"]
