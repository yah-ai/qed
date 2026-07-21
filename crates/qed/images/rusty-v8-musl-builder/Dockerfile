# syntax=docker/dockerfile:1.7
#
# rusty-v8-musl-builder — the *expensive-leg* builder image for the
# `rusty-v8-musl` qed transform recipe (R546).
#
# WHY ALPINE (this REPLACED the earlier debian/rust-slim image): denoland/rusty_v8
# publishes gnu-only prebuilt static libs — every `*-unknown-linux-musl` release
# asset 404s. The only way to get a musl `librusty_v8.a` is to build V8 from
# source, and the clean host for that is Alpine itself: its native musl toolchain
# (apk gn / clang22 / rust 1.96) builds V8 for musl with NO glibc->musl cross and
# NO depot_tools download. This is the Alpine `community/deno` APKBUILD blueprint
# ported to a toolbox image. (Proven on aarch64 2026-06-20: 145 MB genuine-musl
# librusty_v8.a that links + runs JS — R546-T3.)
#
# TOOLBOX image: it stages the toolchain + the build script only. The source
# clone + patches + from-source build are in build-v8.sh, invoked once per
# (rusty_v8 version x target) behind the W212 derivation lock.
#
# NATIVE per-arch (no cross): build + push BOTH platforms so each native build
# finds a matching manifest. The recipe / CI run the matching --platform:
#   docker buildx build --platform linux/amd64,linux/arm64 --push \
#     -t ghcr.io/yah-ai/rusty-v8-musl-builder:v149.4.0 \
#     -f oss/qed/crates/qed/images/rusty-v8-musl-builder/Dockerfile \
#     oss/qed/crates/qed/images/rusty-v8-musl-builder
# Auth: yah keys get github-pat | docker login ghcr.io -u 1e1f --password-stdin
# Then paste the :tag@sha256 into the recipe `image =` line (loader hard-rejects
# an unpinned image — RecipeError::ImageNotPinned).
#
# Catalog spec: oss/qed/crates/qed/images/catalog.toml + W148.

FROM alpine:edge

# Versioned llvm/clang + system libc++ + rtlib, system gn/ninja(samurai), Alpine
# musl rust 1.96 std, and the link-time libs V8 needs (libstdc++ for the C++
# runtime, libatomic for aarch64 atomics). libstdc++-dev carries the per-arch
# bits/c++config.h that build-base alone omits on arm64.
RUN apk add --no-cache \
        gn samurai \
        clang22 clang22-dev clang22-rtlib lld llvm22-dev \
        libc++ libc++-dev libc++-static libstdc++-dev libatomic \
        rust cargo \
        python3 git curl \
        glib-dev linux-headers build-base bash grep tar

# V8's gn build hardcodes a clang-23 resource dir + an unsuffixed builtins path
# under a gnu-triple dir; bridge to Alpine's real llvm22 layout. Arch-specific
# links use the IMAGE build arch (uname -m), so each platform variant is correct.
RUN ln -sf /usr/bin/samu /usr/bin/ninja \
 && ln -sf /usr/lib/llvm22/lib/clang/22 /usr/lib/llvm22/lib/clang/23 \
 && RT=/usr/lib/llvm22/lib/clang/22/lib && A="$(uname -m)" \
 && ln -sf "$A-alpine-linux-musl" "$RT/$A-unknown-linux-gnu" \
 && ln -sf "libclang_rt.builtins-$A.a" "$RT/$A-alpine-linux-musl/libclang_rt.builtins.a"

ENV V8_FROM_SOURCE=1 \
    GN=/usr/bin/gn \
    NINJA=/usr/bin/ninja \
    CLANG_BASE_PATH=/usr/lib/llvm22 \
    RUSTC_BOOTSTRAP=1

COPY build-v8.sh /usr/local/bin/build-v8.sh
RUN chmod +x /usr/local/bin/build-v8.sh

# Smoke-test the toolchain so a broken image fails at build time, not 40 min
# into a V8 compile. (Alpine's gn is a real standalone binary, unlike the
# depot_tools launcher in the old image.)
RUN bash --version >/dev/null && gn --version && ninja --version \
 && clang-22 --version >/dev/null && rustc --version && cargo --version >/dev/null \
 && python3 --version && test -e /usr/lib/llvm22/lib/clang/23

# The recipe packs its whole invocation into one argv string and relies on the
# entrypoint being `bash -c` (a multi-element argv would drop everything past
# [0]) — same constraint as whisper-quantize.toml.
ENTRYPOINT ["bash", "-c"]
