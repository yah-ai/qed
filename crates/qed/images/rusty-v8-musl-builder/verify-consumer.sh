#!/bin/sh
# verify-consumer.sh — prove a built rusty-v8-musl tarball actually satisfies the
# R546 CONSUMER CONTRACT: a real deno_core binary must LINK and RUN against it.
#
#   verify-consumer.sh <unpacked-dir> <target-triple>
#
# Run inside the matching-arch builder image, e.g.:
#   tar xzf rusty-v8-aarch64-unknown-linux-musl.tar.gz -C /tmp/u
#   docker run --rm --platform linux/arm64 -v /tmp/u:/u \
#     ghcr.io/yah-ai/rusty-v8-musl-builder:v149.4.0-arm64 \
#     '/work/verify-consumer.sh /u aarch64-unknown-linux-musl'
#
# WHY THIS EXISTS (R546-T4). build-v8.sh's own checks all passed on an archive
# that no consumer could use: `cargo build` of the v8 *lib* succeeds even when the
# archive is missing simdutf__* symbols, because undefined externs only surface
# when a BINARY is linked. Two arch tarballs were built, published to the CDN,
# hash-pinned and locked before anyone tried to link deno_core against them. This
# script closes that gap — it is the only check that exercises the actual failure
# mode, so run it before publishing, not after.
set -eu

UNPACKED="${1:?usage: verify-consumer.sh <unpacked-dir> <target-triple>}"
TARGET="${2:?usage: verify-consumer.sh <unpacked-dir> <target-triple>}"

# Match build-v8.sh's naming: release / simdutf / no-ptrcomp.
ARCHIVE="$UNPACKED/librusty_v8_simdutf_release_$TARGET.a"
BINDING="$UNPACKED/src_binding_simdutf_release_$TARGET.rs"
[ -f "$ARCHIVE" ] || { echo "verify-consumer: no archive at $ARCHIVE" >&2; exit 1; }
[ -f "$BINDING" ] || { echo "verify-consumer: no binding at $BINDING" >&2; exit 1; }

# The builder image bakes V8_FROM_SOURCE=1 + GN/NINJA/CLANG_BASE_PATH as ENV.
# Inheriting them makes v8's build.rs IGNORE RUSTY_V8_ARCHIVE and shell out to
# gn/ninja — it then fails on a missing icudtl.dat and says nothing about the
# archive, which reads as an archive problem and burns a debug cycle (R546-T9).
# Any consumer basing on this image MUST clear them; that is part of the contract.
unset V8_FROM_SOURCE GN NINJA CLANG_BASE_PATH GN_ARGS 2>/dev/null || true

export RUSTY_V8_ARCHIVE="$ARCHIVE"
export RUSTY_V8_SRC_BINDING_PATH="$BINDING"

# THE LINK ENVIRONMENT BELOW IS THE CONSUMER CONTRACT, and it is copied verbatim
# from mesofact-musl-builder/Dockerfile:307 — the real consumer of this archive
# on the fleet. Stating it any other way here is what let it drift (R555-B13).
#
# What used to be here was `RUSTFLAGS="-C link-arg=-lstdc++ -C link-arg=-latomic"`,
# and it does not work. Measured 2026-09-10 against the published v149.4.0
# archives, both arches:
#   * aarch64 does not link at all — `undefined reference to symbol
#     '__clear_cache@@GCC_3.0'` out of libv8*.rlib(cpu-arm64.o), then
#     `/usr/lib/libgcc_s.so.1: error adding symbols: DSO missing from command
#     line`. -lgcc is absent.
#   * x86_64 links, and THIS SCRIPT PASSED on it, because the builder image has
#     libstdc++/libatomic installed. The binary it produced was DYNAMIC — NEEDED
#     libgcc_s.so.1, libstdc++.so.6, libatomic.so.1 — and on a stock alpine:3.22
#     it dies with `Error loading shared library libstdc++.so.6` and a few
#     hundred relocation errors. That is the curl-install tenant environment the
#     musl artifact exists to serve, so a pass here meant nothing.
#
# CXXSTDLIB="" IS LOAD-BEARING. v8's build.rs print_link_flags() emits
# `cargo:rustc-link-lib=dylib=stdc++` whenever the `use_custom_libcxx` feature is
# off — which is the correct feature setting for this archive — and that flag
# lands AFTER rustc's own `-Wl,-Bdynamic`, where the `-Wl,-Bstatic` below cannot
# reach it. build.rs reads CXXSTDLIB first (build.rs:830) and emits NOTHING when
# it is set-but-empty, which leaves the static -lstdc++ inside the group as the
# only one. Without it the -l flags resolve to the .so files and the binary comes
# out dynamic even though rustc also passed -static.
#
# The trailing `-Wl,-z,stacksize=8388608` is a KNOWN no-op on Alpine binutils ld,
# which answers it with `-z stacksize=8388608 ignored` (it spells the option
# `stack-size`). It is kept verbatim anyway, deliberately: this verifier is worth
# something only if it links the way production links, and the flag's real fix is
# the per-spawn-site .stack_size() calls. See mesofact-musl-builder/Dockerfile
# :258-263 and W236's R330-S26 note — do not "clean it up" here.
#
# `-C target-feature=+crt-static` is the one addition to production's string, and
# it is here because THIS image's rustc is not the one production uses. Alpine's
# distro rustc (host x86_64-alpine-linux-musl) defaults crt-static OFF, where a
# rustup musl toolchain — mesofact-musl-builder, and the R555-T8 smoke harness —
# defaults it ON. Without it the binary comes out PIE + dynamic here regardless
# of the link args, so it is a no-op wherever production runs and load-bearing
# here. The link args themselves stay verbatim.
export CXXSTDLIB=""
export RUSTFLAGS="-C target-feature=+crt-static -C link-arg=-Wl,-Bstatic -C link-arg=-Wl,--start-group -C link-arg=-lstdc++ -C link-arg=-latomic -C link-arg=-lgcc -C link-arg=-lc -C link-arg=-Wl,--end-group -C link-arg=-Wl,-z,stacksize=8388608"

# BUILD FOR THE TOOLCHAIN'S OWN HOST TRIPLE, EXPLICITLY — this is still a native
# build, not a cross, and passing --target is what keeps RUSTFLAGS off the BUILD
# SCRIPTS. Measured here 2026-09-10: with --target absent, cargo hands the link
# args above to every build script's own link too, gcc's spec appends `-lgcc_s`
# AFTER them, the trailing -Wl,-Bstatic then makes ld hunt for a libgcc_s.a
# nobody ships, and `libm`'s build script dies with `cannot find -lgcc_s ... have
# you installed the static version of the gcc_s library`. The final binary never
# gets a chance to be wrong.
HOST_TRIPLE="$(rustc -vV | sed -n 's/^host: //p')"
[ -n "$HOST_TRIPLE" ] || { echo "verify-consumer: could not read rustc's host triple" >&2; exit 1; }
echo "verify-consumer: building natively for $HOST_TRIPLE"

W="$(mktemp -d)"
cat > "$W/Cargo.toml" <<'EOF'
[package]
name = "v8consumer"
version = "0.1.0"
edition = "2021"
[dependencies]
deno_core = "=0.404.0"
[[bin]]
name = "v8consumer"
path = "main.rs"
[workspace]
EOF

# The wide string is >96 UTF-16 units (v8's WTF16_SIMD_THRESHOLD) and non-ASCII,
# so wtf16_to_string takes its #[cfg(feature = "simdutf")] branch at RUNTIME as
# well as referencing simdutf__* at link time.
cat > "$W/main.rs" <<'EOF'
use deno_core::{JsRuntime, RuntimeOptions, scope};

fn main() {
    let mut rt = JsRuntime::new(RuntimeOptions::default());
    let arith = rt.execute_script("<r546>", "3 + 4").unwrap();
    let wide = rt
        .execute_script("<r546-wide>", "'\\u00e9\\u4e2d'.repeat(80)")
        .unwrap();

    scope!(scope, rt);
    let arith = deno_core::v8::Local::new(scope, arith);
    let wide = deno_core::v8::Local::new(scope, wide);

    assert_eq!(arith.to_rust_string_lossy(scope), "7");
    let s = wide.to_rust_string_lossy(scope);
    assert_eq!(s.chars().count(), 160);
    println!("verify-consumer: 3+4=7, wide=160 chars — OK");
}
EOF

echo "verify-consumer: ARCHIVE=$RUSTY_V8_ARCHIVE"
echo "verify-consumer: BINDING=$RUSTY_V8_SRC_BINDING_PATH"
echo "verify-consumer: building a real deno_core binary (link is the actual test)"
( cd "$W" && cargo build --release --target "$HOST_TRIPLE" )

BIN="$W/target/$HOST_TRIPLE/release/v8consumer"

# THE CHECK THIS SCRIPT WAS MISSING (R555-B13), and it runs BEFORE the binary
# does. A successful link proves nothing on its own: the contract this file used
# to document linked cleanly on x86_64 and produced a binary with four NEEDED
# entries that no stock Alpine can start. Static linkage IS the property the musl
# artifact exists to provide, and it is exactly the property a weaker link line
# loses silently — so assert it rather than printing it and hoping someone reads
# the output. The fleet consumer asserts the same thing (build-mesofact.sh:142).
NEEDED="$(readelf -d "$BIN" 2>/dev/null | grep -c '(NEEDED)' || true)"
echo "verify-consumer: linked OK — $(wc -c < "$BIN") bytes, $NEEDED NEEDED shared libs"
if [ "$NEEDED" != "0" ]; then
  echo "verify-consumer: FAIL — binary is not static; the archive's link contract was not honoured" >&2
  readelf -d "$BIN" 2>/dev/null | grep '(NEEDED)' >&2
  readelf -l "$BIN" 2>/dev/null | grep -i "interpreter" >&2
  exit 1
fi
# A static ELF has no PT_INTERP at all. If one is somehow present it is a GNU
# loader path on a musl system, and the run below then fails with a bare `not
# found` that reads as a missing binary rather than a missing interpreter.
readelf -l "$BIN" 2>/dev/null | grep -i "interpreter" || true

echo "verify-consumer: running it"
"$BIN"
echo "verify-consumer: PASS"
