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
# v8's build.rs emits neither on the prebuilt path, and musl needs both:
# -lstdc++ because we build use_custom_libcxx=false, -latomic for aarch64 __atomic_*.
export RUSTFLAGS="-C link-arg=-lstdc++ -C link-arg=-latomic"

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
# The Alpine builder's rustc is already musl-native, so no cross --target.
( cd "$W" && cargo build --release )

BIN="$W/target/release/v8consumer"
echo "verify-consumer: linked OK; running it"
"$BIN"

# Belt and braces: the contract also claims genuine musl + the two dylibs.
echo "verify-consumer: interpreter/libs —"
readelf -l "$BIN" 2>/dev/null | grep -i "interpreter" || true
readelf -d "$BIN" 2>/dev/null | grep -iE "libstdc\+\+|libatomic" || true
echo "verify-consumer: PASS"
