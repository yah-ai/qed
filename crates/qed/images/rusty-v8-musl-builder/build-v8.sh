#!/bin/sh
# build-v8.sh — from-source musl build of rusty_v8's librusty_v8.a (R546).
#
# Baked into the rusty-v8-musl-builder image; invoked by BOTH:
#   - the `rusty-v8-musl` qed recipe   : build-v8.sh <target> <out.tar.gz>
#   - .github/workflows (CI proof)     : build-v8.sh <target> <out.tar.gz>
# so the build logic has ONE source of truth.
#
# Builds NATIVELY for the container's arch (no cross): run under
# --platform linux/amd64 for an x86_64 target, linux/arm64 for aarch64.
# The toolchain (apk pkgs + clang-23/rtlib symlinks) is staged by the
# Dockerfile; this script does source clone + patches + gn build + packaging.
#
# Proven 2026-06-20 on aarch64 (native, ~1h49m): emits a 145 MB genuine-musl
# librusty_v8.a that links + runs JS in a musl binary. See R546-T3.
set -eu

TARGET="${1:?usage: build-v8.sh <target-triple> <output.tar.gz>}"
OUT="${2:?usage: build-v8.sh <target-triple> <output.tar.gz>}"
RUSTY_V8_VER="${RUSTY_V8_VER:-149.4.0}"

# target triple -> (arch, denoland gnu binding triple). Native build only.
case "$TARGET" in
  x86_64-*linux-musl)  ARCH=x86_64;  GNU=x86_64-unknown-linux-gnu ;;
  aarch64-*linux-musl) ARCH=aarch64; GNU=aarch64-unknown-linux-gnu ;;
  *) echo "build-v8: unsupported target '$TARGET' (want {x86_64,aarch64}-*-linux-musl)" >&2; exit 1 ;;
esac
HOSTARCH="$(uname -m)"
[ "$HOSTARCH" = "$ARCH" ] || { echo "build-v8: target arch $ARCH != container arch $HOSTARCH — run under the matching --platform" >&2; exit 1; }

CB=/usr/lib/llvm22
RUST_SYSROOT="$(rustc --print sysroot)"
WORK="$(mktemp -d)"; cd "$WORK"

echo "build-v8: cloning rusty_v8 v$RUSTY_V8_VER ($ARCH native musl)"
git clone --depth 1 --recurse-submodules --shallow-submodules \
  -b "v$RUSTY_V8_VER" https://github.com/denoland/rusty_v8 v8src
cd v8src

# ── source patches (Alpine community/deno parity, ported to v$RUSTY_V8_VER) ──
# 1. strip Chromium-bundled-clang-only cflags that stock clang22 rejects
for f in $(grep -rl --include='*.gn' --include='*.gni' \
    -e 'fno-lifetime-dse' -e 'fdiagnostics-show-inlining-chain' \
    -e 'fsanitize-ignore-for-ubsan-feature' -e 'fcomplete-member-pointers' \
    build buildtools v8 third_party 2>/dev/null); do
  sed -i -e '/fno-lifetime-dse/d' -e '/fdiagnostics-show-inlining-chain/d' \
         -e '/fsanitize-ignore-for-ubsan-feature/d' -e '/fcomplete-member-pointers/d' "$f"
done
# 2. gnu target triples -> Alpine musl triples (v8-compiler.patch). The
#    `*-unknown-linux-gnu` forms AND the bare `aarch64-linux-gnu`/arm forms.
sed -i 's/unknown-linux-gnu/alpine-linux-musl/g' \
  build/config/compiler/BUILD.gn build/config/clang/BUILD.gn build/config/rust.gni
sed -i -e 's/aarch64-linux-gnu/aarch64-alpine-linux-musl/g' \
       -e 's/arm-linux-gnueabihf/armv7-alpine-linux-musleabihf/g' \
  build/config/compiler/BUILD.gn build/config/clang/BUILD.gn
# 3. v149 known-triple allowlist gate (rust.gni asserts the abi target is listed)
KTT=build/rust/known-target-triples.txt
for t in x86_64-alpine-linux-musl aarch64-alpine-linux-musl; do
  grep -qx "$t" "$KTT" 2>/dev/null || echo "$t" >> "$KTT"
done
# 4. build.rs: strip maybe_install_sysroot() (it fetches a Debian glibc sysroot
#    on aarch64/arm) + the use_sysroot=true pushes (v8-build.patch parity)
sed -i -e '/maybe_install_sysroot("/d' -e '/gn_args.push("use_sysroot=true"/d' build.rs
# 5. musl lacks <execinfo.h>/backtrace (v8-no-execinfo.patch); only live under
#    DEBUG (is_debug=false here) but guard defensively
ERT=v8/src/codegen/external-reference-table.cc
if [ -f "$ERT" ] && ! grep -q '__GLIBC__' "$ERT"; then
  sed -i 's|^#include <execinfo.h>|#if defined(__GLIBC__)\n#include <execinfo.h>\n#endif|' "$ERT"
  sed -i 's|^#ifdef SYMBOLIZE_FUNCTION|#if defined(SYMBOLIZE_FUNCTION) \&\& defined(__GLIBC__)|' "$ERT"
fi

# ── build (native musl) ──
# temporal off -> V8 enable_rust defaults off -> no glibc-prebuilt bindgen, no
# nightly -Z rustc. RUSTC_BOOTSTRAP unlocks the cargo-level temporal_* deps.
export V8_FROM_SOURCE=1 GN=/usr/bin/gn NINJA="${NINJA:-/usr/bin/ninja}" \
       CLANG_BASE_PATH="$CB" RUSTC_BOOTSTRAP=1
export GN_ARGS="treat_warnings_as_errors=false v8_enable_temporal_support=false \
clang_use_chrome_plugins=false use_custom_libcxx=false use_sysroot=false \
clang_base_path=\"$CB\" use_system_libffi=true is_debug=false symbol_level=0 \
fatal_linker_warnings=false rust_sysroot_absolute=\"$RUST_SYSROOT\""
echo "build-v8: cargo build --release --features simdutf (the ~30-110m expensive leg)"
# --features simdutf is REQUIRED, not optional (R546-T4). deno_core — the only
# consumer this asset exists for — declares `v8 = { features = ["simdutf"],
# default-features = false }`, and cargo features are additive, so NO consumer
# can turn it back off. With the feature on, v8's src/string.rs takes its
# `#[cfg(feature = "simdutf")]` branches and calls extern "C" simdutf__* symbols
# that only exist in an archive built with gn `rusty_v8_enable_simdutf=true`
# (BUILD.gn:22 pulls in //third_party/simdutf). Building without it produced an
# archive with ZERO simdutf__ symbols, against which every deno_core binary fails
# to LINK — undefined simdutf__validate_utf16le / __convert_latin1_to_utf8 / … from
# v8::String::to_rust_string_lossy and deno_core's op_decode. v8's build.rs
# (L316-319) derives the gn arg from CARGO_FEATURE_SIMDUTF, so setting the cargo
# feature here is the single source of truth — do NOT also push the gn arg into
# GN_ARGS above or gn sees it twice.
# build.rs (V8_FROM_SOURCE) runs gn+ninja -> librusty_v8.a, THEN bindgen, THEN
# cargo compiles the v8 lib. libclang-22 mangles nested enums as WriteFlags_*
# instead of the v8_String_WriteFlags_* that the crate's src/string.rs expects
# (see the binding note below), so that final rustc step fails — but the .a is
# already on disk by then. The ARCHIVE'S PRESENCE, not cargo's exit code, is the
# success criterion here; we ship denoland's binding regardless. So don't let
# `set -e` abort on the expected lib-compile failure.
cargo build --release --features simdutf || echo "build-v8: lib compile failed (expected clang22 binding skew); checking for the archive"

# ── locate the archive (build.rs writes the authoritative outputs to gn_out) ──
LIB=$(find target -path '*/gn_out/*' -name 'librusty_v8.a' -print -quit)
[ -n "$LIB" ] || LIB=$(find target -name 'librusty_v8.a' -print -quit)
[ -n "$LIB" ] || { echo "build-v8: librusty_v8.a not produced — real build failure" >&2; exit 1; }
echo "build-v8: archive = $LIB ($(du -h "$LIB" | cut -f1))"

# Fail closed on the R546-T4 defect: an archive built without
# rusty_v8_enable_simdutf links fine at `cargo build` of the v8 lib but breaks
# every downstream deno_core BINARY at link time. That is far too late to catch
# it — a whole publish cycle shipped an unusable artifact this way. Assert the
# symbols are present here, where it costs nothing.
nm --defined-only "$LIB" 2>/dev/null | grep -q 'simdutf__validate_utf16le' || {
  echo "build-v8: archive has no simdutf__ symbols — built without --features simdutf?" >&2
  exit 1
}
echo "build-v8: simdutf symbols present (deno_core link requirement satisfied)"

# ── binding: ship DENOLAND'S, not our clang22-generated one ──
# libclang 22 mangles nested enums differently (WriteFlags_* vs the
# v8_String_WriteFlags_* that rusty_v8's hand-written Rust expects), so our
# generated gen/src_binding.rs is INCOMPATIBLE with the crate lib.rs. Fetch the
# published crate's per-target binding (libc-independent: the gnu binding is
# byte-correct for musl — same arch ABI, same Itanium C++ mangling, matches lib.rs).
echo "build-v8: fetching denoland binding from crates.io v8=$RUSTY_V8_VER"
BF="$(mktemp -d)"; ( cd "$BF" && cargo init --quiet --name bfetch \
  && printf 'v8 = "=%s"\n' "$RUSTY_V8_VER" >> Cargo.toml && cargo fetch --quiet )
CRATE=$(find "${CARGO_HOME:-$HOME/.cargo}/registry/src" -maxdepth 2 -type d -name "v8-$RUSTY_V8_VER" | head -1)
# The binding variant MUST match the feature set the archive was built with —
# denoland publishes a separate src_binding_simdutf_* because the feature changes
# the generated surface. Mixing a plain binding with a simdutf archive is silent
# ABI skew (R546-T4).
BIND="$CRATE/gen/src_binding_simdutf_release_$GNU.rs"
[ -f "$BIND" ] || { echo "build-v8: denoland binding $BIND not found" >&2; exit 1; }
grep -q v8_String_WriteFlags_kNullTerminate "$BIND" || { echo "build-v8: binding sanity check failed" >&2; exit 1; }

# ── deterministic tar -> OUT (consumer sets RUSTY_V8_ARCHIVE + _SRC_BINDING_PATH) ──
# release / simdutf / no-ptrcomp config => `*_simdutf_release_*` names, matching
# the names v8's build.rs would construct for this feature set (prebuilt_features_
# suffix(), build.rs L555-567). The env-var consumer path ignores these names, but
# keeping them honest is what lets a RUSTY_V8_MIRROR-style consumer work at all.
STAGE="$(mktemp -d)"
cp "$LIB"  "$STAGE/librusty_v8_simdutf_release_$TARGET.a"
cp "$BIND" "$STAGE/src_binding_simdutf_release_$TARGET.rs"
# OUT may name a not-yet-existing dir (e.g. the recipe's /tmp/rusty-v8-musl/…);
# the redirect below can't create it, so materialize the parent first.
mkdir -p "$(dirname "$OUT")"
# GNU tar (apk add tar) — the deterministic flags below (--sort/--numeric-owner/
# --mtime) are GNU-only; Alpine's busybox tar applet rejects --sort=name.
tar -c --sort=name --owner=0 --group=0 --numeric-owner --mtime='2000-01-01 UTC' \
    -C "$STAGE" . | gzip --no-name > "$OUT"
echo "build-v8: wrote $OUT"
ls -l "$OUT"
