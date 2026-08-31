#!/bin/bash
# build-mesofact.sh — build the two shipped mesofact binaries for a musl triple
# and emit the release tarball. Baked into `mesofact-musl-builder` (R560-T7) and
# invoked by `.yah/qed/mesofact-musl.toml` (R560-T8).
#
#   build-mesofact.sh <target-triple> <out.tar.gz> [<repo-root>]
#
# repo-root defaults to /work. mesofact is expected at <repo-root>/oss/mesofact.
#
# WHY THE REPO ROOT AND NOT JUST oss/mesofact. mesofact has path deps that ESCAPE
# its own workspace — yah-mesofact-bundle → ../../../yah-base/crates/…, and
# cheers-core / cheers-server → ../../../cheers/crates/… (see oss/mesofact/
# Cross.toml, which bind-mounts the repo root for exactly this reason). Mount
# only oss/mesofact and cargo dies with "failed to find a workspace root".
#
# WHY TWO BINARIES FROM TWO CRATES (W225 §2a). `mesofact` is the prod binary
# built at the `deploy` feature preset; `mesofact-dev` is a SEPARATE crate
# carrying the file watcher + dev S3 surface. Folding dev into prod would put
# `notify` + `s3s` back into the prod dependency closure. They ship in one
# tarball; they are not one binary. This mirrors release.yml's `mesofact-build`
# job leg-for-leg, deliberately — the fleet musl legs and the GHA gnu/darwin legs
# must produce the same tarball shape or install.sh sees two different products.
#
# WHY `--target` EVEN THOUGH THE HOST IS THE TARGET. The image's rustc is
# musl-native, so this is a native build either way. Passing --target explicitly
# (a) puts the output at the deterministic target/<triple>/release path the
# packaging step and release.yml both assume, and (b) FAIL-CLOSES if this script
# is ever run on an image whose arch does not match the requested triple —
# rustc refuses a target whose std it does not have, instead of silently
# producing a binary for the wrong arch.
set -euo pipefail

TARGET="${1:?usage: build-mesofact.sh <target-triple> <out.tar.gz> [<repo-root>]}"
OUT="${2:?usage: build-mesofact.sh <target-triple> <out.tar.gz> [<repo-root>]}"
ROOT="${3:-/work}"

MESOFACT="$ROOT/oss/mesofact"
[ -d "$MESOFACT" ] || { echo "build-mesofact: no mesofact workspace at $MESOFACT" >&2; exit 1; }
[ -d "$ROOT/oss/yah-base" ] || { echo "build-mesofact: missing sibling $ROOT/oss/yah-base (path dep)" >&2; exit 1; }
[ -d "$ROOT/oss/cheers" ]   || { echo "build-mesofact: missing sibling $ROOT/oss/cheers (path dep)" >&2; exit 1; }

# The archive contract is baked as ENV by the image; assert it rather than
# re-deriving it, so a caller that overrode half of it fails here and not 20
# minutes in at the link step.
: "${RUSTY_V8_ARCHIVE:?build-mesofact: RUSTY_V8_ARCHIVE unset — not running in mesofact-musl-builder?}"
: "${RUSTY_V8_SRC_BINDING_PATH:?build-mesofact: RUSTY_V8_SRC_BINDING_PATH unset}"
[ -f "$RUSTY_V8_ARCHIVE" ] || { echo "build-mesofact: no archive at $RUSTY_V8_ARCHIVE" >&2; exit 1; }
[ -f "$RUSTY_V8_SRC_BINDING_PATH" ] || { echo "build-mesofact: no binding at $RUSTY_V8_SRC_BINDING_PATH" >&2; exit 1; }

# The archive is per-arch. Refuse a triple it was not built for rather than
# letting the linker discover it.
BAKED="$(cat /opt/rusty-v8/TRIPLE 2>/dev/null || echo unknown)"
[ "$BAKED" = "$TARGET" ] || {
  echo "build-mesofact: image bakes the $BAKED archive but was asked for $TARGET" >&2
  echo "build-mesofact: use the arch-matched image variant (this is not a cross-build image)" >&2
  exit 1
}

# Belt and braces on the R546-T9 trap: if any of these leak in from a caller's
# environment, v8's build.rs ignores RUSTY_V8_ARCHIVE and shells out to gn/ninja,
# then fails on a missing icudtl.dat while saying nothing about the archive.
unset V8_FROM_SOURCE GN NINJA CLANG_BASE_PATH GN_ARGS 2>/dev/null || true

echo "build-mesofact: target=$TARGET"
echo "build-mesofact: archive=$RUSTY_V8_ARCHIVE"
echo "build-mesofact: binding=$RUSTY_V8_SRC_BINDING_PATH"
echo "build-mesofact: rustflags=${RUSTFLAGS:-<unset>}"
rustc -vV

cd "$MESOFACT"

# Leg A — the PROD binary. `--features deploy` is the shipped preset, shared with
# Dockerfile.ssr-runtime; never hand-list the feature set. `--bin mesofact` is
# pinned so a future extra [[bin]] in the facade cannot silently join the release.
echo "build-mesofact: [1/4] mesofact (--features deploy)"
cargo build --release --locked --target "$TARGET" -p mesofact --bin mesofact --features deploy

# Leg B — the DEV binary, from its own crate, at DEFAULT features (= ssr). It
# must NOT be built with `deploy`, which is the prod facade's preset.
echo "build-mesofact: [2/4] mesofact-dev (default features)"
cargo build --release --locked --target "$TARGET" -p mesofact-dev --bin mesofact-dev

# W225 §2 boundary, enforced here rather than asserted in a doc — the same check
# release.yml's gnu/darwin legs run, so the musl legs cannot drift off it.
echo "build-mesofact: [3/4] prod closure carries no dev affordances (W225 §2)"
cargo tree -p mesofact --features deploy -e normal --target "$TARGET" > /tmp/deploy-tree.txt
if grep -nE 'mesofact-dev|notify|s3s' /tmp/deploy-tree.txt; then
  echo "build-mesofact: mesofact --features deploy links dev affordances (W225 §2)" >&2
  exit 1
fi
echo "build-mesofact: prod closure clean — no mesofact-dev / notify / s3s"

# Honour CARGO_TARGET_DIR so the caller can mount the source READ-ONLY and send
# build output to a scratch volume — which is what you want on a shared checkout,
# and what keeps a fleet build from writing into the mounted camp tree.
SRC="${CARGO_TARGET_DIR:-$MESOFACT/target}/$TARGET/release"
[ -x "$SRC/mesofact" ]     || { echo "build-mesofact: no $SRC/mesofact" >&2; exit 1; }
[ -x "$SRC/mesofact-dev" ] || { echo "build-mesofact: no $SRC/mesofact-dev" >&2; exit 1; }

# THE CHECK THAT ACTUALLY MATTERS (R546-T4's lesson, one layer up). A green
# `cargo build` is not evidence the V8 archive is usable — undefined externs bind
# at LINK time and V8 initialises at RUN time. Both binaries statically link V8,
# so executing them is the cheap end-to-end proof that this image's archive,
# binding and link args agree. If this fails while the build passed, suspect the
# binding/archive variant pair before anything else.
#
# RUNNING IS NOT SUFFICIENT ON ITS OWN, which is why the static assertion comes
# first. A DYNAMIC musl binary runs perfectly inside this image — libstdc++.so.6
# and libatomic.so.1 are installed here — and then dies on the bare Alpine box
# that `curl … | sh` puts it on. That is the same shape of false green R546 kept
# hitting: the check passed in the one environment where the defect is invisible.
# So assert the artifact property (no interpreter, no NEEDED) in the place that
# can still see it, then run it.
echo "build-mesofact: [4/4] the built binaries are static, and run"
assert_static() {
  bin="$1"
  if readelf -l "$bin" | grep -q "program interpreter"; then
    echo "build-mesofact: $bin requests a program interpreter — it is not static" >&2
    readelf -l "$bin" | grep -A1 "program interpreter" >&2
    echo "build-mesofact: check that RUSTFLAGS still carries -Wl,-Bstatic before the -l flags" >&2
    exit 1
  fi
  if readelf -d "$bin" 2>/dev/null | grep -q "(NEEDED)"; then
    echo "build-mesofact: $bin has NEEDED shared libraries — it is not static" >&2
    readelf -d "$bin" | grep "(NEEDED)" >&2
    exit 1
  fi
  echo "build-mesofact: $(basename "$bin") is statically linked"
}
assert_static "$SRC/mesofact"
assert_static "$SRC/mesofact-dev"
"$SRC/mesofact" --version
"$SRC/mesofact-dev" --version

# Same tarball shape as release.yml's "Package archive" step: one top-level
# stage dir holding both binaries. install.sh strips components, so the stage
# dir's name does not affect installation — it is taken from the output filename
# so the fleet legs and the GHA legs stay legible side by side.
STAGE_NAME="$(basename "$OUT")"
STAGE_NAME="${STAGE_NAME%.tar.gz}"
WORK="$(mktemp -d)"
STAGE="$WORK/$STAGE_NAME"
mkdir -p "$STAGE" "$(dirname "$OUT")"
cp "$SRC/mesofact" "$SRC/mesofact-dev" "$STAGE/"

# Deterministic tar: fixed mtime/uid/gid/order so two builds of identical bytes
# produce an identical archive, which is what makes the W212 derivation lock and
# the content-addressed landing meaningful.
tar --sort=name --owner=0 --group=0 --numeric-owner --mtime='@0' \
    -czf "$OUT" -C "$WORK" "$STAGE_NAME"
rm -rf "$WORK"

ls -la "$OUT"
sha256sum "$OUT"
echo "build-mesofact: PASS"
