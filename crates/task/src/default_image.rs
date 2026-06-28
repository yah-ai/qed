//! Default container image for `ForgeCommand::Subprocess { image: None }`.
//!
//! ## Supply-chain policy
//!
//! The default image is `yah-rust-bun` (heir of the legacy `forge-minimal`),
//! built from `crates/yah/qed/images/yah-rust-bun/Dockerfile` and pushed to
//! `ghcr.io/yah-ai/yah-rust-bun` by the release pipeline (R381-T7 owns the
//! GHA matrix that fans out to every catalog image; T7 is open).
//!
//! Each catalog image is signed with Sigstore keyless OIDC (cosign).
//! Yubaba's R090-F4 environment validator verifies the signature before
//! pulling. The trust chain is identical to what `forge-minimal` had.
//!
//! ## Compile-time digest injection
//!
//! Each catalog image has its own `YAH_<NAME>_DIGEST` env var, read via
//! `option_env!` at build time. Empty → no digest pinning, falls back to
//! `:latest`. Production releases inject all of them:
//!
//! ```sh
//! YAH_BASE_DIGEST=sha256:…       \
//! YAH_RUST_DIGEST=sha256:…       \
//! YAH_RUST_BUN_DIGEST=sha256:…   \
//! YAH_WARDEN_DIGEST=sha256:…     \
//!   cargo build --release …
//! ```
//!
//! `default_forge_image()` honours `YAH_RUST_BUN_DIGEST` because the default
//! image is `yah-rust-bun`. Other catalog entries route through
//! [`catalog_image`].
//!
//! ## Signature verification
//!
//! ```sh
//! cosign verify ghcr.io/yah-ai/yah-rust-bun@sha256:<hash> \
//!   --certificate-oidc-issuer https://token.actions.githubusercontent.com \
//!   --certificate-identity-regexp \
//!     '^https://github\.com/yah-ai/yah/\.github/workflows/release\.yml@'
//! ```

use workload_spec::ImageRef;

/// Registry hostname for all yah catalog images.
pub const YAH_IMAGE_REGISTRY: &str = "ghcr.io";

/// Repository owner for all yah catalog images. The full repository is
/// `<owner>/<name>` (e.g. `yah-ai/yah-rust-bun`).
pub const YAH_IMAGE_OWNER: &str = "yah-ai";

/// Default tag when no digest is pinned.
pub const YAH_IMAGE_TAG: &str = "latest";

// ─── Per-image digest envs ────────────────────────────────────────────────────
//
// Injected at compile time by the release pipeline (R381-T7). `None` on
// local dev builds — yubaba falls back to `:latest`.

/// SHA-256 digest of `ghcr.io/yah-ai/yah-base` for the current release.
pub const YAH_BASE_DIGEST: Option<&str> = option_env!("YAH_BASE_DIGEST");

/// SHA-256 digest of `ghcr.io/yah-ai/yah-rust` for the current release.
pub const YAH_RUST_DIGEST: Option<&str> = option_env!("YAH_RUST_DIGEST");

/// SHA-256 digest of `ghcr.io/yah-ai/yah-rust-bun` for the current release.
/// Read by [`default_forge_image`] because `yah-rust-bun` is the default
/// when a subprocess step doesn't pick its own image.
pub const YAH_RUST_BUN_DIGEST: Option<&str> = option_env!("YAH_RUST_BUN_DIGEST");

/// SHA-256 digest of `ghcr.io/yah-ai/yah-yubaba` for the current release.
/// Read by camp's pond bring-up so the yubaba-container is pinned to the
/// release's signed image (W154, R408-T1).
pub const YAH_WARDEN_DIGEST: Option<&str> = option_env!("YAH_WARDEN_DIGEST");

/// SHA-256 digest of `ghcr.io/yah-ai/yah-miniflare` for the current release.
/// Read by camp's pond bring-up so the miniflare-container is pinned to the
/// release's signed image (R455-F1, W180).
pub const YAH_MINIFLARE_DIGEST: Option<&str> = option_env!("YAH_MINIFLARE_DIGEST");

/// Default repository (`<owner>/<name>`) for catalog entries — useful for
/// callers that don't go through [`catalog_image`] (e.g. doc strings).
pub const DEFAULT_IMAGE_REPOSITORY: &str = "yah-ai/yah-rust-bun";

/// Returns the [`ImageRef`] used when `ForgeCommand::Subprocess { image: None }`.
///
/// Resolves to `ghcr.io/yah-ai/yah-rust-bun:latest` on dev builds and to
/// `ghcr.io/yah-ai/yah-rust-bun@sha256:…` on release builds where
/// `YAH_RUST_BUN_DIGEST` was injected. Callers needing a different image
/// either pass `Some(ImageRef { … })` directly or look up a catalog entry
/// via [`catalog_image`].
pub fn default_forge_image() -> ImageRef {
    catalog_image("yah-rust-bun")
}

/// Build the [`ImageRef`] for a named catalog image. Digest pinning is
/// applied per-image when the matching env var was injected at compile
/// time.
///
/// When the catalog name is unknown OR the compile-time digest env var is
/// unset (dev builds), falls back to [`workload_spec::testing::TEST_DIGEST`].
/// The runtime container pull will then fail clearly when no real image
/// matches that digest, surfacing the missing env var instead of silently
/// pulling a tag-only reference. R438-T3 made digest structurally required;
/// catalog_image keeps its infallible signature by using the test-fixture
/// digest as a "this build is not pinned" sentinel.
pub fn catalog_image(name: &str) -> ImageRef {
    ImageRef {
        registry: YAH_IMAGE_REGISTRY.into(),
        repository: format!("{YAH_IMAGE_OWNER}/{name}"),
        tag: YAH_IMAGE_TAG.into(),
        digest: catalog_digest(name)
            .map(Into::into)
            .unwrap_or_else(workload_spec::testing::test_digest),
    }
}

/// Returns the compile-time digest for a catalog image name, or `None`
/// when the image isn't recognised or when no digest was injected.
pub fn catalog_digest(name: &str) -> Option<&'static str> {
    match name {
        "yah-base" => YAH_BASE_DIGEST,
        "yah-rust" => YAH_RUST_DIGEST,
        "yah-rust-bun" => YAH_RUST_BUN_DIGEST,
        "yah-yubaba" => YAH_WARDEN_DIGEST,
        "yah-miniflare" => YAH_MINIFLARE_DIGEST,
        _ => None,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod default_image {
    use super::*;

    /// Constant shape is correct; digest is sha256-prefixed (real or
    /// test-fixture sentinel).
    #[test]
    fn struct_is_well_formed() {
        let img = default_forge_image();
        assert_eq!(img.registry, YAH_IMAGE_REGISTRY);
        assert_eq!(img.repository, DEFAULT_IMAGE_REPOSITORY);
        assert!(!img.tag.is_empty(), "tag must not be empty");
        assert!(
            img.digest.starts_with("sha256:"),
            "digest must be sha256: prefixed, got {:?}",
            img.digest
        );
        assert!(img.digest.len() > 7, "digest must not be empty after sha256: prefix");
    }

    /// catalog_image returns sane defaults for known + unknown names; unknown
    /// names fall back to the test-fixture digest sentinel.
    #[test]
    fn catalog_image_resolves_known_and_unknown_names() {
        for known in ["yah-base", "yah-rust", "yah-rust-bun", "yah-yubaba", "yah-miniflare"] {
            let img = catalog_image(known);
            assert_eq!(img.registry, YAH_IMAGE_REGISTRY);
            assert_eq!(img.repository, format!("{YAH_IMAGE_OWNER}/{known}"));
        }
        let unknown = catalog_image("yah-cuda-special");
        assert_eq!(unknown.repository, format!("{YAH_IMAGE_OWNER}/yah-cuda-special"));
        assert_eq!(
            unknown.digest,
            workload_spec::testing::TEST_DIGEST,
            "unknown catalog name falls back to the test-fixture digest sentinel",
        );
    }

    /// catalog_digest is None for unknown names.
    #[test]
    fn catalog_digest_unknown_name_is_none() {
        assert!(catalog_digest("does-not-exist").is_none());
    }

    /// Pull `yah-rust-bun` from GHCR and verify the Sigstore signature
    /// end-to-end.
    ///
    /// **Requirements**: Docker daemon running, `cosign` on PATH, network
    /// access, crate compiled with `YAH_RUST_BUN_DIGEST=sha256:<hash>`.
    ///
    /// Run manually after a release:
    /// ```sh
    /// YAH_RUST_BUN_DIGEST=sha256:<hash> \
    ///   cargo test -p task default_image::pull -- --include-ignored
    /// ```
    #[test]
    #[ignore]
    fn pull() {
        let img = default_forge_image();
        assert_ne!(
            img.digest,
            workload_spec::testing::TEST_DIGEST,
            "YAH_RUST_BUN_DIGEST must be set; \
             recompile with: YAH_RUST_BUN_DIGEST=sha256:<hash> cargo test …",
        );
        let full_ref = format!("{}/{}@{}", img.registry, img.repository, img.digest);

        let pull_status = std::process::Command::new("docker")
            .args(["pull", &full_ref])
            .status()
            .expect("docker not found on PATH or daemon not running");
        assert!(pull_status.success(), "docker pull failed for {full_ref}");

        let cosign_status = std::process::Command::new("cosign")
            .args([
                "verify",
                &full_ref,
                "--certificate-oidc-issuer",
                "https://token.actions.githubusercontent.com",
                "--certificate-identity-regexp",
                r"^https://github\.com/yah-ai/yah/\.github/workflows/release\.yml@",
            ])
            .status()
            .expect("cosign not found on PATH");
        assert!(cosign_status.success(), "cosign verify failed for {full_ref}");
    }
}
