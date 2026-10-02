//! Injectable image-build handler for the docker push family (R594).
//!
//! W224 retired *running* `docker/login-action` / `docker/build-push-action` in
//! the runtime — the tier classifier ([`crate::tier`]) declines them with a
//! "replace with a native `build-image` step" stanza. That keeps the standalone
//! qed-gha crate honest: on its own it never shells `docker`.
//!
//! But the qed runner's whole job is to *interpret and execute* a real
//! `.github/workflows/*.yml`, image jobs included. So the runner injects an
//! [`ImageBuilder`] via [`crate::Executor::with_image_builder`]. When one is
//! present, the runtime routes the two docker push-family slugs to it (applying
//! the camp's registry route + auth, then building locally via `docker buildx`
//! or remotely on a build-worker) instead of erroring. When absent, the tier-3
//! error is preserved unchanged — so nothing about the bare crate's contract
//! shifts.
//!
//! The handler returns a [`ToolkitOutcome`] (same shape a toolkit action
//! produces) so the runtime finalizes the step identically — `steps.<id>.
//! outputs.digest` / `.imageid` / `.metadata` flow through for downstream
//! `cosign sign` and the per-binary `DIGEST` env blocks in `release.yml`.
//!

use std::path::Path;

use indexmap::IndexMap;

use crate::expr::Value;
use crate::toolkit::ToolkitOutcome;

/// Per-call inputs handed to an [`ImageBuilder`] — mirrors
/// [`crate::toolkit::ToolkitCall`] (the handler is the injected analogue of a
/// toolkit action for the docker push family).
pub struct ImageBuildCall<'a> {
    /// The `uses:` slug minus `@ref` — `docker/login-action` or
    /// `docker/build-push-action`.
    pub slug: &'a str,
    /// Already-evaluated `with:` inputs (`${{ … }}` expanded, secrets resolved).
    pub with: &'a IndexMap<String, Value>,
    /// Composed step env (workflow + job + step).
    pub env: &'a IndexMap<String, String>,
    /// Working directory the workflow runs steps in (the checkout root).
    pub workspace: &'a Path,
}

/// Implementor contract for the injected image builder. Implemented by the qed
/// runner (`QedImageBuilder`), which owns the camp registry route/auth overlay
/// and the local-buildx / remote-fleet build execution.
pub trait ImageBuilder: Send + Sync {
    /// Handle one docker push-family step. `Err` is an unrecoverable
    /// spawn/IO/config failure (surfaces as [`crate::RuntimeError`]); a graceful
    /// `docker` non-zero exit rides back as
    /// [`ToolkitOutcome`]`{ conclusion: Failure, … }` so `continue-on-error` /
    /// `if: failure()` keep working.
    fn handle(&self, call: &ImageBuildCall<'_>) -> Result<ToolkitOutcome, String>;
}

/// The docker push-family slugs the runtime routes to an injected
/// [`ImageBuilder`]. These are exactly the slugs the tier classifier maps to
/// [`crate::tier::NativeReplacement::RegistryPublish`].
pub fn is_image_push_action(slug: &str) -> bool {
    matches!(slug, "docker/login-action" | "docker/build-push-action")
}
