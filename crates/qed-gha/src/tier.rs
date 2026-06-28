//! Tier classifier (R533-F2): which W224 tier does a parsed workflow step
//! consume, and therefore — import it as runnable compute, or replace it with a
//! native QED facility?
//!
//! W224 ("import, don't emulate") splits "the GitHub environment" into four
//! tiers and gives them *opposite* dispositions:
//!
//! | Tier | Surface | Disposition |
//! |---|---|---|
//! | 1 — toolkit contract | `INPUT_*` env, the four append-files, workflow commands | **adopt** (run it) |
//! | 2 — synthetic repo context | `GITHUB_REPOSITORY` / `_SHA` / `_REF` / `_WORKSPACE` | **fabricate** (run it) |
//! | 3 — GitHub-the-service | token+REST/GraphQL, OIDC, the artifact/cache services | **replace with native** |
//! | 4 — runtime hosting | Node (light) / Docker (heavy) | provide where unavoidable |
//!
//! Tiers 1/2 (and the build half of tier 4) are **runnable compute**: the
//! toolkit-contract executor runs them directly, so the transformer (R533-F4)
//! maps them mechanically. Tier 3 is the seam QED *deliberately declines to
//! imitate* — it already has a better native primitive for every tier-3
//! facility (content-addressed artifacts for `cache`/`upload-artifact`, its own
//! checkout, the W208 publisher adapters for `gh-release`) — so an imported
//! tier-3 step is flagged and a native-replacement stanza is proposed in its
//! place.
//!
//! This module is the **classification half** of that decision. It does not
//! transform anything; it tags each [`Step`] so F4 knows which steps to map
//! mechanically and which to flag. The catalog is written decision-table style:
//! one total function over the known-action space, an exhaustive [`Disposition`]
//! union, and a per-cell test matrix — so the tier of every action yah's
//! workflows touch is *specified and tested*, not emergent.

use crate::expr_str::{ExprString, ExprToken};
use crate::workflow::{Step, StepAction, Workflow};

/// The four W224 tiers of "the GitHub environment" a step's action consumes.
///
/// This is the *why* behind a step's [`Disposition`]; the disposition is what
/// the transformer acts on. A single action can touch several tiers (a
/// build-push both runs a build — tier 4/1 — and integrates with a registry —
/// tier 3); the classifier records the tier that drives the disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// 1 — the toolkit contract (`INPUT_*` env, `$GITHUB_OUTPUT`/`_ENV`/`_PATH`/
    /// `_STEP_SUMMARY`, the `::error::`/`::add-mask::` workflow commands). Small,
    /// fully specified, stable. **Adopt** — this is the whole interop value.
    ToolkitContract,
    /// 2 — synthetic repo context (`GITHUB_REPOSITORY`, `_SHA`, `_REF`,
    /// `_WORKSPACE`, `_EVENT_PATH`). Cheap — QED knows the repo/commit it builds.
    /// **Fabricate.**
    RepoContext,
    /// 3 — GitHub-the-service (live `GITHUB_TOKEN` + REST/GraphQL, OIDC, the
    /// artifact service, the cache service). Heavy, underdocumented. **Do not
    /// mimic — replace with native.**
    GitHubService,
    /// 4 — runtime hosting (Node for JS actions, Docker for container actions).
    /// Provided where unavoidable; the *compute* a hosted action does is still
    /// tier-1/2 runnable.
    RuntimeHosting,
}

impl Tier {
    /// The tier number (1–4), for preflight lines and reports.
    pub fn number(self) -> u8 {
        match self {
            Tier::ToolkitContract => 1,
            Tier::RepoContext => 2,
            Tier::GitHubService => 3,
            Tier::RuntimeHosting => 4,
        }
    }

    /// Short human label.
    pub fn label(self) -> &'static str {
        match self {
            Tier::ToolkitContract => "toolkit contract",
            Tier::RepoContext => "repo context",
            Tier::GitHubService => "GitHub-the-service",
            Tier::RuntimeHosting => "runtime hosting",
        }
    }
}

/// The native QED facility that replaces a tier-3 step at import time. Each
/// variant names *what to build instead*, so the transformer (R533-F4) can emit
/// the right replacement stanza rather than a generic "unsupported" flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeReplacement {
    /// `actions/checkout` → QED already has the source (it owns the workspace);
    /// native checkout is implicit. A foreign-repo checkout becomes an explicit
    /// native clone step.
    Checkout,
    /// `actions/cache` / `Swatinem/rust-cache` → content-addressed artifacts
    /// (the atomic-asset model): build outputs are cached by content hash and
    /// re-used across runs without a cache *service*.
    ContentAddressedCache,
    /// `actions/upload-artifact` → a content-addressed output that downstream
    /// native steps consume via a normal `needs:` edge.
    UploadArtifact,
    /// `actions/download-artifact` → a `needs:` edge onto the producing step's
    /// content-addressed output (no artifact service round-trip).
    DownloadArtifact,
    /// `softprops/action-gh-release` / `actions/create-release` → a W208
    /// publisher adapter (`kind = "publish-*"`).
    ReleasePublisher,
    /// `actions/deploy-pages` & the pages-artifact actions → a native site
    /// publisher.
    PagesPublisher,
    /// `docker/login-action` / `docker/build-push-action` → a native
    /// `build-image` step plus a registry route (`ghcr.io` →
    /// `registry.yah.dev`); the push is the image publisher, not a GHA service.
    RegistryPublish,
}

impl NativeReplacement {
    /// Short label naming the native facility.
    pub fn label(self) -> &'static str {
        match self {
            NativeReplacement::Checkout => "native checkout",
            NativeReplacement::ContentAddressedCache => "content-addressed cache",
            NativeReplacement::UploadArtifact => "content-addressed output",
            NativeReplacement::DownloadArtifact => "content-addressed input (needs:)",
            NativeReplacement::ReleasePublisher => "W208 release publisher",
            NativeReplacement::PagesPublisher => "native site publisher",
            NativeReplacement::RegistryPublish => "native image build + registry route",
        }
    }

    /// One-line guidance the transformer surfaces alongside a flagged tier-3
    /// step — the "here's the native stanza" half of W224's lossy-with-warnings
    /// import.
    pub fn stanza_hint(self) -> &'static str {
        match self {
            NativeReplacement::Checkout => {
                "QED owns the workspace — drop the step (checkout is implicit), \
                 or for a foreign repo emit an explicit native clone."
            }
            NativeReplacement::ContentAddressedCache => {
                "Caching is automatic — declare build outputs as content-addressed \
                 artifacts; they are re-used by content hash with no cache service."
            }
            NativeReplacement::UploadArtifact => {
                "Replace with a content-addressed output; downstream steps consume \
                 it via a `needs:` edge instead of the artifact service."
            }
            NativeReplacement::DownloadArtifact => {
                "Replace with a `needs:` edge onto the producing step's \
                 content-addressed output."
            }
            NativeReplacement::ReleasePublisher => {
                "Replace with a W208 publisher adapter (`kind = \"publish-*\"`); \
                 release assets are content-addressed, not uploaded to a release API."
            }
            NativeReplacement::PagesPublisher => {
                "Replace with a native site publisher; the page bundle is a \
                 content-addressed artifact."
            }
            NativeReplacement::RegistryPublish => {
                "Replace with a native `build-image` step + registry route \
                 (`ghcr.io` → `registry.yah.dev`); push via the image publisher."
            }
        }
    }
}

/// What to do with a step at import time — the deliverable of the classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Tier 1/2 (and the build half of tier 4): runnable compute. The
    /// toolkit-contract executor runs it directly; the transformer maps it
    /// mechanically.
    Compute,
    /// Tier 3: integrates with GitHub-the-service. Decline to imitate; the
    /// transformer replaces it with the named native QED facility.
    ReplaceWithNative(NativeReplacement),
    /// An unrecognized `uses:` slug — not in the tier-3 catalog and not a known
    /// compute action. The assisted transformer surfaces it for human review
    /// rather than guessing; the default *attempt* is to run it as compute.
    Unknown,
}

impl Disposition {
    /// Tier 3 — the step needs a native replacement before it can run on QED.
    pub fn is_tier3(self) -> bool {
        matches!(self, Disposition::ReplaceWithNative(_))
    }

    /// Mechanically mappable as-is (tier 1/2). Note: [`Unknown`](Disposition::Unknown)
    /// is *not* compute — it is flagged even though the transformer will attempt
    /// to run it.
    pub fn is_compute(self) -> bool {
        matches!(self, Disposition::Compute)
    }

    /// The step can't be imported silently — the transformer must surface it
    /// (tier-3 replacement, or an unrecognized action).
    pub fn needs_flag(self) -> bool {
        !self.is_compute()
    }
}

/// A GitHub-the-service touch detected *inside* a `run:` body — tier-3 logic
/// embedded in bash, which the classifier can't auto-replace but must flag so
/// "runs on GHA today" doesn't silently break "runs on QED tomorrow".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceTouch {
    /// A `gh` CLI invocation (`gh release create`, `gh api`, `gh pr comment`, …).
    GhCli,
    /// A direct GitHub REST/GraphQL call (`api.github.com`, `uploads.github.com`,
    /// `$GITHUB_API_URL`).
    GitHubApi,
    /// A reference to the live `GITHUB_TOKEN` / `github.token` — service auth.
    GitHubToken,
}

impl ServiceTouch {
    pub fn label(self) -> &'static str {
        match self {
            ServiceTouch::GhCli => "gh CLI",
            ServiceTouch::GitHubApi => "GitHub REST/GraphQL",
            ServiceTouch::GitHubToken => "GITHUB_TOKEN",
        }
    }
}

/// The classification of a single step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepClass {
    /// The W224 tier driving the disposition. For [`Disposition::Unknown`] this
    /// is a best-guess default ([`Tier::ToolkitContract`] — "attempt to run it").
    pub tier: Tier,
    /// What to do with the step at import time.
    pub disposition: Disposition,
    /// GitHub-the-service touches found inside a `run:` body. Empty for clean
    /// compute steps and for `uses:` steps (those are classified by slug).
    pub service_touches: Vec<ServiceTouch>,
}

impl StepClass {
    /// The step is fully clean tier-1/2 compute: mechanically mappable with no
    /// flag of any kind (no tier-3 disposition, no embedded service touch).
    pub fn is_clean_compute(&self) -> bool {
        self.disposition.is_compute() && self.service_touches.is_empty()
    }
}

/// Classify one parsed workflow step.
pub fn classify_step(step: &Step) -> StepClass {
    match &step.action {
        StepAction::Uses { slug, .. } => {
            let (tier, disposition) = classify_uses(slug);
            StepClass { tier, disposition, service_touches: Vec::new() }
        }
        StepAction::Run { body, .. } => StepClass {
            tier: Tier::ToolkitContract,
            disposition: Disposition::Compute,
            service_touches: scan_service_touches(body),
        },
    }
}

/// The action catalog: map a `uses:` slug to its W224 tier + disposition. Total
/// over all slugs — an unrecognized one falls through to
/// [`Disposition::Unknown`]. This is the decision table; the tests below pin one
/// cell per arm.
pub fn classify_uses(slug: &str) -> (Tier, Disposition) {
    use Disposition::{Compute, ReplaceWithNative, Unknown};
    use NativeReplacement as NR;
    use Tier::{GitHubService, RuntimeHosting, ToolkitContract};

    // Composite-action subpaths (`actions/cache/restore`) classify at the
    // org/repo granularity.
    match org_repo(slug) {
        // ── Tier 3: GitHub-the-service → decline to imitate, replace native ──
        "actions/checkout" => (GitHubService, ReplaceWithNative(NR::Checkout)),
        "actions/cache" | "Swatinem/rust-cache" => {
            (GitHubService, ReplaceWithNative(NR::ContentAddressedCache))
        }
        "actions/upload-artifact" => (GitHubService, ReplaceWithNative(NR::UploadArtifact)),
        "actions/download-artifact" => (GitHubService, ReplaceWithNative(NR::DownloadArtifact)),
        "softprops/action-gh-release" | "actions/create-release" | "ncipollo/release-action" => {
            (GitHubService, ReplaceWithNative(NR::ReleasePublisher))
        }
        "actions/deploy-pages" | "actions/configure-pages" | "actions/upload-pages-artifact" => {
            (GitHubService, ReplaceWithNative(NR::PagesPublisher))
        }
        "docker/login-action" | "docker/build-push-action" => {
            (GitHubService, ReplaceWithNative(NR::RegistryPublish))
        }

        // ── Tier 4: runtime hosting → provide; the build itself is compute.
        // (Matched before the generic `setup-*` heuristic, which would otherwise
        // swallow these.) ──
        "docker/setup-buildx-action" | "docker/setup-qemu-action" => (RuntimeHosting, Compute),

        // ── Tier 1: toolkit contract → adopt (run via the toolkit executor) ──
        "dtolnay/rust-toolchain" | "oven-sh/setup-bun" | "sigstore/cosign-installer" => {
            (ToolkitContract, Compute)
        }
        k if is_setup_action(k) => (ToolkitContract, Compute),

        // ── Unrecognized: surface for human review, don't guess a tier ──
        _ => (ToolkitContract, Unknown),
    }
}

/// First two `/`-separated segments of a `uses:` slug (`org/repo`), collapsing
/// any composite-action subpath. `actions/cache/restore` → `actions/cache`;
/// `actions/checkout` → `actions/checkout`.
fn org_repo(slug: &str) -> &str {
    match slug.match_indices('/').nth(1) {
        Some((idx, _)) => &slug[..idx],
        None => slug,
    }
}

/// `org/setup-*` toolchain installers (`actions/setup-node`, `setup-python`,
/// `setup-go`, `setup-java`, `arduino/setup-protoc`, …) — pure tier-1 compute.
/// The `docker/setup-*` runtime-hosting actions are matched explicitly *before*
/// this heuristic in [`classify_uses`].
fn is_setup_action(key: &str) -> bool {
    key.rsplit('/').next().is_some_and(|repo| repo.starts_with("setup-"))
}

/// Scan a `run:` body for embedded GitHub-the-service usage. Substring/word
/// detection over both the literal text and the raw expression fragments (so a
/// `${{ secrets.GITHUB_TOKEN }}` reference is caught too).
fn scan_service_touches(body: &ExprString) -> Vec<ServiceTouch> {
    let text = body_text(body);
    let lower = text.to_ascii_lowercase();
    let mut out = Vec::new();
    if invokes_gh_cli(&text) {
        out.push(ServiceTouch::GhCli);
    }
    if lower.contains("api.github.com")
        || lower.contains("uploads.github.com")
        || lower.contains("github_api_url")
    {
        out.push(ServiceTouch::GitHubApi);
    }
    if lower.contains("github_token") || lower.contains("github.token") {
        out.push(ServiceTouch::GitHubToken);
    }
    out
}

/// Flatten an `ExprString` to scannable text: literal segments verbatim, each
/// `${{ … }}` fragment's raw source space-padded so adjacent tokens don't fuse.
fn body_text(body: &ExprString) -> String {
    let mut s = String::new();
    for t in &body.tokens {
        match t {
            ExprToken::Literal(x) => s.push_str(x),
            ExprToken::Expr(x) => {
                s.push(' ');
                s.push_str(x);
                s.push(' ');
            }
        }
    }
    s
}

/// True when the script invokes the `gh` CLI. Word-level (a bare `gh` token at a
/// command position) to avoid matching `high`, `weigh`, a `/path/to/gh`, or a
/// `gh=…` assignment.
fn invokes_gh_cli(text: &str) -> bool {
    text.split(|c: char| c.is_whitespace() || matches!(c, ';' | '&' | '|' | '(' | '`'))
        .any(|tok| tok == "gh")
}

/// A step located within its workflow, paired with its classification — the
/// shape the transformer (R533-F4) and the portability preflight walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedStep {
    pub job: String,
    /// 0-based index of the step within its job's `steps:` list.
    pub step_index: usize,
    /// The step's `name:` rendered to best-effort text (expressions kept raw),
    /// or `None` when unnamed.
    pub step_name: Option<String>,
    pub class: StepClass,
}

/// Classify every step in a workflow, in job-declaration then step order.
pub fn classify_workflow(wf: &Workflow) -> Vec<ClassifiedStep> {
    let mut out = Vec::new();
    for (job_id, job) in &wf.jobs {
        for (step_index, step) in job.steps.iter().enumerate() {
            out.push(ClassifiedStep {
                job: job_id.clone(),
                step_index,
                step_name: step.name.as_ref().map(body_text).map(|s| s.trim().to_string()),
                class: classify_step(step),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::StepAction;
    use indexmap::IndexMap;

    fn uses(slug: &str) -> Step {
        Step {
            id: None,
            name: None,
            if_cond: None,
            env: IndexMap::new(),
            continue_on_error: None,
            timeout_minutes: None,
            working_directory: None,
            action: StepAction::Uses {
                slug: slug.to_string(),
                git_ref: Some("v4".into()),
                with: IndexMap::new(),
            },
        }
    }

    fn run(body: &str) -> Step {
        Step {
            id: None,
            name: None,
            if_cond: None,
            env: IndexMap::new(),
            continue_on_error: None,
            timeout_minutes: None,
            working_directory: None,
            action: StepAction::Run { body: ExprString::parse(body), shell: Some("bash".into()) },
        }
    }

    // ── decision table: one cell per catalog arm ──────────────────────────

    #[test]
    fn tier3_checkout_replaces_native() {
        let (tier, d) = classify_uses("actions/checkout");
        assert_eq!(tier, Tier::GitHubService);
        assert_eq!(d, Disposition::ReplaceWithNative(NativeReplacement::Checkout));
        assert!(d.is_tier3());
        assert!(!d.is_compute());
        assert!(d.needs_flag());
    }

    #[test]
    fn tier3_cache_family_maps_to_content_addressed() {
        for slug in ["actions/cache", "actions/cache/restore", "actions/cache/save", "Swatinem/rust-cache"] {
            let (tier, d) = classify_uses(slug);
            assert_eq!(tier, Tier::GitHubService, "{slug}");
            assert_eq!(
                d,
                Disposition::ReplaceWithNative(NativeReplacement::ContentAddressedCache),
                "{slug}"
            );
        }
    }

    #[test]
    fn tier3_artifact_actions() {
        assert_eq!(
            classify_uses("actions/upload-artifact").1,
            Disposition::ReplaceWithNative(NativeReplacement::UploadArtifact)
        );
        assert_eq!(
            classify_uses("actions/download-artifact").1,
            Disposition::ReplaceWithNative(NativeReplacement::DownloadArtifact)
        );
    }

    #[test]
    fn tier3_release_publishers() {
        for slug in ["softprops/action-gh-release", "actions/create-release", "ncipollo/release-action"] {
            assert_eq!(
                classify_uses(slug).1,
                Disposition::ReplaceWithNative(NativeReplacement::ReleasePublisher),
                "{slug}"
            );
        }
    }

    #[test]
    fn tier3_pages_publishers() {
        for slug in ["actions/deploy-pages", "actions/configure-pages", "actions/upload-pages-artifact"] {
            assert_eq!(
                classify_uses(slug).1,
                Disposition::ReplaceWithNative(NativeReplacement::PagesPublisher),
                "{slug}"
            );
        }
    }

    #[test]
    fn tier3_docker_registry_actions() {
        for slug in ["docker/login-action", "docker/build-push-action"] {
            let (tier, d) = classify_uses(slug);
            assert_eq!(tier, Tier::GitHubService, "{slug}");
            assert_eq!(d, Disposition::ReplaceWithNative(NativeReplacement::RegistryPublish), "{slug}");
        }
    }

    #[test]
    fn tier4_docker_runtime_hosting_is_compute_not_setup_heuristic() {
        // These contain `setup-` but must NOT be swallowed by the setup-* arm.
        for slug in ["docker/setup-buildx-action", "docker/setup-qemu-action"] {
            let (tier, d) = classify_uses(slug);
            assert_eq!(tier, Tier::RuntimeHosting, "{slug}");
            assert_eq!(d, Disposition::Compute, "{slug}");
        }
    }

    #[test]
    fn tier1_explicit_compute_actions() {
        for slug in ["dtolnay/rust-toolchain", "oven-sh/setup-bun", "sigstore/cosign-installer"] {
            let (tier, d) = classify_uses(slug);
            assert_eq!(tier, Tier::ToolkitContract, "{slug}");
            assert_eq!(d, Disposition::Compute, "{slug}");
        }
    }

    #[test]
    fn tier1_setup_heuristic_covers_toolchain_installers() {
        for slug in ["actions/setup-node", "actions/setup-python", "actions/setup-go", "arduino/setup-protoc"] {
            let (tier, d) = classify_uses(slug);
            assert_eq!(tier, Tier::ToolkitContract, "{slug}");
            assert_eq!(d, Disposition::Compute, "{slug}");
        }
    }

    #[test]
    fn unrecognized_action_is_unknown_and_flagged() {
        let (_, d) = classify_uses("some-org/exotic-action");
        assert_eq!(d, Disposition::Unknown);
        assert!(!d.is_compute());
        assert!(d.needs_flag());
        assert!(!d.is_tier3(), "unknown is its own bucket, not tier-3");
    }

    #[test]
    fn org_repo_collapses_subpaths_and_handles_bare_slugs() {
        assert_eq!(org_repo("actions/cache/restore"), "actions/cache");
        assert_eq!(org_repo("actions/checkout"), "actions/checkout");
        assert_eq!(org_repo("docker/build-push-action"), "docker/build-push-action");
        assert_eq!(org_repo("bare"), "bare");
    }

    // ── run-step service touches ─────────────────────────────────────────

    #[test]
    fn plain_run_step_is_clean_compute() {
        let c = classify_step(&run("cargo build --release\necho done"));
        assert!(c.is_clean_compute());
        assert_eq!(c.disposition, Disposition::Compute);
        assert!(c.service_touches.is_empty());
    }

    #[test]
    fn run_step_gh_cli_is_flagged() {
        let c = classify_step(&run("gh release create v1.0.0 ./dist/*"));
        assert_eq!(c.disposition, Disposition::Compute, "still runs on the executor");
        assert!(!c.is_clean_compute(), "but is not clean — it touches the service");
        assert_eq!(c.service_touches, vec![ServiceTouch::GhCli]);
    }

    #[test]
    fn run_step_gh_word_boundary_avoids_false_positives() {
        // `high`, `weigh`, and a `/usr/bin/gh` path must not trip the gh detector.
        let c = classify_step(&run("echo high && weigh_in --flag; cat /usr/bin/ghost"));
        assert!(c.service_touches.is_empty(), "no bare `gh` command token");
    }

    #[test]
    fn run_step_github_api_and_token() {
        let c = classify_step(&run(
            "curl -H \"Authorization: Bearer $GITHUB_TOKEN\" https://api.github.com/repos/x/y/releases",
        ));
        assert!(c.service_touches.contains(&ServiceTouch::GitHubApi));
        assert!(c.service_touches.contains(&ServiceTouch::GitHubToken));
    }

    #[test]
    fn run_step_token_in_expression_fragment_is_caught() {
        // `${{ secrets.GITHUB_TOKEN }}` is an Expr token, not literal text.
        let c = classify_step(&run("echo ${{ secrets.GITHUB_TOKEN }} | docker login"));
        assert!(c.service_touches.contains(&ServiceTouch::GitHubToken));
    }

    #[test]
    fn classify_step_dispatches_uses_vs_run() {
        let u = classify_step(&uses("actions/checkout"));
        assert!(u.disposition.is_tier3());
        let r = classify_step(&run("make"));
        assert!(r.is_clean_compute());
    }

    // ── native-replacement metadata is total ─────────────────────────────

    #[test]
    fn every_native_replacement_has_label_and_hint() {
        for nr in [
            NativeReplacement::Checkout,
            NativeReplacement::ContentAddressedCache,
            NativeReplacement::UploadArtifact,
            NativeReplacement::DownloadArtifact,
            NativeReplacement::ReleasePublisher,
            NativeReplacement::PagesPublisher,
            NativeReplacement::RegistryPublish,
        ] {
            assert!(!nr.label().is_empty());
            assert!(nr.stanza_hint().len() > 20, "hint should be actionable guidance");
        }
    }

    #[test]
    fn tier_numbers_and_labels() {
        assert_eq!(Tier::ToolkitContract.number(), 1);
        assert_eq!(Tier::RepoContext.number(), 2);
        assert_eq!(Tier::GitHubService.number(), 3);
        assert_eq!(Tier::RuntimeHosting.number(), 4);
        for t in [Tier::ToolkitContract, Tier::RepoContext, Tier::GitHubService, Tier::RuntimeHosting] {
            assert!(!t.label().is_empty());
        }
    }
}
