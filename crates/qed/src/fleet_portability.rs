//! Fleet portability (R555-F11, W235 §Verdict §3c) — *may* a step's work move
//! to a build worker, as distinct from *should* it.
//!
//! `--where=remote` is a **routing** force. It picks where a step is dispatched
//! and does nothing whatever to make the step's work survive the trip. The
//! load-bearing line lives at the dispatch site
//! ([`crate::runner::PipelineRunner`]'s `execute_step_remote`, R560-T8):
//!
//! > a remote subprocess gets image + argv + the `/yah/produced` mount and
//! > **NOTHING ELSE — in particular, no source.**
//!
//! So before this module existed you could point `--where=remote` at any of the
//! camp's 44 recipes, the step would dispatch, the container would start, and
//! `./scripts/whatever.sh` would not be there — a failure minutes in, deep
//! inside a container, naming nothing. Exactly two camp recipes (`mesofact-musl`
//! and `yah-cli-release`) declare `source_context` at all; measured 2026-09-10.
//!
//! # The three requirements, and why only two of them refuse
//!
//! | # | Requirement | Decidable? | Verdict |
//! |---|---|---|---|
//! | 1 | source travels, or the argv doesn't need it | yes, conservatively | blocking |
//! | 2 | every `produces` path under `/yah/produced` | yes | blocking |
//! | 3 | an `image` carrying the toolchain | **no** | advisory |
//!
//! **(1) is not "declares `source_context`".** Writing it that way would refuse
//! `rusty-v8-musl`, which is a live, working, offloading pipeline whose remote
//! step is genuinely self-sufficient: `build-v8.sh` is baked into the builder
//! image and clones V8 from the internet, so image + argv is the whole of it.
//! W235 §2 says the same thing in words ("it does not need the camp tree and
//! this does"), and `participant-smoke.toml` says it in a comment ("Nothing here
//! reads the tree: both steps ship as argv"). The question is therefore whether
//! the argv reaches for the camp tree *without* shipping it — see
//! [`camp_tree_reference`] for the heuristic and its deliberate bias.
//!
//! **(3) cannot be a refusal**, because a step that names no `image` does not
//! run bare: it gets the default forge image (`yah-rust-bun`, rust + bun), which
//! is a real toolchain that plenty of steps genuinely fit inside. Whether *this*
//! argv fits is not answerable from the recipe — it would need the image's
//! contents. So it is reported and never refused.
//!
//! # Where the remedies come from
//!
//! Every [`PortabilityGap::remedy`] is written off `mesofact-musl`, the one
//! recipe in the camp that satisfies all three requirements, because its own
//! comments record what satisfying them cost: git-tracked-files-only packing,
//! `tar --no-same-owner` on extract, an entrypoint-shaped single-string argv,
//! and a derived cache key. A reason that says "add `source_context`" without
//! saying what travels with it sends the reader into an hour someone has already
//! paid for.

use crate::types::{QedStep, StepKind};

/// One reason a step is not fleet-portable, or is portable but worth a word.
///
/// `Display` renders the short label used in the per-step preflight line;
/// [`PortabilityGap::remedy`] renders the full route-to-the-fix text the gate
/// and `yah qed preflight` print.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortabilityGap {
    /// Requirement (1): the argv reaches into the camp tree and no
    /// `source_context` ships it. Carries the token that gave it away, so the
    /// operator is told *which* path will not be there rather than being handed
    /// a category.
    SourceDoesNotTravel {
        /// The argv token that reads the camp tree (e.g. `./scripts/check.sh`,
        /// `scripts/smoke.sh`, `oss/mesofact`).
        camp_reference: String,
    },
    /// Requirement (1), the other shape: the argv runs a build tool that reads
    /// its manifest out of the working directory (`cargo`, `bun`, `make`) and no
    /// `source_context` puts a tree there. Split from
    /// [`SourceDoesNotTravel`](PortabilityGap::SourceDoesNotTravel) because the
    /// remedy sentence differs — "stop referencing `cargo`" is not advice.
    BuildToolNeedsTheTree {
        /// The tool at the head of the argv.
        tool: String,
    },
    /// Requirement (2): a `produces` path outside `/yah/produced`, the durable
    /// host-backed bind mount. Enforced again at dispatch (R603-T5); catching it
    /// here moves the refusal from minutes-in to second zero.
    ProducesOffDurableDir {
        /// The declared container-side path.
        path: String,
    },
    /// Requirement (3), advisory: no `image`, so the step runs in the default
    /// forge image. Never blocking — see the module docs.
    DefaultForgeImage,
}

/// The part of the `source_context` remedy that is the same whichever way the
/// step reaches for the tree — and the part that is the whole point of writing
/// remedies rather than deficiencies. Each clause is an hour someone has already
/// paid for, recorded in `.yah/qed/mesofact-musl.toml`'s own comments.
const WHAT_TRAVELS_WITH_SOURCE_CONTEXT: &str =
    "FIX: `source_context = [\"<subtree>\", …]`, camp-root-relative, naming every subtree \
     the argv needs — mesofact-musl names four, because mesofact's path deps escape its own \
     workspace into oss/yah-base and oss/cheers. THREE THINGS TRAVEL WITH THAT KEY and none \
     are optional: (a) only GIT-TRACKED files are packed, so `git add` a new file or the \
     worker gets a `mod` line without its file; (b) the argv fetches and unpacks the tarball \
     itself from `$YAH_SOURCE_CONTEXT_URL` — use `tar --no-same-owner`, or the extract dies \
     with \"Cannot change ownership\" on every file before the build starts; (c) if the image \
     ENTRYPOINT is [\"bash\",\"-c\"] the whole invocation is ONE argv string, because a \
     multi-element argv drops everything past [0]. Worked example, comments and all: \
     .yah/qed/mesofact-musl.toml.";

impl PortabilityGap {
    /// Whether this gap refuses a run. False only for
    /// [`DefaultForgeImage`](PortabilityGap::DefaultForgeImage), which names a
    /// risk the recipe cannot settle either way.
    pub fn blocking(&self) -> bool {
        !matches!(self, PortabilityGap::DefaultForgeImage)
    }

    /// A stable key for deduplicating remedies in a report: two steps missing
    /// `source_context` want the long paragraph printed once, not once each.
    pub fn kind(&self) -> &'static str {
        match self {
            PortabilityGap::SourceDoesNotTravel { .. } => "source-does-not-travel",
            PortabilityGap::BuildToolNeedsTheTree { .. } => "build-tool-needs-the-tree",
            PortabilityGap::ProducesOffDurableDir { .. } => "produces-off-durable-dir",
            PortabilityGap::DefaultForgeImage => "default-forge-image",
        }
    }

    /// Full operator-facing text: the deficiency **and** the remedy, including
    /// what travels with the remedy. See the module docs on why the remedy half
    /// is not optional.
    pub fn remedy(&self) -> String {
        match self {
            PortabilityGap::SourceDoesNotTravel { camp_reference } => format!(
                "declares no `source_context`, but its argv reads the camp tree \
                 (`{camp_reference}`). A remote subprocess gets image + argv + the \
                 /yah/produced mount and nothing else, so that path does not exist on the \
                 worker and the step dies minutes in, inside a container. \
                 {WHAT_TRAVELS_WITH_SOURCE_CONTEXT} \
                 If the step genuinely needs no camp source (rusty-v8-musl clones V8 \
                 inside the container), the other fix is to stop referencing \
                 `{camp_reference}` and bake it into the image instead."
            ),
            PortabilityGap::BuildToolNeedsTheTree { tool } => format!(
                "declares no `source_context`, but runs `{tool}`, which reads its manifest \
                 out of the working directory — and a remote subprocess has no camp tree \
                 to read one from (image + argv + the /yah/produced mount, nothing else). \
                 The step starts, and dies on a missing manifest. \
                 {WHAT_TRAVELS_WITH_SOURCE_CONTEXT}"
            ),
            PortabilityGap::ProducesOffDurableDir { path } => format!(
                "declares produced artifact `{path}`, which is not under \
                 `/yah/produced`. That dir is the durable host-backed bind mount; \
                 anything written elsewhere lands in the container's writable layer and \
                 is gone the moment the build-worker reaps the container (R603-T5). \
                 FIX: point the build's output at `/yah/produced/…` and set \
                 `produces.path` to that CONTAINER-side path — mesofact-musl writes \
                 `/yah/produced/mesofact-x86_64-unknown-linux-musl.tar.gz` and lets \
                 retrieve_remote_artifacts land it content-addressed on the camp."
            ),
            PortabilityGap::DefaultForgeImage => String::from(
                "names no `image`, so it runs in the default forge image \
                 (`yah-rust-bun`: rust + bun). The worker has no host userland to \
                 borrow, so that image is the whole environment. Fine for a plain cargo \
                 build; if the argv needs anything else — a musl sysroot, a prebuilt \
                 librusty_v8.a, python — name a digest-pinned image the way \
                 mesofact-musl does (a bare catalog name resolves to \
                 ghcr.io/yah-ai/<name>:latest and can silently pull the wrong \
                 registry, R590-B5). NOT a refusal: the default image is real, and \
                 whether this argv fits inside it is not answerable from the recipe.",
            ),
        }
    }
}

impl std::fmt::Display for PortabilityGap {
    /// The short label, for the one-line-per-step preflight.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PortabilityGap::SourceDoesNotTravel { camp_reference } => {
                write!(f, "no source_context, argv reads `{camp_reference}`")
            }
            PortabilityGap::BuildToolNeedsTheTree { tool } => {
                write!(f, "no source_context, runs `{tool}` against the tree")
            }
            PortabilityGap::ProducesOffDurableDir { path } => {
                write!(f, "produces `{path}` outside /yah/produced")
            }
            PortabilityGap::DefaultForgeImage => write!(f, "no image (default yah-rust-bun)"),
        }
    }
}

/// Build tools that read their manifest out of the working directory — i.e. out
/// of the camp tree a worker does not have. Matched only as `argv[0]` (or the
/// first word of a single-string shell argv), never anywhere in the text: a
/// `cargo build` buried inside a heredoc that already unpacked a
/// `source_context` tarball is the *correct* shape, and flagging it would refuse
/// mesofact-musl.
const CAMP_TREE_TOOLS: &[&str] = &[
    "cargo", "xtask", "bun", "npm", "pnpm", "yarn", "make", "just",
];

/// The argv PATH that proves this step reads the camp tree, if any. (The
/// build-tool shape is [`build_tool_head`], kept separate because its remedy
/// sentence differs.)
///
/// Two shapes:
///
/// 1. **A relative path that exists in the camp tree** — `scripts/smoke.sh`,
///    `./scripts/check.sh`, `oss/mesofact`. This is the decisive one and it is
///    grounded rather than guessed: the path resolves *here*, and a build worker
///    has no camp tree for it to resolve in. Requires `camp_root`.
/// 2. Any word starting with `./` or `../`, whether or not it resolves — the
///    fallback when no `camp_root` is available, and the `./scripts/whatever.sh`
///    case W235 §2 is written about.
///
/// # Why rule 1 stats the filesystem instead of pattern-matching
///
/// The first cut matched "a word containing `/` that is not absolute", and the
/// corpus proved that wrong in both directions at once: it MISSED `argv =
/// ["bash", "scripts/smoke-yah-dev.sh"]` (no `./` prefix, so 21 of 44 camp
/// recipes classified portable when almost none are), and it would have MATCHED
/// `s/^/hot-ship version: /` inside a sed expression, `0.0.0.0/0`, and any URL.
/// Asking the filesystem answers exactly the question the gate is about — "will
/// this path be there?" — and gets both cases right without a growing list of
/// shapes to exclude.
///
/// A relative path that does not exist here is not flagged. That step is broken
/// locally too, and diagnosing it is not this gate's job.
///
/// Corpus-checked (`app/yah/cli/tests/camp_qed_fleet_portability.rs`):
/// `rusty-v8-musl` (`build-v8.sh <triple> /yah/produced/…` — a PATH lookup
/// inside the builder image) and `participant-smoke`'s python heredoc both come
/// back clean, which is correct: neither reads the tree.
pub fn camp_tree_reference(argv: &[String], camp_root: Option<&std::path::Path>) -> Option<String> {
    for element in argv {
        for word in element.split_whitespace() {
            let word = word.trim_matches(|c| c == '\'' || c == '"' || c == '`' || c == ',');
            if word.starts_with("./") || word.starts_with("../") {
                return Some(word.to_string());
            }
            let Some(root) = camp_root else { continue };
            if !looks_like_a_relative_path(word) {
                continue;
            }
            if root.join(word).exists() {
                return Some(word.to_string());
            }
        }
    }
    None
}

/// The build tool at the head of `argv`, if it is one that reads its manifest
/// out of the working directory. `argv[0]` (or the first word of a single-string
/// shell argv) only — see [`CAMP_TREE_TOOLS`] for why matching anywhere in the
/// text would refuse `mesofact-musl`.
pub fn build_tool_head(argv: &[String]) -> Option<String> {
    let head = argv.first()?.split_whitespace().next()?;
    CAMP_TREE_TOOLS.contains(&head).then(|| head.to_string())
}

/// Cheap pre-filter before the `exists()` in [`camp_tree_reference`]: skip words
/// that cannot be a camp-relative path, so a heredoc's few hundred words cost a
/// handful of stats rather than one each.
fn looks_like_a_relative_path(word: &str) -> bool {
    !word.is_empty()
        && word.len() < 256
        && word.contains('/')
        && !word.starts_with('/')
        && !word.starts_with('-')
        && !word.starts_with('$')
        && !word.contains("://")
        // `${VAR}/sub`, `$(cmd)/x`, globs — nothing a `join` could resolve.
        && !word.contains('$')
        && !word.contains('*')
}

/// Every fleet-portability gap of `step`, blocking and advisory, in requirement
/// order. Empty ⇒ the step is portable and has nothing worth remarking on.
///
/// `camp_root` grounds requirement (1) — see [`camp_tree_reference`]. Passing
/// `None` degrades to the pattern-only rules and will under-report; the runner
/// always has a camp root and always passes it.
///
/// Deliberately says nothing about *whether* the step is routed to the fleet —
/// that is placement, and the caller
/// ([`crate::runner::PipelineRunner::fleet_portability_gate`]) owns it.
pub fn gaps(step: &QedStep, camp_root: Option<&std::path::Path>) -> Vec<PortabilityGap> {
    let mut out = Vec::new();
    if step.source_context.is_empty() {
        // A named path beats a build-tool head: it is the grounded answer, and
        // it tells the operator exactly what will not be there.
        if let Some(camp_reference) = camp_tree_reference(&step.argv, camp_root) {
            out.push(PortabilityGap::SourceDoesNotTravel { camp_reference });
        } else if let Some(tool) = build_tool_head(&step.argv) {
            out.push(PortabilityGap::BuildToolNeedsTheTree { tool });
        }
    }
    for artifact in &step.produces {
        let path = std::path::Path::new(&artifact.path);
        if !workload_spec::forge_produced::is_durable_path(path) {
            out.push(PortabilityGap::ProducesOffDurableDir {
                path: artifact.path.clone(),
            });
        }
    }
    if step.image.is_none() {
        out.push(PortabilityGap::DefaultForgeImage);
    }
    out
}

/// Can `step` run on a build worker? `Err` carries only the **blocking** gaps,
/// so `Ok(())` means "dispatching this would not fail for a reason the recipe
/// already tells us about" — not "this is guaranteed to pass".
pub fn fleet_portable(
    step: &QedStep,
    camp_root: Option<&std::path::Path>,
) -> Result<(), Vec<PortabilityGap>> {
    let blocking: Vec<_> = gaps(step, camp_root)
        .into_iter()
        .filter(|g| g.blocking())
        .collect();
    if blocking.is_empty() {
        Ok(())
    } else {
        Err(blocking)
    }
}

/// Only `kind = "subprocess"` steps are ever dispatched to a worker — every
/// other [`StepKind`] runs on the camp regardless of `--where` (see the
/// `run_one_step` kind match). Asking portability of a sub-pipeline or a manual
/// gate would refuse runs that were never leaving the box.
pub fn is_dispatchable_kind(step: &QedStep) -> bool {
    matches!(step.kind, StepKind::Subprocess)
}

/// The clause appended to a step's portability-preflight line: `portable`, or
/// the short labels of its gaps. Advisory gaps are included — the whole point of
/// the preflight half is answering "what would it take", and "it would take an
/// image" is part of that answer.
pub fn fleet_clause(step: &QedStep, camp_root: Option<&std::path::Path>) -> String {
    if !is_dispatchable_kind(step) {
        return format!("fleet = n/a (kind = {:?} runs on the camp)", step.kind);
    }
    let gaps = gaps(step, camp_root);
    if gaps.is_empty() {
        return "fleet = portable".to_string();
    }
    let blocking = gaps.iter().any(|g| g.blocking());
    let labels = gaps
        .iter()
        .map(|g| g.to_string())
        .collect::<Vec<_>>()
        .join("; ");
    if blocking {
        format!("fleet = NOT portable ({labels})")
    } else {
        format!("fleet = portable ({labels})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OnFail, ProducedArtifact, StepActivation};

    /// `QedStep` has no `Default` (and must not grow one — `enabled` defaults to
    /// *true* through serde, so a derived `Default` would silently disable every
    /// step built in code), hence the explicit literal.
    fn step(name: &str, argv: &[&str]) -> QedStep {
        QedStep {
            expect_slow: false,
            name: name.to_string(),
            argv: argv.iter().map(|s| s.to_string()).collect(),
            cwd: None,
            env: std::collections::HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: StepKind::Subprocess,
            image: None,
            tag: None,
            push: false,
            platforms: Vec::new(),
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            source_context: Vec::new(),
            cache: false,
            load: false,
            sub_pipeline: None,
            outputs: Vec::new(),
            inputs: Vec::new(),
            secret: false,
            gha_workflow: None,
            import: None,
            matrix: None,
            enabled: true,
            activation: StepActivation::default(),
            if_cond: None,
            background: false,
            background_until: None,
            wait_for: None,
            manual: None,
            manifest_stitch: None,
            platform: None,
            toolchain: None,
            needs: None,
            resource: None,
            participant: None,
        }
    }

    fn produced(path: &str) -> ProducedArtifact {
        ProducedArtifact {
            binary: "out".to_string(),
            path: path.to_string(),
            triple: None,
        }
    }

    /// The exact W235 §2 failure: `--where=remote` at an ordinary camp recipe.
    #[test]
    fn a_repo_relative_script_is_not_portable() {
        let s = step("check", &["./scripts/check-workspace-members.sh"]);
        let err = fleet_portable(&s, None).unwrap_err();
        assert_eq!(
            err,
            vec![PortabilityGap::SourceDoesNotTravel {
                camp_reference: "./scripts/check-workspace-members.sh".into()
            }],
        );
        // The remedy names what travels with `source_context`, not just the key.
        let remedy = err[0].remedy();
        assert!(remedy.contains("source_context"), "{remedy}");
        assert!(remedy.contains("GIT-TRACKED"), "{remedy}");
        assert!(remedy.contains("--no-same-owner"), "{remedy}");
        assert!(remedy.contains("mesofact-musl"), "{remedy}");
    }

    /// `argv = ["cargo", "check", "-p", …]` — the shape of most camp recipes.
    /// Its own variant, because "stop referencing `cargo`" is not advice.
    #[test]
    fn a_bare_build_tool_head_is_not_portable() {
        let s = step("check", &["cargo", "check", "-p", "cloud"]);
        assert!(matches!(
            fleet_portable(&s, None).unwrap_err().as_slice(),
            [PortabilityGap::BuildToolNeedsTheTree { tool }] if tool == "cargo"
        ));
        let remedy = fleet_portable(&s, None).unwrap_err()[0].remedy();
        assert!(remedy.contains("reads its manifest"), "{remedy}");
        assert!(remedy.contains("GIT-TRACKED"), "{remedy}");
        assert!(
            !remedy.contains("stop referencing"),
            "the path-shaped advice must not leak into the tool case: {remedy}"
        );
    }

    /// `rusty-v8-musl`'s remote step: a PATH lookup inside the builder image,
    /// an absolute produced path, a pinned image. It needs no camp source and
    /// must NOT be refused — it is one of the two pipelines that actually
    /// offloads today.
    #[test]
    fn a_self_sufficient_image_step_is_portable_without_source_context() {
        let mut s = step(
            "build-v8-musl",
            &["build-v8.sh 'x86_64-unknown-linux-musl' '/yah/produced/rusty-v8-x86_64-unknown-linux-musl.tar.gz'"],
        );
        s.image = Some("cr.yah.dev/rusty-v8-musl-builder:v149.4.0-amd64@sha256:7e9f".into());
        s.produces = vec![produced("/yah/produced/rusty-v8-x86_64-unknown-linux-musl.tar.gz")];
        assert_eq!(fleet_portable(&s, None), Ok(()));
        assert_eq!(gaps(&s, None), Vec::new());
        assert_eq!(fleet_clause(&s, None), "fleet = portable");
    }

    /// The `mesofact-musl` shape — the one recipe satisfying all three
    /// requirements, and the case the whole chain has to keep working end to
    /// end (it is also the recipe R555-B10's demotion was filed for).
    #[test]
    fn the_mesofact_musl_shape_passes_the_gate() {
        let mut s = step(
            "build-mesofact-x86_64-musl",
            &["set -eu\ncurl -fsSL \"$YAH_SOURCE_CONTEXT_URL\" -o /tmp/src.tar.gz\ntar --no-same-owner -xzf /tmp/src.tar.gz -C /work\ncd /work/oss/mesofact\ncargo build --release"],
        );
        s.source_context = vec![
            std::path::PathBuf::from("oss/mesofact"),
            std::path::PathBuf::from("oss/yah-base"),
        ];
        s.image = Some("cr.yah.dev/mesofact-musl-builder:v149.4.0-rust1.97-amd64@sha256:0f87".into());
        s.produces = vec![produced("/yah/produced/mesofact-x86_64-unknown-linux-musl.tar.gz")];
        assert_eq!(fleet_portable(&s, None), Ok(()));
    }

    /// Rule 1, the decisive one: a bare relative path with no `./` prefix that
    /// RESOLVES in the camp tree. This is the shape the first cut missed —
    /// `argv = ["bash", "scripts/smoke-yah-dev.sh"]` — and missing it made 21 of
    /// 44 camp recipes classify portable when almost none are.
    #[test]
    fn a_relative_path_that_exists_in_the_camp_tree_is_a_camp_reference() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        std::fs::create_dir_all(tmp.path().join("scripts")).expect("scripts dir");
        std::fs::write(tmp.path().join("scripts/smoke.sh"), "#!/bin/sh\n").expect("script");

        let argv = vec!["bash".to_string(), "scripts/smoke.sh".to_string()];
        assert_eq!(
            camp_tree_reference(&argv, Some(tmp.path())),
            Some("scripts/smoke.sh".to_string()),
        );
        // Same argv, no camp root to ground it: the pattern rules alone cannot
        // see it, and under-reporting is the documented degradation.
        assert_eq!(camp_tree_reference(&argv, None), None);
    }

    /// …and the other half of why rule 1 stats rather than pattern-matches: a
    /// sed expression, a CIDR block and a URL all contain slashes and none of
    /// them is a path. A "contains `/`, not absolute" rule flags all three.
    #[test]
    fn slashes_that_are_not_paths_are_not_camp_references() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        for argv in [
            vec!["bash".into(), "-c".into(), "echo x | sed 's/^/pre: /'".to_string()],
            vec!["ip".into(), "route".into(), "add".into(), "0.0.0.0/0".to_string()],
            vec!["curl".into(), "-fsSL".into(), "https://cdn.yah.dev/x.tar.gz".to_string()],
        ] {
            assert_eq!(
                camp_tree_reference(&argv, Some(tmp.path())),
                None,
                "argv: {argv:?}",
            );
        }
    }

    /// An inner `cargo build` after a source_context unpack is the CORRECT
    /// shape and must not be flagged — matching only `argv[0]` is what keeps
    /// the heuristic from refusing mesofact-musl.
    #[test]
    fn an_inner_cargo_after_an_unpack_is_not_a_camp_reference() {
        let argv = vec![
            "set -eu\ntar -xzf /tmp/src.tar.gz -C /work\ncd /work/oss/mesofact\ncargo build --release"
                .to_string(),
        ];
        assert_eq!(camp_tree_reference(&argv, None), None);
    }

    /// A produced path outside the durable mount is caught at preflight rather
    /// than at dispatch (R603-T5 enforces the same thing minutes later).
    #[test]
    fn a_produced_path_off_the_durable_dir_is_blocking() {
        let mut s = step("build", &["build.sh"]);
        s.image = Some("cr.yah.dev/whatever@sha256:0f87".into());
        s.produces = vec![produced("/tmp/out.tar.gz")];
        let err = fleet_portable(&s, None).unwrap_err();
        assert_eq!(
            err,
            vec![PortabilityGap::ProducesOffDurableDir {
                path: "/tmp/out.tar.gz".into()
            }],
        );
        assert!(err[0].remedy().contains("/yah/produced"));
    }

    /// The default forge image is reported and never refused: it is a real
    /// toolchain, and whether this argv fits inside it is not in the recipe.
    #[test]
    fn a_missing_image_is_advisory_not_blocking() {
        let s = step("build", &["build-v8.sh /yah/produced/out.tar.gz"]);
        assert_eq!(fleet_portable(&s, None), Ok(()));
        assert_eq!(gaps(&s, None), vec![PortabilityGap::DefaultForgeImage]);
        assert_eq!(
            fleet_clause(&s, None),
            "fleet = portable (no image (default yah-rust-bun))"
        );
    }

    /// Only subprocess steps are ever dispatched — a sub-pipeline under
    /// `--where=remote` still runs on the camp, so asking portability of it
    /// would refuse a run that was never leaving the box.
    #[test]
    fn a_non_subprocess_kind_is_not_asked() {
        let mut s = step("child", &[]);
        s.kind = StepKind::SubPipeline;
        assert!(!is_dispatchable_kind(&s));
        assert!(fleet_clause(&s, None).contains("n/a"));
    }
}
