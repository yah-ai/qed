//! Step executor + workflow walker.
//!
//! F4 walks the [`crate::graph::Plan`] wave by wave (sequentially within a
//! wave — concurrency is a later concern), evaluating `if:` at each tier and
//! running steps through [`run_step`]. `run:` blocks spawn `bash`, capture
//! `::set-output::` / `$GITHUB_OUTPUT` / `$GITHUB_ENV`, and thread results
//! into `steps.<id>.outputs.*` for the next step. `uses:` blocks route
//! through the tier-1/2 [`ToolkitRegistry`] (W224 R533-T7). A slug that isn't a
//! registered toolkit action is classified by [`crate::tier`]: a tier-3
//! service action becomes a [`RuntimeError::Tier3RequiresNative`] (import it as
//! a native QED step, don't run it); an unrecognized slug stays a loud
//! [`RuntimeError::UnknownAction`].
//!
//! @yah:ticket(R605-F2, "Docker/buildx-capable QED runner substrate for the image-yah-{base,rust,rust-bun} jobs (retire GitHub-hosted builders)")
//! @yah:status(review)
//! @yah:at(2026-08-16T20:09:12Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R605)
//! @yah:next("The setup-buildx/qemu `uses:` verifiers are already overridden in qed-gha (toolkit_builtin), but the image jobs still need a live docker/buildx daemon to run the builds. Provision QED runners with docker on the remote-runner tier rather than re-implementing a builder here.")
//! @yah:next("Route the image-yah-{base,rust,rust-bun} legs to a docker-capable node via the R555 remote-run placement + tier/quota grant, kamaji-admitted (signed recipes only, R555-F4).")
//! @yah:next("Reuse the R546 build-worker tier pattern (us-west-002) as the amd64 docker-capable substrate proof; measure pull+build+push to ghcr/registry.yah.dev.")
//! @yah:verify("A `yah qed run release` image slice builds and pushes image-yah-base to the registry from a QED-provisioned docker-capable runner with no GitHub-hosted builder in the loop")
//! @yah:depends_on(R555)
//! @yah:depends_on(R546)
//! @yah:depends_on(R563)
//! @yah:handoff("Fleet dispatch wired for docker/build-push-action: QedImageBuilder gained with_remote(tokio_handle, remote_driver, build_context_publisher) (oss/qed/crates/qed/src/image_overlay.rs) and do_build_push_remote, which reuses the SAME substrate the native `build-image` step kind already uses successfully (runner::execute_step_build_image_remote) -- ForgeCommand::BuildImage + RemoteForgeDriver + BuildContextPublisher + TaskPlacement::RemoteAny{tier, mesh_tags} -- instead of re-implementing a builder, per this ticket's own next-step warning. Not a second builder: same request shape, same tier=infra default, same context-pack-and-publish path (crate::build_context::pack_context).")
//! @yah:handoff("Opt-in, not default-on: a slug goes remote only when its W200 overlay entry sets config.remote = true (config.tier/config.arch override the infra/x86_64 defaults) -- remote_build_requested() in image_overlay.rs. Left off in the committed .yah/qed/gha-actions.toml (commented example added there explaining why: worker-side registry push creds aren't delivered yet -- R555-F5 -- and us-west-002 is documented ephemeral/often-offline). A camp/dev opts in via the per-machine overlay once a worker is confirmed reachable with push creds for the target registry.")
//! @yah:handoff("Wiring: runner.rs's execute_step_gha_workflow now captures tokio::runtime::Handle::current() + clones self.remote_driver/self.build_context_publisher BEFORE the spawn_blocking crossing (Handle::current() must be called from the async context, not the sync closure) and passes all three into QedImageBuilder::with_remote at the construction site (runner.rs ~4213). Made runner::tag_to_filename pub(crate) so image_overlay.rs derives the same collision-free publish-key stem instead of duplicating the mapping.")
//! @yah:handoff("Guard: a slug with config.remote=true but nothing wired (no with_remote call, e.g. a bare embedding) fails loudly with a named message rather than silently falling back to local docker -- matches NoBuildContextPublisher's existing refuse-loudly philosophy. Unit-tested.")
//! @yah:handoff("Multi-arch and push/load pass straight through unchanged: platforms (GHA's with.platforms CSV, e.g. `linux/amd64,linux/arm64`) maps to buildctl's --opt platform=<csv> on ONE dispatched worker, same as a real ubuntu-latest runner's qemu-backed buildx -- no per-arch split needed for release.yml's three image jobs.")
//! @yah:handoff("Explicitly NOT done (documented in code + the overlay comment, not silently dropped): outputs.digest/outputs.imageid come back empty on the remote leg -- no metadata-file readback exists for a remote build today (R555-F6's 'logs stream to QED/task pane' is still open, itself gated on R729's server-side GET /workloads/{id}/logs, currently a hard 501). This ticket's own verify criterion (build+push lands in the registry) does not need digest; a downstream cosign-sign step keyed on it does, and needs that separately-tracked work first -- same shape as F1's own next-step already flagged this coupling.")
//! @yah:verify("cargo test -p yah-qed --lib (cd oss/qed) -- 879 pass, 0 fail, 1 ignored (was 876 before this change +3 new: remote_not_requested_by_default, remote_config_parses_defaults_and_overrides, remote_opt_in_without_wiring_fails_loudly_not_silently_local). VERIFIED.")
//! @yah:verify("cargo check -p yah-qed (root workspace) -- clean. VERIFIED.")
//! @yah:verify("cargo check -p yah (root CLI crate, sanity pass for downstream consumers) -- pre-existing, UNRELATED failure at app/yah/cli/src/cli.rs:5044 (missing field `tool_args` on ToolInvocationData) caused by a live in-flight peer edit to crates/yah/policy-dsl (5 files uncommitted, not touched by this ticket) -- not this change; yah-qed itself is green.")
//! @yah:verify("STILL UNVERIFIABLE FROM THIS SANDBOX, and the ticket's own original criterion: a live `yah qed run release` image slice actually building+pushing image-yah-base from a real build-worker with no GitHub-hosted builder in the loop -- needs a live daemon with fleet config, a reachable build-worker, and confirmed registry push creds on that worker (R555-F5 gap noted above), none of which this sandbox has. Mirrors exactly how F1 (this same ticket's sibling) left its own cosign-verify criterion for a follow-up session with real infra access.")
//! @yah:gotcha("Worker-side registry push credentials are NOT delivered by this change or by anything else today -- BuildKit-in-containerd on the remote worker pushes using whatever containerd/docker registry auth is already configured ON THAT BOX (manual docker login, per us-west-002's machine-file bootstrap notes), not anything qed ships over the wire. R555-F5 (per-run ephemeral vault grants) is the ticket that closes this gap generally; until then, flipping config.remote=true only works against a worker an operator has pre-authed for the target registry.")
//! @yah:gotcha("R605-S6 (agent:bundle-anthropic-miravel, open spike on Firecracker-isolated x86 build capacity) flagged this ticket by name as adjacent-not-duplicate before I claimed it: S6 is about general build/job CAPACITY provisioning (a kamaji runtime backend), this ticket is about wiring the GHA-emulator's specific docker/build-push-action call site to existing capacity. No file overlap; no coordination collision found.")
//!
//! @yah:ticket(R605-T4, "Per-job shared-resource key so the qed-gha emulator can raise max_parallel_jobs above 1")
//! @yah:status(review)
//! @yah:at(2026-08-19T04:54:49Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R605)
//! @yah:next("Shipped in R605-F3: run_wave in qed-gha runtime.rs runs a wave on scoped threads up to Executor.max_parallel_jobs, honouring strategy.max-parallel per job underneath. The cap DEFAULTS TO 1, so nothing fans out yet.")
//! @yah:next("What is missing is a per-job shared-resource key: on GitHub each job gets a fresh runner, here every instance in a wave shares one Executor.workspace, one cargo target dir and one docker daemon. Two jobs that both write dist/ are independent in the DAG and destructive on the disk, and nothing in a workflow.yml says which pairs those are.")
//! @yah:next("Two candidate sources for the key, pick one: (a) parse GHA job-level concurrency.group, which qed-ghas Job struct does not carry today (workflow.rs:124) -- real GHA semantics, authored in the file; (b) a qed-side Executor.job_resources map the runner populates from the wrapping QedStep. (a) is the honest one; (b) is the escape hatch when the workflow author never wrote a group.")
//! @yah:gotcha("Do NOT raise the default cap without the key. The native-pipeline side got QedStep.resource for exactly this and defaults max_parallel to 4 only because an absent needs makes every legacy pipeline a serial chain -- the emulator has no such safety, since its waves are ALREADY wide and raising the cap fans out every wrapped workflow at once.")
//! @yah:verify("A workflow whose two same-wave jobs both write the same path runs them serially at max_parallel_jobs=4 once they share a resource key, and concurrently once they do not.")
//! @yah:handoff("Implemented option (a) from the ticket's own next-steps: job-level `concurrency:` (real GHA syntax) is now parsed into workflow.rs Job.concurrency (workflow.rs:124-136, parse.rs parse_job) reusing the existing Concurrency type/parse_concurrency helper that already served the workflow-level key.")
//! @yah:handoff("runtime.rs run_wave (R605-T4): resource_key_for() evaluates each in-wave instance's job.concurrency.group ExprString against that instance's own matrix/needs context, precomputed once per wave before any worker thread spawns. WaveSchedule gained running_resources: HashSet<String>; claim_next now blocks head-of-line on a resource key already in flight, same head-of-line-blocking semantics as the existing per-job strategy.max-parallel gate. release() frees both.")
//! @yah:handoff("No default-cap change: max_parallel_jobs still defaults to 1 per the ticket's own gotcha -- this only makes raising the cap safe for workflows that declare concurrency.group, it doesn't raise anything itself.")
//! @yah:verify("cargo test -p yah-qed-gha --lib (oss/qed) -- 135 pass, 0 fail, 0 ignored (was 39/39 in runtime::tests alone, +2 new: same_wave_jobs_sharing_a_concurrency_group_serialize_under_a_raised_cap, same_wave_jobs_with_distinct_concurrency_groups_still_run_concurrently). New tests assert wall-clock: two jobs sharing group:dist at cap 3 take >=1.9s (serialized) not <1.5s (would-be-concurrent); two jobs with distinct groups at cap 2 take <1.9s (still concurrent). VERIFIED.")
//! @yah:verify("cargo check -p yah-qed-gha (oss/qed) -- clean, no warnings. VERIFIED.")
//! @yah:verify("cargo check -p yah-qed (oss/qed, the crate's sole in-repo consumer) -- clean, confirms no other Job{} construction site broke from the new required field. VERIFIED.")
//!
//! @yah:relay(R785, "qed-gha: run: steps ignore working-directory under local/in-process execution")
//! @yah:at(2026-08-19T06:06:00Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//!
//! @yah:ticket(R785-B1, "run_bash_step ignores step-level working-directory — every `run:` step executes from repo root")
//! @yah:status(review)
//! @yah:at(2026-08-19T06:20:55Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R785)
//! @yah:severity(high)
//! @yah:verify("A `run:` step with working-directory: <subdir> set actually executes with that subdir as cwd under `yah qed run` local/in-process execution (new test in oss/qed/crates/qed-gha, e.g. asserting a `pwd`-equivalent step output matches the resolved workspace-joined path) — mirrors the existing runner.environment-parity precedent (R654-T1) of testing both GHA-hosted and local-execution behavior explicitly.")
//! @yah:verify("Re-running the exact failing steps from qed run 690455c1 (mesofact-build's 'Assert prod closure' step, yubaba-build's 'Build yubaba (static musl)' step) succeeds after the fix.")
//! @yah:verify("cargo test -p yah-qed-gha clean.")
//! @yah:gotcha("Root-caused by direct code read, not inferred: run_bash_step (oss/qed/crates/qed-gha/src/runtime.rs:1100-1145) is the ONLY executor for `run:` steps and always does `cmd.current_dir(&executor.workspace)` (repo root) — `step.working_directory` (parsed fine in parse.rs/workflow.rs) is never referenced anywhere in runtime.rs (`grep -n working_directory runtime.rs` = zero hits). So `working-directory: oss/<subdir>` on any `run:` step is silently a no-op under qed-gha's local/in-process execution, even though it's honored correctly on real GitHub-hosted runners — a correctness gap between the two execution modes qed-gha exists to keep identical (W200/W201).")
//! @yah:gotcha("Reproduced live in qed run 690455c1-78ef-4754-b6e5-1e6fa26afe73 (release.yml, 2026-08-19), TWO independent steps, same symptom: (1) `mesofact-build` job's 'Assert prod closure carries no dev affordances' step (working-directory: oss/mesofact) failed with `cargo tree -p mesofact --features deploy` -> 'error: cannot specify features for packages outside of workspace' — reproduces as CLEAN when the same command is run by hand from oss/mesofact, only fails when qed-gha resolves the cwd. (2) `yubaba-build` job's pre-existing 'Build yubaba (static musl)' step (working-directory: oss/yubaba, comment on that step already documents this exact failure mode and why working-directory is set) failed identically: `cross` warned 'unable to get metadata for package', fell back to host cargo, and host cargo hit the same 'cannot specify features for packages outside of workspace' from the wrong cwd. yubaba-build predates R746-F9 entirely, so this is not new/isolated to one job — it's a pre-existing, general qed-gha defect. A third, independent hit of the identical error string was found in an unrelated session's tool-result log (session:0feac5a8), confirming this recurs.")
//! @yah:gotcha("Blast radius in that one run: the yubaba-build failure cascaded to skip image-yah-yubaba, cli-build (x5 matrix), camp-build (x2), smoke — and then the publish-image-* jobs for base/rust/rust-bun/rust-sccache/rusty-v8-musl-builder/miniflare all FAILED (not skipped) trying to alias a `:smoke-<sha>` image tag that was never pushed this run, surfacing as `ghcr.io/yah-ai/<image>@sha256:...: not found`. That 'not found' is downstream damage from THIS bug, not a registry problem or evidence ghcr.io itself is broken — worth flagging on the ticket explicitly so nobody chases a phantom registry issue.")
//! @yah:gotcha("Flagged by R746-F9 (2026-08-19) as out of its blast radius rather than fixed inline; this ticket is that promised followup, filed with the actual root-caused location instead of a general pointer.")
//! @yah:assumes("Tier: Warrior — one executor function needs to join `executor.workspace` with `step.working_directory` (relative-path resolution matching GHA's own semantics: relative to workspace, absolute paths used as-is) plus a regression test; small in code size but touches the shared step-execution path every workflow runs through, so warrants care over a one-line patch.")
//! @yah:handoff("run_bash_step (runtime.rs) now resolves cwd via a new resolve_working_directory() helper implementing GHA's real precedence: step working-directory > job defaults.run.working-directory > workflow defaults.run.working-directory > executor.workspace. Relative values join onto executor.workspace (GHA semantics), absolute values pass through unchanged.")
//! @yah:handoff("job_defaults/workflow_defaults are computed once per job (both were parsed already by parse.rs but never read anywhere in runtime.rs) and threaded through run_step -> run_bash_step as new trailing params. run_uses_step is untouched -- GHA's working-directory has no effect on uses: steps.")
//! @yah:handoff("GITHUB_WORKSPACE stays pinned to executor.workspace (unchanged) -- only the shell's actual cwd moves; that matches GHA, where GITHUB_WORKSPACE is always the repo root regardless of a step's working-directory.")
//! @yah:verify("cargo test -p yah-qed-gha --lib (cd oss/qed) -- 139 pass, 0 fail (was 135; +4 new: step_working_directory_moves_the_shell_cwd, job_defaults_working_directory_applies_without_a_step_override, step_working_directory_overrides_job_defaults, and a live-fixture test below). VERIFIED.")
//! @yah:verify("cargo check -p yah-qed-gha -- clean, no warnings. VERIFIED.")
//! @yah:verify("cargo check -p yah-qed (oss/qed's sole in-repo consumer) -- clean (4m build, one pre-existing unrelated warning in yah-object-store/r2.rs, not touched by this change). VERIFIED.")
//! @yah:verify("New test roundtrip_tests::mesofact_prod_closure_step_runs_from_its_declared_working_directory extracts the ACTUAL 'Assert prod closure carries no dev affordances' step from the real .github/workflows/release.yml (not a copy), runs it through execute_workflow with executor.workspace = the real repo root, and asserts Success -- this is the exact mesofact-build step that failed in qed run 690455c1. Passes after the fix. VERIFIED live against the real oss/mesofact tree.")
//! @yah:verify("Also manually reproduced by hand: `cd oss/mesofact && cargo tree -p mesofact --features deploy -e normal` succeeds (no dev-affordance hits) confirming the command itself is fine and the bug was purely the cwd. VERIFIED.")
//! @yah:verify("yubaba-build's 'Build yubaba (static musl)' step (the ticket's other named repro) needs `cross` + a docker/musl cross toolchain + network -- STILL UNVERIFIABLE FROM THIS SANDBOX. Same class of fix applies (working-directory: oss/yubaba is now honored the same way as oss/mesofact's), but a live re-run of qed run needs real infra this sandbox doesn't have -- mirrors the precedent left by this ticket's own sibling (R605-F2's verify notes).")

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use indexmap::IndexMap;
use thiserror::Error;

use crate::expr::{self, Context, ExprError, JobStatus, Value};
use crate::expr_str::{ExprString, ExprToken};
use crate::graph::{
    build_context_for_instance, evaluate_outputs, plan as build_plan, CompletedInstance,
    GraphError, JobInstance, JobResult, Plan,
};
use crate::tier::{classify_uses, Disposition, NativeReplacement};
use crate::toolkit::{Lookup, StepConclusion, ToolkitCall, ToolkitRegistry};
#[cfg(test)]
use crate::toolkit::ToolkitOutcome;
use crate::workflow::{Job, RunDefaults, Step, StepAction, Workflow};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("expression error in {site}: {source}")]
    Expr {
        site: String,
        #[source]
        source: ExprError,
    },
    #[error("graph error: {0}")]
    Graph(#[from] GraphError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unrecognized action `{slug}` — not a tier-1/2 toolkit action and not in the tier-3 native-replacement catalog. Import it as a native QED step (W224 R533-T7)")]
    UnknownAction { slug: String },
    #[error("tier-3 action `{slug}` is replaced by a native QED facility — {replacement}. Import it, don't run it: {stanza}")]
    Tier3RequiresNative {
        slug: String,
        replacement: String,
        stanza: String,
    },
    #[error("toolkit action `{slug}` failed: {message}")]
    ToolkitFailed { slug: String, message: String },
}

/// Public executor handle. Workflow-level inputs (github / inputs / runner_os)
/// stay on the executor so a single instance can run several workflows; the
/// tier-1/2 [`ToolkitRegistry`] is owned here so callers wire the built-in
/// toolkit actions once (via [`Executor::new`]).
pub struct Executor {
    pub workspace: PathBuf,
    pub registry: ToolkitRegistry,
    pub github: Value,
    pub inputs: Value,
    pub runner_os: String,
    /// Host arch in the GHA `runner.arch` vocabulary (`X64` / `ARM64` / …).
    /// Defaults to the running host (see [`detect_runner_arch`]). The QED
    /// runner overwrites this from its self-detected host triple (R531-T1)
    /// so a workflow gating on `runner.arch` sees the real host it's running
    /// on, not just `runner.os`.
    pub runner_arch: String,
    /// `runner.environment` — `github-hosted` or `self-hosted` in GHA's
    /// vocabulary. Defaults to the running host (see
    /// [`detect_runner_environment`]), which is `self-hosted` everywhere
    /// except inside a GitHub-hosted runner. Workflows gate their
    /// runner-shape steps on this (relocating Docker's storage onto the
    /// hosted scratch volume, `sudo apt-get install`ing what the hosted
    /// image lacks) so those steps no-op when QED runs the same workflow on
    /// a dev box or a fleet slot.
    pub runner_environment: String,
    /// Forward the parent process env into step subprocesses. Tests usually
    /// want this off so the workflow env is hermetic; production wants it on
    /// so steps see PATH, HOME, etc.
    pub env_passthrough: bool,
    /// R499-F3 (phase 2): restrict execution to specific matrix instances.
    /// Keys are [`JobInstance::key`] (`<job_id>` for non-matrix jobs,
    /// `<job_id>#<row>` for matrix rows). `None` runs every planned
    /// instance (back-compat). `Some(set)` skips any instance whose key
    /// is not in the set — they surface as [`JobResult::Skipped`] so
    /// `needs.X.result` aggregation stays correct (skipped is neither
    /// success nor failure, so dependent jobs behave the same as a GHA
    /// `if:` skip). Empty set is a caller bug; validate at the qed-runner
    /// boundary, not here.
    pub included_instance_keys: Option<std::collections::HashSet<String>>,
    /// Restrict execution to matrix rows whose dimension VALUES match — the
    /// declarative sibling of [`Self::included_instance_keys`].
    ///
    /// Each entry is `dimension => required value`, and an instance is skipped
    /// only when it *has* that dimension and its value differs. An instance with
    /// no matrix at all, or whose matrix lacks the dimension, is unaffected: this
    /// narrows a fan-out, it does not disable jobs. So filtering
    /// `{"board": "rpi_zero2w"}` over a workflow whose `build` job fans out on
    /// `board` and whose `lint` job does not runs one `build` row and the whole
    /// `lint` job.
    ///
    /// This exists because `included_instance_keys` is POSITIONAL (`build#0`),
    /// and a position is not a thing a pipeline author knows or should have to
    /// track: inserting a value into the matrix silently repoints every key after
    /// it. The dashboard's operator-driven picker seeds keys from an observed run,
    /// where positions are real; a checked-in pipeline TOML has to say what it
    /// means. Both compose (AND) when set.
    pub matrix_filter: std::collections::HashMap<String, String>,
    /// Optional live-event sink (W200 R487 follow-up). When set,
    /// [`execute_workflow`] emits a [`crate::GhaEvent`] at each job/step
    /// boundary so the qed-runner can mirror the nested tree into its own
    /// [`crate::QedEvent`] stream. When `None` the runtime is silent and
    /// the only observable surface is the returned [`WorkflowRun`].
    pub events: Option<crate::events::GhaEventSink>,
    /// Pre-resolved `secrets.*` context (R487 follow-up). On GHA, the
    /// runner injects secrets from the repo + org settings. On QED the
    /// caller resolves them up-front from a name-bridge mapping (e.g.
    /// `~/.yah/qed/secrets.toml`) and lays them onto the executor as a
    /// plain `Value::Object` so the expression evaluator sees the same
    /// `${{ secrets.X }}` shape it sees on GHA. `Value::Object(empty)` by
    /// default — a workflow that references an undefined secret evaluates
    /// to the empty string (matches GHA behavior for unset secrets).
    pub secrets: crate::expr::Value,
    /// Optional injected image builder for the docker push family (R594). When
    /// `Some`, `docker/login-action` + `docker/build-push-action` route here
    /// (registry route/auth + local-buildx or remote-fleet build) instead of
    /// the tier-3 `RegistryPublish` error. `None` (the default) preserves the
    /// honest "replace with a native build-image step" error, so the bare crate
    /// still never shells `docker`. Set via [`Executor::with_image_builder`].
    pub image_builder: Option<std::sync::Arc<dyn crate::image_builder::ImageBuilder>>,
    /// Optional injected artifact store for `actions/upload-artifact` /
    /// `actions/download-artifact` (R594). Same injection gate as
    /// [`Self::image_builder`]: `None` (default) keeps the tier-3 error.
    pub artifact_store: Option<std::sync::Arc<dyn crate::artifact_store::ArtifactStore>>,
    /// R605-F3 half (a): ceiling on job instances executing at once within one
    /// wave. **Defaults to 1 — serial**, which is the behaviour this runtime
    /// had from F4 up to the moment the cap existed.
    ///
    /// The waves themselves were always computed correctly by
    /// [`crate::graph::build_plan`]; only the executor was serial, so turning
    /// concurrency on is a scheduling change and not a correctness one *for the
    /// graph*. It is a correctness question for the **workspace**, which is why
    /// the default stays 1: on GitHub each job gets a fresh runner, and here
    /// every instance in a wave shares one [`Self::workspace`], one cargo
    /// `target/` and one docker daemon. Two jobs that both write `dist/` are
    /// independent in the DAG and destructive on the disk, and nothing in a
    /// `workflow.yml` says which pairs those are.
    ///
    /// What would let this default higher is a per-job shared-resource key —
    /// the emulator-side analogue of
    /// [`QedStep::resource`](../../yah_qed/types/struct.QedStep.html). Until
    /// one exists, the global cap *is* the resource guard, at its most
    /// conservative setting, and raising it is the caller asserting that this
    /// particular workflow's parallel jobs don't share output paths.
    ///
    /// A job's own `strategy.max-parallel` is honoured underneath this cap: it
    /// bounds concurrent rows *of that job*, exactly as on GitHub.
    pub max_parallel_jobs: usize,
}

impl Executor {
    /// New executor with the tier-1/2 toolkit actions pre-registered. This is
    /// the right default for production callers — a workflow whose `uses:`
    /// only references toolkit-contract compute slugs (`rust-toolchain`,
    /// `setup-bun`, the buildx/qemu setup verifiers, `cosign-installer`) runs
    /// straight through without extra wiring.
    pub fn new(workspace: impl Into<PathBuf>) -> Self {
        let mut e = Self::bare(workspace);
        crate::toolkit_builtin::register_toolkit(&mut e.registry);
        e
    }

    /// Empty-registry executor for tests that want hermetic dispatch (no
    /// built-ins, no `rustup`/`bun`/`docker` shelled out by accident). Used to
    /// assert the unknown-action / tier-3 dispatch errors fire.
    pub fn bare(workspace: impl Into<PathBuf>) -> Self {
        Self {
            workspace: workspace.into(),
            registry: ToolkitRegistry::new(),
            github: Value::object(),
            inputs: Value::object(),
            runner_os: detect_runner_os().into(),
            runner_arch: detect_runner_arch().into(),
            runner_environment: detect_runner_environment(),
            env_passthrough: true,
            included_instance_keys: None,
            matrix_filter: std::collections::HashMap::new(),
            events: None,
            secrets: Value::object(),
            image_builder: None,
            artifact_store: None,
            // R605-F3: serial by default — see the field docs for why the
            // emulator does not get to fan out on its own initiative.
            max_parallel_jobs: 1,
        }
    }

    /// Raise the within-wave concurrency ceiling (R605-F3 half (a)). See
    /// [`Self::max_parallel_jobs`] for what the caller is asserting by doing
    /// so. `0` is clamped to `1`.
    pub fn with_max_parallel_jobs(mut self, n: usize) -> Self {
        self.max_parallel_jobs = n.max(1);
        self
    }

    /// Configure an event sink. The returned executor emits one
    /// [`crate::GhaEvent`] per job/step boundary plus one per captured bash
    /// output line. Pre-existing callers that don't care leave it as `None`.
    pub fn with_events(mut self, sink: crate::events::GhaEventSink) -> Self {
        self.events = Some(sink);
        self
    }

    /// Configure the pre-resolved `secrets.*` context. See [`Executor::secrets`].
    pub fn with_secrets(mut self, secrets: crate::expr::Value) -> Self {
        self.secrets = secrets;
        self
    }

    /// Inject an image builder for the docker push family (R594). Enables the
    /// runtime to actually build + push the image jobs in a workflow instead of
    /// declining them. See [`Executor::image_builder`].
    pub fn with_image_builder(
        mut self,
        builder: std::sync::Arc<dyn crate::image_builder::ImageBuilder>,
    ) -> Self {
        self.image_builder = Some(builder);
        self
    }

    /// Inject an artifact store for `actions/upload-artifact` /
    /// `actions/download-artifact` (R594). See [`Executor::artifact_store`].
    pub fn with_artifact_store(
        mut self,
        store: std::sync::Arc<dyn crate::artifact_store::ArtifactStore>,
    ) -> Self {
        self.artifact_store = Some(store);
        self
    }
}

fn detect_runner_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "macOS",
        "linux" => "Linux",
        "windows" => "Windows",
        _ => "Linux",
    }
}

/// Host arch in the GHA `runner.arch` vocabulary. Mirrors the mapping in
/// `qed::platform::gha_runner_arch`, kept here so qed-gha stays free of a
/// dep edge back onto the qed runner crate.
fn detect_runner_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "X64",
        "aarch64" | "arm64" => "ARM64",
        "x86" | "i686" => "X86",
        "arm" => "ARM",
        _ => "X64",
    }
}

/// `runner.environment` for this host. GitHub's own runner exports
/// `RUNNER_ENVIRONMENT` into every step, so when QED is itself running inside
/// a GHA job (a QED pipeline invoked from a workflow) we inherit and report
/// the truth. Everywhere else — a dev box, a fleet slot — a QED run is not on
/// a GitHub-hosted runner, and `self-hosted` is GHA's word for that.
///
/// Deliberately NOT keyed off `GITHUB_ACTIONS`: that variable says "some GHA
/// runner is in the picture", not which kind, and a self-hosted GHA runner
/// sets it too.
fn detect_runner_environment() -> String {
    match std::env::var("RUNNER_ENVIRONMENT") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => "self-hosted".into(),
    }
}

// ─── results ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WorkflowRun {
    pub instances: Vec<InstanceRun>,
}

impl WorkflowRun {
    pub fn instance(&self, job_id: &str) -> Option<&InstanceRun> {
        self.instances.iter().find(|i| i.job_id == job_id)
    }

    pub fn instance_at(&self, job_id: &str, matrix_index: usize) -> Option<&InstanceRun> {
        self.instances
            .iter()
            .find(|i| i.job_id == job_id && i.matrix_index == Some(matrix_index))
    }
}

#[derive(Debug, Clone)]
pub struct InstanceRun {
    pub job_id: String,
    pub matrix_index: Option<usize>,
    pub result: JobResult,
    pub steps: Vec<StepResult>,
    pub outputs: IndexMap<String, Value>,
    /// Human-readable cause when [`Self::result`] is [`JobResult::Skipped`] —
    /// which `needs:` dependency didn't succeed, the `if:` text that
    /// evaluated falsy, or the instance-selector/matrix filter that excluded
    /// this row (R330-B41). `None` for every other result, and for a
    /// `Skipped` result reached by a path this runtime doesn't yet label.
    pub skip_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StepResult {
    pub step_id: Option<String>,
    pub name: Option<String>,
    pub conclusion: StepConclusion,
    pub outputs: IndexMap<String, Value>,
    pub stdout: String,
    pub stderr: String,
}

// ─── workflow walker ───────────────────────────────────────────────────────

/// Should this instance be skipped rather than run? Both selectors compose with
/// AND: an instance has to survive the positional key set (when one is given)
/// *and* every matrix-value constraint. Returns the skip reason to surface on
/// the [`InstanceRun`] (R330-B41), or `None` when the instance should run.
///
/// See [`Executor::matrix_filter`] for why a value constraint over a dimension
/// the instance does not have is a no-op rather than an exclusion.
fn instance_excluded(executor: &Executor, instance: &JobInstance) -> Option<String> {
    if let Some(set) = executor.included_instance_keys.as_ref() {
        if !set.contains(&instance.key()) {
            return Some(format!(
                "instance selector excluded `{}` (not in the requested run set)",
                instance.key()
            ));
        }
    }
    if executor.matrix_filter.is_empty() {
        return None;
    }
    let Some(Value::Object(row)) = instance.matrix.as_ref() else {
        // No matrix (or a matrix that isn't an object): nothing to constrain.
        return None;
    };
    for (dim, required) in &executor.matrix_filter {
        if let Some(actual) = row.get(dim) {
            let actual = actual.as_str_lossy();
            if &actual != required {
                return Some(format!(
                    "matrix filter {dim}={required} excluded this row ({dim}={actual})"
                ));
            }
        }
    }
    None
}

pub fn execute_workflow(
    workflow: &Workflow,
    executor: &Executor,
) -> Result<WorkflowRun, RuntimeError> {
    // R654-F2: the matrix is expanded against the workflow-level context, so a
    // `strategy.matrix.<dim>: ${{ fromJSON(inputs.x && … || '[…]') }}` narrows
    // the fan-out from the caller's inputs. `vars` has no executor field yet;
    // it stays empty rather than being faked from `env`.
    let plan_ctx = crate::graph::PlanContext {
        github: executor.github.clone(),
        inputs: executor.inputs.clone(),
        vars: Value::object(),
    };
    let plan: Plan = build_plan(workflow, &plan_ctx)?;
    let mut completed: Vec<CompletedInstance> = Vec::new();
    let mut runs: Vec<InstanceRun> = Vec::new();

    for wave in &plan.waves {
        // R605-F3 half (a): the instances of one wave are independent by
        // construction, so they may run concurrently — bounded by
        // `executor.max_parallel_jobs` (default 1, i.e. the F4 serial
        // behaviour) and, per job, by its own `strategy.max-parallel`.
        //
        // Results are folded back in WAVE order, not completion order:
        // `completed` feeds the next wave's `needs.*` aggregation and
        // `runs` is the returned transcript, and neither should depend on
        // which of two independent jobs happened to finish first.
        for run in run_wave(wave, workflow, executor, &completed)? {
            completed.push(CompletedInstance {
                job_id: run.job_id.clone(),
                matrix_index: run.matrix_index,
                result: run.result,
                outputs: run.outputs.clone(),
            });
            runs.push(run);
        }
    }

    Ok(WorkflowRun { instances: runs })
}

/// Execute one wave's instances, up to `executor.max_parallel_jobs` at a time,
/// returning their runs in wave order (R605-F3).
///
/// Scoped OS threads rather than a runtime: [`run_instance`] is synchronous all
/// the way down (it shells out with [`std::process::Command`]), so there is
/// nothing to `await` and no executor to borrow into. `&Executor`, `&Workflow`
/// and `&[CompletedInstance]` are shared immutably across the workers.
///
/// The serial path is preserved exactly — cap 1 or a single-instance wave takes
/// the same in-line loop it always did, spawning no threads at all — so the
/// default configuration cannot regress on a scheduler it never enters.
fn run_wave(
    wave: &[JobInstance],
    workflow: &Workflow,
    executor: &Executor,
    completed: &[CompletedInstance],
) -> Result<Vec<InstanceRun>, RuntimeError> {
    let cap = executor.max_parallel_jobs.max(1).min(wave.len().max(1));
    if cap == 1 {
        return wave
            .iter()
            .map(|instance| run_one(instance, workflow, executor, completed))
            .collect();
    }

    // R605-T4: resolve each instance's shared-resource key (its job's
    // `concurrency.group`, evaluated against that instance's own matrix/needs
    // context) up front, in the parent thread, before any worker exists to
    // race the evaluation. `None` means the job declared no `concurrency:` —
    // free to run alongside anything else the cap admits.
    let resource_keys: Vec<Option<String>> = wave
        .iter()
        .map(|instance| resource_key_for(instance, workflow, executor, completed))
        .collect::<Result<Vec<_>, _>>()?;

    // Slot per instance, filled in place so wave order survives.
    let slots: Vec<std::sync::Mutex<Option<Result<InstanceRun, RuntimeError>>>> =
        (0..wave.len()).map(|_| std::sync::Mutex::new(None)).collect();
    let sched = std::sync::Mutex::new(WaveSchedule {
        next: 0,
        running_per_job: std::collections::HashMap::new(),
        running_resources: std::collections::HashSet::new(),
    });
    let idle = std::sync::Condvar::new();

    std::thread::scope(|scope| {
        for _ in 0..cap {
            scope.spawn(|| loop {
                let Some(i) = claim_next(&sched, &idle, wave, workflow, &resource_keys) else {
                    return;
                };
                let outcome = run_one(&wave[i], workflow, executor, completed);
                *slots[i].lock().expect("wave slot") = Some(outcome);
                sched
                    .lock()
                    .expect("wave schedule")
                    .release(&wave[i].job_id, resource_keys[i].as_deref());
                // A finished instance can unblock a row its job's own
                // `max-parallel` was holding back, or another job waiting on
                // the same resource key.
                idle.notify_all();
            });
        }
    });

    // First error in wave order wins, for the same reason the runs are ordered:
    // which of two concurrent failures landed first is not a fact about the
    // workflow.
    let mut out = Vec::with_capacity(wave.len());
    for slot in slots {
        match slot.into_inner().expect("wave slot") {
            Some(Ok(run)) => out.push(run),
            Some(Err(e)) => return Err(e),
            // Unreachable: every index is claimed exactly once before the
            // workers exit, and a panicking worker propagates out of `scope`.
            None => unreachable!("wave slot never filled"),
        }
    }
    Ok(out)
}

/// Who is next, and is anyone allowed to take them. Guarded by one mutex; the
/// per-job counter is what makes a job's `strategy.max-parallel` mean the same
/// thing here as it does on GitHub. `running_resources` (R605-T4) is the
/// shared-resource-key guard: an instance whose key is already in this set
/// blocks, the emulator-side analogue of GHA's per-job fresh runner meaning
/// two instances never actually contend for the same disk.
struct WaveSchedule {
    /// Next unclaimed wave index. Instances are claimed strictly in order, so a
    /// job blocked on its own `max-parallel` blocks the ones behind it too —
    /// the simple reading, and the one that keeps a large matrix from
    /// starving whatever follows it.
    next: usize,
    running_per_job: std::collections::HashMap<String, usize>,
    running_resources: std::collections::HashSet<String>,
}

impl WaveSchedule {
    fn release(&mut self, job_id: &str, resource_key: Option<&str>) {
        if let Some(n) = self.running_per_job.get_mut(job_id) {
            *n = n.saturating_sub(1);
        }
        if let Some(key) = resource_key {
            self.running_resources.remove(key);
        }
    }
}

/// A job's `concurrency.group` (real GHA syntax, R605-T4 half (a)),
/// evaluated against this instance's own matrix/needs context, as the
/// scheduler's shared-resource key. `None` when the job carries no
/// `concurrency:` block — the emulator asserts nothing about that job's disk
/// footprint and lets the cap alone govern it.
///
/// Evaluated once per instance, in the parent thread, before any worker spawns
/// — cheap (no I/O, no subprocess) and keeps the key stable for the whole wave
/// rather than re-derived per claim attempt.
fn resource_key_for(
    instance: &JobInstance,
    workflow: &Workflow,
    executor: &Executor,
    completed: &[CompletedInstance],
) -> Result<Option<String>, RuntimeError> {
    let Some(job) = workflow.jobs.get(&instance.job_id) else {
        return Ok(None);
    };
    let Some(concurrency) = job.concurrency.as_ref() else {
        return Ok(None);
    };
    let ctx = build_context_for_instance(
        instance,
        workflow,
        completed,
        executor.github.clone(),
        executor.inputs.clone(),
        crate::graph::RunnerInfo {
            os: &executor.runner_os,
            arch: &executor.runner_arch,
            environment: &executor.runner_environment,
        },
        executor.secrets.clone(),
    )?;
    let group = crate::graph::eval_exprstring(&concurrency.group, &ctx).map_err(|source| {
        RuntimeError::Expr {
            site: format!("jobs.{}.concurrency.group", instance.job_id),
            source,
        }
    })?;
    Ok(Some(group.as_str_lossy()))
}

/// A job's own `strategy.max-parallel`, or `usize::MAX` when it declares none.
fn job_row_cap(workflow: &Workflow, job_id: &str) -> usize {
    workflow
        .jobs
        .get(job_id)
        .and_then(|j| j.strategy.as_ref())
        .and_then(|s| s.max_parallel)
        .map(|n| (n as usize).max(1))
        .unwrap_or(usize::MAX)
}

/// Take the next instance a worker is allowed to run, blocking while one is
/// pending but capped, or while its resource key is already in flight
/// (R605-T4). `None` once the wave is exhausted.
fn claim_next(
    sched: &std::sync::Mutex<WaveSchedule>,
    idle: &std::sync::Condvar,
    wave: &[JobInstance],
    workflow: &Workflow,
    resource_keys: &[Option<String>],
) -> Option<usize> {
    let mut guard = sched.lock().expect("wave schedule");
    loop {
        if guard.next >= wave.len() {
            return None;
        }
        let i = guard.next;
        let job_id = &wave[i].job_id;
        let running = guard.running_per_job.get(job_id).copied().unwrap_or(0);
        let resource_free = resource_keys[i]
            .as_deref()
            .map(|key| !guard.running_resources.contains(key))
            .unwrap_or(true);
        if running < job_row_cap(workflow, job_id) && resource_free {
            guard.next += 1;
            *guard.running_per_job.entry(job_id.clone()).or_insert(0) += 1;
            if let Some(key) = resource_keys[i].as_deref() {
                guard.running_resources.insert(key.to_string());
            }
            return Some(i);
        }
        // Head-of-line blocked by its own job's row cap or by a resource key
        // another in-flight instance holds; wait for a release.
        guard = idle.wait(guard).expect("wave schedule");
    }
}

/// Run (or short-circuit) one instance — the body the serial loop used to hold
/// inline, unchanged except for being callable from a worker thread.
fn run_one(
    instance: &JobInstance,
    workflow: &Workflow,
    executor: &Executor,
    completed: &[CompletedInstance],
) -> Result<InstanceRun, RuntimeError> {
    // R499-F3 phase 2: instance filter. Non-selected rows short-circuit
    // to Skipped — same path as a GHA `if: false` — so needs aggregation
    // (failure > cancelled > skipped > success) and downstream `if:`
    // checks still see them.
    if let Some(reason) = instance_excluded(executor, instance) {
        return Ok(InstanceRun {
            job_id: instance.job_id.clone(),
            matrix_index: instance.matrix_index,
            result: JobResult::Skipped,
            steps: vec![],
            outputs: IndexMap::new(),
            skip_reason: Some(reason),
        });
    }
    emit_job_started(executor, instance, workflow);
    let r = run_instance(instance, workflow, executor, completed)?;
    emit_job_finished(executor, instance, &r);
    Ok(r)
}

fn run_instance(
    instance: &JobInstance,
    workflow: &Workflow,
    executor: &Executor,
    completed: &[CompletedInstance],
) -> Result<InstanceRun, RuntimeError> {
    let job = workflow
        .jobs
        .get(&instance.job_id)
        .expect("plan only references known jobs");

    // GHA implicit needs-gate: a job with no explicit `if:` runs only when
    // every job in its `needs:` succeeded. A failed / cancelled / skipped
    // dependency short-circuits the job to Skipped — matching GHA, where a
    // dependent job is skipped unless it opts in via an explicit `if:`
    // (`always()` / a `needs.X.result` check). Without this gate a consumer
    // job (e.g. `image-yah-yubaba`, which downloads `yubaba-bins-*`) runs
    // even when its producer (`yubaba-build`) failed and uploaded nothing,
    // so its `actions/download-artifact` step hits an empty store. The fix
    // is structural: the consumer never runs, so the failure surfaces at the
    // producing job — not as a bogus download error three waves later
    // (R516-B1).
    if let Some(reason) = needs_gate_passes(job, completed) {
        return Ok(InstanceRun {
            job_id: instance.job_id.clone(),
            matrix_index: instance.matrix_index,
            result: JobResult::Skipped,
            steps: vec![],
            outputs: IndexMap::new(),
            skip_reason: Some(reason),
        });
    }

    // Pre-step context: matrix + needs + env composed but ctx.steps empty.
    let mut ctx = build_context_for_instance(
        instance,
        workflow,
        completed,
        executor.github.clone(),
        executor.inputs.clone(),
        crate::graph::RunnerInfo {
            os: &executor.runner_os,
            arch: &executor.runner_arch,
            environment: &executor.runner_environment,
        },
        executor.secrets.clone(),
    )?;

    if !should_run_job(job, &ctx)? {
        let reason = job
            .if_cond
            .as_ref()
            .map(|c| format!("if: `{}` evaluated false", c.raw_source()))
            .unwrap_or_else(|| "if: evaluated false".to_string());
        return Ok(InstanceRun {
            job_id: instance.job_id.clone(),
            matrix_index: instance.matrix_index,
            result: JobResult::Skipped,
            steps: vec![],
            outputs: IndexMap::new(),
            skip_reason: Some(reason),
        });
    }

    // Per-job step accumulator + env file (updates from $GITHUB_ENV flow
    // forward to subsequent steps).
    let mut steps_obj: IndexMap<String, Value> = IndexMap::new();
    let mut env_overlay: IndexMap<String, String> = IndexMap::new();
    let mut step_results: Vec<StepResult> = Vec::new();
    let mut job_failed = false;
    let job_cancelled = false;

    // R785-B1: `working-directory:` precedence source, resolved once per job
    // (neither changes per step) and threaded down to each `run:` step.
    let job_defaults = job.defaults.as_ref().and_then(|d| d.run.as_ref());
    let workflow_defaults = workflow.defaults.as_ref().and_then(|d| d.run.as_ref());

    for (idx, step) in job.steps.iter().enumerate() {
        // Refresh ctx.steps from the accumulator before each step so the
        // current step can see outputs from prior steps in this job.
        ctx.steps = Value::Object(steps_obj.clone());
        ctx.env = merge_env_overlay(&ctx.env, &env_overlay);
        ctx.job_status = Some(if job_failed {
            JobStatus::Failure
        } else if job_cancelled {
            JobStatus::Cancelled
        } else {
            JobStatus::Success
        });

        let run_this = should_run_step(step, &ctx, job_failed || job_cancelled)?;
        let synthetic_id = step
            .id
            .clone()
            .unwrap_or_else(|| format!("__step{idx}"));

        if !run_this {
            let skipped = StepResult {
                step_id: step.id.clone(),
                name: step.name.as_ref().and_then(exprstring_static),
                conclusion: StepConclusion::Skipped,
                outputs: IndexMap::new(),
                stdout: String::new(),
                stderr: String::new(),
            };
            steps_obj.insert(synthetic_id, step_value(&skipped));
            step_results.push(skipped);
            continue;
        }

        emit_step_started(executor, instance, idx, step);
        let res = run_step(
            step,
            &ctx,
            executor,
            &env_overlay,
            instance,
            idx,
            job_defaults,
            workflow_defaults,
        )?;
        emit_step_finished(executor, instance, idx, &res);
        // Workflow commands setting env (`echo "K=V" >> $GITHUB_ENV`) bleed
        // into subsequent steps in the same job.
        if let Some(env_updates) = pop_env_updates(&res) {
            for (k, v) in env_updates {
                env_overlay.insert(k, v);
            }
        }
        let failed = matches!(res.conclusion, StepConclusion::Failure);
        let continue_on_error = step.continue_on_error.unwrap_or(false);
        steps_obj.insert(synthetic_id, step_value(&res));
        step_results.push(res);
        if failed && !continue_on_error {
            job_failed = true;
            // Remaining steps still get to run if their if: opts in to
            // failure() / always() — we keep iterating but with job_status
            // flipped so success() short-circuits.
        }
    }

    // Job outputs evaluate against final steps context.
    ctx.steps = Value::Object(steps_obj.clone());
    ctx.env = merge_env_overlay(&ctx.env, &env_overlay);
    let outputs = evaluate_outputs(&job.outputs, &ctx)?;

    let result = if job_failed {
        JobResult::Failure
    } else if job_cancelled {
        JobResult::Cancelled
    } else {
        JobResult::Success
    };

    Ok(InstanceRun {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        result,
        steps: step_results,
        outputs,
        skip_reason: None,
    })
}

fn merge_env_overlay(env: &Value, overlay: &IndexMap<String, String>) -> Value {
    let mut map = match env {
        Value::Object(m) => m.clone(),
        _ => IndexMap::new(),
    };
    for (k, v) in overlay {
        map.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(map)
}

fn step_value(step: &StepResult) -> Value {
    let mut entry = IndexMap::new();
    entry.insert(
        "outputs".to_string(),
        Value::Object(step.outputs.clone()),
    );
    entry.insert(
        "conclusion".to_string(),
        Value::String(step.conclusion.as_str().into()),
    );
    // GHA distinguishes outcome (pre-continue-on-error) from conclusion
    // (post-) — they coincide unless continue-on-error is set. Coincidence
    // is the right default for F4; F5 can split when we add a fixture that
    // needs it.
    entry.insert(
        "outcome".to_string(),
        Value::String(step.conclusion.as_str().into()),
    );
    Value::Object(entry)
}

// ─── if-cond eval (job + step) ─────────────────────────────────────────────

/// GHA status-check functions. Their presence anywhere in a job `if:` is what
/// makes GHA drop the implicit `success()` needs-gate — *not* the mere presence
/// of an `if:`. An `if:` built only from event/ref filters keeps the gate.
const STATUS_FUNCTIONS: [&str; 4] = ["always", "success", "failure", "cancelled"];

/// Whether an `if:` condition references a GHA status-check function as a call
/// (`always()`, `failure()`, …). Scans the raw token bodies; matches the name
/// only when it stands as its own identifier immediately followed by `(`, so
/// `needs.failure_count` or a `success_url` field don't trip it.
fn references_status_function(expr_str: &ExprString) -> bool {
    expr_str.tokens.iter().any(|t| {
        let body = match t {
            ExprToken::Literal(s) | ExprToken::Expr(s) => s.as_str(),
        };
        STATUS_FUNCTIONS.iter().any(|f| body_calls(body, f))
    })
}

/// True if `body` contains `name` as a standalone identifier followed (after
/// optional whitespace) by `(`.
fn body_calls(body: &str, name: &str) -> bool {
    let bytes = body.as_bytes();
    let mut from = 0;
    while let Some(rel) = body[from..].find(name) {
        let start = from + rel;
        let end = start + name.len();
        let prev_ok = start == 0
            || !matches!(bytes[start - 1], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_');
        let next_is_paren = body[end..].trim_start().starts_with('(');
        if prev_ok && next_is_paren {
            return true;
        }
        from = end;
    }
    false
}

/// GHA implicit needs-gate. Returns `false` when at least one `needs:`
/// dependency did not aggregate to success (matrix rows aggregate per
/// [`JobResult::aggregate`]: any failure wins).
///
/// GHA injects an implicit `success()` (all-needs-succeeded) into a job's `if:`
/// *unless* the author's condition references a status-check function — so
/// `if: <event/ref filter>` is really `success() && (<filter>)` and a job with
/// such a filter is still skipped when a `needs:` producer failed or was
/// skipped. Only `always()` / `failure()` / `cancelled()` / explicit
/// `success()` opt out of the auto gate (which is why `if: always() && …`
/// publish jobs still run after a skipped dependency). The earlier
/// `if_cond.is_some()` short-circuit was too coarse: it let `smoke` (an
/// event/ref `if:` with no status function) run after its `cli-build` producer
/// was skipped, then fail on an empty artifact store three waves later (R516-B1).
/// `None` when the job's needs-gate passes (or is bypassed by a status-check
/// `if:`); `Some(reason)` naming the first `needs:` dependency that didn't
/// succeed otherwise (R330-B41) — the loop order matches GHA's own gate, so
/// the first offender is the one an operator would look for first.
fn needs_gate_passes(job: &Job, completed: &[CompletedInstance]) -> Option<String> {
    if let Some(cond) = &job.if_cond {
        if references_status_function(cond) {
            return None;
        }
    }
    for need in &job.needs {
        let agg = JobResult::aggregate(
            completed
                .iter()
                .filter(|c| &c.job_id == need)
                .map(|c| c.result),
        );
        if agg != JobResult::Success {
            return Some(format!(
                "needs `{need}` which {} instead of succeeding",
                match agg {
                    JobResult::Failure => "failed",
                    JobResult::Cancelled => "was cancelled",
                    JobResult::Skipped => "was skipped",
                    JobResult::Success => unreachable!("filtered above"),
                }
            ));
        }
    }
    None
}

fn should_run_job(job: &Job, ctx: &Context) -> Result<bool, RuntimeError> {
    let Some(expr_str) = &job.if_cond else { return Ok(true) };
    eval_implicit_expr(expr_str, ctx, "job.if").map(|v| v.is_truthy())
}

fn should_run_step(
    step: &Step,
    ctx: &Context,
    prior_failure: bool,
) -> Result<bool, RuntimeError> {
    let Some(expr_str) = &step.if_cond else { return Ok(!prior_failure) };
    eval_implicit_expr(expr_str, ctx, "step.if").map(|v| v.is_truthy())
}

/// Implicit-expression body extractor for `if:`-style scalars (whole body is
/// an expression, with or without `${{ }}` delimiters). Mirrors the
/// `graph::should_run_job` helper but lives here so the step executor can
/// reuse the path.
fn eval_implicit_expr(s: &ExprString, ctx: &Context, site: &str) -> Result<Value, RuntimeError> {
    let body = match s.tokens.as_slice() {
        [ExprToken::Literal(b)] | [ExprToken::Expr(b)] => b.clone(),
        _ => {
            // Mixed-token if: is malformed GHA but worth a graceful path —
            // fall back to ExprString eval rather than panicking.
            return crate::graph::eval_exprstring(s, ctx).map_err(|source| RuntimeError::Expr {
                site: site.into(),
                source,
            });
        }
    };
    expr::evaluate(&body, ctx).map_err(|source| RuntimeError::Expr {
        site: site.into(),
        source,
    })
}

// ─── step exec ─────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn run_step(
    step: &Step,
    ctx: &Context,
    executor: &Executor,
    env_overlay: &IndexMap<String, String>,
    instance: &JobInstance,
    step_index: usize,
    job_defaults: Option<&RunDefaults>,
    workflow_defaults: Option<&RunDefaults>,
) -> Result<StepResult, RuntimeError> {
    let env = compose_step_env(step, ctx, env_overlay, executor)?;
    let res = match &step.action {
        StepAction::Run { body, shell } => run_bash_step(
            step,
            body,
            shell.as_deref(),
            &env,
            ctx,
            executor,
            instance,
            step_index,
            job_defaults,
            workflow_defaults,
        )?,
        StepAction::Uses { slug, git_ref, with } => {
            run_uses_step(step, slug, git_ref.as_deref(), with, &env, ctx, executor)?
        }
    };
    Ok(res)
}

/// Resolve the cwd for a `run:` step, matching GHA's own precedence: step
/// `working-directory:` wins, then the job's `defaults.run.working-directory`,
/// then the workflow's, else the executor's workspace root. A relative value
/// resolves against `executor.workspace` (GHA's own semantics — relative to
/// `GITHUB_WORKSPACE`, not the process cwd); an absolute value is used as-is.
///
/// Only `run:` steps take this — GHA's `working-directory:` has no effect on
/// `uses:` steps, so [`run_uses_step`] is untouched.
fn resolve_working_directory(
    step: &Step,
    ctx: &Context,
    executor: &Executor,
    job_defaults: Option<&RunDefaults>,
    workflow_defaults: Option<&RunDefaults>,
) -> Result<PathBuf, RuntimeError> {
    let raw: Option<String> = if let Some(wd) = step.working_directory.as_ref() {
        Some(
            crate::graph::eval_exprstring(wd, ctx)
                .map_err(|source| RuntimeError::Expr {
                    site: "step.working-directory".into(),
                    source,
                })?
                .as_str_lossy(),
        )
    } else if let Some(d) = job_defaults.and_then(|d| d.working_directory.as_ref()) {
        Some(d.clone())
    } else {
        workflow_defaults.and_then(|d| d.working_directory.clone())
    };

    Ok(match raw {
        Some(rel) => {
            let p = PathBuf::from(rel);
            if p.is_absolute() {
                p
            } else {
                executor.workspace.join(p)
            }
        }
        None => executor.workspace.clone(),
    })
}

/// The W224 tier-2 environment floor: the synthetic repo context every GHA
/// step is entitled to read, projected out of [`Executor::github`] into the
/// `GITHUB_*` env names the runner exports, plus the generic `CI` flag.
///
/// tier.rs classifies this surface as **fabricate** — QED knows the repo and
/// commit it is building, so withholding it buys nothing and costs fidelity.
/// Before this existed the only env a step saw was `GITHUB_OUTPUT`/`_ENV`/
/// `_STEP_SUMMARY` + `RUNNER_*`, which is the tier-1 toolkit contract alone;
/// anything reading `$GITHUB_SHA` or `$CI` silently saw a dev shell.
///
/// **`GITHUB_ACTIONS` is deliberately absent**, and that omission is the whole
/// design. It is the tier-3 flag: tools read it to mean "GitHub-the-service is
/// reachable" and go on to assume `GITHUB_TOKEN`, the REST API, OIDC, and the
/// artifact/cache services. W224 declines to mimic tier 3, so claiming it here
/// would make the runtime lie about facilities it does not provide — a step
/// that branched on it would fail deeper in, with a worse message, than one
/// that never took the branch. `CI=true` carries the honest half of the claim:
/// this is an automated non-interactive pipeline run. Consumers that want "am
/// I in CI" (cargo, test harnesses, `qed`'s own placement gate) key on `CI`;
/// consumers that want "can I call the GitHub API" correctly see nothing.
///
/// Lowest precedence by construction — the caller lays workflow / job / step
/// `env:` on top, matching GHA, where the runner's env is the base a workflow
/// may shadow. Values absent from the github context are skipped rather than
/// exported empty, so `${GITHUB_SHA:-}` fallbacks in a step behave the same as
/// on a runner that never set them.
fn gha_env_floor(executor: &Executor) -> IndexMap<String, String> {
    let mut out: IndexMap<String, String> = IndexMap::new();
    // Generic automation flag. Set by every CI provider; the one signal here
    // that is unambiguously true of a QED workflow run on any host.
    out.insert("CI".into(), "true".into());
    out.insert(
        "GITHUB_WORKSPACE".into(),
        executor.workspace.display().to_string(),
    );
    let Value::Object(github) = &executor.github else {
        return out;
    };
    // (github context key, exported env name). Only the tier-2 members: repo
    // identity and the commit/ref under build. No `GITHUB_TOKEN`, no
    // `GITHUB_API_URL`, no run-identity (`GITHUB_RUN_ID`) — those are tier-3
    // service handles, and a fabricated run id is worse than none.
    const PROJECTION: &[(&str, &str)] = &[
        ("repository", "GITHUB_REPOSITORY"),
        ("sha", "GITHUB_SHA"),
        ("ref", "GITHUB_REF"),
        ("ref_name", "GITHUB_REF_NAME"),
        ("event_name", "GITHUB_EVENT_NAME"),
        ("actor", "GITHUB_ACTOR"),
    ];
    for (key, env_name) in PROJECTION {
        if let Some(v) = github.get(*key) {
            let s = v.as_str_lossy();
            if !s.is_empty() {
                out.insert((*env_name).into(), s);
            }
        }
    }
    out
}

/// Compose the env passed to a step: workflow.env + job.env are already in
/// ctx.env; merge per-step env (typed values lowered to strings) on top. The
/// `env_overlay` argument is the prior-step `$GITHUB_ENV` accumulator — it's
/// already folded into ctx.env by [`run_instance`], so this just lays the
/// step's own ExprString-evaluated env on the result.
fn compose_step_env(
    step: &Step,
    ctx: &Context,
    _env_overlay: &IndexMap<String, String>,
    executor: &Executor,
) -> Result<IndexMap<String, String>, RuntimeError> {
    let mut out: IndexMap<String, String> = gha_env_floor(executor);
    // Lowest-precedence host default (inserted first so workflow / job / step
    // `env:` below override it): when the runner host isn't x86_64, point docker
    // at linux/amd64. Steps that pull the amd64-only cross base images
    // (`cross build`) otherwise fail on an arm64 host with "no match for
    // platform in manifest"; with this they resolve under emulation. Explicit
    // `docker buildx build --platform …` in the multi-arch image jobs still
    // wins over this default, and it's a no-op on x86_64 hosts (and on real
    // GHA, which never executes through this runner).
    if executor.runner_arch != "X64" {
        out.insert("DOCKER_DEFAULT_PLATFORM".into(), "linux/amd64".into());
    }
    if let Value::Object(m) = &ctx.env {
        for (k, v) in m {
            out.insert(k.clone(), v.as_str_lossy());
        }
    }
    for (k, v) in &step.env {
        let value = crate::graph::eval_exprstring(v, ctx).map_err(|source| RuntimeError::Expr {
            site: format!("step.env.{k}"),
            source,
        })?;
        out.insert(k.clone(), value.as_str_lossy());
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn run_bash_step(
    step: &Step,
    body: &ExprString,
    shell: Option<&str>,
    env: &IndexMap<String, String>,
    ctx: &Context,
    executor: &Executor,
    instance: &JobInstance,
    step_index: usize,
    job_defaults: Option<&RunDefaults>,
    workflow_defaults: Option<&RunDefaults>,
) -> Result<StepResult, RuntimeError> {
    let body_str = crate::graph::eval_exprstring(body, ctx)
        .map_err(|source| RuntimeError::Expr { site: "step.run".into(), source })?
        .as_str_lossy();

    let shell = shell.unwrap_or("bash");
    if shell != "bash" {
        return Err(RuntimeError::Expr {
            site: "step.shell".into(),
            source: ExprError::Eval(format!("unsupported shell `{shell}` (F4 supports bash only)")),
        });
    }

    let tmp = tempfile::tempdir()?;
    let script_path = tmp.path().join("step.sh");
    {
        let mut f = std::fs::File::create(&script_path)?;
        // `set -e` matches GHA default. `set -o pipefail` mirrors what the
        // GHA bash invocation does so failures inside `|` chains surface.
        writeln!(f, "#!/usr/bin/env bash")?;
        writeln!(f, "set -eo pipefail")?;
        f.write_all(body_str.as_bytes())?;
        if !body_str.ends_with('\n') {
            writeln!(f)?;
        }
    }

    let output_path = tmp.path().join("output");
    let env_path = tmp.path().join("env");
    let step_summary_path = tmp.path().join("step_summary");
    std::fs::File::create(&output_path)?;
    std::fs::File::create(&env_path)?;
    std::fs::File::create(&step_summary_path)?;

    let workdir = resolve_working_directory(step, ctx, executor, job_defaults, workflow_defaults)?;

    let mut cmd = Command::new(shell);
    cmd.arg(&script_path);
    cmd.current_dir(&workdir);
    if !executor.env_passthrough {
        cmd.env_clear();
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.env("GITHUB_OUTPUT", &output_path);
    cmd.env("GITHUB_ENV", &env_path);
    cmd.env("GITHUB_STEP_SUMMARY", &step_summary_path);
    cmd.env("RUNNER_OS", &executor.runner_os);
    cmd.env("RUNNER_ARCH", &executor.runner_arch);
    cmd.env("RUNNER_ENVIRONMENT", &executor.runner_environment);

    // Pipe stdout + stderr so we can stream lines through the event sink
    // (when configured) and still capture full buffers for the returned
    // StepResult. Two reader threads drain each pipe; both join before we
    // wait on the child so the script's exit status reflects the final
    // command and we don't lose tail bytes.
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let child_stdout = child.stdout.take().expect("piped");
    let child_stderr = child.stderr.take().expect("piped");

    let sink_out = executor.events.clone();
    let sink_err = executor.events.clone();
    let job_id = instance.job_id.clone();
    let matrix_index = instance.matrix_index;
    let job_id_err = job_id.clone();

    let stdout_handle = std::thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let mut buf = String::new();
        let reader = BufReader::new(child_stdout);
        for line in reader.lines().flatten() {
            if let Some(s) = &sink_out {
                let _ = s.send(crate::events::GhaEvent::StepOutput {
                    job_id: job_id.clone(),
                    matrix_index,
                    step_index,
                    stream: crate::events::GhaOutputStream::Stdout,
                    line: line.clone(),
                });
            }
            buf.push_str(&line);
            buf.push('\n');
        }
        buf
    });
    let stderr_handle = std::thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        let mut buf = String::new();
        let reader = BufReader::new(child_stderr);
        for line in reader.lines().flatten() {
            if let Some(s) = &sink_err {
                let _ = s.send(crate::events::GhaEvent::StepOutput {
                    job_id: job_id_err.clone(),
                    matrix_index,
                    step_index,
                    stream: crate::events::GhaOutputStream::Stderr,
                    line: line.clone(),
                });
            }
            buf.push_str(&line);
            buf.push('\n');
        }
        buf
    });

    let status = child.wait()?;
    let stdout = stdout_handle.join().unwrap_or_default();
    let stderr = stderr_handle.join().unwrap_or_default();
    let success = status.success();

    // Capture outputs: legacy `::set-output name=K::V` on stdout + modern
    // `K=V` (or `K<<EOF\n…\nEOF`) lines in $GITHUB_OUTPUT. Both are valid;
    // workflows in the wild mix them.
    let mut outputs = parse_set_output_lines(&stdout);
    let output_file = std::fs::read_to_string(&output_path)?;
    for (k, v) in parse_env_file(&output_file) {
        outputs.insert(k, Value::String(v));
    }

    // Stash $GITHUB_ENV updates inside stderr-style sidechannel by reading
    // and folding into the StepResult as a special marker — see
    // `pop_env_updates`.
    let env_file = std::fs::read_to_string(&env_path)?;
    let env_updates = parse_env_file(&env_file);

    let conclusion = if success {
        StepConclusion::Success
    } else {
        StepConclusion::Failure
    };

    let mut step_res = StepResult {
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        conclusion,
        outputs,
        stdout,
        stderr,
    };
    if !env_updates.is_empty() {
        // Encode env updates as a magic prefix on stderr so `pop_env_updates`
        // can pluck them back out without a separate plumbing field. F5 can
        // promote this to a typed field if more state grows here.
        let mut payload = String::from(ENV_UPDATE_PREFIX);
        for (k, v) in &env_updates {
            payload.push_str(&format!("{k}\t{v}\n"));
        }
        payload.push_str(ENV_UPDATE_SUFFIX);
        step_res.stderr.push_str(&payload);
    }
    Ok(step_res)
}

const ENV_UPDATE_PREFIX: &str = "__qed_gha_env_updates_BEGIN__\n";
const ENV_UPDATE_SUFFIX: &str = "__qed_gha_env_updates_END__\n";

fn pop_env_updates(res: &StepResult) -> Option<Vec<(String, String)>> {
    let stderr = &res.stderr;
    let start = stderr.find(ENV_UPDATE_PREFIX)?;
    let body_start = start + ENV_UPDATE_PREFIX.len();
    let end = stderr[body_start..].find(ENV_UPDATE_SUFFIX)?;
    let body = &stderr[body_start..body_start + end];
    let mut out = Vec::new();
    for line in body.lines() {
        if let Some((k, v)) = line.split_once('\t') {
            out.push((k.to_string(), v.to_string()));
        }
    }
    Some(out)
}

/// Parse `::set-output name=KEY::VALUE` lines from a step's stdout.
fn parse_set_output_lines(stdout: &str) -> IndexMap<String, Value> {
    let mut out = IndexMap::new();
    for line in stdout.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("::set-output name=") {
            if let Some((key, value)) = rest.split_once("::") {
                out.insert(key.to_string(), Value::String(value.to_string()));
            }
        }
    }
    out
}

/// Parse the modern `$GITHUB_OUTPUT` / `$GITHUB_ENV` file format:
///   - `KEY=VALUE`            (single-line)
///   - `KEY<<DELIM\n...\nDELIM` (multi-line, with a user-chosen DELIM)
fn parse_env_file(contents: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let lines: Vec<&str> = contents.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim_end_matches('\r');
        if line.is_empty() {
            i += 1;
            continue;
        }
        if let Some((key, rest)) = line.split_once("<<") {
            let delim = rest.trim();
            let key = key.trim().to_string();
            let mut buf = String::new();
            i += 1;
            while i < lines.len() {
                let body_line = lines[i].trim_end_matches('\r');
                if body_line == delim {
                    i += 1;
                    break;
                }
                if !buf.is_empty() {
                    buf.push('\n');
                }
                buf.push_str(body_line);
                i += 1;
            }
            out.push((key, buf));
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            out.push((key.trim().to_string(), value.to_string()));
        }
        i += 1;
    }
    out
}

fn exprstring_static(s: &ExprString) -> Option<String> {
    s.as_pure_literal()
}

// ─── uses dispatch ─────────────────────────────────────────────────────────

/// True when an `actions/checkout` step targets a *different* repository (a
/// non-empty `repository:` input) — the one case W224 says still needs a native
/// clone. A same-repo checkout (the overwhelming default: no `repository:`, or an
/// empty one) is implicit on QED and skipped as a no-op.
fn checks_out_foreign_repo(with: &IndexMap<String, Value>) -> bool {
    matches!(with.get("repository"), Some(Value::String(repo)) if !repo.trim().is_empty())
}

/// Read an `actions/checkout` `with:` input as a trimmed non-empty string.
/// Numbers (`fetch-depth: 0`) come through `as_str_lossy`, so this works for
/// both string and numeric YAML scalars.
fn checkout_input(with: &IndexMap<String, Value>, key: &str) -> Option<String> {
    let s = with.get(key)?.as_str_lossy();
    (!s.trim().is_empty()).then(|| s.trim().to_string())
}

/// W224 R533-T12: emit an explicit native `git clone` for a *foreign-repo*
/// `actions/checkout`. Same-repo checkout is a no-op (the workspace already IS
/// the checkout); only a `with: repository:` naming a different repo lands here.
///
/// Honors the three inputs the [`NativeReplacement::Checkout`] stanza promises:
/// - `ref` → `git clone --branch <ref>` (a branch or tag; a bare commit SHA is
///   not supported by `--branch` and fails here — pin foreign repos to a
///   branch/tag, or import an explicit fetch+checkout step for a SHA),
/// - `path` → the clone subdir under the workspace (default: the repo's short
///   name, never the workspace root, so a foreign checkout can't clobber the
///   run's own positioned tree),
/// - `fetch-depth` → `--depth N` (GHA's default is `1` = shallow; `0` = full
///   history, no `--depth`).
///
/// A non-zero `git` exit surfaces as a [`StepConclusion::Failure`] step (with
/// the git stderr captured), exactly like a failing `run:` step — not a hard
/// [`RuntimeError`] — so normal job-failure handling applies. A spawn failure
/// (no `git` on PATH) is the one [`RuntimeError::Io`] case.
fn run_native_checkout(
    step: &Step,
    with: &IndexMap<String, Value>,
    executor: &Executor,
) -> Result<StepResult, RuntimeError> {
    let repository = checkout_input(with, "repository")
        .expect("caller verified a non-empty repository via checks_out_foreign_repo");

    // `owner/repo` → `https://github.com/owner/repo.git`. A literal URL (any
    // scheme) or a filesystem path (absolute or `.`-relative) passes through
    // unchanged — the latter lets a clone target a local repo with no network,
    // which is also what the unit test exercises.
    let url = if repository.contains("://")
        || repository.starts_with('/')
        || repository.starts_with('.')
    {
        repository.clone()
    } else {
        format!("https://github.com/{repository}.git")
    };

    let dest_rel = checkout_input(with, "path").unwrap_or_else(|| {
        repository
            .trim_end_matches(".git")
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("checkout")
            .to_string()
    });
    let dest = executor.workspace.join(&dest_rel);

    let depth: u64 = checkout_input(with, "fetch-depth")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let mut cmd = Command::new("git");
    cmd.arg("clone");
    if depth > 0 {
        cmd.arg("--depth").arg(depth.to_string());
    }
    if let Some(git_ref) = checkout_input(with, "ref") {
        cmd.arg("--branch").arg(git_ref);
    }
    cmd.arg("--").arg(&url).arg(&dest);
    cmd.current_dir(&executor.workspace);
    if !executor.env_passthrough {
        cmd.env_clear();
    }

    let output = cmd.output()?; // spawn failure ⇒ RuntimeError::Io
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let conclusion = if output.status.success() {
        StepConclusion::Success
    } else {
        StepConclusion::Failure
    };
    Ok(StepResult {
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        conclusion,
        outputs: IndexMap::new(),
        stdout,
        stderr,
    })
}

fn run_uses_step(
    step: &Step,
    slug: &str,
    git_ref: Option<&str>,
    with: &IndexMap<String, ExprString>,
    env: &IndexMap<String, String>,
    ctx: &Context,
    executor: &Executor,
) -> Result<StepResult, RuntimeError> {
    let mut typed_with: IndexMap<String, Value> = IndexMap::new();
    for (k, v) in with {
        let value = crate::graph::eval_exprstring(v, ctx).map_err(|source| RuntimeError::Expr {
            site: format!("step.with.{k}"),
            source,
        })?;
        typed_with.insert(k.clone(), value);
    }

    let outcome = match executor.registry.lookup(slug) {
        Lookup::Found { action } => {
            let call = ToolkitCall {
                slug,
                git_ref,
                with: &typed_with,
                env,
                workspace: &executor.workspace,
            };
            action
                .execute(&call)
                .map_err(|message| RuntimeError::ToolkitFailed {
                    slug: slug.into(),
                    message,
                })?
        }
        // Not a tier-1/2 toolkit action. W224 R533-T7: decline to imitate
        // tier-3 GitHub services — the tier classifier names the native QED
        // replacement so the failure is honest ("import this as a native step")
        // instead of mysterious. A slug in neither bucket is a genuine unknown.
        Lookup::Unknown => {
            let (_, disposition) = classify_uses(slug);
            // W224: `actions/checkout` against the *same* repo is implicit on QED
            // — it already owns the workspace (the camp root IS the checkout), so
            // re-cloning over a live tree is wrong. Treat a same-repo checkout as
            // a successful no-op rather than erroring; the workflow stays valid on
            // GitHub-the-service (where the step does real work) while running
            // unchanged here. Only a *foreign-repo* checkout (a `repository:`
            // input naming another repo) genuinely needs a native clone, so that
            // case still falls through to the tier-3 error below.
            if matches!(disposition, Disposition::ReplaceWithNative(NativeReplacement::Checkout)) {
                // W224: a *same-repo* checkout is implicit on QED — the camp root
                // (or the run's positioned worktree) IS the checkout, so re-cloning
                // over the live tree is wrong. Treat it as a successful no-op.
                if !checks_out_foreign_repo(&typed_with) {
                    return Ok(StepResult {
                        step_id: step.id.clone(),
                        name: step.name.as_ref().and_then(exprstring_static),
                        conclusion: StepConclusion::Success,
                        outputs: IndexMap::new(),
                        stdout: "checkout is implicit on QED (workspace already present) — step skipped"
                            .into(),
                        stderr: String::new(),
                    });
                }
                // R533-T12: a *foreign-repo* checkout (`with: repository: other/repo`)
                // genuinely needs a clone — the NativeReplacement::Checkout stanza
                // promises one. Emit an explicit native git clone into a subdir of
                // the workspace, honoring `ref` / `path` / `fetch-depth`.
                return run_native_checkout(step, &typed_with, executor);
            }
            // R594: retired tier-3 *services* the qed runner executes for real
            // when it injects a handler — the docker push family (via
            // `image_builder`) and the artifact actions (via `artifact_store`).
            // Each is gated on injection: with no handler (the bare crate, most
            // tests) we fall through to the honest tier-3 error below, so
            // qed-gha on its own still never shells docker or touches a store.
            let injected: Option<Result<crate::toolkit::ToolkitOutcome, String>> =
                if crate::image_builder::is_image_push_action(slug) {
                    executor.image_builder.as_ref().map(|builder| {
                        let call = crate::image_builder::ImageBuildCall {
                            slug,
                            with: &typed_with,
                            env,
                            workspace: &executor.workspace,
                        };
                        builder.handle(&call)
                    })
                } else if crate::artifact_store::is_artifact_action(slug) {
                    executor.artifact_store.as_ref().map(|store| {
                        let call = crate::artifact_store::ArtifactCall {
                            with: &typed_with,
                            workspace: &executor.workspace,
                        };
                        if slug == "actions/upload-artifact" {
                            store.upload(&call)
                        } else {
                            store.download(&call)
                        }
                    })
                } else {
                    None
                };
            match injected {
                Some(res) => res.map_err(|message| RuntimeError::ToolkitFailed {
                    slug: slug.into(),
                    message,
                })?,
                None => {
                    return Err(match disposition {
                        Disposition::ReplaceWithNative(nr) => RuntimeError::Tier3RequiresNative {
                            slug: slug.into(),
                            replacement: nr.label().into(),
                            stanza: nr.stanza_hint().into(),
                        },
                        // Compute/Unknown with no registered toolkit impl: we
                        // have no executor for it. Surface for human review
                        // rather than guessing.
                        Disposition::Compute | Disposition::Unknown => {
                            RuntimeError::UnknownAction { slug: slug.into() }
                        }
                    });
                }
            }
        }
    };

    // A failing toolkit action (conclusion=Failure) mirrors its log into stderr
    // so the qed-runner's stderr-tail diagnostic surfaces *why* the step failed
    // (it reads `StepResult::stderr`, not stdout). Without this, a graceful
    // action failure shows up downstream as "(no stderr)".
    let stderr = if matches!(outcome.conclusion, StepConclusion::Failure) {
        outcome.log.clone()
    } else {
        String::new()
    };
    Ok(StepResult {
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        conclusion: outcome.conclusion,
        outputs: outcome.outputs,
        stdout: outcome.log,
        stderr,
    })
}

// ─── event emit helpers ────────────────────────────────────────────────────

fn emit_job_started(executor: &Executor, instance: &JobInstance, workflow: &Workflow) {
    let Some(sink) = executor.events.as_ref() else { return };
    let total_steps = workflow
        .jobs
        .get(&instance.job_id)
        .map(|j| j.steps.len())
        .unwrap_or(0);
    let _ = sink.send(crate::events::GhaEvent::JobStarted {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        key: instance.key(),
        total_steps,
    });
}

fn emit_job_finished(executor: &Executor, instance: &JobInstance, run: &InstanceRun) {
    let Some(sink) = executor.events.as_ref() else { return };
    let _ = sink.send(crate::events::GhaEvent::JobFinished {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        key: instance.key(),
        result: run.result,
    });
}

fn emit_step_started(
    executor: &Executor,
    instance: &JobInstance,
    step_index: usize,
    step: &Step,
) {
    let Some(sink) = executor.events.as_ref() else { return };
    let action_kind = match &step.action {
        StepAction::Run { .. } => "run".to_string(),
        StepAction::Uses { slug, .. } => format!("uses:{slug}"),
    };
    let _ = sink.send(crate::events::GhaEvent::StepStarted {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        step_index,
        step_id: step.id.clone(),
        name: step.name.as_ref().and_then(exprstring_static),
        action_kind,
    });
}

fn emit_step_finished(
    executor: &Executor,
    instance: &JobInstance,
    step_index: usize,
    res: &StepResult,
) {
    let Some(sink) = executor.events.as_ref() else { return };
    let msg = if matches!(res.conclusion, StepConclusion::Failure) {
        let tail: Vec<&str> = res
            .stderr
            .lines()
            .filter(|l| {
                let t = l.trim();
                !t.starts_with(ENV_UPDATE_PREFIX.trim())
                    && !t.starts_with(ENV_UPDATE_SUFFIX.trim())
                    && !t.is_empty()
            })
            .collect();
        let start = tail.len().saturating_sub(20);
        let s = tail[start..].join("\n");
        if s.is_empty() { None } else { Some(s) }
    } else {
        None
    };
    let _ = sink.send(crate::events::GhaEvent::StepFinished {
        job_id: instance.job_id.clone(),
        matrix_index: instance.matrix_index,
        step_index,
        conclusion: res.conclusion,
        msg,
        outputs: res.outputs.clone(),
    });
}

// ─── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolkit::ToolkitAction;
    use crate::parse_workflow;

    fn workflow(yaml: &str) -> Workflow {
        parse_workflow(yaml).unwrap_or_else(|e| panic!("parse: {e}"))
    }

    fn workspace_path() -> PathBuf {
        // Tests share one tmpdir-style cwd. The executor doesn't depend on
        // the workspace contents (we only spawn bash on a script we wrote
        // to its own tmp dir), so the CWD is mostly aesthetic.
        std::env::temp_dir()
    }

    fn run_with_path(wf: &Workflow) -> WorkflowRun {
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true; // need PATH for bash/coreutils
        e.runner_os = "Linux".into();
        execute_workflow(wf, &e).unwrap_or_else(|err| panic!("execute: {err}"))
    }

    // ── R605-F3 half (a): within-wave concurrency ─────────────────────────

    /// Wall-clock proof, from inside the workflow itself: each job records the
    /// interval it occupied, and two jobs in one wave must overlap once the cap
    /// allows it. Written with `date +%s%N`-free arithmetic — the steps just
    /// sleep and the assertion is on the elapsed wall-clock of the whole run,
    /// which is the only observable a workflow can't fake.
    #[test]
    fn a_wave_runs_concurrently_once_the_cap_is_raised() {
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 1
  b:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 1
  c:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 1
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();

        let started = std::time::Instant::now();
        let run = execute_workflow(&wf, &e).expect("serial execute");
        let serial = started.elapsed();
        assert_eq!(run.instances.len(), 3);
        assert!(
            serial >= std::time::Duration::from_millis(2_900),
            "the default cap of 1 must still be serial (took {serial:?})",
        );

        let mut parallel_exec = Executor::new(workspace_path());
        parallel_exec.env_passthrough = true;
        parallel_exec.runner_os = "Linux".into();
        let parallel_exec = parallel_exec.with_max_parallel_jobs(3);
        let started = std::time::Instant::now();
        let run = execute_workflow(&wf, &parallel_exec).expect("parallel execute");
        let parallel = started.elapsed();
        assert!(
            parallel < std::time::Duration::from_millis(2_500),
            "three independent jobs at cap 3 must overlap (took {parallel:?}, \
             serial was {serial:?})",
        );
        // Order of the transcript is wave order, not completion order.
        let ids: Vec<&str> = run.instances.iter().map(|i| i.job_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
        assert!(run.instances.iter().all(|i| i.result == JobResult::Success));
    }

    /// R605-T4: two same-wave jobs that declare the same `concurrency.group`
    /// share a disk resource and must serialize even though the global cap
    /// would otherwise let them overlap; a third job with no `concurrency:`
    /// block is unconstrained and races ahead of both.
    #[test]
    fn same_wave_jobs_sharing_a_concurrency_group_serialize_under_a_raised_cap() {
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    concurrency:
      group: dist
    steps:
      - run: sleep 1
  b:
    runs-on: ubuntu-latest
    concurrency:
      group: dist
    steps:
      - run: sleep 1
  c:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 1
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        let e = e.with_max_parallel_jobs(3);

        let started = std::time::Instant::now();
        let run = execute_workflow(&wf, &e).expect("execute");
        let elapsed = started.elapsed();
        assert_eq!(run.instances.len(), 3);
        assert!(run.instances.iter().all(|i| i.result == JobResult::Success));
        // `a` and `b` share `group: dist` so they never overlap — one full
        // second each, back to back — while `c` runs alongside whichever of
        // them goes first. Serial a+b alone is ~2s; if the resource key were
        // ignored all three would overlap and finish under 1.5s.
        assert!(
            elapsed >= std::time::Duration::from_millis(1_900),
            "jobs sharing a concurrency.group must not run concurrently \
             even under a raised cap (took {elapsed:?})",
        );
        assert!(
            elapsed < std::time::Duration::from_millis(2_900),
            "`c` (no concurrency group) should still overlap one of a/b \
             rather than the whole wave going fully serial (took {elapsed:?})",
        );
    }

    /// Same-wave jobs with *different* `concurrency.group` values (or no
    /// group at all) are unconstrained by the resource key and overlap
    /// exactly as before R605-T4 introduced it.
    #[test]
    fn same_wave_jobs_with_distinct_concurrency_groups_still_run_concurrently() {
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    concurrency:
      group: group-a
    steps:
      - run: sleep 1
  b:
    runs-on: ubuntu-latest
    concurrency:
      group: group-b
    steps:
      - run: sleep 1
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        let e = e.with_max_parallel_jobs(2);

        let started = std::time::Instant::now();
        let run = execute_workflow(&wf, &e).expect("execute");
        let elapsed = started.elapsed();
        assert_eq!(run.instances.len(), 2);
        assert!(run.instances.iter().all(|i| i.result == JobResult::Success));
        assert!(
            elapsed < std::time::Duration::from_millis(1_900),
            "distinct concurrency groups must not serialize the wave \
             (took {elapsed:?})",
        );
    }

    /// A job's own `strategy.max-parallel` is honoured under the global cap —
    /// the same thing it means on GitHub. Four rows, global cap 4, job cap 1 ⇒
    /// serial anyway.
    #[test]
    fn a_jobs_own_max_parallel_bounds_its_rows_under_the_global_cap() {
        let yaml = r#"
on: [push]
jobs:
  fan:
    runs-on: ubuntu-latest
    strategy:
      max-parallel: 1
      matrix:
        n: [1, 2, 3]
    steps:
      - run: sleep 1
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        let e = e.with_max_parallel_jobs(4);

        let started = std::time::Instant::now();
        let run = execute_workflow(&wf, &e).expect("execute");
        let elapsed = started.elapsed();
        assert_eq!(run.instances.len(), 3);
        assert!(
            elapsed >= std::time::Duration::from_millis(2_900),
            "max-parallel: 1 must keep the rows serial even at global cap 4 \
             (took {elapsed:?})",
        );
    }

    /// The wave boundary is still a barrier: a dependent job may not start
    /// before its predecessors finished, whatever the cap says.
    #[test]
    fn a_raised_cap_does_not_cross_a_wave_boundary() {
        let yaml = r#"
on: [push]
jobs:
  first:
    runs-on: ubuntu-latest
    steps:
      - id: emit
        run: echo "v=1" >> "$GITHUB_OUTPUT"
    outputs:
      v: ${{ steps.emit.outputs.v }}
  second:
    needs: first
    runs-on: ubuntu-latest
    steps:
      - run: test "${{ needs.first.outputs.v }}" = "1"
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        let e = e.with_max_parallel_jobs(8);
        let run = execute_workflow(&wf, &e).expect("execute");
        // `second` could only pass its `test` if `first`'s output was already
        // aggregated — i.e. the wave boundary held.
        assert_eq!(run.instance("second").unwrap().result, JobResult::Success);
    }

    /// A failure inside a concurrent wave surfaces as the error, and picks the
    /// first failure in WAVE order rather than whichever thread lost the race.
    #[test]
    fn a_concurrent_wave_reports_its_failure_deterministically() {
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    steps:
      - uses: nope/one@v1
  b:
    runs-on: ubuntu-latest
    steps:
      - uses: nope/two@v1
"#;
        let wf = workflow(yaml);
        let mut e = Executor::bare(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        let e = e.with_max_parallel_jobs(2);
        for _ in 0..5 {
            let err = execute_workflow(&wf, &e).expect_err("unknown actions fail");
            assert!(
                err.to_string().contains("nope/one"),
                "the wave-order-first failure must win every time, got: {err}",
            );
        }
    }

    #[test]
    fn bash_run_step_succeeds_and_captures_legacy_set_output() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: build
        run: |
          echo '::set-output name=digest::sha256:abc'
          echo 'ok'
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(inst.steps.len(), 1);
        let s = &inst.steps[0];
        assert_eq!(s.conclusion, StepConclusion::Success);
        assert_eq!(
            s.outputs.get("digest"),
            Some(&Value::String("sha256:abc".into()))
        );
    }

    #[test]
    fn matrix_expr_in_run_body_is_interpolated() {
        // Regression: render_run_body used to drop ${{ }} tokens, so
        // `cargo build --target ${{ matrix.target }}` shipped to bash as
        // `cargo build --target ` (trailing flag, no value).
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64-unknown-linux-musl, aarch64-unknown-linux-musl]
    steps:
      - id: emit
        run: |
          echo "::set-output name=t::${{ matrix.target }}"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst0 = run.instance_at("build", 0).unwrap();
        let inst1 = run.instance_at("build", 1).unwrap();
        assert_eq!(
            inst0.steps[0].outputs.get("t"),
            Some(&Value::String("x86_64-unknown-linux-musl".into()))
        );
        assert_eq!(
            inst1.steps[0].outputs.get("t"),
            Some(&Value::String("aarch64-unknown-linux-musl".into()))
        );
    }

    #[test]
    fn bash_run_step_captures_github_output_file() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: build
        run: |
          echo "digest=sha256:def" >> "$GITHUB_OUTPUT"
          printf 'changelog<<EOF\nline1\nline2\nEOF\n' >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let s = &run.instance("one").unwrap().steps[0];
        assert_eq!(
            s.outputs.get("digest"),
            Some(&Value::String("sha256:def".into()))
        );
        assert_eq!(
            s.outputs.get("changelog"),
            Some(&Value::String("line1\nline2".into()))
        );
    }

    #[test]
    fn bash_failure_marks_job_failure() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - run: |
          exit 7
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Failure);
        assert_eq!(inst.steps[0].conclusion, StepConclusion::Failure);
    }

    #[test]
    fn continue_on_error_lets_job_keep_running() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: bad
        continue-on-error: true
        run: |
          exit 2
      - id: ok
        if: always()
        run: |
          echo "still running"
          echo "ran=yes" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("one").unwrap();
        // continue-on-error: true means the failing step does NOT fail the
        // job; downstream steps keep running and the job's aggregate result
        // is success. The step itself still records conclusion=failure so
        // `steps.bad.conclusion` is visible to expressions.
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(inst.steps.len(), 2);
        assert_eq!(inst.steps[0].conclusion, StepConclusion::Failure);
        assert_eq!(inst.steps[1].conclusion, StepConclusion::Success);
        assert_eq!(
            inst.steps[1].outputs.get("ran"),
            Some(&Value::String("yes".into()))
        );
    }

    #[test]
    fn github_env_propagates_between_steps() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "MYVAR=hello" >> "$GITHUB_ENV"
      - run: |
          echo "MYVAR=$MYVAR"
          echo "saw=$MYVAR" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(
            inst.steps[1].outputs.get("saw"),
            Some(&Value::String("hello".into()))
        );
    }

    #[test]
    fn step_outputs_flow_into_downstream_job_via_needs() {
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      digest: ${{ steps.b.outputs.digest }}
    steps:
      - id: b
        run: |
          echo "digest=sha256:42" >> "$GITHUB_OUTPUT"
  publish:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - id: echo
        run: |
          echo "got=$DIGEST" >> "$GITHUB_OUTPUT"
        env:
          DIGEST: ${{ needs.build.outputs.digest }}
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        assert_eq!(run.instance("build").unwrap().result, JobResult::Success);
        let pub_inst = run.instance("publish").unwrap();
        assert_eq!(pub_inst.result, JobResult::Success);
        assert_eq!(
            pub_inst.steps[0].outputs.get("got"),
            Some(&Value::String("sha256:42".into()))
        );
    }

    #[test]
    fn non_amd64_host_injects_docker_default_platform() {
        // On an arm64 host, steps see DOCKER_DEFAULT_PLATFORM=linux/amd64 so
        // `cross build` can pull the amd64-only cross base image under emulation
        // instead of failing with "no match for platform in manifest".
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: a
        run: |
          echo "plat=$DOCKER_DEFAULT_PLATFORM" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        e.runner_arch = "ARM64".into();
        let run = execute_workflow(&wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(
            inst.steps[0].outputs.get("plat"),
            Some(&Value::String("linux/amd64".into())),
        );
    }

    #[test]
    fn workflow_env_overrides_injected_docker_default_platform() {
        // The injected value is a *default* — an explicit workflow/job/step
        // `env:` still wins (lowest-precedence insertion).
        let yaml = r#"
on: [push]
env:
  DOCKER_DEFAULT_PLATFORM: linux/arm64
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: a
        run: |
          echo "plat=$DOCKER_DEFAULT_PLATFORM" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        e.runner_arch = "ARM64".into();
        let run = execute_workflow(&wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(
            inst.steps[0].outputs.get("plat"),
            Some(&Value::String("linux/arm64".into())),
        );
    }

    // ── uses dispatch

    struct FakeAction {
        slug: &'static str,
    }
    impl ToolkitAction for FakeAction {
        fn execute(&self, call: &ToolkitCall<'_>) -> Result<ToolkitOutcome, String> {
            let mut outputs = IndexMap::new();
            outputs.insert(
                "slug".into(),
                Value::String(call.slug.to_string()),
            );
            outputs.insert(
                "ref".into(),
                Value::String(call.git_ref.unwrap_or("").to_string()),
            );
            // Reflect the with: inputs back so the caller can assert eval
            // happened.
            for (k, v) in call.with.iter() {
                outputs.insert(format!("in_{k}"), v.clone());
            }
            // Use the slug to identify which action fired.
            let _ = self.slug;
            Ok(ToolkitOutcome {
                outputs,
                log: "fake".into(),
                conclusion: StepConclusion::Success,
            })
        }
    }

    #[test]
    fn uses_unknown_action_raises_error() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: nope/missing@v1
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        let err = execute_workflow(&wf, &e).expect_err("must error on unknown action");
        match err {
            RuntimeError::UnknownAction { slug } => assert_eq!(slug, "nope/missing"),
            other => panic!("expected UnknownAction, got {other}"),
        }
    }

    #[test]
    fn uses_registered_override_receives_with_inputs() {
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: act
        uses: test/echo@v3
        with:
          registry: ghcr.io
          tag: ${{ inputs.tag }}
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.inputs = crate::obj([("tag", "v1.2.3")]);
        e.registry
            .register("test/echo", Box::new(FakeAction { slug: "test/echo" }));
        let run = execute_workflow(&wf, &e).unwrap();
        let s = &run.instance("one").unwrap().steps[0];
        assert_eq!(s.outputs.get("slug"), Some(&Value::String("test/echo".into())));
        assert_eq!(s.outputs.get("ref"), Some(&Value::String("v3".into())));
        assert_eq!(
            s.outputs.get("in_registry"),
            Some(&Value::String("ghcr.io".into()))
        );
        // `tag` came in via `${{ inputs.tag }}` — confirms ExprString eval
        // happened against the per-step ctx before the override saw the
        // value.
        assert_eq!(
            s.outputs.get("in_tag"),
            Some(&Value::String("v1.2.3".into()))
        );
    }

    #[test]
    fn uses_tier3_action_requires_native_replacement() {
        // W224 R533-T7: a tier-3 service action (here `actions/upload-artifact`)
        // is no longer reimplemented — it routes through the tier classifier to a
        // Tier3RequiresNative error naming the native QED facility, so the
        // failure says "import this as a native step" instead of running a
        // half-faithful clone of the GitHub service.
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v4
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        let err = execute_workflow(&wf, &e).expect_err("tier-3 must not run");
        match err {
            RuntimeError::Tier3RequiresNative { slug, replacement, stanza } => {
                assert_eq!(slug, "actions/upload-artifact");
                assert!(!replacement.is_empty(), "replacement label present");
                assert!(!stanza.is_empty(), "native stanza hint present");
            }
            other => panic!("expected Tier3RequiresNative, got {other}"),
        }
    }

    #[test]
    fn docker_build_push_without_builder_is_tier3() {
        // R594: with no injected image builder, the docker push family stays a
        // tier-3 error — the bare crate never shells docker.
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: docker/build-push-action@v5
        with:
          push: true
          tags: ghcr.io/yah-ai/x:dev
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        let err = execute_workflow(&wf, &e).expect_err("no builder ⇒ tier-3");
        match err {
            RuntimeError::Tier3RequiresNative { slug, .. } => {
                assert_eq!(slug, "docker/build-push-action");
            }
            other => panic!("expected Tier3RequiresNative, got {other}"),
        }
    }

    #[test]
    fn docker_build_push_with_injected_builder_runs_and_surfaces_outputs() {
        // R594: an injected ImageBuilder handles the docker push family — the
        // step runs (not a tier-3 error) and its outputs (digest/…) flow through
        // to steps.<id>.outputs.* exactly like a toolkit action's.
        use crate::image_builder::{ImageBuildCall, ImageBuilder};
        use crate::toolkit::{StepConclusion, ToolkitOutcome};

        struct FakeBuilder;
        impl ImageBuilder for FakeBuilder {
            fn handle(&self, call: &ImageBuildCall<'_>) -> Result<ToolkitOutcome, String> {
                assert_eq!(call.slug, "docker/build-push-action");
                // The `with:` inputs are evaluated before we see them.
                assert_eq!(
                    call.with.get("tags"),
                    Some(&Value::String("ghcr.io/yah-ai/x:dev".into()))
                );
                let mut outputs = IndexMap::new();
                outputs.insert("digest".into(), Value::String("sha256:deadbeef".into()));
                Ok(ToolkitOutcome {
                    outputs,
                    log: "built".into(),
                    conclusion: StepConclusion::Success,
                })
            }
        }

        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: build
        uses: docker/build-push-action@v5
        with:
          push: true
          tags: ghcr.io/yah-ai/x:dev
"#;
        let wf = workflow(yaml);
        let e = Executor::new(workspace_path())
            .with_image_builder(std::sync::Arc::new(FakeBuilder));
        let run = execute_workflow(&wf, &e).expect("builder handles the step");
        let s = &run.instance("one").unwrap().steps[0];
        assert_eq!(s.conclusion, StepConclusion::Success);
        assert_eq!(
            s.outputs.get("digest"),
            Some(&Value::String("sha256:deadbeef".into()))
        );
    }

    #[test]
    fn uses_same_repo_checkout_is_implicit_noop() {
        // W224: `actions/checkout` against the same repo is implicit on QED —
        // the camp root IS the workspace, so the step is a successful no-op
        // rather than a Tier3RequiresNative error. This keeps a stock release
        // workflow (every job opens with `- uses: actions/checkout@v4`) runnable
        // on QED unchanged while staying valid on GitHub-the-service.
        let yaml = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        let run = execute_workflow(&wf, &e).expect("same-repo checkout no-ops");
        let step = &run.instances[0].steps[0];
        assert_eq!(step.conclusion, StepConclusion::Success);
        assert!(step.stdout.contains("implicit"), "no-op note surfaced: {}", step.stdout);
    }

    #[test]
    fn uses_foreign_repo_checkout_emits_a_native_clone() {
        // R533-T12: a `repository:` input naming another repo can't be the
        // implicit workspace — QED emits an explicit native `git clone` into a
        // subdir of the workspace, honoring `ref` / `path` / `fetch-depth`,
        // instead of the old Tier3RequiresNative refusal.
        let git = |dir: &std::path::Path, args: &[&str]| {
            let ok = Command::new("git")
                .current_dir(dir)
                .args(args)
                .output()
                .unwrap()
                .status
                .success();
            assert!(ok, "git {args:?} failed in {}", dir.display());
        };
        // A local source repo with a committed file, also on branch `release`.
        // An absolute path is treated as a literal clone URL, so the test needs
        // no network.
        let src = tempfile::tempdir().unwrap();
        git(src.path(), &["init", "-b", "main"]);
        git(src.path(), &["config", "user.email", "t@t.t"]);
        git(src.path(), &["config", "user.name", "t"]);
        std::fs::write(src.path().join("hello.txt"), "from-foreign-repo").unwrap();
        git(src.path(), &["add", "."]);
        git(src.path(), &["commit", "-m", "init"]);
        git(src.path(), &["branch", "release"]);

        // A fresh, empty workspace to clone into (not the shared temp_dir).
        let ws = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          repository: {src}
          ref: release
          path: vendored/dep
          fetch-depth: 1
"#,
            src = src.path().display()
        );
        let wf = workflow(&yaml);
        let mut e = Executor::new(ws.path());
        e.env_passthrough = true; // need git on PATH
        let run = execute_workflow(&wf, &e).expect("foreign-repo checkout clones natively");
        let step = &run.instances[0].steps[0];
        assert_eq!(
            step.conclusion,
            StepConclusion::Success,
            "stderr: {}",
            step.stderr
        );
        // `path:` placed the clone under the workspace; `ref:` checked it out.
        let cloned = ws.path().join("vendored/dep/hello.txt");
        assert!(cloned.exists(), "clone landed at the requested path");
        assert_eq!(
            std::fs::read_to_string(&cloned).unwrap(),
            "from-foreign-repo"
        );
    }

    #[test]
    fn foreign_checkout_default_path_is_the_repo_short_name() {
        // No `path:` ⇒ the clone lands in a subdir named after the repo, never
        // the workspace root (which would clobber the run's own positioned tree).
        let git = |dir: &std::path::Path, args: &[&str]| {
            assert!(Command::new("git")
                .current_dir(dir)
                .args(args)
                .output()
                .unwrap()
                .status
                .success());
        };
        let src = tempfile::tempdir().unwrap();
        git(src.path(), &["init", "-b", "main"]);
        git(src.path(), &["config", "user.email", "t@t.t"]);
        git(src.path(), &["config", "user.name", "t"]);
        std::fs::write(src.path().join("f"), "x").unwrap();
        git(src.path(), &["add", "."]);
        git(src.path(), &["commit", "-m", "i"]);
        // Rename the source dir's leaf so the default-path derivation is
        // observable: clone into `<workspace>/<leaf>`.
        let leaf = src
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();

        let ws = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          repository: {src}
"#,
            src = src.path().display()
        );
        let wf = workflow(&yaml);
        let mut e = Executor::new(ws.path());
        e.env_passthrough = true;
        let run = execute_workflow(&wf, &e).expect("clones with default path");
        assert_eq!(run.instances[0].steps[0].conclusion, StepConclusion::Success);
        assert!(
            ws.path().join(&leaf).join("f").exists(),
            "default clone path is the repo short name `{leaf}`"
        );
        // The workspace root itself was not turned into the clone.
        assert!(!ws.path().join("f").exists());
    }

    #[test]
    fn included_instance_keys_skip_non_selected_matrix_rows() {
        // R499-F3 phase 2: an operator picks one row of a matrix; the other
        // rows short-circuit to Skipped (same wire as a GHA `if: false`)
        // so downstream `needs.X.result` aggregation still sees them.
        let yaml = r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [a, b, c]
    steps:
      - run: echo "building ${{ matrix.target }}"
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        e.included_instance_keys = Some(["build#1".to_string()].into_iter().collect());
        let run = execute_workflow(&wf, &e).expect("execute");
        // Three instances scheduled; only row 1 actually executes.
        assert_eq!(run.instances.len(), 3);
        assert_eq!(run.instance_at("build", 0).unwrap().result, JobResult::Skipped);
        assert_eq!(run.instance_at("build", 1).unwrap().result, JobResult::Success);
        assert_eq!(run.instance_at("build", 2).unwrap().result, JobResult::Skipped);
        // Selected row actually ran a step; skipped rows have no steps.
        assert_eq!(run.instance_at("build", 0).unwrap().steps.len(), 0);
        assert_eq!(run.instance_at("build", 1).unwrap().steps.len(), 1);
    }

    #[test]
    fn matrix_filter_selects_by_value_not_position() {
        // The point of the value selector: a checked-in pipeline says WHICH board
        // it builds, and stays correct when the matrix gains a row ahead of it.
        // Same workflow, same filter, a value inserted at the front — and the
        // selected row moves with its value instead of staying at an index.
        let before = workflow(
            r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: [orangepi_zero2w, rpi_zero2w]
    steps:
      - run: echo "building ${{ matrix.board }}"
"#,
        );
        let after = workflow(
            r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: [stm32mp157c_dk2, orangepi_zero2w, rpi_zero2w]
    steps:
      - run: echo "building ${{ matrix.board }}"
"#,
        );
        let filtered = || {
            let mut e = Executor::new(workspace_path());
            e.env_passthrough = true;
            e.runner_os = "Linux".into();
            e.matrix_filter = [("board".to_string(), "rpi_zero2w".to_string())]
                .into_iter()
                .collect();
            e
        };

        let run = execute_workflow(&before, &filtered()).expect("execute before");
        assert_eq!(run.instances.len(), 2);
        assert_eq!(run.instance_at("build", 0).unwrap().result, JobResult::Skipped);
        assert_eq!(run.instance_at("build", 1).unwrap().result, JobResult::Success);
        // R330-B41: a matrix-filtered skip names the filter, not just "skipped".
        let reason = run.instance_at("build", 0).unwrap().skip_reason.as_deref().unwrap();
        assert!(reason.contains("rpi_zero2w"), "reason should name the filter: {reason}");

        // rpi_zero2w is now at index 2. A positional selector pinned to `build#1`
        // would have silently started building orangepi here.
        let run = execute_workflow(&after, &filtered()).expect("execute after");
        assert_eq!(run.instances.len(), 3);
        assert_eq!(run.instance_at("build", 0).unwrap().result, JobResult::Skipped);
        assert_eq!(run.instance_at("build", 1).unwrap().result, JobResult::Skipped);
        assert_eq!(run.instance_at("build", 2).unwrap().result, JobResult::Success);
        assert_eq!(run.instance_at("build", 2).unwrap().steps.len(), 1);
    }

    #[test]
    fn matrix_filter_leaves_jobs_without_that_dimension_alone() {
        // A filter NARROWS a fan-out; it does not disable jobs. `lint` has no
        // board dimension, so pinning a board must not make it disappear — and a
        // matrix job on an unrelated dimension is likewise untouched.
        let wf = workflow(
            r#"
on: [push]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - run: echo linting
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: [orangepi_zero2w, rpi_zero2w]
    steps:
      - run: echo "building ${{ matrix.board }}"
  docs:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        lang: [en, fr]
    steps:
      - run: echo "docs ${{ matrix.lang }}"
"#,
        );
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        e.matrix_filter = [("board".to_string(), "orangepi_zero2w".to_string())]
            .into_iter()
            .collect();
        let run = execute_workflow(&wf, &e).expect("execute");

        // `lint` has no matrix, so it has no matrix_index — reached via
        // `instance`, not `instance_at`.
        assert_eq!(run.instance("lint").unwrap().result, JobResult::Success);
        assert_eq!(run.instance_at("build", 0).unwrap().result, JobResult::Success);
        assert_eq!(run.instance_at("build", 1).unwrap().result, JobResult::Skipped);
        assert_eq!(run.instance_at("docs", 0).unwrap().result, JobResult::Success);
        assert_eq!(run.instance_at("docs", 1).unwrap().result, JobResult::Success);
    }

    #[test]
    fn matrix_filter_and_instance_keys_compose_as_and() {
        // Both selectors set: an instance has to survive both. The positional set
        // admits rows 0 and 1; the value filter admits only rpi_zero2w (row 1), so
        // exactly row 1 runs. An operator narrowing a run in the dashboard must not
        // be able to widen what a pinned pipeline builds.
        let wf = workflow(
            r#"
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: [orangepi_zero2w, rpi_zero2w, stm32mp157c_dk2]
    steps:
      - run: echo "building ${{ matrix.board }}"
"#,
        );
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_os = "Linux".into();
        e.included_instance_keys =
            Some(["build#0".to_string(), "build#1".to_string()].into_iter().collect());
        e.matrix_filter = [("board".to_string(), "rpi_zero2w".to_string())]
            .into_iter()
            .collect();
        let run = execute_workflow(&wf, &e).expect("execute");

        assert_eq!(run.instance_at("build", 0).unwrap().result, JobResult::Skipped);
        assert_eq!(run.instance_at("build", 1).unwrap().result, JobResult::Success);
        assert_eq!(run.instance_at("build", 2).unwrap().result, JobResult::Skipped);
    }

    fn run_in_fresh_workspace(wf: &Workflow) -> WorkflowRun {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = Executor::new(tmp.path());
        e.env_passthrough = true; // need PATH for bash/coreutils
        e.runner_os = "Linux".into();
        let run = execute_workflow(wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        std::mem::forget(tmp); // keep .qed-artifacts alive through assertions
        run
    }

    #[test]
    fn needs_ordered_producer_runs_before_consumer() {
        // R516-B1 happy path (recast off tier-3 artifacts onto run: steps after
        // R533-T7 retired upload/download-artifact): a `needs`-ordered
        // producer→consumer pair both succeed, pinning that the wave scheduler
        // runs the producer before the consumer.
        let yaml = r#"
on: [push]
jobs:
  producer:
    runs-on: ubuntu-latest
    steps:
      - run: echo "payload-bytes" > artifact.txt
  consumer:
    needs: [producer]
    runs-on: ubuntu-latest
    steps:
      - run: echo "consume"
"#;
        let wf = workflow(yaml);
        let run = run_in_fresh_workspace(&wf);
        assert_eq!(run.instance("producer").unwrap().result, JobResult::Success);
        let consumer = run.instance("consumer").unwrap();
        assert_eq!(consumer.result, JobResult::Success);
        assert_eq!(consumer.steps[0].conclusion, StepConclusion::Success);
    }

    #[test]
    fn failed_producer_skips_consumer_via_needs_gate() {
        // R516-B1 regression: when the producer fails, the consumer that
        // `needs` it must be SKIPPED (GHA implicit needs-gate), and the workflow
        // must complete (return Ok), not abort. (Recast off tier-3 artifacts
        // onto run: steps after R533-T7 retired upload/download-artifact.)
        let yaml = r#"
on: [push]
jobs:
  producer:
    runs-on: ubuntu-latest
    steps:
      - run: exit 1
      - run: echo "never reached"
  consumer:
    needs: [producer]
    runs-on: ubuntu-latest
    steps:
      - run: echo "consume"
"#;
        let wf = workflow(yaml);
        let run = run_in_fresh_workspace(&wf);
        assert_eq!(run.instance("producer").unwrap().result, JobResult::Failure);
        // Second step skipped (prior step failed without continue-on-error).
        let producer = run.instance("producer").unwrap();
        assert_eq!(producer.steps[1].conclusion, StepConclusion::Skipped);
        // Consumer skipped by the needs-gate; it never reached its step.
        let consumer = run.instance("consumer").unwrap();
        assert_eq!(consumer.result, JobResult::Skipped);
        assert!(consumer.steps.is_empty(), "consumer must not run any step");
        // R330-B41: the skip names the dependency that failed, not just "skipped".
        let reason = consumer.skip_reason.as_deref().unwrap();
        assert!(reason.contains("producer"), "reason should name the dep: {reason}");
        assert!(reason.contains("failed"), "reason should say why: {reason}");
    }

    #[test]
    fn explicit_if_overrides_needs_gate() {
        // A consumer with an explicit `if: always()` opts out of the implicit
        // needs-gate (GHA semantics) and runs even though its producer failed.
        // This is the `publish-*` job shape in release.yml.
        let yaml = r#"
on: [push]
jobs:
  producer:
    runs-on: ubuntu-latest
    steps:
      - run: exit 1
  consumer:
    needs: [producer]
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: echo "ran anyway"
"#;
        let wf = workflow(yaml);
        let run = run_in_fresh_workspace(&wf);
        assert_eq!(run.instance("producer").unwrap().result, JobResult::Failure);
        assert_eq!(run.instance("consumer").unwrap().result, JobResult::Success);
    }

    #[test]
    fn skip_propagates_via_needs_result() {
        // Job B's `if:` reads `needs.A.result` — A is skipped, B should still
        // run as long as its gate accepts skipped/success.
        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    if: false
    steps:
      - run: echo "won't run"
  b:
    needs: [a]
    if: always() && needs.a.result != 'failure'
    runs-on: ubuntu-latest
    steps:
      - run: echo "B ran"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let a = run.instance("a").unwrap();
        assert_eq!(a.result, JobResult::Skipped);
        // R330-B41: an if:-false skip names the condition, not just "skipped".
        let reason = a.skip_reason.as_deref().unwrap();
        assert!(reason.contains("false"), "reason should name the if: {reason}");
        assert_eq!(run.instance("b").unwrap().result, JobResult::Success);
    }

    /// The workflow shape R654-T1 exists for: a hosted-runner-shape step
    /// (relocating Docker's storage onto the hosted scratch volume,
    /// `sudo apt-get install`ing what the hosted image lacks) gated so it
    /// no-ops off GitHub, with a local-only sibling gated the other way.
    const RUNNER_ENVIRONMENT_GATE: &str = r#"
on: [push]
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: hosted
        if: runner.environment == 'github-hosted'
        run: echo "ran=hosted" >> "$GITHUB_OUTPUT"
      - id: local
        if: runner.environment != 'github-hosted'
        run: echo "ran=local" >> "$GITHUB_OUTPUT"
      - id: report
        run: |
          echo "ctx=${{ runner.environment }}" >> "$GITHUB_OUTPUT"
          echo "envvar=$RUNNER_ENVIRONMENT" >> "$GITHUB_OUTPUT"
"#;

    #[test]
    fn self_hosted_runner_environment_skips_the_hosted_only_step() {
        // A QED run is not on a GitHub-hosted runner, so the hosted-only step
        // must skip and its local sibling must run. Before R654-T1 `runner`
        // carried only {os, arch}, so `runner.environment` resolved to null.
        let wf = workflow(RUNNER_ENVIRONMENT_GATE);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_environment = "self-hosted".into();
        let run = execute_workflow(&wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(inst.steps[0].conclusion, StepConclusion::Skipped);
        assert_eq!(inst.steps[1].conclusion, StepConclusion::Success);
        assert_eq!(
            inst.steps[1].outputs.get("ran"),
            Some(&Value::String("local".into()))
        );
        // The value is readable as an expression AND as the env var GitHub's
        // own runner exports, so a `run:` body can branch on either.
        assert_eq!(
            inst.steps[2].outputs.get("ctx"),
            Some(&Value::String("self-hosted".into()))
        );
        assert_eq!(
            inst.steps[2].outputs.get("envvar"),
            Some(&Value::String("self-hosted".into()))
        );
    }

    // ── R785-B1: `working-directory:` actually moves the step's cwd ───────

    /// A `run:` step's `working-directory:` must resolve relative to the
    /// executor's workspace and actually become the shell's cwd — before the
    /// fix `run_bash_step` always used `executor.workspace`, so `pwd` inside
    /// the step reported the repo root regardless of what the step declared.
    #[test]
    fn step_working_directory_moves_the_shell_cwd() {
        let sub = workspace_path().join("R785-B1-step-sub");
        std::fs::create_dir_all(&sub).unwrap();
        let expected = std::fs::canonicalize(&sub).unwrap();

        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    steps:
      - id: where
        working-directory: R785-B1-step-sub
        run: echo "cwd=$(pwd -P)" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("a").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        let cwd = inst.steps[0]
            .outputs
            .get("cwd")
            .unwrap_or_else(|| panic!("no cwd output; steps: {:?}", inst.steps))
            .as_str_lossy();
        let got = std::fs::canonicalize(PathBuf::from(cwd)).unwrap();
        assert_eq!(got, expected);
    }

    /// With no step-level override, `defaults.run.working-directory` on the
    /// job applies — the GHA precedence `resolve_working_directory` (R785-B1)
    /// implements: step > job defaults > workflow defaults > workspace root.
    #[test]
    fn job_defaults_working_directory_applies_without_a_step_override() {
        let sub = workspace_path().join("R785-B1-job-sub");
        std::fs::create_dir_all(&sub).unwrap();
        let expected = std::fs::canonicalize(&sub).unwrap();

        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: R785-B1-job-sub
    steps:
      - id: where
        run: echo "cwd=$(pwd -P)" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("a").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        let cwd = inst.steps[0].outputs.get("cwd").unwrap().as_str_lossy();
        let got = std::fs::canonicalize(PathBuf::from(cwd)).unwrap();
        assert_eq!(got, expected);
    }

    /// A step-level `working-directory:` wins over the job's
    /// `defaults.run.working-directory` — matches GHA's own precedence.
    #[test]
    fn step_working_directory_overrides_job_defaults() {
        let job_sub = workspace_path().join("R785-B1-precedence-job");
        let step_sub = workspace_path().join("R785-B1-precedence-step");
        std::fs::create_dir_all(&job_sub).unwrap();
        std::fs::create_dir_all(&step_sub).unwrap();
        let expected = std::fs::canonicalize(&step_sub).unwrap();

        let yaml = r#"
on: [push]
jobs:
  a:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: R785-B1-precedence-job
    steps:
      - id: where
        working-directory: R785-B1-precedence-step
        run: echo "cwd=$(pwd -P)" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let run = run_with_path(&wf);
        let inst = run.instance("a").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        let cwd = inst.steps[0].outputs.get("cwd").unwrap().as_str_lossy();
        let got = std::fs::canonicalize(PathBuf::from(cwd)).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn github_hosted_runner_environment_runs_the_hosted_only_step() {
        // The other direction: QED invoked from inside a GitHub-hosted job
        // inherits RUNNER_ENVIRONMENT=github-hosted, and the same workflow
        // then runs its hosted-shape step and skips the local one.
        let wf = workflow(RUNNER_ENVIRONMENT_GATE);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.runner_environment = "github-hosted".into();
        let run = execute_workflow(&wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        let inst = run.instance("one").unwrap();
        assert_eq!(inst.result, JobResult::Success);
        assert_eq!(inst.steps[0].conclusion, StepConclusion::Success);
        assert_eq!(
            inst.steps[0].outputs.get("ran"),
            Some(&Value::String("hosted".into()))
        );
        assert_eq!(inst.steps[1].conclusion, StepConclusion::Skipped);
        assert_eq!(
            inst.steps[2].outputs.get("ctx"),
            Some(&Value::String("github-hosted".into()))
        );
    }

    #[test]
    fn dynamic_matrix_narrows_the_executed_fan_out_from_inputs() {
        // R654-F2 end to end: `executor.inputs` reaches the matrix expansion,
        // so a caller-narrowed matrix actually runs one row — not the two the
        // literal fallback would produce, and not one row whose `matrix.board`
        // is the un-evaluated `${{ … }}` source text.
        let yaml = r#"
on: [workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        board: ${{ fromJSON(inputs.board && format('["{0}"]', inputs.board) || '["rpi_zero2w","rpi4"]') }}
    steps:
      - id: a
        run: echo "board=${{ matrix.board }}" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);

        let mut wide = Executor::new(workspace_path());
        wide.env_passthrough = true;
        let run = execute_workflow(&wf, &wide).unwrap_or_else(|err| panic!("execute: {err}"));
        assert_eq!(run.instances.len(), 2);

        let mut narrow = Executor::new(workspace_path());
        narrow.env_passthrough = true;
        narrow.inputs = crate::expr::obj([("board", "rpi4")]);
        let run = execute_workflow(&wf, &narrow).unwrap_or_else(|err| panic!("execute: {err}"));
        assert_eq!(run.instances.len(), 1);
        assert_eq!(run.instances[0].result, JobResult::Success);
        assert_eq!(
            run.instances[0].steps[0].outputs.get("board"),
            Some(&Value::String("rpi4".into()))
        );
    }

    #[test]
    fn runner_environment_defaults_to_self_hosted_off_a_github_runner() {
        // The default the noisetable wrap depends on. Guarded so the assertion
        // stays honest if this suite ever runs inside a GitHub-hosted job,
        // where inheriting `github-hosted` is the correct answer.
        let e = Executor::bare(workspace_path());
        match std::env::var("RUNNER_ENVIRONMENT") {
            Ok(v) if !v.trim().is_empty() => assert_eq!(e.runner_environment, v),
            _ => assert_eq!(e.runner_environment, "self-hosted"),
        }
    }

    // ─── tier-2 env floor ───────────────────────────────────────────────────

    fn github_ctx(pairs: &[(&str, &str)]) -> Value {
        let mut m: IndexMap<String, Value> = IndexMap::new();
        for (k, v) in pairs {
            m.insert((*k).into(), Value::String((*v).into()));
        }
        Value::Object(m)
    }

    #[test]
    fn env_floor_projects_the_tier2_repo_context() {
        let mut e = Executor::bare(workspace_path());
        e.github = github_ctx(&[
            ("repository", "yah-ai/yah"),
            ("sha", "deadbeef"),
            ("ref", "refs/tags/v0.8.20"),
            ("ref_name", "v0.8.20"),
            ("event_name", "push"),
            ("actor", "Yah Dev"),
        ]);
        let floor = gha_env_floor(&e);
        assert_eq!(floor.get("CI").map(String::as_str), Some("true"));
        assert_eq!(
            floor.get("GITHUB_REPOSITORY").map(String::as_str),
            Some("yah-ai/yah")
        );
        assert_eq!(floor.get("GITHUB_SHA").map(String::as_str), Some("deadbeef"));
        assert_eq!(
            floor.get("GITHUB_REF").map(String::as_str),
            Some("refs/tags/v0.8.20")
        );
        assert_eq!(
            floor.get("GITHUB_REF_NAME").map(String::as_str),
            Some("v0.8.20")
        );
        assert_eq!(
            floor.get("GITHUB_EVENT_NAME").map(String::as_str),
            Some("push")
        );
        assert_eq!(floor.get("GITHUB_ACTOR").map(String::as_str), Some("Yah Dev"));
        assert_eq!(
            floor.get("GITHUB_WORKSPACE").map(String::as_str),
            Some(workspace_path().display().to_string().as_str())
        );
    }

    #[test]
    fn env_floor_withholds_github_actions_and_service_handles() {
        // The load-bearing omission (W224 tier 3): QED provides no GITHUB_TOKEN,
        // no REST API, no OIDC. Claiming `GITHUB_ACTIONS=true` would invite a
        // step to take a branch that needs all three. `CI` carries the honest
        // half. If this assertion is ever relaxed, tier-3 has to land first.
        let mut e = Executor::bare(workspace_path());
        e.github = github_ctx(&[("repository", "yah-ai/yah"), ("sha", "deadbeef")]);
        let floor = gha_env_floor(&e);
        assert!(!floor.contains_key("GITHUB_ACTIONS"));
        assert!(!floor.contains_key("GITHUB_TOKEN"));
        assert!(!floor.contains_key("GITHUB_RUN_ID"));
        assert!(!floor.contains_key("GITHUB_API_URL"));
    }

    #[test]
    fn env_floor_skips_unknown_context_members_rather_than_exporting_empty() {
        // A workspace with no git origin has no `repository`; a step doing
        // `${GITHUB_REPOSITORY:-fallback}` must see the fallback, which an
        // exported empty string would defeat.
        let mut e = Executor::bare(workspace_path());
        e.github = github_ctx(&[("repository", ""), ("sha", "deadbeef")]);
        let floor = gha_env_floor(&e);
        assert!(!floor.contains_key("GITHUB_REPOSITORY"));
        assert!(floor.contains_key("GITHUB_SHA"));
    }

    #[test]
    fn workflow_env_overrides_the_floor_and_steps_observe_ci() {
        // Two contracts in one run: `$CI` reaches a real bash step (the thing
        // the placement gate keys on), and the floor sits at the BOTTOM of the
        // precedence stack the way a real runner's env does.
        let yaml = r#"
on: [push]
env:
  GITHUB_REF_NAME: shadowed-by-workflow-env
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - id: report
        run: |
          echo "ci=$CI" >> "$GITHUB_OUTPUT"
          echo "sha=$GITHUB_SHA" >> "$GITHUB_OUTPUT"
          echo "refname=$GITHUB_REF_NAME" >> "$GITHUB_OUTPUT"
"#;
        let wf = workflow(yaml);
        let mut e = Executor::new(workspace_path());
        e.env_passthrough = true;
        e.github = github_ctx(&[("sha", "deadbeef"), ("ref_name", "v0.8.20")]);
        let run = execute_workflow(&wf, &e).unwrap_or_else(|err| panic!("execute: {err}"));
        let s = &run.instance("one").unwrap().steps[0];
        assert_eq!(s.conclusion, StepConclusion::Success);
        assert_eq!(s.outputs.get("ci"), Some(&Value::String("true".into())));
        assert_eq!(s.outputs.get("sha"), Some(&Value::String("deadbeef".into())));
        assert_eq!(
            s.outputs.get("refname"),
            Some(&Value::String("shadowed-by-workflow-env".into()))
        );
        // `GITHUB_ACTIONS` is asserted absent at the floor, not here: this run
        // uses `env_passthrough`, so on a real GitHub runner the ambient value
        // legitimately leaks through — and there it is true.
    }
}
