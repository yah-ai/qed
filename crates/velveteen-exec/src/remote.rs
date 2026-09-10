//! @yah:ticket(R299-F7, "Remote step dispatch (where=remote): run qed steps as task::remote workloads on yubaba")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-23T01:46:59Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R299)
//! @arch:see(.yah/docs/working/W126-yah-qed.md)
//! @yah:depends_on(R299-T5)
//! @yah:handoff("Remote step dispatch wired in crates/yah/qed/src/runner.rs. PipelineRunner::new_remote(pipeline, scryer, yubaba) added; execute_step_remote maps QedStep → ForgeSpec (RemoteAny/infra tier), dispatches via RemoteForgeDriver, records forge_id in StepStatus.task_run_id. RunWhere enum exported from qed lib. CLI --where=remote gives clear error 'yubaba RPC client (R091) not yet implemented'. 3 new tests (remote_step_success, remote_step_failure, remote_abort_on_fail) all pass. cargo test -p qed: 6/6 ok, cargo check -p yah: clean.")
//! @yah:verify("cargo test -p qed  # 6/6 pass (includes 3 new remote_* tests)")
//! @yah:verify("cargo check -p qed -p yah  # clean")
//! @yah:verify("yah qed run --where=remote check  # exits with 'yubaba RPC client (R091) not yet implemented'")
//!
//! @yah:ticket(R380-T2, "Migrate task crate internal callsites to TaskPlacement (remote.rs, meta.rs, list.rs)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:06:04Z)
//! @yah:status(review)
//! @yah:parent(R380)
//! @yah:next("Replace ForgeSpec.where_ field type from ForgeWhere → TaskPlacement.")
//! @yah:next("remote.rs build_workload_spec switches on placement.location for tier defaulting; the placement.runtime must be Container or it's a programmer error (return InvalidSpec).")
//! @yah:next("list.rs wants_remote / wants_integration become matches on TaskPlacement.location.")
//! @yah:next("meta.rs ForgeMeta.where_ stays a placement field (no Integration there yet — T8 sorts that out).")
//! @yah:handoff("T2 complete: ForgeSpec.where_ and ForgeMeta.where_ both migrated from ForgeWhere → TaskPlacement. ForgeListFilter.where_ likewise. remote::build_workload_spec now requires runtime=Container (returns InvalidSpec for remote+native — pre-stub for T7) and matches on TaskPlacement.location for tier defaulting. From<TaskRunMeta> for ForgeMeta sets where_ to {Local, Native}. list.rs: wants_local + wants_remote now match TaskPlacement.location; wants_integration dropped (TaskLocation has no Integration variant); integration_metas always merge into results (placement filter can't address them until T8 adds a sibling species field). qed/runner.rs::execute_step_remote bridged to construct TaskPlacement directly (one-line change, leaves the wider qed migration — RunWhere refactor + --runtime CLI flag — to T3). All 47 task crate tests pass; cargo check --workspace clean.")
//! @yah:next("T8 sweep: add ForgeMeta.species: ForgeSpecies (Local|Remote|Integration) sibling field + restore filtered Integration enumeration in ForgeListFilter; delete the integration_metas_always_merge_until_t8 placeholder test.")
//! @yah:verify("cargo test -p task --lib  # 47/47 pass")
//! @yah:verify("cargo check --workspace  # clean")
//! @yah:gotcha("Pre-existing failure in qed::tests::test_builtin_release_build_pipeline (asserts 4 steps but builtin_release_build() now has 6) — unrelated to this ticket; should be fixed in its own bug ticket.")
//! @yah:gotcha("filter_by_where_integration test was deleted; replaced by integration_metas_always_merge_until_t8 which pins the new interim semantics. T8 should reintroduce species-based integration filtering.")
//!
//! @yah:ticket(R380-T7, "Remote + native quadrant: refuse at WardenClient seam in v1 (with clear error + v2 hook)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:06:28Z)
//! @yah:status(review)
//! @yah:parent(R380)
//! @yah:next("Decision recommendation: refuse remote+native in v1. Implement only when a real use case arrives (BuildKit shelling to host docker on a yubaba node is the leading candidate).")
//! @yah:next("WardenClient::deploy receives a TaskPlacement; if runtime=Native, return RemoteForgeError::InvalidSpec('yubaba-native exec not supported in v1; use runtime=container').")
//! @yah:next("Document the v2 hook: WardenClient gets a separate exec_native(spec) method later, parallel to deploy(). Don't add it now — type-level option only.")
//! @yah:next("Test: a remote+native ForgeSpec fails with InvalidSpec at start() and emits no events.")
//! @yah:handoff("v1 refusal seam for remote+native locked in. The refusal lives in task::remote::build_workload_spec — the very first thing RemoteForgeDriver::start does — so a remote+native ForgeSpec never allocates a ForgeId in scryer's namespace, never spawns the log-ingest task, and never reaches WardenClient::deploy. The error message now reads 'remote + native is not supported in v1 — set placement.runtime = container, or run locally with placement.location = local. A future yubaba `exec_native` surface lands when a real use case arrives (R380-T7 / W149).'")
//! @yah:handoff("WardenClient trait docs now carry the v2 contract: an explicit 'Remote + native: not in v1' section explains that `deploy` is image-backed only and that v2 adds a sibling `async fn exec_native(spec) -> ...` when a real use case lands (BuildKit shelling to host docker on a yubaba node is the leading W149 candidate). No exec_native method added — type-level option only, per the ticket's instructions.")
//! @yah:handoff("Test: remote::remote_native_refused_at_start_emits_no_events asserts (a) start() returns Err(InvalidSpec) with the right message, (b) yubaba.deploy was never called, (c) scryer holds no Forge-scoped events after the refusal. ScriptedWardenClient gained a deploy_called: Arc<Mutex<bool>> so the assertion is structural, not log-grep. cargo test -p task --lib: 54/54 pass (53 before + the new refusal test). cargo check --workspace clean.")
//! @yah:next("R380-T8 (cleanup, the last child) picks up next: drop ForgeWhere from task + tower-rules, move Integration off the placement enum onto a sibling ForgeMeta.species field, delete ForgeWhere.ts, restore species-based Integration filtering in ForgeListFilter (replaces the integration_metas_always_merge_until_t8 placeholder test that T2 left behind), and sweep .yah/docs/architecture/A035-yah-forge.md + arch refs for stale ForgeWhere mentions.")
//! @yah:next("Future work for v2 exec_native: when the first real use case arrives (e.g. BuildKit shelling to host docker on a yubaba node), the v2 PR adds `async fn exec_native(&self, spec: &NativeExecSpec) -> Result<..., RemoteForgeError>` to WardenClient and a parallel `start_native` path on RemoteForgeDriver. Until then the type-level hook stays trait-docs-only — no NativeExecSpec, no exec_native method, no premature surface.")
//! @yah:verify("cargo test -p task --lib  # 54 pass, 2 ignored")
//! @yah:verify("cargo test -p task --lib remote_native_refused_at_start_emits_no_events  # the new test in isolation")
//! @yah:verify("cargo check --workspace  # clean (pre-existing desktop warnings unrelated)")
//!
//!
//! @yah:ticket(R636-B2, "Rootless BuildKit cannot start under kamaji's OCI sandbox — every offloaded build-image step dies before the first layer")
//! @yah:status(review)
//! @yah:at(2026-08-05T04:30:58Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R636)
//! @yah:severity(high)
//! @yah:next("MEASURED LADDER (us-west-002, 2026-08-04): reproduced kamaji's OCI knobs one at a time with `sudo docker run` against moby/buildkit:v0.12.5-rootless and the real rusty-v8-musl-builder context. kamaji's spec (kamaji-containerd-core build_oci_spec_with): caps dropped to CAP_NET_BIND_SERVICE only (GRANTED_CAPABILITY), noNewPrivileges=true, NO seccomp profile, no /dev/fuse.")
//! @yah:next("(A) default docker seccomp: 'rootlesskit:parent error: failed to start the child: fork/exec /proc/self/exe: operation not permitted'.")
//! @yah:next("(B) seccomp=unconfined + no-new-privileges: 'failed to setup UID/GID map: newuidmap ... fork/exec /usr/bin/newuidmap: operation not permitted'. newuidmap is setuid-root, so no-new-privileges blocks it, and the reduced bounding set blocks CAP_SETUID anyway.")
//! @yah:next("(C) + --cap-add=SETUID --cap-add=SETGID and NO no-new-privileges: rootlesskit STARTS and the R636-B1 remote context loads ('#1 load remote build context', '#2 copy /context /'), then fails 'lsetxattr /verify-consumer.sh: operation not supported'.")
//! @yah:next("(D) C + --device /dev/fuse: identical lsetxattr failure. So the privilege chain up to the snapshotter is fully characterized and the snapshotter is the one remaining unknown — try BUILDKITD_FLAGS=--oci-worker-snapshotter=native, or run the NON-rootless moby/buildkit under a wider grant.")
//! @yah:next("THE DECISION THIS NEEDS IS THE OPERATOR'S, WHICH IS WHY IT IS A SEPARATE TICKET: making this work means kamaji granting CAP_SETUID+CAP_SETGID and dropping noNewPrivileges for a class of workloads. That is a real widening of the container sandbox on every build-worker. Scope it as a guarded opt-in (annotation, tier=infra only) mirroring how workload_spec::HOST_NETWORK_ANNOTATION is already gated — not as a blanket relaxation of GRANTED_CAPABILITY.")
//! @yah:verify("`yah qed images build rusty-v8-musl-builder --platform linux/amd64` from the arm64 camp Mac runs BuildKit to completion on us-west-002 and writes the OCI archive to /var/lib/yah/qed/build-out.")
//! @yah:verify("The widened privileges are opt-in and scoped: a non-forge, non-infra workload still gets kamaji's CAP_NET_BIND_SERVICE-only + noNewPrivileges baseline. Assert it with a build_oci_spec test, not by inspection.")
//! @yah:gotcha("This is NOT the R636-B1 cross-host context gap and is not caused by it. B1 is fixed and independently proven: the same context tarball, fetched over `--opt context=<url>`, builds the real rusty-v8-musl-builder Dockerfile (syntax frontend, alpine:edge, apk layer running) under a privileged buildkit. What B1's fix did was clear the two mount failures that were masking this one — the camp-root bind and the missing /var/lib/yah/qed/build-out — so this is the next wall, not a regression.")
//! @yah:gotcha("The kamaji-side symptom is nearly silent: the run reports 'buildkit exited with code 1' with ZERO streamed log lines, and the container's stdout/stderr are FIFOs under /var/log/yah/yah/forge-<id>/ that nothing persists. The only visible trace is `journalctl -u kamaji`: 'could not connect to unix:///run/user/1000/buildkit/buildkitd.sock after 10 trials'. Budget for that: any diagnosis here starts by reproducing under `sudo docker run` on the box, not by reading qed output.")
//! @yah:gotcha("CROSS-RELAY COUPLING with R555-F4 (noted by R555-S1/spade, 2026-08-04). The widening this ticket needs - CAP_SETUID + CAP_SETGID, noNewPrivileges off, on every build-worker - is the same kamaji policy surface R555-F4 ('kamaji admission: only signed recipes may run remotely') exists to TIGHTEN. F4's gotcha states the stake: 'remote execution without signed-recipe verification is a remote-code-execution surface on shared infra.' Widening raises the cost of an admission gap from 'arbitrary code in a tight sandbox' to 'arbitrary code with SETUID and no-new-privs off'.")
//! @yah:gotcha("GOOD NEWS: this ticket's proposed shape already IS the resolution. The 'guarded opt-in (annotation, tier=infra only) mirroring HOST_NETWORK_ANNOTATION' in the next-steps is exactly the admission mechanism R555-F4 has to build. So the two should land as ONE piece of work rather than a widening that F4 retrofits a gate around later. Whoever takes this first should read R555-F4 and W235 section (c) before designing the annotation - and note F4's own HARD prereqs (W217 signed asset catalog + the W233 signing conclusion) apply to the gate, not just to F4's paperwork.")
//! @yah:handoff("ROOT-CAUSED AND BUILT (2026-08-04). (1) The lsetxattr wall was NEVER the snapshotter. 'lsetxattr /verify-consumer.sh: operation not supported' came from SCHILY.xattr.com.apple.provenance pax headers in a context tarball packed by macOS tar -- every source file in this repo carries that xattr on the camp Mac, and Linux rejects the com.apple.* namespace outright, so BuildKit dies at 'copy /context /' before it reads the Dockerfile. A/B proof under IDENTICAL caps on us-west-002: mac-tar context -> the exact lsetxattr error; GNU-tar context -> build green through 'exporting to oci image format'. The production path never had this bug (qed::build_context::pack_context uses the Rust tar crate, which builds headers from fs::Metadata and has no xattr support), so ladder step (D) was diagnosing a hand-made tarball. Locked by build_context::tests::packs_without_extended_attributes.")
//! @yah:handoff("(2) MINIMAL GRANT MEASURED EXACTLY. CAP_SETUID + CAP_SETGID + noNewPrivileges=false, each INDIVIDUALLY necessary: baseline -> 'fork/exec /usr/bin/newuidmap: operation not permitted'; +SETUID only -> 'fork/exec /usr/bin/newgidmap: operation not permitted'; +SETUID+SETGID with nnp ON -> 'newuidmap: Could not set caps'; all three -> starts, build runs to completion. Not CAP_SYS_ADMIN (that is what a NON-rootless buildkitd would need instead -- far wider). Emptying /etc/subuid to force rootlesskit's single-mapping path does not avoid the setuid helpers either: it fails earlier with 'No subuid ranges found', and a self-only range (user:1000:1) still execs newuidmap. So there is no zero-widening path through rootless BuildKit.")
//! @yah:handoff("(3) SHIPPED: a guarded opt-in exactly as the ticket specified. workload_spec::NESTED_SANDBOX_ANNOTATION ('yah.sandbox') / NESTED_SANDBOX_VALUE ('nested') + WorkloadSpec::wants_nested_sandbox() (oss/yah-base/crates/workload-spec/src/lib.rs, mirroring wants_host_network); the grant in kamaji-containerd-core::build_oci_spec_with keyed on it (NESTED_SANDBOX_CAPABILITIES const), which also raises RLIMIT_NOFILE 1024->65536 as HEADROOM (explicitly NOT a measured requirement -- a small build passes at 1024); tier=infra refusal in BOTH backends (kamaji-bin/src/containerd.rs validate_spec_for_constable, kamaji/src/containerd.rs deploy_workload); and exactly ONE setter in the whole tree, velveteen-exec's build_image_workload_spec.")
//! @yah:handoff("TESTS (all green): kamaji-containerd-core oci_spec_baseline_sandbox_is_unchanged_without_the_annotation (the ticket's second verify criterion, asserted not inspected -- also proves host-networking does not drag the caps along), oci_spec_nested_sandbox_grants_setuid_setgid_and_drops_no_new_privs, oci_spec_nested_sandbox_leaves_namespaces_and_mounts_alone; kamaji-bin validate_{rejects,allows}_nested_sandbox_for_{non_infra,infra}_tier; workload-spec nested_sandbox_marker_is_opt_in_and_reads_back + _is_independent_of_the_other_markers; velveteen-exec build_image_workload_asks_for_the_nested_sandbox_grant + subprocess_workload_does_not_ask_for_the_nested_sandbox_grant. Suites: kamaji workspace --all-features all green (incl. 32/32 containerd-core), yah-workload-spec 67/67, velveteen-exec 95/95, yah-qed 714/715, root cargo check --workspace clean.")
//! @yah:gotcha("CORRECTION to this ticket's own ladder (2026-08-04, R636-B2 session): step (D)'s 'the snapshotter is the one remaining unknown' is FALSE and should not be carried forward. The lsetxattr failure was macOS-tar xattrs in the context tarball, not the snapshotter -- BUILDKITD_FLAGS=--oci-worker-snapshotter=native changes nothing, and the default overlay snapshotter builds fine once the tarball is clean. Left as an append rather than a rewrite because (A)-(C) were true as measured; only (D)'s conclusion was wrong.")
//! @yah:verify("cargo test -p kamaji-containerd-core --lib --features containerd-integration  # 32/32, incl. the baseline-unchanged + grant + namespaces-untouched trio")
//! @yah:verify("cargo test -p yah-qed --lib build_context  # 6/6, incl. packs_without_extended_attributes (the macOS-xattr regression lock)")
//! @yah:verify("yah-qed lib has ONE unrelated flake: waitfor::tests::tcp_probe_fails_against_a_dead_port fails under full-suite parallelism and passes in isolation (port race, not this change).")
//! @yah:next("THE ONLY REMAINING STEP IS THE OPERATOR-CONSENT DEPLOY, and it is deliberately not done. The code is complete and green but INERT on the fleet: us-west-002 runs the kamaji built 2026-07-19, which knows nothing of yah.sandbox, so an offloaded build-image step still fails exactly as it does today. Nothing widens until someone rolls kamaji. I asked via ask_user and the form timed out unanswered after 30m; I did not deploy on a timeout.")
//! @yah:next("DEPLOY RECIPE (grounded, follows the box's own .bak convention -- /usr/local/bin already holds kamaji.0.8.17/18/19.bak): (1) cross-build from oss/kamaji with CC_x86_64_unknown_linux_musl=x86_64-linux-musl-gcc CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc cargo build -p kamaji-bin --bin kamaji --target x86_64-unknown-linux-musl --features containerd-integration --release; (2) ssh -i ~/.ssh/yah struc@100.64.0.4, sudo cp /usr/local/bin/kamaji /usr/local/bin/kamaji.0.8.20.bak; (3) scp the new binary to /usr/local/bin/kamaji; (4) sudo systemctl restart kamaji (unit is /etc/systemd/system/kamaji.service, enabled, currently up since 2026-08-04 02:30 PDT); (5) run the first @yah:verify from the camp Mac. Rollback is a cp from the .bak plus a restart.")
//! @yah:next("BLAST RADIUS OF THE DEPLOY, stated plainly so the sign-off is informed: us-west-002 only. It is the qed dogfood box, carries taints no-server/no-appliance/no-voter, and hosts no serving workloads. The grant itself reaches only a workload that BOTH sets yah.sandbox=nested AND is tier=infra; the sole setter in the tree is velveteen-exec::build_image_workload_spec. A subprocess forge on the same node is unaffected.")
//! @yah:next("WIRE PATH IS ALREADY PROVEN, so the deploy should not surprise: annotations are a plain HashMap field on WorkloadSpec and survive the postcard UDS (kamaji-proto codec::deploy_container_round_trip, 25/25 green, and its fixture spec carries an annotation), and yubaba admission READS annotations off the spec rather than rebuilding it (cloud/src/config.rs:1045 does exactly this for NODE_SELECTOR_MESH_TAGS_ANNOTATION, which is live-proven by the R594/R631 offload routing). So there is no untested hop between the camp and build_oci_spec_with.")
//! @yah:gotcha("DO NOT DEPLOY A KAMAJI BUILT FROM THIS WORKING TREE AS-IS. R577-T1 (@Ashguard:eclipse, live at time of writing) has substantial UNCOMMITTED in-flight work in the same crate: a new `native-exec` cargo feature (kamaji-bin/Cargo.toml), --native-exec-dir / KAMAJI_NATIVE_EXEC_DIR plumbing in kamaji-bin/src/main.rs, and ~443 changed lines in kamaji-bin/src/server.rs. I cross-built x86_64-unknown-linux-musl green (19.6s, only the pre-existing benign `-z stacksize ignored` linker warning), which derisks the compile step -- but that binary carries eclipse's R577 work too. The `native-exec` FEATURE is off by default, which bounds the risk, but the server.rs changes are in the default build path. Before rolling to us-west-002, either wait for R577-T1 to land and re-verify, or build from a commit that has my four files and not theirs. This is exactly the shared-tree trap: `git status` on the kamaji crates looks like one change and is two.")
//! @yah:gotcha("MY FILES, so a deploy build can be scoped precisely: oss/yah-base/crates/workload-spec/src/lib.rs (annotation const + wants_nested_sandbox + 2 tests), oss/kamaji/crates/kamaji-containerd-core/src/lib.rs (NESTED_SANDBOX_CAPABILITIES + the build_oci_spec_with branch + 3 tests), oss/kamaji/crates/kamaji/src/containerd.rs (tier guard), oss/kamaji/crates/kamaji-bin/src/containerd.rs (tier guard + 2 tests), oss/qed/crates/velveteen-exec/src/remote.rs (the single annotation setter + 2 tests), oss/qed/crates/qed/src/build_context.rs (the no-xattr regression test). Nothing else in the diff of those crates is mine.")
//! @yah:handoff("CODE COMPLETE, DEPLOY DELIBERATELY NOT DONE -- in review for the operator consent the ticket itself said this needed. The sandbox widening is built as the guarded opt-in the ticket specified, every unit test is green, and the x86_64-musl cross-build succeeds; nothing on the fleet has changed and nothing will until someone rolls kamaji to us-west-002. See @yah:next for the grounded deploy recipe and @yah:gotcha for the shared-tree trap that makes a naive build of this tree unsafe to ship.")

// @yah:ticket(R094-F3, "Remote-forge driver: synthesize WorkloadSpec from ForgeSpec, deploy via yubaba RPC, attach containerd-logs scryer adapter scoped to Forge(id)")
// @yah:assignee(agent:claude)
// @yah:status(review)
// @yah:phase(P2)
// @yah:parent(R094)
// @yah:handoff("crates/yah/task/src/remote.rs — WardenClient trait (deploy/connect_logs/teardown/exit_code) + RemoteForgeDriver::start (allocates ForgeId, synthesizes WorkloadSpec::for_forge, deploys via yubaba, spawns log-ingestion task with EventScope::Forge scope, returns ForgeRunHandle backed by tokio::sync::watch) + ForgeRunHandle::wait (async, terminal-state poll) + ForgeRunHandle::kill. ScriptedWardenClient + HangingWardenClient test helpers in test_support mod. task Cargo.toml: added scryer, tokio, async-trait, thiserror deps. Two verify tests pass: remote::happy (Done exit_code=0 + 2 events queryable via scryer.events(Forge(id))) and remote::timeout (TimedOut + yubaba.teardown called). cargo test -p task 18/18 ok; cargo check --workspace clean.")
// @yah:next("Human review: (a) check crates/yah/task/src/remote.rs — WardenClient trait shape, ForgeRunHandle watch semantics, build_workload_spec tier defaulting (Remote(ident) → infra), default_forge_image placeholder tagged as F8 follow-up; (b) check test_support::ScriptedWardenClient + HangingWardenClient for correctness; (c) confirm the Forge(id) scryer scope is correct per arch doc §Remote-forge ingestion-side branch.")
// @yah:next("Smoke under R091 smoke tier: forge.run({ command: Subprocess { argv: [\"cargo\", \"check\"] }, where: RemoteAny { tier: \"infra\" } }) runs against real Hetzner — deferred to R091 integration test infrastructure landing.")
// @arch:see(.yah/docs/architecture/A035-yah-forge.md)
//!
//! Remote-forge driver.
//!
//! Synthesizes a [`WorkloadSpec`] from a [`ForgeSpec`], deploys via the
//! [`WardenClient`] seam, ingests the container's log stream into scryer with
//! `Forge(id)` scope, and tracks the terminal status via a `watch` channel.
//!
//! # Seam
//!
//! [`WardenClient`] is the trait yubaba (or test code) implements.  Production
//! yubaba uses containerd gRPC (R091).  Tests use
//! [`test_support::ScriptedWardenClient`] and
//! [`test_support::HangingWardenClient`].

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use observation::{Event, EventScope, EventSource, ForgeId, Level, TaskRunId};
use yah_scryer::service::Scryer;
use serde_json::json;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use workload_spec::{
    EnvValue, EnvVar, ImageRef, MeshIdent, TierTag, VolumeMount, VolumeSource, WorkloadSpec,
};

use crate::executor::{
    ExecContext, ExecEvent, ExecOutcome, ForgeExecutor, ForgeExecutorError, OutputStream,
};
use velveteen::{ForgeCommand, ForgeSpec, ForgeStatus, TaskLocation, TaskRuntime};

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum RemoteForgeError {
    #[error("yubaba deploy: {0}")]
    Deploy(String),
    #[error("yubaba log stream: {0}")]
    LogStream(String),
    #[error("yubaba teardown: {0}")]
    Teardown(String),
    #[error("yubaba exit code: {0}")]
    ExitCode(String),
    #[error("scryer push: {0}")]
    Push(String),
    #[error("invalid spec: {0}")]
    InvalidSpec(String),
    #[error("yubaba artifact fetch: {0}")]
    Fetch(String),
}

// ─── WardenClient ─────────────────────────────────────────────────────────────

/// Seam between the remote-forge driver and yubaba's containerd-backed RPC.
///
/// Production: yubaba's containerd gRPC client (R091).  Tests:
/// [`test_support::ScriptedWardenClient`] / [`test_support::HangingWardenClient`].
///
/// # Remote + native: shipped as a marked container workload, not a sibling method
///
/// R380-T7 left a documented v2 hook here — `async fn exec_native(&self, spec:
/// &NativeExecSpec)`, parallel to `deploy` — to be added when a real use case
/// arrived. One did (the W254 Darwin build leg), and the hook was **not** the
/// shape taken. **This trait is unchanged**; there is no `exec_native`.
///
/// The reason is that the difference between a Darwin build and a Linux build
/// is not the *transport*, it is only which runtime the node ends up forking.
/// Everything this trait exists to do — deploy, stream logs, read the exit
/// code, fetch produced files, tear down — is identical for both. A sibling
/// method would have duplicated all five for a one-bit difference. So a native
/// forge goes out through `deploy` like any other, as a `WorkloadSpec` carrying
/// [`workload_spec::NATIVE_EXEC_ANNOTATION`], and kamaji routes on that marker
/// to its `Backend::Native`. See [`build_workload_spec`] for the synthesis and
/// [`WorkloadSpec::wants_native_exec`] for the marker contract.
///
/// `image` on a native workload is identity metadata only — nothing is pulled.
/// The one quadrant still refused before touching this trait is remote+native
/// for a non-subprocess forge command (a `Workload` or `BuildImage` forge is
/// image-backed by construction).
#[async_trait]
pub trait WardenClient: Send + Sync {
    /// Submit a container workload for deployment.  Returns once the RPC
    /// completes; does NOT wait for the container to reach Ready.
    ///
    /// **Every** remote forge flows through here, including native ones — a
    /// native workload is an ordinary [`WorkloadSpec`] carrying
    /// [`workload_spec::NATIVE_EXEC_ANNOTATION`], and kamaji routes on that
    /// marker (R577-T1). There is no `exec_native` sibling; the trait-level
    /// docs above explain why the v2 hook R380-T7 planned was not the shape
    /// taken.
    async fn deploy(&self, spec: &WorkloadSpec) -> Result<(), RemoteForgeError>;

    /// Open a line-oriented log stream for the named container.  The returned
    /// `Receiver` yields one line per item; when the sender is dropped the
    /// stream is considered cleanly closed.
    async fn connect_logs(
        &self,
        ident: &MeshIdent,
    ) -> Result<mpsc::Receiver<String>, RemoteForgeError>;

    /// Tear down the container.  Returns `Ok` even if already gone.
    async fn teardown(&self, ident: &MeshIdent) -> Result<(), RemoteForgeError>;

    /// Query the exit code of a terminated container.  `None` if still running
    /// or exit code is unavailable.
    async fn exit_code(&self, ident: &MeshIdent) -> Result<Option<i32>, RemoteForgeError>;

    /// Read the full bytes of a file the container produced (R590-F6 leg 2).
    ///
    /// A remote build step (e.g. the rusty_v8 musl build on us-west-002) writes
    /// its output tarball to a path *inside* the build-worker; nothing pulls it
    /// back to camp. This is the transport seam the runner calls after the step
    /// exits to retrieve those bytes for content-addressed landing +
    /// [`crate` publish]. `remote_path` is the container-side path declared on
    /// the step's `produces`.
    ///
    /// # Default is the unwired fallback; the real transport is a host read
    ///
    /// The default returns [`RemoteForgeError::Fetch`] for clients that have no
    /// mesh transport. The production `MeshYubabaClient` (R603-T5) DOES implement
    /// this: it reads the bytes off the build-worker's **host-persistent**
    /// produced dir (`/var/lib/yah/qed/produced/<forge_id>/…`, bind-mounted at
    /// `/yah/produced` in the container) via `GET /workloads/{ident}/produced`.
    /// Reading the host path — not the container rootfs — is what makes
    /// retrieval survive kamaji reaping the exited container after a daemon
    /// outage (the R603-T4 reaping-window failure). This reshapes R590-F6's
    /// deferred containerd file-read into a plain host read. Test clients
    /// override this to serve scripted bytes by path.
    async fn fetch_produced_file(
        &self,
        _ident: &MeshIdent,
        remote_path: &Path,
    ) -> Result<Vec<u8>, RemoteForgeError> {
        Err(RemoteForgeError::Fetch(format!(
            "fetch_produced_file({}) not wired for this WardenClient — R590-F6 \
             transport is landed but the yubaba containerd file-read RPC is \
             deferred to post-redeploy integration",
            remote_path.display(),
        )))
    }
}

// ─── RemoteForgeDriver ────────────────────────────────────────────────────────

/// Drives one-shot forge runs on yubaba-managed machines.
///
/// [`start`](Self::start) deploys the workload, spawns the log-ingestion task,
/// and returns a [`ForgeRunHandle`] immediately.  The caller calls
/// [`ForgeRunHandle::wait`] to block until a terminal state is reached.
pub struct RemoteForgeDriver {
    scryer: Arc<Scryer>,
    yubaba: Arc<dyn WardenClient>,
}

impl RemoteForgeDriver {
    pub fn new(scryer: Arc<Scryer>, yubaba: Arc<dyn WardenClient>) -> Self {
        Self { scryer, yubaba }
    }

    /// Start a remote forge run.
    ///
    /// Synthesizes a `WorkloadSpec`, deploys it via yubaba, and spawns the
    /// log-ingestion task.  Returns a `ForgeRunHandle` before the run finishes.
    pub async fn start(&self, spec: ForgeSpec) -> Result<ForgeRunHandle, RemoteForgeError> {
        self.start_with_sink(spec, None).await
    }

    /// Like [`start`](Self::start) but also tees each container log line into
    /// `sink` as an [`ExecEvent::Output`].
    ///
    /// The qed runner passes its live-event sink here so a yubaba-dispatched
    /// step streams stdout lines into `QedEvent::StepOutput` *during* the run
    /// rather than batching them post-completion (R508). The sink is a pure
    /// fan-out: lines still flow into scryer under `Forge(id)` scope exactly as
    /// before. Container logs are line-merged with no stdout/stderr split, so
    /// every forwarded line is tagged [`OutputStream::Stdout`].
    pub async fn start_with_sink(
        &self,
        spec: ForgeSpec,
        sink: Option<mpsc::UnboundedSender<ExecEvent>>,
    ) -> Result<ForgeRunHandle, RemoteForgeError> {
        self.start_with_context(spec, sink, &ExecContext::default())
            .await
    }

    /// Like [`start_with_sink`](Self::start_with_sink) but also applies the
    /// host-side [`ExecContext`] the [`ForgeExecutor`] surface carries.
    ///
    /// `ForgeSpec` is deliberately portable across the camp↔cloud boundary, so
    /// cwd/env live out-of-band in `ExecContext`. Remotely they map onto the
    /// workload spec: `cwd` → [`WorkloadSpec::workdir`], `env` → literal
    /// [`EnvVar`]s. `platform` has no remote referent and is refused — see
    /// [`ExecContext`] handling in the [`ForgeExecutor`] impl below.
    ///
    /// Note the `cwd` mapping is a *container-side* path: the caller is
    /// declaring the workdir inside the image, not bind-mounting a host
    /// directory the way [`crate::local::LocalForgeDriver`] does. A caller
    /// passing a host-absolute path (the cloud reconciler's `workspace_root`)
    /// gets a workdir that doesn't exist on the node.
    pub async fn start_with_context(
        &self,
        spec: ForgeSpec,
        sink: Option<mpsc::UnboundedSender<ExecEvent>>,
        ctx: &ExecContext,
    ) -> Result<ForgeRunHandle, RemoteForgeError> {
        let forge_id = ForgeId::new();
        let ident = forge_mesh_ident(&forge_id);
        let timeout = spec.timeout.map(|ms| Duration::from_millis(ms.as_ms()));

        let mut workload = build_workload_spec(&forge_id, &spec)?;
        apply_exec_context(&mut workload, ctx)?;
        self.yubaba.deploy(&workload).await?;

        let (status_tx, status_rx) = watch::channel(ForgeStatus::Running);
        let scryer = self.scryer.clone();
        let yubaba = self.yubaba.clone();
        let id = forge_id.clone();

        tokio::spawn(async move {
            let status =
                run_log_task(id, ident, timeout, scryer, yubaba, sink).await;
            let _ = status_tx.send(status);
        });

        Ok(ForgeRunHandle { id: forge_id, status_rx })
    }

    /// Tear down a running forge container.
    ///
    /// The background log task will detect the stream closing and resolve the
    /// run to a terminal state (typically `Lost` or the actual exit code if
    /// yubaba reports one before the stream closes).
    pub async fn kill(&self, forge_id: &ForgeId) -> Result<(), RemoteForgeError> {
        self.yubaba.teardown(&forge_mesh_ident(forge_id)).await
    }

    /// Retrieve the bytes of a file the finished forge container produced
    /// (R590-F6 leg 2). Resolves the run's mesh identity and delegates to
    /// [`WardenClient::fetch_produced_file`]. Call after [`ForgeRunHandle::wait`]
    /// reports a successful terminal status; the caller lands the bytes in a
    /// content-addressed store.
    pub async fn fetch_produced_file(
        &self,
        forge_id: &ForgeId,
        remote_path: &Path,
    ) -> Result<Vec<u8>, RemoteForgeError> {
        self.yubaba
            .fetch_produced_file(&forge_mesh_ident(forge_id), remote_path)
            .await
    }

    /// Pull an [`ExecContext::produced`] file off the worker and land it at
    /// `dest` (R555-F3).
    ///
    /// The container-dir check is the same invariant the qed runner enforces at
    /// *dispatch* for `produces` paths, applied here at *retrieval* instead:
    /// only [`forge_produced::CONTAINER_DIR`](workload_spec::forge_produced::CONTAINER_DIR)
    /// is bind-mounted onto host-persistent storage, so a path outside it is
    /// unreadable the moment kamaji reaps the container — and a caller that
    /// asked for one has a recipe bug, not a transport fault. Saying so before
    /// the RPC turns "fetch failed" into "your output path is wrong".
    async fn retrieve_produced(
        &self,
        forge_id: &ForgeId,
        produced: &crate::executor::ProducedFile,
    ) -> Result<(), ForgeExecutorError> {
        let container_dir = Path::new(workload_spec::forge_produced::CONTAINER_DIR);
        if !produced.remote_path.starts_with(container_dir) {
            return Err(ForgeExecutorError::Remote(format!(
                "produced path {} is not under {} — only that dir is bind-mounted onto \
                 host-persistent storage, so nothing written elsewhere survives the \
                 container being reaped",
                produced.remote_path.display(),
                container_dir.display(),
            )));
        }
        let bytes = self
            .fetch_produced_file(forge_id, &produced.remote_path)
            .await
            .map_err(|e| {
                ForgeExecutorError::Remote(format!(
                    "retrieving {} off the worker: {e}",
                    produced.remote_path.display()
                ))
            })?;
        if let Some(parent) = produced.dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&produced.dest, &bytes).await?;
        Ok(())
    }
}

/// How many trailing log lines to keep for [`ExecOutcome::stderr_tail`].
///
/// Container logs arrive line-merged with no stdout/stderr split, so the "tail"
/// is the tail of *everything*. Unlike the local driver — which buffers all of
/// stderr because a host subprocess's is bounded in practice — a remote forge
/// is exactly the long job (a multi-hour V8 build emits hundreds of thousands
/// of lines), so this is capped. Full logs are in scryer under `Forge(id)`.
const REMOTE_TAIL_LINES: usize = 40;

/// R555-T2 — dispatch a remote forge run through the uniform executor surface.
///
/// This is the impl `executor.rs` described as "a follow-up when a consumer
/// needs to dispatch uniformly through `dyn ForgeExecutor`". Remote QED (W235)
/// is that consumer: the cloud reconciler holds an `Arc<dyn ForgeExecutor>`,
/// and a recipe declaring `placement.location = { kind = "remote_any", … }`
/// has to reach yubaba through it.
///
/// Semantics against the trait contract:
///
/// - **Blocking.** `execute` returns when the run is terminal; the
///   [`start`](RemoteForgeDriver::start) / [`ForgeRunHandle::wait`] split is
///   still there for callers that want the handle (the qed runner's
///   `execute_step_remote` records the `ForgeId` mid-run and keeps using it).
/// - **`sink`.** Forwarded to the log-ingest task, so lines stream during the
///   run exactly as `start_with_sink` does — plus an [`ExecEvent::Finished`]
///   the raw `start_with_sink` path leaves to its caller.
/// - **Errors.** A yubaba-side failure is [`ForgeExecutorError::Remote`]. A
///   spec that can't be placed at all (local location, remote+native) is
///   [`ForgeExecutorError::Unsupported`] — a config bug, not a transport one,
///   and the qed runner already renders those as `InvalidConfig`.
#[async_trait]
impl ForgeExecutor for RemoteForgeDriver {
    async fn execute(
        &self,
        spec: ForgeSpec,
        ctx: ExecContext,
        sink: Option<mpsc::UnboundedSender<ExecEvent>>,
    ) -> Result<ExecOutcome, ForgeExecutorError> {
        if matches!(spec.where_.location, TaskLocation::Local) {
            return Err(ForgeExecutorError::Unsupported(
                "RemoteForgeDriver received a spec with placement.location = local — \
                 route it to LocalForgeDriver",
            ));
        }

        // Tee the log stream: forward every event to the caller's sink (if any)
        // while keeping a bounded tail for ExecOutcome.stderr_tail. The caller
        // can't do this for us — it owns the far end of its own sink.
        let (tee_tx, mut tee_rx) = mpsc::unbounded_channel::<ExecEvent>();
        let collector = tokio::spawn(async move {
            let mut tail: std::collections::VecDeque<String> = std::collections::VecDeque::new();
            while let Some(ev) = tee_rx.recv().await {
                if let ExecEvent::Output { line, .. } = &ev {
                    if tail.len() == REMOTE_TAIL_LINES {
                        tail.pop_front();
                    }
                    tail.push_back(line.clone());
                }
                if let Some(tx) = &sink {
                    let _ = tx.send(ev);
                }
            }
            (tail, sink)
        });

        let handle = self
            .start_with_context(spec, Some(tee_tx), &ctx)
            .await
            .map_err(exec_error)?;
        let forge_id = handle.id.clone();
        let status = handle.wait().await;

        // `start_with_context` moved the tee sender into the log task, which
        // drops it once the stream closes — so the collector terminates on its
        // own and this join can't hang past the run.
        let (tail, sink) = collector.await.unwrap_or_default();
        if let Some(tx) = &sink {
            let _ = tx.send(ExecEvent::Finished {
                status: status.clone(),
            });
        }

        let outcome = ExecOutcome {
            status,
            stderr_tail: tail.into_iter().collect::<Vec<_>>().join("\n").trim().to_string(),
        };

        // Retrieval is the second half of a remote run, and it only makes sense
        // for a run that succeeded: on a failure the caller wants the log tail,
        // not a fetch error for a file the build never got as far as writing.
        if let (Some(produced), true) = (ctx.produced.as_ref(), outcome.succeeded()) {
            self.retrieve_produced(&forge_id, produced).await?;
        }

        Ok(outcome)
    }
}

/// Map a [`RemoteForgeError`] onto the executor-surface error.
///
/// `InvalidSpec` is the only variant that means "this spec can never run" as
/// opposed to "this attempt failed", so it's the only one that becomes
/// `Unsupported`; the rest are transport/lifecycle faults that a retry could
/// plausibly clear.
fn exec_error(e: RemoteForgeError) -> ForgeExecutorError {
    match e {
        RemoteForgeError::InvalidSpec(msg) => ForgeExecutorError::Remote(format!(
            "spec cannot be placed remotely: {msg}"
        )),
        other => ForgeExecutorError::Remote(other.to_string()),
    }
}

/// Fold the host-side [`ExecContext`] into the synthesized [`WorkloadSpec`].
///
/// Every field is either honored or refused — nothing is silently dropped,
/// because each one silently dropped is a *wrong artifact*, not a crash:
///
/// - `cwd` → [`WorkloadSpec::workdir`] (container-side path). On a **native**
///   forge a *relative* cwd is refused instead — see below.
/// - `env` → literal [`EnvVar`]s, appended after whatever the spec already
///   carries. Later entries win at deploy, and the caller's immediate intent
///   should beat a `ForgeCommand::Workload`'s baked-in env. This mapping is
///   the one that needs no native special-casing: for a fork+exec'd process
///   these become real process env, which is stronger than the container
///   reading, not weaker. It is also why [`mark_native_exec`] leans on
///   `YAH_PRODUCED_DIR` rather than on `workdir` — appending cannot clobber it.
/// - `platform` → **refused**. It exists to ask a *host* container runtime for
///   foreign-arch emulation (Rosetta / qemu). A yubaba node has no such knob;
///   remote runs pick architecture by *scheduling* — `location.mesh_tags =
///   ["arch:x86"]`. Honoring it silently would hand back an artifact built for
///   the wrong architecture, which is precisely the R546 failure this seam
///   exists to retire.
pub(crate) fn apply_exec_context(ws: &mut WorkloadSpec, ctx: &ExecContext) -> Result<(), RemoteForgeError> {
    if let Some(dir) = &ctx.produced_dir {
        // R560-B12: local-only, and refused rather than ignored because the
        // remote path ALREADY has a produced-dir mount — `build_workload_spec`
        // adds `forge_produced::durable_mount(forge_id)` at the same container
        // path. Honoring this would put a second bind on top of it and send the
        // build's output to a host dir on the WORKER named after a directory on
        // the coordinator; ignoring it would leave a caller believing it had
        // redirected an output it had not.
        return Err(RemoteForgeError::InvalidSpec(format!(
            "produced_dir = {} is a local-container affordance with no remote equivalent. \
             A remote forge already binds its durable produced dir at {} \
             (forge_produced::durable_mount) and the retrieval leg reads it back off the \
             worker — drop `produced_dir` and let that transport do it (R560-B12).",
            dir.display(),
            workload_spec::forge_produced::CONTAINER_DIR,
        )));
    }
    if let Some(platform) = &ctx.platform {
        return Err(RemoteForgeError::InvalidSpec(format!(
            "placement.platform = {platform:?} is a local-only emulation knob and has no \
             remote equivalent. A remote run selects architecture by scheduling: set \
             location = {{ kind = \"remote_any\", tier = \"…\", mesh_tags = [\"arch:x86\"] }} \
             and drop `platform` (W235 / R546)."
        )));
    }
    if let Some(cwd) = &ctx.cwd {
        // R577-T1: a native forge is fork+exec'd on the worker's own userland,
        // so `workdir` reaches `Command::current_dir` directly. A RELATIVE path
        // there resolves against *kamaji's* working directory — whatever
        // launchd/systemd happened to give the daemon — not against a source
        // checkout. The step would then run somewhere arbitrary or die ENOENT,
        // and `desktop-release` is exactly this shape (`cwd = "packages/yah/ui"`,
        // `cwd = "app/yah/desktop"`).
        //
        // A container resolves a relative workdir against its image root, which
        // is at least well-defined, so this refusal is native-only.
        //
        // This is a guard, not a resolution: what a relative cwd *should*
        // resolve against is a worker-side checkout of the recipe's workspace,
        // and no coordinator→worker input channel exists yet (see the
        // `desktop-release` header comment). Until one does, failing loudly at
        // dispatch beats building the wrong tree on real hardware.
        if ws.wants_native_exec() && cwd.is_relative() {
            return Err(RemoteForgeError::InvalidSpec(format!(
                "cwd {} is relative, but this step runs natively on the build-worker's own \
                 userland, where a relative path resolves against the kamaji daemon's working \
                 directory rather than a source checkout. Give an absolute path on the worker \
                 (R577-T1)."
                ,
                cwd.display()
            )));
        }
        ws.workdir = Some(cwd.clone());
    }
    for (name, value) in &ctx.env {
        ws.env.push(EnvVar {
            name: name.clone(),
            value: EnvValue::Literal {
                value: value.clone(),
            },
        });
    }
    // R555-F5: declared vault credentials. Appended rather than assigned so a
    // `ForgeCommand::Workload`'s own mounts survive — but note the grant covers
    // the FULL resulting list, so a workload spec that arrives carrying secrets
    // the recipe did not declare is refused on the node rather than merged in
    // quietly.
    ws.secrets.extend(ctx.secrets.iter().cloned());
    // R555-F4: the admission envelope goes on LAST, after every field the grant
    // is checked against has settled. Attaching it earlier would still work
    // today — `attach` only writes annotations — but the grant covers `workdir`
    // and `env`, which the two loops above are still moving, and "the signature
    // is attached to the finished spec" is the property worth being able to
    // read off the order rather than reason about.
    if let Some(envelope) = &ctx.admission {
        workload_spec::admission::attach(
            ws,
            &envelope.grant,
            &envelope.signature,
            &envelope.public_key,
        );
    }
    Ok(())
}

// ─── ForgeRunHandle ───────────────────────────────────────────────────────────

/// Handle to a running (or completed) remote-forge run.
///
/// Returned by [`RemoteForgeDriver::start`].  Call [`wait`](Self::wait) to
/// block until a terminal state arrives.
pub struct ForgeRunHandle {
    pub id: ForgeId,
    status_rx: watch::Receiver<ForgeStatus>,
}

impl ForgeRunHandle {
    /// Block until the run reaches a terminal state and return it.
    pub async fn wait(mut self) -> ForgeStatus {
        loop {
            if self.status_rx.borrow().is_terminal() {
                return self.status_rx.borrow().clone();
            }
            if self.status_rx.changed().await.is_err() {
                return ForgeStatus::Lost { reason: "status sender dropped".into() };
            }
        }
    }

    /// Return the current (possibly in-flight) status without waiting.
    pub fn current_status(&self) -> ForgeStatus {
        self.status_rx.borrow().clone()
    }
}

// ─── Internals ────────────────────────────────────────────────────────────────

fn forge_mesh_ident(id: &ForgeId) -> MeshIdent {
    MeshIdent(format!("forge.{id}"))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Synthesise a [`WorkloadSpec`] from a [`ForgeSpec`].
///
/// # Remote + native (R577-T1 / W254)
///
/// R380-T7 refused this quadrant outright in v1, deferring an `exec_native`
/// surface "until a real use case arrives". It has: the Darwin build leg.
/// `cargo tauri build` for `aarch64-apple-darwin`, `codesign` and `xcrun
/// notarytool` need a live macOS userland, and no container can supply one —
/// you cannot containerize the Darwin kernel. A `native = true` step whose
/// target OS differs from the coordinator's therefore has nowhere to run
/// unless remote+native works.
///
/// The route taken is **not** a parallel `exec_native` RPC. A native forge is
/// still a `Workload::Container(WorkloadSpec)` on the wire, marked with
/// [`workload_spec::NATIVE_EXEC_ANNOTATION`]; kamaji reads that marker and
/// dispatches to its existing `Backend::Native` (fork+exec) instead of a
/// container backend. Everything between here and there — yubaba admission,
/// mesh-tag node selection, mesh assignment, the state-poll → exit-code
/// mapping, produced-file retrieval, teardown — is shared with the container
/// path rather than reimplemented, and `kamaji-proto`'s codec needs no new
/// variant. See [`WorkloadSpec::wants_native_exec`] for the full rationale.
///
/// Only [`ForgeCommand::Subprocess`] can go native: the other two command
/// shapes are image-backed by construction (a `Workload` forge carries a
/// caller-supplied container spec, and `BuildImage` runs BuildKit *in* a
/// container). Those still refuse, before any state is allocated or any
/// yubaba RPC is issued — no forge id published, no scryer events, no
/// `deploy` call.
pub(crate) fn build_workload_spec(
    forge_id: &ForgeId,
    spec: &ForgeSpec,
) -> Result<WorkloadSpec, RemoteForgeError> {
    let mut native_exec = false;
    let mut microvm = false;
    match spec.where_.runtime {
        TaskRuntime::Container => {}
        TaskRuntime::Native => {
            if !matches!(spec.command, ForgeCommand::Subprocess { .. }) {
                return Err(RemoteForgeError::InvalidSpec(
                    "remote + native is supported only for a subprocess forge command — \
                     a workload forge carries its own container spec and a build-image \
                     forge runs BuildKit inside a container, so neither has a native \
                     shape. Set placement.runtime = container (R577-T1 / W254)."
                        .into(),
                ));
            }
            native_exec = true;
        }
        TaskRuntime::MicroVm => {
            // R605-F8. Same restriction as native, reached from the opposite
            // direction. Native refuses the image-backed commands because they
            // have no *host* shape; microVM refuses them because a guest boots
            // the node's rootfs, not the step's image — a `BuildImage` forge
            // running BuildKit and a `Workload` forge carrying its own
            // container spec would both have their image silently ignored, and
            // the run would report success having built the wrong thing.
            if !matches!(spec.command, ForgeCommand::Subprocess { .. }) {
                return Err(RemoteForgeError::InvalidSpec(
                    "remote + microvm is supported only for a subprocess forge command — a \
                     guest boots the node's own rootfs, so a workload forge's container spec \
                     and a build-image forge's BuildKit image have nowhere to be honored. \
                     Set placement.runtime = container (R605-F8 / W325)."
                        .into(),
                ));
            }
            microvm = true;
        }
    }

    let (tier, mesh_tags, pinned_node) = match &spec.where_.location {
        TaskLocation::RemoteAny { tier, mesh_tags } => (tier.clone(), mesh_tags.clone(), None),
        // Pin to a specific node: tier defaults to infra (conventional for forge).
        //
        // R833-F8: the node name used to be DROPPED here — a `Remote { node }`
        // spec was admitted exactly like an unconstrained `RemoteAny`, so
        // "run it on us-west-003" landed on whichever machine won the
        // declaration-order tie-break. It now travels as an annotation (below)
        // and narrows admission by name. No mesh tags are requested alongside
        // it: naming the box IS the constraint, and adding an inferred
        // capability filter on top could only make an explicit target
        // unschedulable.
        TaskLocation::Remote { node } => (TierTag("infra".into()), vec![], Some(node.0.clone())),
        TaskLocation::Local => {
            return Err(RemoteForgeError::InvalidSpec(
                "RemoteForgeDriver received a local ForgeSpec".into(),
            ));
        }
    };

    let mut ws = match &spec.command {
        ForgeCommand::Subprocess { argv, image } => {
            let image = image.clone().unwrap_or_else(crate::default_image::default_forge_image);
            let mut ws =
                WorkloadSpec::for_forge(&forge_id.to_string(), image, tier, vec![]);
            ws.command = Some(argv.clone());
            // R603-T5: give every remote forge subprocess a durable, host-backed
            // output dir at the conventional `/yah/produced`. A build that writes
            // its `produces` there lands the bytes on the worker's host
            // filesystem, so retrieval (yubaba reading the host path) survives
            // kamaji reaping the EXITED container after a daemon outage. The
            // enforcement that declared `produces` sit under this dir lives in
            // the qed runner (`execute_step_remote`), which has the step.
            ws.volumes
                .push(workload_spec::forge_produced::durable_mount(&forge_id.to_string()));
            ws
        }
        ForgeCommand::Workload { spec: inner } => inner.clone(),
        ForgeCommand::BuildImage {
            dockerfile,
            context,
            context_url,
            tags,
            platforms,
            build_args,
            push,
            load,
        } => build_image_workload_spec(
            forge_id,
            dockerfile,
            context,
            context_url.as_deref(),
            tags,
            platforms,
            build_args,
            *push,
            *load,
            tier,
        )?,
    };

    // R594: carry the mesh-tag node-selector to yubaba as an annotation. yubaba
    // admission (ticket: mesh-tag+arch admission) reads this to restrict the
    // candidate nodes to those whose mesh tags ⊇ the requested set — e.g. the
    // build-worker fleet, arch-matched. Empty ⇒ no annotation ⇒ any node in tier.
    if !mesh_tags.is_empty() {
        ws.annotations
            .insert(NODE_SELECTOR_MESH_TAGS_ANNOTATION.into(), mesh_tags.join(","));
    }

    // R833-F8: the imperative half of the same seam. Same mechanism (an
    // annotation yubaba admission reads off the spec), one candidate instead of
    // a filtered set. Mutually exclusive with the tags by construction — the
    // match above yields one or the other, never both.
    if let Some(node) = pinned_node {
        ws.annotations
            .insert(NODE_SELECTOR_NODE_ANNOTATION.into(), node);
    }

    // R590-B7: remote forge workloads are ephemeral tier=infra build tasks that
    // must reach the network to fetch sources (git clone of v8/chromium, cargo
    // registry, image layers). kamaji gives every container an isolated,
    // loopback-only netns by default — no egress, no DNS — which fails any build
    // that pulls from the internet (rusty-v8-musl died at `git clone`, exit 128).
    // Request host networking so the build shares the node's network stack; the
    // pairing bind-mount of /etc/resolv.conf lives in kamaji's build_oci_spec.
    // kamaji guards this annotation to tier=infra, which forge always satisfies.
    //
    // R605-F8: NOT for a microVM. The annotation asks kamaji to put a container
    // in the host's network namespace, and a guest has no namespace to place —
    // it has a virtual NIC on a TAP the microVM backend sets up, and it reaches
    // the registry through that. Setting it anyway would be inert at the
    // backend but not harmless upstream: `AdmissionGrant::from_spec` records
    // `host_network` from this very annotation, so the signed grant would
    // assert a host-network privilege that was never taken. A grant that
    // overstates is a grant that stops meaning anything.
    if !microvm {
        ws.annotations.insert(
            workload_spec::HOST_NETWORK_ANNOTATION.into(),
            workload_spec::HOST_NETWORK_VALUE.into(),
        );
    }

    // R876-F4: the host-persistent build cache. Third mount under
    // `forge_state::HOST_ROOT`, and — as R603-B6's handoff promised a fifth
    // would be — it needed no yubaba code and no unit-file edit, because
    // `ensure_forge_state_dirs` mkdirs any forge bind under that root.
    //
    // Refused rather than ignored on the two image-backed command shapes: a
    // `Workload` forge carries its own volume list (adding one behind the
    // caller's back would silently contradict a spec they wrote), and a
    // `BuildImage` forge's caching is BuildKit's, not a bind's. Ignoring a
    // declared cache is the failure mode this ticket exists to avoid — a mount
    // that looks like it works.
    if let Some(key) = &spec.cache_key {
        if !matches!(spec.command, ForgeCommand::Subprocess { .. }) {
            return Err(RemoteForgeError::InvalidSpec(
                "a build cache is supported only for a subprocess forge command — a \
                 workload forge carries its own volumes and a build-image forge caches \
                 through BuildKit (R876-F4)."
                    .into(),
            ));
        }
        // Same reasoning one step further out: a natively executed forge has no
        // mount namespace at all, so the bind would be inert and the step would
        // build cold while reporting a cache. `apply_exec_context` already
        // refuses `produced_dir` on that path for the identical reason.
        if native_exec {
            return Err(RemoteForgeError::InvalidSpec(
                "a build cache is a container affordance — a native forge has no mount \
                 namespace for the bind, so the cache would be silently inert. Set \
                 placement.runtime = container (R876-F4)."
                    .into(),
            ));
        }
        let mount = workload_spec::forge_cache::durable_mount(key).ok_or_else(|| {
            RemoteForgeError::InvalidSpec(format!(
                "build cache key `{key}` is not a safe single path component under {} \
                 — keys are derived with forge_cache::key_from_parts, not written by \
                 hand (R876-F4).",
                workload_spec::forge_cache::HOST_ROOT,
            ))
        })?;
        ws.volumes.push(mount);
    }

    if native_exec {
        mark_native_exec(&mut ws, forge_id);
    }
    if microvm {
        mark_microvm(&mut ws);
    }

    Ok(ws)
}

/// Env var naming the directory a **natively executed** forge step must write
/// its declared `produces` into (R577-T1).
///
/// A container step writes to the fixed container-side
/// [`forge_produced::CONTAINER_DIR`](workload_spec::forge_produced::CONTAINER_DIR)
/// (`/yah/produced`), which the bind mount maps onto the per-run host dir. A
/// native step has no mount namespace, so the host dir *is* the only path —
/// and it is per-run, so it cannot be a constant. The step learns it from this
/// variable rather than from a hardcoded path.
pub const PRODUCED_DIR_ENV: &str = "YAH_PRODUCED_DIR";

/// Turn a container-shaped forge spec into a natively-executed one
/// (R577-T1 / W254).
///
/// Three edits, all of which exist because a fork+exec'd process has no mount
/// namespace of its own:
///
/// 1. The [`NATIVE_EXEC_ANNOTATION`](workload_spec::NATIVE_EXEC_ANNOTATION)
///    marker kamaji routes on.
/// 2. `workdir` + [`PRODUCED_DIR_ENV`] point at the per-run *host* produced
///    dir. The bind mount that would have surfaced it at `/yah/produced` is
///    inert for a native workload, so the step is told the real path.
///
///    [`PRODUCED_DIR_ENV`] is the load-bearing half of that pair, not
///    `workdir`: `apply_exec_context` runs *after* this and replaces `workdir`
///    outright when the caller supplied an [`ExecContext::cwd`] — legitimately
///    so, since a build usually wants to run in its source tree rather than in
///    its output dir. Env vars are appended, so the produced-dir path survives
///    that override. A step should resolve its outputs through the variable and
///    treat the workdir as a convenience default.
/// 3. The durable-mount volume is **kept**, even though nothing mounts it.
///    yubaba's deploy-time `ensure_forge_state_dirs` walks `spec.volumes` and
///    creates any bind source under the forge state root — that mkdir is what
///    makes `workdir` exist before the child is spawned, and it is also what
///    the retrieval side (`fetch_produced_file` → `forge_produced::host_path`)
///    reads back from. Dropping the volume as "unused" would silently break
///    both, so it stays and this comment says why.
fn mark_native_exec(ws: &mut WorkloadSpec, forge_id: &ForgeId) {
    ws.annotations.insert(
        workload_spec::NATIVE_EXEC_ANNOTATION.into(),
        workload_spec::NATIVE_EXEC_VALUE.into(),
    );

    let produced = workload_spec::forge_produced::host_dir(&forge_id.to_string());
    ws.env.push(workload_spec::EnvVar {
        name: PRODUCED_DIR_ENV.into(),
        value: workload_spec::EnvValue::Literal {
            value: produced.to_string_lossy().into_owned(),
        },
    });
    ws.workdir = Some(produced);
}

/// Turn a container-shaped forge spec into a microVM one (R605-F8 / W325 §5).
///
/// **One edit**, and the brevity next to [`mark_native_exec`]'s three is the
/// point rather than an omission. The native path has to rewrite `workdir` and
/// publish [`PRODUCED_DIR_ENV`] because a fork+exec'd process has no mount
/// namespace, so the container-side `/yah/produced` simply does not exist for
/// it and the step has to be told the real host path instead.
///
/// A guest does have a mount namespace — it has a whole kernel — so the
/// declared volume targets are honoured as declared. kamaji's microVM backend
/// copies each bind source onto the guest's scratch disk and the guest's init
/// bind-mounts it back at `target`, which means a step writing to
/// `/yah/produced` works unchanged, and `forge_produced::host_path` reads the
/// artifacts back from the same host directory it always did. The durable-mount
/// volume that `build_workload_spec` pushed is doing real work here, not
/// surviving as inert bookkeeping the way it does on the native path.
fn mark_microvm(ws: &mut WorkloadSpec) {
    ws.annotations.insert(
        workload_spec::NATIVE_EXEC_ANNOTATION.into(),
        workload_spec::MICROVM_EXEC_VALUE.into(),
    );
}

/// Annotation key carrying the R594 mesh-tag node-selector (comma-joined) from
/// [`TaskLocation::RemoteAny::mesh_tags`] to yubaba admission.
pub const NODE_SELECTOR_MESH_TAGS_ANNOTATION: &str = "yah.node-selector.mesh-tags";

/// Annotation key carrying the R833-F8 **imperative** node selector — the
/// machine name from [`TaskLocation::Remote::node`] — to yubaba admission.
///
/// The sibling of [`NODE_SELECTOR_MESH_TAGS_ANNOTATION`] and deliberately a
/// second key rather than a `node:<name>` mesh tag: mesh tags are *inferred*
/// capability constraints matched as a superset, while this is the operator
/// naming one box. Folding the name into the tag set would make the two
/// indistinguishable at admission, and would silently pass for any node that
/// happened to declare such a tag (none do).
///
/// Consumed by `cloud::config::node_selector_node` / `RequiredSpec::nodes`.
pub const NODE_SELECTOR_NODE_ANNOTATION: &str = "yah.node-selector.node";

// ─── BuildKit workload synthesis (R381-T5) ────────────────────────────────────

/// Conventional output dir bind-mounted into the BuildKit container when an
/// OCI archive is requested. Worker-local: unlike the context, this one is a
/// correct bind on any host, because yubaba creates it at deploy
/// ([`workload_spec::forge_state`]) rather than expecting it to already exist
/// on the machine that composed the spec.
///
/// Cross-node consumers of the *archive* should still set `push=true` and let
/// the registry handle distribution — nothing pulls this file back to camp yet.
const BUILDKIT_HOST_OUT_DIR: &str = workload_spec::forge_state::BUILD_OUT_DIR;

/// Default BuildKit image used by remote build-image dispatch.
///
/// Held in `option_env!` so a deployment can pin a different version without
/// rebuilding qed.  The default is a current rootless BuildKit release: the
/// rootless variant runs `buildkitd` in user-space inside the container so the
/// workload doesn't require yubaba to grant `CAP_SYS_ADMIN`.
fn default_buildkit_image() -> ImageRef {
    let tag = option_env!("YAH_BUILDKIT_TAG").unwrap_or("v0.12.5-rootless");
    let digest = option_env!("YAH_BUILDKIT_DIGEST")
        .map(Into::into)
        .unwrap_or_else(workload_spec::testing::test_digest);
    ImageRef {
        registry: "docker.io".into(),
        repository: "moby/buildkit".into(),
        tag: tag.into(),
        digest,
    }
}

/// Program the BuildKit workload runs. The rootless image bakes
/// `ENTRYPOINT ["rootlesskit","buildkitd"]`, and kamaji's argv rule is
/// `(spec.entrypoint OR image.Entrypoint) ++ (spec.command OR image.Cmd)`
/// ([`build_oci_spec_with`] in kamaji-containerd-core) — so leaving
/// `entrypoint` unset makes the container run
/// `rootlesskit buildkitd buildctl-daemonless.sh build …`, i.e. buildkitd with
/// the whole buildctl invocation as junk flags. It does not error; it *hangs*,
/// serving a socket nobody connects to, until the step's timeout. Setting the
/// entrypoint explicitly is what makes `command` the buildctl argv it reads as.
const BUILDCTL: &str = "buildctl-daemonless.sh";

/// Synthesise the BuildKit workload that performs a remote build-image step.
///
/// The container runs [`BUILDCTL`] (provided by the rootless image) which boots
/// an in-process `buildkitd` and pipes the build through it.  When `push=true`
/// the result is pushed straight to the tag's registry, otherwise an OCI
/// archive is written to a bind-mounted host directory
/// ([`BUILDKIT_HOST_OUT_DIR`]) — that one is worker-*local*, so it is a correct
/// bind on any host.
///
/// # Where the context comes from
///
/// `context_url` set (R636-B1) ⇒ BuildKit loads a tar over HTTP and the
/// Dockerfile is read from inside it. Nothing of the composing host's
/// filesystem is referenced, which is the only shape that works when the
/// worker is a different machine.
///
/// `context_url` unset ⇒ the build context and dockerfile parent are
/// bind-mounted at conventional paths, which requires the worker to see those
/// exact host paths (yubaba on the qed host).
///
/// Bind volume mounts require `tier == "infra"`, which yubaba's shape
/// validation enforces; the forge convention picks infra by default so this is
/// safe for the v1 dogfood path.
#[allow(clippy::too_many_arguments)]
fn build_image_workload_spec(
    forge_id: &ForgeId,
    dockerfile: &Path,
    context: &Path,
    context_url: Option<&str>,
    tags: &[String],
    platforms: &[String],
    build_args: &[(String, String)],
    push: bool,
    load: bool,
    tier: TierTag,
) -> Result<WorkloadSpec, RemoteForgeError> {
    let first_tag = tags.first().ok_or_else(|| {
        RemoteForgeError::InvalidSpec(
            "build-image spec has no tags — at least one is required".to_string(),
        )
    })?;
    let dockerfile_basename = dockerfile
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            RemoteForgeError::InvalidSpec(format!(
                "build-image dockerfile path has no filename component: {}",
                dockerfile.display()
            ))
        })?;
    let image = default_buildkit_image();
    let mut ws = WorkloadSpec::for_forge(&forge_id.to_string(), image, tier, vec![]);

    // Image builds routinely peak at several hundred MiB; the for_forge
    // defaults were too tight for buildkit. `memory_mb` is deliberately NOT
    // overridden any more: this line used to read `= 2048` against a 256 MiB
    // default, which R590-B10 then raised to 32 GiB — so the override quietly
    // inverted from raising buildkit's cgroup ceiling to *lowering* it by 16x,
    // still narrating the old direction. Inheriting for_forge's ceiling is what
    // the comment always meant. The placement floor is unaffected either way;
    // it comes from the MEMORY_REQUEST_ANNOTATION for_forge sets.
    ws.resources.cpu_millis = 2000;
    ws.resources.ephemeral_storage_mb = 4096;

    // R636-B2: this is the one workload in the fleet that runs a container
    // runtime *inside* its own container. `rootlesskit` (the rootless BuildKit
    // image's entrypoint) sets up a user namespace by exec'ing the setuid-root
    // `newuidmap`/`newgidmap` helpers, which kamaji's baseline sandbox —
    // CAP_NET_BIND_SERVICE only, `no_new_privs` on — forbids; buildkitd then
    // never binds its socket and the step dies before the first layer with
    // nothing but "could not connect to …/buildkitd.sock after 10 trials".
    // Ask for the narrow, tier=infra-guarded nested-sandbox grant. Note this
    // is the *only* setter of the annotation: no other forge workload gets it.
    ws.annotations.insert(
        workload_spec::NESTED_SANDBOX_ANNOTATION.into(),
        workload_spec::NESTED_SANDBOX_VALUE.into(),
    );

    // Composer-host paths are only meaningful to a builder that shares this
    // filesystem. With a URL context we mount nothing from it at all.
    if context_url.is_none() {
        let dockerfile_parent = dockerfile.parent().ok_or_else(|| {
            RemoteForgeError::InvalidSpec(format!(
                "build-image dockerfile path has no parent directory: {}",
                dockerfile.display()
            ))
        })?;
        ws.volumes.push(VolumeMount {
            source: VolumeSource::Bind { host_path: context.to_path_buf() },
            target: PathBuf::from("/yah/build/context"),
            read_only: true,
        });
        ws.volumes.push(VolumeMount {
            source: VolumeSource::Bind { host_path: dockerfile_parent.to_path_buf() },
            target: PathBuf::from("/yah/build/dockerfile"),
            read_only: true,
        });
    }

    // An OCI archive is only emitted when we neither push nor load into the
    // worker's local image store — the archive is the sole way an out-of-band
    // consumer retrieves the bytes. Its basename derives from the first tag.
    let oci_archive_remote = (!push && !load).then(|| {
        ws.volumes.push(VolumeMount {
            source: VolumeSource::Bind {
                host_path: PathBuf::from(BUILDKIT_HOST_OUT_DIR),
            },
            target: PathBuf::from("/yah/build/out"),
            read_only: false,
        });
        format!("/yah/build/out/{}", oci_archive_basename(first_tag))
    });

    // Split program from arguments so the image's baked `rootlesskit buildkitd`
    // entrypoint is REPLACED rather than prefixed — see [`BUILDCTL`].
    let mut argv = buildctl_argv(
        dockerfile_basename,
        context_url,
        tags,
        platforms,
        build_args,
        push,
        oci_archive_remote.as_deref(),
    );
    let program = argv.remove(0);
    ws.entrypoint = Some(vec![program]);
    ws.command = Some(argv);
    Ok(ws)
}

/// Map a docker tag (`reg/repo:ver`) to a filesystem-safe basename for the
/// OCI archive output. Mirrors `qed::runner::tag_to_filename`; duplicated here
/// to keep the workload-spec layer free of qed deps.
fn oci_archive_basename(tag: &str) -> String {
    let safe: String = tag
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{safe}.tar")
}

/// `buildctl-daemonless.sh` argv for a one-shot Dockerfile build. Element 0 is
/// the program; the caller splits it off into the workload's `entrypoint`.
///
/// Multi-tag, multi-platform, and build-args are all threaded here (R590-F2):
/// - `tags` collapse into the output's `name=` attribute. Because the output
///   descriptor is comma-separated CSV, a multi-tag `name` value is wrapped in
///   double quotes so buildkit's CSV reader treats the embedded commas as part
///   of the value, not attribute delimiters (`type=image,"name=a,b",push=true`).
/// - `platforms` map to `--opt platform=<csv>` (buildkit's multi-platform key).
/// - `build_args` map to one `--opt build-arg:<KEY>=<VALUE>` per pair.
///
/// `context_url` selects between the two context shapes. Remote is a single
/// `--opt context=<url>`: the dockerfile frontend fetches the tar, unpacks it,
/// and resolves `filename=` *inside* it, so neither `--local` flag applies (and
/// passing them alongside is what would reintroduce the host-path dependency).
#[allow(clippy::too_many_arguments)]
fn buildctl_argv(
    dockerfile_basename: &str,
    context_url: Option<&str>,
    tags: &[String],
    platforms: &[String],
    build_args: &[(String, String)],
    push: bool,
    oci_archive_path: Option<&str>,
) -> Vec<String> {
    let mut argv = vec![
        BUILDCTL.to_string(),
        "build".to_string(),
        "--frontend".to_string(),
        "dockerfile.v0".to_string(),
    ];
    match context_url {
        Some(url) => {
            argv.push("--opt".to_string());
            argv.push(format!("context={url}"));
        }
        None => {
            argv.push("--local".to_string());
            argv.push("context=/yah/build/context".to_string());
            argv.push("--local".to_string());
            argv.push("dockerfile=/yah/build/dockerfile".to_string());
        }
    }
    argv.push("--opt".to_string());
    argv.push(format!("filename={dockerfile_basename}"));

    if !platforms.is_empty() {
        argv.push("--opt".to_string());
        argv.push(format!("platform={}", platforms.join(",")));
    }
    for (k, v) in build_args {
        argv.push("--opt".to_string());
        argv.push(format!("build-arg:{k}={v}"));
    }

    let name_attr = buildctl_name_attr(tags);
    if push {
        argv.push("--output".to_string());
        argv.push(format!("type=image,{name_attr},push=true"));
    } else if let Some(archive) = oci_archive_path {
        argv.push("--output".to_string());
        argv.push(format!("type=oci,{name_attr},dest={archive}"));
    } else {
        argv.push("--output".to_string());
        argv.push(format!("type=image,{name_attr}"));
    }
    argv
}

/// Render the buildkit output `name=` attribute for one or more tags. A single
/// tag is emitted bare (`name=tag`); multiple tags are comma-joined and the
/// whole value double-quoted so buildkit's CSV parser keeps them together.
fn buildctl_name_attr(tags: &[String]) -> String {
    match tags {
        [single] => format!("name={single}"),
        many => format!("\"name={}\"", many.join(",")),
    }
}

async fn run_log_task(
    forge_id: ForgeId,
    ident: MeshIdent,
    timeout: Option<Duration>,
    scryer: Arc<Scryer>,
    yubaba: Arc<dyn WardenClient>,
    sink: Option<mpsc::UnboundedSender<ExecEvent>>,
) -> ForgeStatus {
    let ingest = ingest_logs(forge_id.clone(), &ident, &scryer, yubaba.as_ref(), sink);

    let ingest_result = match timeout {
        None => ingest.await,
        Some(d) => match tokio::time::timeout(d, ingest).await {
            Ok(r) => r,
            Err(_) => {
                let _ = yubaba.teardown(&ident).await;
                return ForgeStatus::TimedOut { ended_at: now_ms() };
            }
        },
    };

    match ingest_result {
        Err(e) => ForgeStatus::Lost { reason: e.to_string() },
        Ok(()) => match yubaba.exit_code(&ident).await {
            Ok(Some(code)) => ForgeStatus::Done { exit_code: code, ended_at: now_ms() },
            Ok(None) => ForgeStatus::Lost {
                reason: "container exited but no exit code available".into(),
            },
            Err(e) => ForgeStatus::Lost { reason: e.to_string() },
        },
    }
}

async fn ingest_logs(
    forge_id: ForgeId,
    ident: &MeshIdent,
    scryer: &Scryer,
    yubaba: &dyn WardenClient,
    sink: Option<mpsc::UnboundedSender<ExecEvent>>,
) -> Result<(), RemoteForgeError> {
    let mut rx =
        yubaba.connect_logs(ident).await.map_err(|e| RemoteForgeError::LogStream(e.to_string()))?;

    let scope = EventScope::Forge(forge_id.clone());
    let run_id: TaskRunId = forge_id.into();
    let mut seq = 0u32;

    while let Some(line) = rx.recv().await {
        // Fan the line out to the live sink first (a closed sink is benign —
        // the run continues; only the live tail loses the line). scryer
        // remains the durable record.
        if let Some(s) = &sink {
            let _ = s.send(ExecEvent::Output {
                stream: OutputStream::Stdout,
                line: line.clone(),
            });
        }
        let ev = Event {
            run_id: run_id.clone(),
            seq,
            offset_ms: 0,
            level: Level::Info,
            target: "forge.remote".into(),
            msg: line,
            fields: json!({}),
            anchor: None,
            source: EventSource::Synth,
        };
        scryer
            .push(scope.clone(), ev)
            .map_err(|e| RemoteForgeError::Push(e.to_string()))?;
        seq += 1;
    }

    Ok(())
}

// ─── Test support ─────────────────────────────────────────────────────────────

/// Test-only yubaba client implementations.
#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::sync::Mutex;

    /// A yubaba client that sends a fixed set of log lines then exits with a
    /// configured exit code.
    pub struct ScriptedWardenClient {
        pub lines: Vec<String>,
        pub exit_code: i32,
        pub deploy_called: Arc<Mutex<bool>>,
        pub teardown_called: Arc<Mutex<bool>>,
        /// Container-path → bytes the finished container "produced", served by
        /// [`WardenClient::fetch_produced_file`] (R590-F6 retrieval tests).
        pub produced_files: std::collections::HashMap<PathBuf, Vec<u8>>,
    }

    impl ScriptedWardenClient {
        pub fn new(lines: Vec<String>, exit_code: i32) -> Arc<Self> {
            Arc::new(Self {
                lines,
                exit_code,
                deploy_called: Default::default(),
                teardown_called: Default::default(),
                produced_files: Default::default(),
            })
        }

        /// Like [`new`](Self::new) but seeds the produced-file map so
        /// [`WardenClient::fetch_produced_file`] serves `bytes` at `path`.
        pub fn with_produced_file(
            lines: Vec<String>,
            exit_code: i32,
            path: impl Into<PathBuf>,
            bytes: Vec<u8>,
        ) -> Arc<Self> {
            let mut produced_files = std::collections::HashMap::new();
            produced_files.insert(path.into(), bytes);
            Arc::new(Self {
                lines,
                exit_code,
                deploy_called: Default::default(),
                teardown_called: Default::default(),
                produced_files,
            })
        }
    }

    #[async_trait]
    impl WardenClient for ScriptedWardenClient {
        async fn deploy(&self, _spec: &WorkloadSpec) -> Result<(), RemoteForgeError> {
            *self.deploy_called.lock().unwrap() = true;
            Ok(())
        }

        async fn connect_logs(
            &self,
            _ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, RemoteForgeError> {
            let (tx, rx) = mpsc::channel(64);
            let lines = self.lines.clone();
            tokio::spawn(async move {
                for line in lines {
                    if tx.send(line).await.is_err() {
                        break;
                    }
                }
                // Dropping tx closes the stream → ingest_logs returns Ok(()).
            });
            Ok(rx)
        }

        async fn teardown(&self, _ident: &MeshIdent) -> Result<(), RemoteForgeError> {
            *self.teardown_called.lock().unwrap() = true;
            Ok(())
        }

        async fn exit_code(
            &self,
            _ident: &MeshIdent,
        ) -> Result<Option<i32>, RemoteForgeError> {
            Ok(Some(self.exit_code))
        }

        async fn fetch_produced_file(
            &self,
            _ident: &MeshIdent,
            remote_path: &Path,
        ) -> Result<Vec<u8>, RemoteForgeError> {
            self.produced_files.get(remote_path).cloned().ok_or_else(|| {
                RemoteForgeError::Fetch(format!(
                    "no produced file scripted at {}",
                    remote_path.display()
                ))
            })
        }
    }

    /// A yubaba client whose log stream never closes — simulates a hung
    /// container so that timeout behavior can be tested.
    pub struct HangingWardenClient {
        pub initial_lines: Vec<String>,
        pub teardown_called: Arc<Mutex<bool>>,
    }

    impl HangingWardenClient {
        pub fn new(initial_lines: Vec<String>) -> Arc<Self> {
            Arc::new(Self { initial_lines, teardown_called: Default::default() })
        }
    }

    #[async_trait]
    impl WardenClient for HangingWardenClient {
        async fn deploy(&self, _spec: &WorkloadSpec) -> Result<(), RemoteForgeError> {
            Ok(())
        }

        async fn connect_logs(
            &self,
            _ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, RemoteForgeError> {
            let (tx, rx) = mpsc::channel::<String>(8);
            let lines = self.initial_lines.clone();
            tokio::spawn(async move {
                for line in lines {
                    if tx.send(line).await.is_err() {
                        return;
                    }
                }
                // Keep tx alive indefinitely — the stream never closes.
                tokio::time::sleep(Duration::from_secs(3600)).await;
                drop(tx);
            });
            Ok(rx)
        }

        async fn teardown(&self, _ident: &MeshIdent) -> Result<(), RemoteForgeError> {
            *self.teardown_called.lock().unwrap() = true;
            Ok(())
        }

        async fn exit_code(
            &self,
            _ident: &MeshIdent,
        ) -> Result<Option<i32>, RemoteForgeError> {
            Ok(None)
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod remote {
    use super::test_support::*;
    use super::*;
    use velveteen::TaskPlacement;
    use observation::EventScope;
    use yah_scryer::service::{EventFilter, Scryer, ScryerConfig};
    use task_runs::Initiator;
    use tempfile::TempDir;
    use workload_spec::{Millis, TierTag};

    fn make_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    fn subprocess_spec(where_: TaskPlacement, timeout: Option<Millis>) -> ForgeSpec {
        ForgeSpec {
            command: ForgeCommand::Subprocess {
                argv: vec!["true".into()],
                image: None,
            },
            where_,
            timeout,
            label: None,
            initiator: Initiator::Human { camp: "test-camp".into() },
            mesh_access: velveteen::MeshAccess::None,
            cache_key: None,
        }
    }

    fn remote_any_infra() -> TaskPlacement {
        TaskPlacement::new(
            TaskLocation::RemoteAny { tier: TierTag("infra".into()), mesh_tags: vec![] },
            TaskRuntime::Container,
        )
    }

    /// R603-T5: every remote forge subprocess gets a durable, host-backed
    /// `/yah/produced` bind mount so a build's output survives kamaji reaping
    /// the exited container. The host source is the per-forge dir under the
    /// convention root, and it's writable.
    #[test]
    fn subprocess_workload_carries_durable_produced_mount() {
        let forge_id = ForgeId::new();
        let spec = subprocess_spec(remote_any_infra(), None);
        let ws = build_workload_spec(&forge_id, &spec).expect("synthesis ok");

        let mount = ws
            .volumes
            .iter()
            .find(|v| v.target == PathBuf::from(workload_spec::forge_produced::CONTAINER_DIR))
            .expect("subprocess forge must mount the durable /yah/produced dir");
        assert!(
            !mount.read_only,
            "durable produced mount must be writable so the build can write to it"
        );
        assert_eq!(
            mount.source,
            VolumeSource::Bind {
                host_path: workload_spec::forge_produced::host_dir(&forge_id.to_string()),
            },
            "host source must be the per-forge durable dir under the convention root"
        );
    }

    /// R876-F4: a `cache_key` lowers to a writable bind of
    /// `forge_cache::HOST_ROOT/<key>` at `/yah/cache`. The host path must sit
    /// under the forge state root or yubaba's `ensure_forge_state_dirs` will
    /// not mkdir it and runc refuses the bind — the opaque failure R603-B6 and
    /// R636-B1 each rediscovered on a live box.
    #[test]
    fn a_cache_key_lowers_to_a_bind_under_the_forge_state_root() {
        let forge_id = ForgeId::new();
        let mut spec = subprocess_spec(remote_any_infra(), None);
        spec.cache_key = Some("mesofact-musl.build.x86_64-unknown-linux-musl".into());
        let ws = build_workload_spec(&forge_id, &spec).expect("synthesis ok");

        let mount = ws
            .volumes
            .iter()
            .find(|v| v.target == PathBuf::from(workload_spec::forge_cache::CONTAINER_DIR))
            .expect("a cache_key must produce a /yah/cache mount");
        assert!(!mount.read_only, "a build cache the step cannot write is useless");
        let VolumeSource::Bind { host_path } = &mount.source else {
            panic!("expected a bind, got {:?}", mount.source);
        };
        assert!(
            workload_spec::forge_state::is_forge_state_path(host_path),
            "{host_path:?} must be under the forge state root or yubaba will not create it"
        );
    }

    #[test]
    fn no_cache_key_means_no_cache_mount() {
        let forge_id = ForgeId::new();
        let spec = subprocess_spec(remote_any_infra(), None);
        let ws = build_workload_spec(&forge_id, &spec).expect("synthesis ok");
        assert!(
            !ws.volumes
                .iter()
                .any(|v| v.target == PathBuf::from(workload_spec::forge_cache::CONTAINER_DIR)),
            "the default is still cold-every-run — no cache mount unless asked for"
        );
    }

    /// A key that is not a safe single path component is REFUSED, not
    /// sanitized into something else: mounting a different directory than the
    /// caller named is how a cache silently becomes shared.
    #[test]
    fn a_traversing_cache_key_is_refused() {
        let forge_id = ForgeId::new();
        let mut spec = subprocess_spec(remote_any_infra(), None);
        spec.cache_key = Some("../../etc".into());
        let err = build_workload_spec(&forge_id, &spec)
            .expect_err("a traversing key must not synthesise a mount");
        assert!(
            matches!(err, RemoteForgeError::InvalidSpec(ref m) if m.contains("cache key")),
            "{err:?}"
        );
    }

    /// R876-F4: refused rather than ignored on a native forge — it has no
    /// mount namespace, so the cache would be inert while the step reported
    /// one. Same refusal `produced_dir` gets on the local native arm.
    #[test]
    fn a_cache_key_on_a_native_forge_is_refused_rather_than_ignored() {
        let forge_id = ForgeId::new();
        let mut spec = subprocess_spec(
            TaskPlacement::new(
                TaskLocation::RemoteAny {
                    tier: TierTag("infra".into()),
                    mesh_tags: vec![],
                },
                TaskRuntime::Native,
            ),
            None,
        );
        spec.cache_key = Some("k".into());
        let err = build_workload_spec(&forge_id, &spec)
            .expect_err("a native forge must refuse a build cache");
        assert!(
            matches!(err, RemoteForgeError::InvalidSpec(ref m) if m.contains("mount namespace")),
            "{err:?}"
        );
    }

    /// R094-F3 accept: forge.run with RemoteAny + Subprocess, scripted yubaba
    /// that emits two log lines and exits 0.  After wait(), status is Done and
    /// the two events are queryable via scryer.events(Forge(id)).
    #[tokio::test]
    async fn happy() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = ScriptedWardenClient::new(
            vec!["line one".to_string(), "line two".to_string()],
            0,
        );

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        let spec = subprocess_spec(remote_any_infra(), None);

        let handle = driver.start(spec).await.unwrap();
        let id = handle.id.clone();
        let status = handle.wait().await;

        assert!(
            matches!(status, ForgeStatus::Done { exit_code: 0, .. }),
            "expected Done exit_code=0, got {status:?}"
        );

        scryer.flush_ring().unwrap();
        let events =
            scryer.events(&EventScope::Forge(id), &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 2, "expected 2 events");
        assert_eq!(events[0].msg, "line one");
        assert_eq!(events[1].msg, "line two");
        assert_eq!(events[0].target, "forge.remote");
    }

    /// R508 accept: `start_with_sink` tees every yubaba log line into the
    /// caller's sink as an `ExecEvent::Output` *as well as* scryer, so the qed
    /// runner can stream remote-step output live. After the run completes the
    /// sink has seen the same two lines, in order, that scryer recorded.
    #[tokio::test]
    async fn streams_log_lines_to_sink() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = ScriptedWardenClient::new(
            vec!["alpha".to_string(), "beta".to_string()],
            0,
        );

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        let (tx, mut rx) = mpsc::unbounded_channel::<ExecEvent>();
        let spec = subprocess_spec(remote_any_infra(), None);

        let handle = driver.start_with_sink(spec, Some(tx)).await.unwrap();
        let id = handle.id.clone();
        let status = handle.wait().await;
        assert!(
            matches!(status, ForgeStatus::Done { exit_code: 0, .. }),
            "expected Done exit_code=0, got {status:?}"
        );

        // The driver's ingest task drops its sink clone when the log stream
        // closes, so draining to None terminates.
        let mut lines = Vec::new();
        while let Some(ExecEvent::Output { stream, line }) = rx.recv().await {
            assert_eq!(stream, OutputStream::Stdout, "container logs are stdout-tagged");
            lines.push(line);
        }
        assert_eq!(lines, vec!["alpha".to_string(), "beta".to_string()]);

        // scryer still holds the durable copy — the sink is pure fan-out.
        scryer.flush_ring().unwrap();
        let events =
            scryer.events(&EventScope::Forge(id), &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 2, "scryer must still record both lines");
    }

    /// R577-T1: a remote + native *subprocess* forge is now synthesized, not
    /// refused — it goes out as an ordinary container-shaped workload carrying
    /// the native-exec marker kamaji routes on.
    ///
    /// This supersedes R380-T7's `remote_native_refused_at_start_emits_no_events`.
    /// That test pinned the v1 refusal; the refusal was always explicitly
    /// conditional on "a real use case arrives", and the W254 Darwin leg is it.
    /// The half of R380-T7's contract that still holds — refusal *before* any
    /// yubaba RPC or scryer event, for the shapes that genuinely have no native
    /// form — is pinned by
    /// [`remote_native_non_subprocess_refused_at_start_emits_no_events`].
    #[test]
    fn remote_native_subprocess_is_marked_for_kamajis_native_backend() {
        let forge_id = ForgeId::new();
        let placement = TaskPlacement::new(
            TaskLocation::RemoteAny {
                tier: TierTag("infra".into()),
                mesh_tags: vec!["tag:build-worker".into(), "os:darwin".into()],
            },
            TaskRuntime::Native,
        );
        let ws = build_workload_spec(&forge_id, &subprocess_spec(placement, None))
            .expect("remote + native subprocess must synthesize");

        assert!(
            ws.wants_native_exec(),
            "kamaji routes on this marker; without it the Darwin build lands in a Linux container"
        );

        // The per-run host produced dir reaches the step as an env var, since a
        // fork+exec'd process never sees the `/yah/produced` bind.
        let produced = workload_spec::forge_produced::host_dir(&forge_id.to_string());
        let env = ws
            .env
            .iter()
            .find(|e| e.name == PRODUCED_DIR_ENV)
            .unwrap_or_else(|| panic!("native workload must carry {PRODUCED_DIR_ENV}"));
        assert_eq!(
            env.value,
            workload_spec::EnvValue::Literal {
                value: produced.to_string_lossy().into_owned()
            }
        );
        assert_eq!(ws.workdir.as_deref(), Some(produced.as_path()));

        // The volume stays even though nothing mounts it: yubaba's
        // `ensure_forge_state_dirs` walks `volumes` to mkdir the host dir, and
        // that dir is both the workdir above and what `fetch_produced_file`
        // later reads. Dropping it as "unused" breaks retrieval silently.
        assert!(
            ws.volumes.iter().any(|v| matches!(
                &v.source,
                workload_spec::VolumeSource::Bind { host_path } if host_path == &produced
            )),
            "durable produced mount must survive onto the native spec: {:?}",
            ws.volumes
        );

        // Same spec, container runtime: no marker. The Linux offload leg proven
        // live on us-west-002 must be byte-for-byte unaffected by this change.
        let container = build_workload_spec(
            &forge_id,
            &subprocess_spec(
                TaskPlacement::new(
                    TaskLocation::RemoteAny {
                        tier: TierTag("infra".into()),
                        mesh_tags: vec![],
                    },
                    TaskRuntime::Container,
                ),
                None,
            ),
        )
        .expect("container synthesis");
        assert!(!container.wants_native_exec());
        assert!(container.workdir.is_none());
        assert!(!container.env.iter().any(|e| e.name == PRODUCED_DIR_ENV));
    }

    /// R605-F8: a remote + microVM subprocess forge synthesizes an ordinary
    /// container-shaped workload carrying the microVM marker kamaji routes on.
    #[test]
    fn remote_microvm_subprocess_is_marked_for_kamajis_microvm_backend() {
        let forge_id = ForgeId::new();
        let placement = TaskPlacement::new(
            TaskLocation::RemoteAny {
                tier: TierTag("infra".into()),
                mesh_tags: vec!["tag:build-worker".into(), "arch:x86".into()],
            },
            TaskRuntime::MicroVm,
        );
        let ws = build_workload_spec(&forge_id, &subprocess_spec(placement, None))
            .expect("remote + microvm subprocess must synthesize");

        assert!(
            ws.wants_microvm(),
            "kamaji routes on this marker; without it the build shares the node's kernel"
        );
        assert!(
            !ws.wants_native_exec(),
            "one `yah.exec` key holds one value — the two substrates cannot both be selected"
        );

        // Unlike the native path, the produced dir needs no env-var indirection:
        // the guest has a mount namespace, so kamaji surfaces the durable volume
        // at the target the spec declared and the step writes to `/yah/produced`
        // exactly as it would in a container.
        assert!(
            !ws.env.iter().any(|e| e.name == PRODUCED_DIR_ENV),
            "a microVM step resolves produced files through the declared mount, not an env var"
        );
        assert!(ws.workdir.is_none());

        let produced = workload_spec::forge_produced::host_dir(&forge_id.to_string());
        assert!(
            ws.volumes.iter().any(|v| matches!(
                &v.source,
                workload_spec::VolumeSource::Bind { host_path } if host_path == &produced
            )),
            "the durable produced mount is how artifacts leave the guest: {:?}",
            ws.volumes
        );
    }

    /// R605-F8: a microVM workload must NOT carry the host-network annotation.
    ///
    /// Inert at the backend — a guest has no namespace to place in the host's —
    /// but `AdmissionGrant::from_spec` reads `host_network` off exactly this
    /// annotation, so leaving it on would make every microVM grant assert a
    /// privilege the run never took.
    #[test]
    fn a_microvm_workload_does_not_claim_host_networking() {
        let forge_id = ForgeId::new();
        let vm = build_workload_spec(
            &forge_id,
            &subprocess_spec(
                TaskPlacement::new(
                    TaskLocation::RemoteAny {
                        tier: TierTag("infra".into()),
                        mesh_tags: vec![],
                    },
                    TaskRuntime::MicroVm,
                ),
                None,
            ),
        )
        .expect("synthesis");
        assert!(!vm.wants_host_network());

        // The container leg is untouched: it still needs host networking to
        // reach crates.io, and R590-B7 proved that the hard way.
        let container = build_workload_spec(
            &forge_id,
            &subprocess_spec(
                TaskPlacement::new(
                    TaskLocation::RemoteAny {
                        tier: TierTag("infra".into()),
                        mesh_tags: vec![],
                    },
                    TaskRuntime::Container,
                ),
                None,
            ),
        )
        .expect("synthesis");
        assert!(container.wants_host_network());
        assert!(!container.wants_microvm());
    }

    /// R605-F8: the image-backed forge commands refuse a microVM placement
    /// before any state is allocated, exactly as they refuse a native one.
    #[test]
    fn remote_microvm_refuses_an_image_backed_forge_command() {
        let forge_id = ForgeId::new();
        let spec = ForgeSpec {
            command: ForgeCommand::BuildImage {
                dockerfile: "Dockerfile".into(),
                context: PathBuf::from("."),
                context_url: None,
                tags: vec!["example:latest".into()],
                platforms: vec![],
                build_args: Default::default(),
                push: false,
                load: false,
            },
            where_: TaskPlacement::new(
                TaskLocation::RemoteAny {
                    tier: TierTag("infra".into()),
                    mesh_tags: vec![],
                },
                TaskRuntime::MicroVm,
            ),
            ..subprocess_spec(
                TaskPlacement::new(
                    TaskLocation::RemoteAny {
                        tier: TierTag("infra".into()),
                        mesh_tags: vec![],
                    },
                    TaskRuntime::MicroVm,
                ),
                None,
            )
        };
        let err = build_workload_spec(&forge_id, &spec).unwrap_err();
        assert!(
            format!("{err}").contains("microvm"),
            "the refusal must name the placement that caused it: {err}"
        );
    }

    /// R577-T1 × R555-T2: `ExecContext` folding is correct for a native forge
    /// on two of its three fields and refuses the third.
    ///
    /// `env` needs no special-casing — for a fork+exec'd process those become
    /// real process env, a stronger reading than the container one. A relative
    /// `cwd` is refused, because `workdir` reaches `Command::current_dir`
    /// directly and would resolve against the kamaji daemon's cwd rather than a
    /// source checkout. An absolute one is honored and remains the caller's
    /// responsibility, exactly as for a container.
    #[test]
    fn native_forge_refuses_a_relative_cwd_but_honors_an_absolute_one() {
        let forge_id = ForgeId::new();
        let native = || {
            let placement = TaskPlacement::new(
                TaskLocation::RemoteAny {
                    tier: TierTag("infra".into()),
                    mesh_tags: vec![],
                },
                TaskRuntime::Native,
            );
            build_workload_spec(&forge_id, &subprocess_spec(placement, None)).expect("synthesis")
        };

        // The `desktop-release` shape: a repo-relative cwd.
        let mut ws = native();
        let err = apply_exec_context(
            &mut ws,
            &ExecContext {
                cwd: Some(PathBuf::from("app/yah/desktop")),
                ..Default::default()
            },
        )
        .expect_err("a relative cwd must not reach a fork+exec'd step");
        match err {
            RemoteForgeError::InvalidSpec(msg) => {
                assert!(msg.contains("app/yah/desktop"), "got {msg:?}");
                assert!(msg.contains("relative"), "got {msg:?}");
            }
            other => panic!("expected InvalidSpec, got {other:?}"),
        }

        // Absolute is honored, and overrides the produced-dir default.
        let mut ws = native();
        apply_exec_context(
            &mut ws,
            &ExecContext {
                cwd: Some(PathBuf::from("/var/lib/yah/qed/checkout")),
                env: vec![("CARGO_TERM_COLOR".into(), "never".into())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        )
        .expect("absolute cwd is the caller's responsibility, not a refusal");
        assert_eq!(
            ws.workdir.as_deref(),
            Some(std::path::Path::new("/var/lib/yah/qed/checkout"))
        );
        // …and the produced-dir contract survives the workdir override, because
        // env is appended rather than replaced. This is the invariant
        // `mark_native_exec` documents and depends on.
        assert!(ws.env.iter().any(|e| e.name == PRODUCED_DIR_ENV));
        assert!(ws.env.iter().any(|e| e.name == "CARGO_TERM_COLOR"));

        // A CONTAINER forge keeps the old behaviour: relative is fine there,
        // since the runtime resolves it against the image root.
        let placement = TaskPlacement::new(
            TaskLocation::RemoteAny {
                tier: TierTag("infra".into()),
                mesh_tags: vec![],
            },
            TaskRuntime::Container,
        );
        let mut container =
            build_workload_spec(&forge_id, &subprocess_spec(placement, None)).expect("synthesis");
        apply_exec_context(
            &mut container,
            &ExecContext {
                cwd: Some(PathBuf::from("app/yah/desktop")),
                ..Default::default()
            },
        )
        .expect("relative cwd is well-defined against an image root");
        assert_eq!(
            container.workdir.as_deref(),
            Some(std::path::Path::new("app/yah/desktop"))
        );
    }

    /// R577-T1 (retaining R380-T7's live half): remote + native is still
    /// refused for the two image-backed forge command shapes, and still refused
    /// *early* — before any yubaba RPC fires and before any event reaches
    /// scryer.
    #[tokio::test]
    async fn remote_native_non_subprocess_refused_at_start_emits_no_events() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = ScriptedWardenClient::new(vec!["should not arrive".into()], 0);
        let deploy_called = yubaba.deploy_called.clone();

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        // A build-image forge runs BuildKit *inside* a container — there is no
        // native shape for it, so the quadrant stays refused here.
        let mut spec = build_image_spec(false);
        spec.where_ = TaskPlacement::new(
            TaskLocation::RemoteAny {
                tier: TierTag("infra".into()),
                mesh_tags: vec![],
            },
            TaskRuntime::Native,
        );

        let err = match driver.start(spec).await {
            Ok(_) => panic!("remote+native build-image must be refused"),
            Err(e) => e,
        };
        match err {
            RemoteForgeError::InvalidSpec(msg) => {
                assert!(
                    msg.contains("remote + native"),
                    "error must name the refused quadrant; got {msg:?}",
                );
                assert!(
                    msg.contains("R577-T1") || msg.contains("W254"),
                    "error should reference the ticket / doc for context; got {msg:?}",
                );
            }
            other => panic!("expected InvalidSpec, got {other:?}"),
        }

        assert!(
            !*deploy_called.lock().unwrap(),
            "yubaba.deploy must NOT be called when the spec is refused upstream",
        );

        // Give any (unexpected) background ingest task a tick to push, then
        // verify scryer is empty of Forge-scoped events.
        tokio::time::sleep(Duration::from_millis(20)).await;
        scryer.flush_ring().unwrap();
        // We can't query `Forge(*)` directly — but no forge_id was returned to
        // the caller, so there's no scope to check. Query every recently-used
        // scope to make sure nothing snuck through.  scryer.events on a fresh
        // ForgeId returns empty, which is the strongest assertion we can make
        // without a wildcard query.
        let probe_id = ForgeId::new();
        let events =
            scryer.events(&EventScope::Forge(probe_id), &EventFilter::default()).await.unwrap();
        assert!(events.is_empty(), "no events expected; got {events:?}");
    }

    // ── R381-T5 BuildKit synthesis ──────────────────────────────────────────

    fn build_image_spec(push: bool) -> ForgeSpec {
        build_image_spec_with_url(push, None)
    }

    fn build_image_spec_with_url(push: bool, context_url: Option<&str>) -> ForgeSpec {
        ForgeSpec {
            command: ForgeCommand::BuildImage {
                dockerfile: PathBuf::from(
                    "/tmp/camp/.yah/cache/buildkit/yah-rust.Dockerfile",
                ),
                context: PathBuf::from("/tmp/camp"),
                context_url: context_url.map(str::to_string),
                tags: vec!["ghcr.io/yah-ai/yah-rust:dev".into()],
                platforms: vec![],
                build_args: vec![],
                push,
                load: false,
            },
            where_: remote_any_infra(),
            timeout: None,
            label: None,
            initiator: Initiator::Human { camp: "test-camp".into() },
            mesh_access: velveteen::MeshAccess::None,
            cache_key: None,
        }
    }

    /// Helper: destructure a `BuildImage` spec into the args
    /// `build_image_workload_spec` now takes.
    struct BuildImageParts {
        dockerfile: PathBuf,
        context: PathBuf,
        context_url: Option<String>,
        tags: Vec<String>,
        platforms: Vec<String>,
        build_args: Vec<(String, String)>,
        push: bool,
        load: bool,
    }

    fn build_image_parts(spec: ForgeSpec) -> BuildImageParts {
        match spec.command {
            ForgeCommand::BuildImage {
                dockerfile,
                context,
                context_url,
                tags,
                platforms,
                build_args,
                push,
                load,
            } => BuildImageParts {
                dockerfile,
                context,
                context_url,
                tags,
                platforms,
                build_args,
                push,
                load,
            },
            _ => unreachable!(),
        }
    }

    fn synth(parts: &BuildImageParts) -> WorkloadSpec {
        build_image_workload_spec(
            &ForgeId::new(),
            &parts.dockerfile,
            &parts.context,
            parts.context_url.as_deref(),
            &parts.tags,
            &parts.platforms,
            &parts.build_args,
            parts.push,
            parts.load,
            TierTag("infra".into()),
        )
        .expect("synthesis ok")
    }

    /// R833-F8: a `TaskLocation::Remote { node }` spec carries the node name to
    /// admission as the imperative node-selector annotation.
    ///
    /// It used to be DROPPED here — the match arm read `Remote { .. }` and
    /// produced the same (tier=infra, no tags) pair as an unconstrained
    /// `RemoteAny`, so "run it on us-west-003" was indistinguishable from "run
    /// it anywhere" by the time yubaba saw it.
    #[test]
    fn a_pinned_node_reaches_admission_as_an_annotation() {
        let forge_id = ForgeId::new();
        let placement = TaskPlacement::new(
            TaskLocation::Remote {
                node: workload_spec::MeshIdent("us-west-003".into()),
            },
            TaskRuntime::Container,
        );
        let ws = build_workload_spec(&forge_id, &subprocess_spec(placement, None))
            .expect("synthesis ok");
        assert_eq!(
            ws.annotations
                .get(NODE_SELECTOR_NODE_ANNOTATION)
                .map(String::as_str),
            Some("us-west-003"),
        );
        assert!(
            !ws.annotations.contains_key(NODE_SELECTOR_MESH_TAGS_ANNOTATION),
            "a named node is the constraint; no inferred tag filter on top",
        );
        assert_eq!(ws.tier.0, "infra");
    }

    /// The dual: a tag-selected `RemoteAny` is unchanged by R833-F8 — mesh tags
    /// travel, no node annotation appears.
    #[test]
    fn an_unpinned_remote_any_carries_no_node_annotation() {
        let forge_id = ForgeId::new();
        let placement = TaskPlacement::new(
            TaskLocation::RemoteAny {
                tier: TierTag("infra".into()),
                mesh_tags: vec!["tag:build-worker".into(), "arch:x86".into()],
            },
            TaskRuntime::Container,
        );
        let ws = build_workload_spec(&forge_id, &subprocess_spec(placement, None))
            .expect("synthesis ok");
        assert_eq!(
            ws.annotations
                .get(NODE_SELECTOR_MESH_TAGS_ANNOTATION)
                .map(String::as_str),
            Some("tag:build-worker,arch:x86"),
        );
        assert!(!ws.annotations.contains_key(NODE_SELECTOR_NODE_ANNOTATION));
    }

    /// R636-B2: the buildkit workload — and *only* it — asks for the
    /// nested-sandbox grant. Without the annotation kamaji's baseline sandbox
    /// (CAP_NET_BIND_SERVICE only, `no_new_privs` on) stops `rootlesskit`
    /// before buildkitd ever binds its socket, and the step dies with no
    /// streamed output at all.
    #[test]
    fn build_image_workload_asks_for_the_nested_sandbox_grant() {
        let ws = synth(&build_image_parts(build_image_spec(false)));
        assert_eq!(
            ws.annotations
                .get(workload_spec::NESTED_SANDBOX_ANNOTATION)
                .map(String::as_str),
            Some(workload_spec::NESTED_SANDBOX_VALUE),
        );
        assert!(ws.wants_nested_sandbox());

        // The grant is guarded to tier=infra by kamaji; a build-image forge is
        // always infra, so the request is always honourable.
        assert_eq!(ws.tier.0, "infra");
    }

    /// The other half of the same claim: an ordinary remote subprocess step
    /// must NOT carry the grant. Every remote forge is tier=infra, so the tier
    /// gate alone would let one through — the annotation being absent is what
    /// actually keeps the sandbox tight for everything but buildkit.
    #[test]
    fn subprocess_workload_does_not_ask_for_the_nested_sandbox_grant() {
        let forge_id = ForgeId::new();
        let ws = build_workload_spec(&forge_id, &subprocess_spec(remote_any_infra(), None))
            .expect("synthesis ok");
        assert!(
            !ws.wants_nested_sandbox(),
            "only the buildkit build-image workload may request CAP_SETUID/CAP_SETGID"
        );
        assert!(!ws
            .annotations
            .contains_key(workload_spec::NESTED_SANDBOX_ANNOTATION));
    }

    /// build_image_workload_spec assembles a buildkit-shaped WorkloadSpec:
    /// rootless moby/buildkit image, buildctl one-shot argv, context +
    /// dockerfile bind-mounts, OCI archive output dir when push=false.
    #[test]
    fn build_image_workload_spec_shape_push_false() {
        let ws = synth(&build_image_parts(build_image_spec(false)));

        assert_eq!(ws.image.registry, "docker.io");
        assert_eq!(ws.image.repository, "moby/buildkit");
        assert!(
            ws.image.tag.contains("rootless"),
            "default buildkit tag must be a rootless variant: {:?}",
            ws.image.tag
        );

        // CPU upsized vs the for_forge default (512m); the memory ceiling is
        // inherited rather than overridden, so assert it is at least the
        // buildkit floor the old `= 2048` override was reaching for.
        assert!(ws.resources.memory_mb >= 1024, "memory should be ≥1GiB");
        assert!(
            ws.resources.cpu_millis >= 1000,
            "cpu_millis should be ≥ one full core"
        );
        // The placement request is the small number, and it must not track the
        // ceiling — this is the regression that made 8 GiB build-workers
        // unschedulable for every offloaded step.
        assert_eq!(
            ws.memory_request_mb(),
            workload_spec::FORGE_MEMORY_REQUEST_MB
        );
        assert!(
            ws.memory_request_mb() < ws.resources.memory_mb,
            "request must stay below the ceiling: {} vs {}",
            ws.memory_request_mb(),
            ws.resources.memory_mb
        );

        // Bind mounts: context (ro), dockerfile dir (ro), out dir (rw).
        let mounts: Vec<_> = ws
            .volumes
            .iter()
            .map(|v| (v.target.to_string_lossy().into_owned(), v.read_only))
            .collect();
        assert!(
            mounts
                .iter()
                .any(|(t, ro)| t == "/yah/build/context" && *ro),
            "context bind-mount missing or not read-only: {mounts:?}"
        );
        assert!(
            mounts
                .iter()
                .any(|(t, ro)| t == "/yah/build/dockerfile" && *ro),
            "dockerfile bind-mount missing or not read-only: {mounts:?}"
        );
        assert!(
            mounts
                .iter()
                .any(|(t, ro)| t == "/yah/build/out" && !*ro),
            "build-out dir bind-mount must be writable when push=false: {mounts:?}"
        );

        // The image's baked `rootlesskit buildkitd` entrypoint must be
        // REPLACED, not prefixed — kamaji concatenates the two.
        assert_eq!(
            ws.entrypoint.as_deref(),
            Some(["buildctl-daemonless.sh".to_string()].as_slice()),
            "entrypoint must override the image's buildkitd entrypoint",
        );

        // Command carries buildctl's arguments and points at the OCI archive
        // when push=false.
        let argv = ws.command.expect("command must be set");
        assert_eq!(argv.first().map(String::as_str), Some("build"));
        assert!(
            argv.iter().any(|a| a.starts_with("type=oci,")),
            "push=false must emit --output type=oci: {argv:?}",
        );
        assert!(
            argv.iter().any(|a| a == "filename=yah-rust.Dockerfile"),
            "buildctl --opt filename=<basename> must be set: {argv:?}",
        );
    }

    /// push=true switches the buildctl output to a registry push and drops the
    /// build-out bind-mount.
    #[test]
    fn build_image_workload_spec_push_true_uses_registry_output() {
        let ws = synth(&build_image_parts(build_image_spec(true)));

        let argv = ws.command.expect("command must be set");
        assert!(
            argv.iter().any(|a| a.contains("type=image") && a.contains("push=true")),
            "push=true must emit --output type=image,…,push=true: {argv:?}",
        );

        let has_out_dir = ws
            .volumes
            .iter()
            .any(|v| v.target == PathBuf::from("/yah/build/out"));
        assert!(
            !has_out_dir,
            "push=true must NOT mount the build-out dir: {:?}",
            ws.volumes,
        );
    }

    /// R590-F2: multiple tags, target platforms, and build-args all thread
    /// into the buildctl argv. Multi-tag `name=` is quoted so buildkit's CSV
    /// reader keeps the comma-joined value together.
    #[test]
    fn buildctl_argv_threads_tags_platforms_and_build_args() {
        let argv = buildctl_argv(
            "yah-rust.Dockerfile",
            None,
            &["reg/img:a".into(), "reg/img:b".into()],
            &["linux/amd64".into(), "linux/arm64".into()],
            &[("RUST_VERSION".into(), "1.85".into())],
            true,
            None,
        );
        // platform csv
        assert!(
            argv.iter().any(|a| a == "platform=linux/amd64,linux/arm64"),
            "platforms must join into one --opt platform=<csv>: {argv:?}",
        );
        // build-arg
        assert!(
            argv.iter().any(|a| a == "build-arg:RUST_VERSION=1.85"),
            "build-args must emit --opt build-arg:K=V: {argv:?}",
        );
        // quoted multi-tag name in a pushing image output
        assert!(
            argv.iter()
                .any(|a| a == "type=image,\"name=reg/img:a,reg/img:b\",push=true"),
            "multi-tag push output must quote the comma-joined name: {argv:?}",
        );
    }

    /// A single tag stays bare (unquoted) — the common case, and the shape the
    /// pre-F2 tests asserted.
    #[test]
    fn buildctl_argv_single_tag_is_unquoted() {
        let argv = buildctl_argv("D.Dockerfile", None, &["reg/img:x".into()], &[], &[], false, None);
        assert!(
            argv.iter().any(|a| a == "type=image,name=reg/img:x"),
            "single-tag non-push output must be bare name=<tag>: {argv:?}",
        );
    }

    // ── R636-B1 cross-host build context ────────────────────────────────────

    /// With a `context_url`, the synthesized workload must reference NOTHING
    /// from the composing host's filesystem — that is the whole bug. The one
    /// surviving bind is `/yah/build/out`, which is a worker-local path.
    #[test]
    fn remote_context_url_drops_every_camp_host_bind_mount() {
        let ws = synth(&build_image_parts(build_image_spec_with_url(
            false,
            Some("https://cdn.yah.dev/yah-cloud/qed-context/deadbeef.tar.gz"),
        )));

        let binds: Vec<String> = ws
            .volumes
            .iter()
            .filter_map(|v| match &v.source {
                VolumeSource::Bind { host_path } => {
                    Some(host_path.to_string_lossy().into_owned())
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            binds,
            vec![BUILDKIT_HOST_OUT_DIR.to_string()],
            "the only bind left may be the worker-local OCI out dir: {binds:?}",
        );
        assert!(
            !binds.iter().any(|b| b.starts_with("/tmp/camp")),
            "no path from the qed host may reach the worker: {binds:?}",
        );

        let argv = ws.command.expect("command must be set");
        assert!(
            argv.iter()
                .any(|a| a == "context=https://cdn.yah.dev/yah-cloud/qed-context/deadbeef.tar.gz"),
            "remote context must be passed as --opt context=<url>: {argv:?}",
        );
        assert!(
            !argv.iter().any(|a| a.starts_with("context=/yah/build")
                || a.starts_with("dockerfile=/yah/build")),
            "the --local flags must be gone, or buildkit reads the empty mounts: {argv:?}",
        );
        // The Dockerfile is resolved from inside the fetched tar by basename.
        assert!(
            argv.iter().any(|a| a == "filename=yah-rust.Dockerfile"),
            "filename= must still name the Dockerfile inside the tar: {argv:?}",
        );
    }

    /// Without a URL the pre-R636-B1 shape is unchanged — same-host yubaba
    /// deployments keep working off bind mounts.
    #[test]
    fn no_context_url_keeps_the_bind_mount_shape() {
        let ws = synth(&build_image_parts(build_image_spec(false)));
        let argv = ws.command.expect("command must be set");
        assert!(
            argv.iter().any(|a| a == "context=/yah/build/context"),
            "bind-mount shape must still emit --local context=…: {argv:?}",
        );
        assert!(
            !argv.iter().any(|a| a.starts_with("context=http")),
            "no URL context without one being asked for: {argv:?}",
        );
    }

    /// Remote BuildImage dispatch round-trips through RemoteForgeDriver +
    /// ScriptedWardenClient: the synthesized BuildKit workload is deployed,
    /// the scripted exit code surfaces as a Done status, and yubaba.deploy
    /// was actually called.
    #[tokio::test]
    async fn remote_build_image_success_round_trip() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = ScriptedWardenClient::new(
            vec!["#1 [internal] load build definition".into(), "#5 DONE".into()],
            0,
        );
        let deploy_called = yubaba.deploy_called.clone();

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        let handle = driver.start(build_image_spec(true)).await.unwrap();
        let id = handle.id.clone();
        let status = handle.wait().await;

        assert!(
            matches!(status, ForgeStatus::Done { exit_code: 0, .. }),
            "expected Done exit_code=0, got {status:?}"
        );
        assert!(
            *deploy_called.lock().unwrap(),
            "yubaba.deploy should have been called for remote build-image"
        );

        scryer.flush_ring().unwrap();
        let events =
            scryer.events(&EventScope::Forge(id), &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 2, "expected 2 buildkit log lines");
    }

    /// R590-F6: the finished container's produced file is retrievable verbatim
    /// through `RemoteForgeDriver::fetch_produced_file` — the bytes come back
    /// unchanged (byte-preservation is what the content-addressed landing then
    /// checksums). A path with no scripted file surfaces a clean `Fetch` error,
    /// and the trait default (no override) also errs rather than silently
    /// returning empty.
    #[tokio::test]
    async fn fetch_produced_file_round_trips_bytes() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let payload = b"librusty_v8 tarball bytes \x00\x01\x02".to_vec();
        let remote_path = PathBuf::from("/tmp/rusty-v8-musl/librusty_v8.tar.gz");
        let yubaba = ScriptedWardenClient::with_produced_file(
            vec!["build done".into()],
            0,
            remote_path.clone(),
            payload.clone(),
        );

        let driver = RemoteForgeDriver::new(scryer, yubaba);
        let forge_id = ForgeId::new();

        let got = driver
            .fetch_produced_file(&forge_id, &remote_path)
            .await
            .expect("scripted produced file must be retrievable");
        assert_eq!(got, payload, "bytes must survive the transport unchanged");

        let missing = driver
            .fetch_produced_file(&forge_id, Path::new("/tmp/nope"))
            .await;
        assert!(
            matches!(missing, Err(RemoteForgeError::Fetch(_))),
            "an unscripted path must be a Fetch error, got {missing:?}",
        );
    }

    /// R094-F3 accept: forge.run with a 50ms timeout against a hanging stream.
    /// Status must be TimedOut and yubaba teardown must have been called.
    #[tokio::test]
    async fn timeout() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);

        let yubaba = HangingWardenClient::new(vec!["slow start".to_string()]);
        let teardown_called = yubaba.teardown_called.clone();

        let driver = RemoteForgeDriver::new(scryer.clone(), yubaba);
        let spec = subprocess_spec(remote_any_infra(), Some(Millis::from_ms(50)));

        let handle = driver.start(spec).await.unwrap();
        let status = handle.wait().await;

        assert!(
            matches!(status, ForgeStatus::TimedOut { .. }),
            "expected TimedOut, got {status:?}"
        );
        assert!(
            *teardown_called.lock().unwrap(),
            "yubaba.teardown must be called on timeout"
        );
    }
}

// ─── ForgeExecutor-surface tests (R555-T2) ────────────────────────────────────

#[cfg(test)]
mod executor_surface {
    use super::test_support::*;
    use super::*;
    use std::sync::Mutex;
    use task_runs::Initiator;
    use tempfile::TempDir;
    use velveteen::TaskPlacement;
    use yah_scryer::service::{Scryer, ScryerConfig};

    fn make_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    fn subprocess_spec(where_: TaskPlacement) -> ForgeSpec {
        ForgeSpec {
            command: ForgeCommand::Subprocess {
                argv: vec!["true".into()],
                image: None,
            },
            where_,
            timeout: None,
            label: None,
            initiator: Initiator::Human {
                camp: "test-camp".into(),
            },
            mesh_access: velveteen::MeshAccess::None,
            cache_key: None,
        }
    }

    fn remote_any_infra() -> TaskPlacement {
        TaskPlacement::new(
            TaskLocation::RemoteAny {
                tier: TierTag("infra".into()),
                mesh_tags: vec![],
            },
            TaskRuntime::Container,
        )
    }

    /// `ScriptedWardenClient` records only *that* deploy happened; the
    /// ExecContext tests need the spec it was handed.
    struct CapturingWardenClient {
        inner: Arc<ScriptedWardenClient>,
        deployed: Arc<Mutex<Vec<WorkloadSpec>>>,
    }

    impl CapturingWardenClient {
        fn new(lines: Vec<String>, exit_code: i32) -> (Arc<Self>, Arc<Mutex<Vec<WorkloadSpec>>>) {
            let deployed: Arc<Mutex<Vec<WorkloadSpec>>> = Default::default();
            let client = Arc::new(Self {
                inner: ScriptedWardenClient::new(lines, exit_code),
                deployed: deployed.clone(),
            });
            (client, deployed)
        }
    }

    #[async_trait]
    impl WardenClient for CapturingWardenClient {
        async fn deploy(&self, spec: &WorkloadSpec) -> Result<(), RemoteForgeError> {
            self.deployed.lock().unwrap().push(spec.clone());
            self.inner.deploy(spec).await
        }
        async fn connect_logs(
            &self,
            ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, RemoteForgeError> {
            self.inner.connect_logs(ident).await
        }
        async fn teardown(&self, ident: &MeshIdent) -> Result<(), RemoteForgeError> {
            self.inner.teardown(ident).await
        }
        async fn exit_code(&self, ident: &MeshIdent) -> Result<Option<i32>, RemoteForgeError> {
            self.inner.exit_code(ident).await
        }
    }

    /// The whole point of the impl: a caller holding `Arc<dyn ForgeExecutor>`
    /// (the cloud reconciler) can dispatch a remote recipe step without knowing
    /// it's remote.
    #[tokio::test]
    async fn executes_through_dyn_forge_executor_and_reports_the_exit_code() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = ScriptedWardenClient::new(vec!["building".into(), "done".into()], 0);

        let driver: Arc<dyn ForgeExecutor> =
            Arc::new(RemoteForgeDriver::new(scryer, yubaba.clone()));

        let outcome = driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default(),
                None,
            )
            .await
            .expect("remote dispatch ok");

        assert!(outcome.succeeded(), "got {:?}", outcome.status);
        assert!(
            *yubaba.deploy_called.lock().unwrap(),
            "the run must have reached yubaba.deploy"
        );
    }

    /// A non-zero remote exit must surface as a *failed outcome*, not an Err —
    /// same contract the local driver honors, so `materialize_transform`'s
    /// `outcome.succeeded()` branch works identically for both.
    #[tokio::test]
    async fn nonzero_remote_exit_is_a_failed_outcome_with_a_log_tail() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = ScriptedWardenClient::new(vec!["ld: cannot find -lstdc++".into()], 1);

        let driver = RemoteForgeDriver::new(scryer, yubaba);
        let outcome = driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default(),
                None,
            )
            .await
            .expect("a failing run is still a successful dispatch");

        assert!(!outcome.succeeded(), "got {:?}", outcome.status);
        assert!(
            outcome.stderr_tail.contains("cannot find -lstdc++"),
            "the tail must carry the failure text, got {:?}",
            outcome.stderr_tail
        );
    }

    /// R555-F3: a caller holding only `Arc<dyn ForgeExecutor>` gets the built
    /// artifact back on its own filesystem. Without this leg the reconciler
    /// sees exit 0 and an absent output file — the exact "produced no output"
    /// bail R546-B8 already paid for once.
    #[tokio::test]
    async fn produced_file_is_pulled_back_onto_the_caller_filesystem() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let remote_path =
            PathBuf::from(workload_spec::forge_produced::CONTAINER_DIR).join("out.tar.gz");
        let yubaba = ScriptedWardenClient::with_produced_file(
            vec!["building".into()],
            0,
            remote_path.clone(),
            b"artifact bytes".to_vec(),
        );

        // Deliberately a path whose parent does not exist yet — a driver that
        // only wrote the file would fail here, and the caller's next step is a
        // read of exactly this path.
        let dest = dir.path().join("landing").join("out.tar.gz");
        let driver: Arc<dyn ForgeExecutor> = Arc::new(RemoteForgeDriver::new(scryer, yubaba));
        let outcome = driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default().with_produced(remote_path, dest.clone()),
                None,
            )
            .await
            .expect("remote dispatch ok");

        assert!(outcome.succeeded(), "got {:?}", outcome.status);
        assert_eq!(std::fs::read(&dest).unwrap(), b"artifact bytes");
    }

    /// A failed build has nothing to retrieve, and a fetch error on top of the
    /// real failure buries the log tail the operator actually needs.
    #[tokio::test]
    async fn a_failed_run_reports_its_log_tail_instead_of_a_fetch_error() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let remote_path =
            PathBuf::from(workload_spec::forge_produced::CONTAINER_DIR).join("out.tar.gz");
        // No produced file scripted: fetch would error if it were attempted.
        let yubaba = ScriptedWardenClient::new(vec!["gn: fatal error".into()], 1);

        let dest = dir.path().join("out.tar.gz");
        let driver = RemoteForgeDriver::new(scryer, yubaba);
        let outcome = driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default().with_produced(remote_path, dest.clone()),
                None,
            )
            .await
            .expect("a failing build is still a successful dispatch");

        assert!(!outcome.succeeded());
        assert!(outcome.stderr_tail.contains("gn: fatal error"));
        assert!(!dest.exists(), "nothing to land from a failed build");
    }

    /// Only `/yah/produced` is bind-mounted onto host-persistent storage, so a
    /// path outside it is gone once kamaji reaps the container. Refusing names
    /// the recipe bug; letting the RPC 404 would read as a transport fault.
    #[tokio::test]
    async fn a_produced_path_outside_the_durable_dir_is_refused() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = ScriptedWardenClient::with_produced_file(
            vec![],
            0,
            PathBuf::from("/tmp/out.tar.gz"),
            b"bytes".to_vec(),
        );

        let driver = RemoteForgeDriver::new(scryer, yubaba);
        let err = driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default().with_produced(
                    PathBuf::from("/tmp/out.tar.gz"),
                    dir.path().join("out.tar.gz"),
                ),
                None,
            )
            .await
            .expect_err("a non-durable produced path must be refused");
        let msg = err.to_string();
        assert!(msg.contains("/yah/produced"), "{msg}");
    }

    /// The caller's sink still sees every line during the run, plus the
    /// terminal `Finished` the trait contract promises.
    #[tokio::test]
    async fn forwards_lines_to_the_caller_sink_then_finished() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = ScriptedWardenClient::new(vec!["one".into(), "two".into()], 0);

        let (tx, mut rx) = mpsc::unbounded_channel::<ExecEvent>();
        let driver = RemoteForgeDriver::new(scryer, yubaba);
        driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default(),
                Some(tx),
            )
            .await
            .unwrap();

        let mut lines = Vec::new();
        let mut finished = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                ExecEvent::Output { line, .. } => lines.push(line),
                ExecEvent::Finished { .. } => finished = true,
                ExecEvent::Started => {}
            }
        }
        assert_eq!(lines, vec!["one".to_string(), "two".to_string()]);
        assert!(finished, "trait contract promises exactly one Finished");
    }

    /// The tail is capped — a multi-hour build must not buffer its whole log
    /// just to report a failure message.
    #[tokio::test]
    async fn log_tail_is_bounded() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let lines: Vec<String> = (0..REMOTE_TAIL_LINES * 3)
            .map(|i| format!("line {i}"))
            .collect();
        let yubaba = ScriptedWardenClient::new(lines, 1);

        let driver = RemoteForgeDriver::new(scryer, yubaba);
        let outcome = driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default(),
                None,
            )
            .await
            .unwrap();

        let kept: Vec<&str> = outcome.stderr_tail.lines().collect();
        assert_eq!(kept.len(), REMOTE_TAIL_LINES);
        assert_eq!(
            *kept.last().unwrap(),
            format!("line {}", REMOTE_TAIL_LINES * 3 - 1),
            "the tail must keep the LAST lines, not the first"
        );
    }

    /// Symmetric with `LocalForgeDriver`'s refusal: neither driver silently
    /// runs work that belongs to the other.
    #[tokio::test]
    async fn refuses_a_local_spec() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = ScriptedWardenClient::new(vec![], 0);

        let driver = RemoteForgeDriver::new(scryer, yubaba.clone());
        let err = driver
            .execute(
                subprocess_spec(TaskPlacement::new(
                    TaskLocation::Local,
                    TaskRuntime::Container,
                )),
                ExecContext::default(),
                None,
            )
            .await
            .expect_err("a local spec must not dispatch remotely");

        assert!(
            matches!(err, ForgeExecutorError::Unsupported(_)),
            "got {err:?}"
        );
        assert!(
            !*yubaba.deploy_called.lock().unwrap(),
            "refusal must happen before any yubaba RPC"
        );
    }

    /// `[placement] platform` asks the *host* runtime for foreign-arch
    /// emulation. A yubaba node has no such knob, and honoring it silently
    /// would hand back a wrong-arch artifact — the exact R546 failure this seam
    /// exists to retire. Refuse, and name the replacement.
    #[tokio::test]
    async fn refuses_a_platform_request_and_names_mesh_tags() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = ScriptedWardenClient::new(vec![], 0);

        let driver = RemoteForgeDriver::new(scryer, yubaba.clone());
        let err = driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default().with_platform("linux/amd64".into()),
                None,
            )
            .await
            .expect_err("platform + remote must not silently drop the platform");

        let msg = err.to_string();
        assert!(msg.contains("mesh_tags"), "must name the fix, got: {msg}");
        assert!(
            !*yubaba.deploy_called.lock().unwrap(),
            "refusal must happen before any yubaba RPC"
        );
    }

    /// cwd and env are the two ExecContext fields with a real remote referent.
    /// Dropping them would be silent misbehaviour, so they're wired through.
    #[tokio::test]
    async fn cwd_and_env_land_on_the_workload_spec() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let (yubaba, deployed) = CapturingWardenClient::new(vec![], 0);

        let driver = RemoteForgeDriver::new(scryer, yubaba);
        driver
            .execute(
                subprocess_spec(remote_any_infra()),
                ExecContext::default()
                    .with_cwd(PathBuf::from("/build"))
                    .with_env(vec![("RUSTY_V8_MIRROR".into(), "https://example".into())]),
                None,
            )
            .await
            .unwrap();

        let specs = deployed.lock().unwrap();
        let ws = specs.first().expect("one deploy");
        assert_eq!(ws.workdir.as_deref(), Some(Path::new("/build")));
        let env = ws
            .env
            .iter()
            .find(|e| e.name == "RUSTY_V8_MIRROR")
            .expect("ctx env must reach the workload spec");
        assert_eq!(
            env.value,
            EnvValue::Literal {
                value: "https://example".into()
            }
        );
    }

    /// `start_with_sink` is unchanged for its existing callers — it delegates
    /// to `start_with_context` with an empty context and must not start
    /// injecting a workdir or env.
    #[tokio::test]
    async fn start_with_sink_still_deploys_an_unmodified_spec() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let (yubaba, deployed) = CapturingWardenClient::new(vec![], 0);

        let driver = RemoteForgeDriver::new(scryer, yubaba);
        let handle = driver
            .start_with_sink(subprocess_spec(remote_any_infra()), None)
            .await
            .unwrap();
        let _ = handle.wait().await;

        let specs = deployed.lock().unwrap();
        let ws = specs.first().expect("one deploy");
        assert!(ws.workdir.is_none(), "no ctx ⇒ no workdir override");
        assert!(
            !ws.env.iter().any(|e| e.name == "RUSTY_V8_MIRROR"),
            "no ctx ⇒ no injected env"
        );
    }
}
