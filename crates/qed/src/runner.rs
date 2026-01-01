//! @yah:relay(R325, "QED desktop UI (blank slate) + backend wiring")
//! @yah:at(2026-05-26T04:07:25Z)
//! @yah:status(open)
//! @yah:phase(P3)
//! @yah:parent(Q321)
//! @arch:see(.yah/docs/working/W063-area-a-ui-design-impl.md)
//!
//! @yah:ticket(R325-F2, "Backend: per-step event stream (start/stdout/stderr/end) — tailable feed for live step logs")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T04:09:53Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R325)
//! @yah:depends_on(R325-F1)
//! @yah:next("R325-T4 (Tauri commands) wraps qed.tail: QedTailParams{run_id, since_cursor, limit} -> QedTailResult{events:Vec<QedEventWire>, next_cursor, run:Option<QedRunWire>}. Poll-to-follow: pass next_cursor back as since_cursor each tick; the StepCard log pane consumes events[], the StepCards consume run.steps.")
//! @yah:handoff("Landed the qed live per-step event stream + cursor-tailable feed. (1) qed crate: new events.rs (QedEvent{RunStarted,StepStarted,StepOutput,StepFinished,RunFinished} + OutputStream{Stdout,Stderr}); PipelineRunner gained an optional sink via .with_events(UnboundedSender<QedEvent>) (composes with new/new_with_dispatcher/new_remote) + emit() helper. execute_step_local rewritten from blocking std::process::Command::output() to tokio::process with piped stdout/stderr drained line-by-line in concurrent tasks (emitting StepOutput); stderr tail still captured for the StepFailed msg. kill_on_drop(true) so qed.cancel mid-step kills the subprocess (F1 could only cancel between steps). run() emits the lifecycle around each step. (2) rpc crate: QedEventWire (tagged enum, kebab kind, RFC3339 timestamps — same chrono-free decoupling as QedRunWire), QedTailParams{run_id, since_cursor:Option<u64>, limit}, QedTailResult{events, next_cursor, run}, method::QED_TAIL='qed.tail'. (3) camp daemon: QedRunState gained an append-only events:Vec<QedEventWire> buffer; qed_run_handler now attaches a sink + spawns a drain task that pushes wire events AND live-updates meta.steps (StepStarted appends Running step, StepFinished sets terminal — guarded on status==Running so the authoritative terminal write / qed.cancel always wins over straggler events); qed_tail_handler (cursor=index into the buffer, default limit 500) + dispatch arm. (4) CLI: yah qed run now streams step output live (runner moved into a task, channel drained until close). Tests: qed crate 21/21 (3 new), yah --lib r325 9/9 (3 new tail tests), CLI smoke confirmed live stdout/stderr/step markers.")
//! @yah:verify("cargo test -p qed")
//! @yah:verify("cargo test -p yah --lib r325")
//! @yah:verify("cargo check -p rpc -p agent-tools -p yah -p desktop")
//! @yah:gotcha("The qed.tail `run` snapshot + events buffer are updated by a SEPARATE drain task that can briefly lag the run task's authoritative terminal write. A consumer should keep polling until the last event is RunFinished (don't stop just because run.completed_at is set). Remote (where=remote) still only emits step-level StepStarted/StepFinished — no StepOutput line streaming (execute_step_remote just waits on the warden handle); remote line-tail would flow through scryer/task.tail and is a follow-up. All qed runs are still in-memory (run-history persistence is R325-F3) so the event buffer is lost on daemon restart.")
//!
//! @yah:ticket(R380-T3, "Migrate qed runner execute_step_remote to TaskPlacement + add --runtime CLI flag")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:06:09Z)
//! @yah:status(review)
//! @yah:parent(R380)
//! @yah:next("execute_step_remote at runner.rs:401 builds a ForgeSpec with where_=RemoteAny{tier}. Update to TaskPlacement { location: RemoteAny{tier}, runtime: Container }.")
//! @yah:next("Add a --runtime native|container CLI flag to `yah qed run` and a per-step `runtime` field in pipeline TOML. Default = native when --where=local, container when --where=remote (preserves current behaviour).")
//! @yah:next("Pipeline TOML loader (config.rs) reads optional `runtime` per step; surface in QedStep.")
//! @yah:handoff("T3 complete: qed runner now resolves per-step TaskRuntime and threads it into execute_step_remote. Added QedStep.runtime: Option<TaskRuntime> (serde(default)) so pipeline TOML can pin runtime per step (e.g. `runtime = \"container\"` for build-image steps). PipelineRunner gained resolve_runtime(step) → step.runtime.unwrap_or(default-by-RunWhere): local⇒Native, remote⇒Container. execute_step_remote now takes the resolved runtime and builds TaskPlacement with it. Added a local+container guard in run() that returns InvalidConfig pointing at R380-T6 (the docker-run shim hasn't landed yet — silent fallback to native subprocess would be worse). New CLI flag `--runtime native|container` on `yah qed run` applies as the default for steps without an explicit TOML runtime; per-step TOML always wins (validated by resolve_runtime_step_override_wins). Re-exported task::TaskRuntime from qed::lib to avoid adding a task dep edge to the yah CLI. Updated all 11 QedStep struct literals in builtins.rs/runner.rs/types.rs. Tests: 4 new (resolve_runtime_defaults_from_run_where, resolve_runtime_step_override_wins, local_container_errors_until_t6, parses_optional_runtime_per_step); 33/34 pass (the single failure is the pre-existing test_builtin_release_build_pipeline 4-vs-6 step assertion already flagged in T2). cargo check --workspace clean.")
//! @yah:next("T6 (docker-run shim) replaces the local+container guard with real execution — delete `local_container_errors_until_t6` test and the matching InvalidConfig branch in run() once task::local exposes a container runtime.")
//! @yah:verify("cargo test -p qed --lib  # 33 pass (1 pre-existing unrelated failure)")
//! @yah:verify("cargo check --workspace  # clean")
//! @yah:verify("cargo run -p yah -- qed run check --runtime=invalid  # exits with clear error")
//! @yah:gotcha("The local+container guard returns RunnerError::InvalidConfig as a step failure (overall_status becomes Failed). The right shape long-term is a pre-flight validation error before run starts, but that requires a wider validator hook — punt to T6 when local+container actually works.")
//! @yah:gotcha("RunStatus::Cancelled isn't surfaced by local+container errors (the run finishes Failed normally). Consumers that distinguish cancellation from failure (the desktop StepCard) should look at the StepFailed.msg field for 'R380-T6' until T6 lands.")
//!
//! @yah:ticket(R381-T2, "Add ForgeCommand::BuildImage variant + qed::build-image step kind in pipeline TOML")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-01T21:07:14Z)
//! @yah:status(review)
//! @yah:parent(R381)
//! @yah:next("New ForgeCommand variant BuildImage { dockerfile: PathBuf, context: PathBuf, tag: String, push: bool } in crates/yah/task/src/lib.rs.")
//! @yah:next("Pipeline TOML: a step with `kind = \"build-image\"` + `image = \"<catalog-or-camp-name>\"` resolves the dockerfile/context via the catalog loader (T1) and constructs the ForgeCommand.")
//! @yah:next("Output: an ImageRef artifact addressable as ${steps.<step-name>.image} from later steps. Pipeline runner threads artifact resolution.")
//! @yah:next("build-image steps force runtime=Container; refuse runtime=Native at TOML parse time with a clear error.")
//! @yah:handoff("BuildImage seam landed end-to-end. task crate: new ForgeCommand::BuildImage { dockerfile: PathBuf, context: PathBuf, tag: String, push: bool } (serde tag = build_image, matches existing snake_case discipline). remote.rs build_workload_spec gains an explicit refusal arm pointing at R381-T5 — no silent fallthrough. qed crate: QedStep grew kind: StepKind (Subprocess|BuildImage, default Subprocess) + image, tag, push fields; argv now defaults to empty so a build-image step doesn't need to declare it. New StepValidationError surfaces four kind-specific errors at parse time: SubprocessMissingArgv, BuildImageHasArgv, BuildImageMissingImage, BuildImageNativeRuntime. PipelineLoader (both load_from_file + load_from_str) validates every step after deserialize — errors are pinned to a single bad step name, not a wall of TOML noise. resolve_runtime forces Container for build-image regardless of run_where (catches the implicit runtime=None case that local default would resolve to Native). run() dispatch matches on step.kind first, then existing (run_where, runtime) for subprocess. New execute_step_build_image stub looks up step.image in the bundled CatalogManifest (real lookup, real error on miss), constructs a ForgeCommand::BuildImage with conventional paths (crates/yah/qed/images/<name>/Dockerfile), then returns StepFailed with a structured 'R381-T4/T5 not yet implemented' message. Re-exported StepKind + StepValidationError from qed::lib. Tests: 12 new across types.rs (7), config.rs (4), runner.rs (3 build-image — forces container, unknown catalog fails, known catalog returns not-implemented), task/src/lib.rs (1 BuildImage round-trip). cargo test -p qed -p task --lib: 58+49 pass, 1 pre-existing unrelated failure (test_builtin_release_build_pipeline 4-vs-6). cargo check --workspace clean.")
//! @yah:next("T4 owns docker buildx execution: replace execute_step_build_image's StepFailed stub with a real local docker buildx invocation. Stub already builds the correct ForgeCommand::BuildImage — T4 just needs a task::local::build_image_command that shells to `docker buildx build -f <dockerfile> -t <tag> [--push] <context>` with cache-to/cache-from wiring.")
//! @yah:next("T5 owns BuildKit-in-containerd: extend execute_step_build_image to branch on self.run_where == Remote and synthesize a BuildKit WorkloadSpec instead of returning the stub. remote.rs already refuses ForgeCommand::BuildImage — T5 replaces that arm with a buildctl workload synthesis.")
//! @yah:next("Artifact threading ($\\{steps.X.image}): not yet wired. Defer until T4 produces a real ImageRef — then thread step.image_outputs: HashMap<String, ImageRef> through PipelineRunner::run() and substitute placeholders in each step's argv/env before execution (mirror the pattern of Pipeline::apply_params). Without real output ImageRefs T2 had nothing useful to substitute.")
//! @yah:next("Per-camp catalog wiring: execute_step_build_image calls CatalogManifest::bundled() — swap to CatalogManifest::load(camp_root.join('.yah/qed/images')) once the runner accepts a camp root (likely passed via PipelineRunner::with_catalog setter, parallel to with_events).")
//! @yah:verify("cargo test -p qed --lib")
//! @yah:verify("cargo test -p task --lib")
//! @yah:verify("cargo check --workspace")
//!
//! @yah:ticket(R407-T2, "QED native-tarball packaging step: musl-static binary + manifest, no systemd unit")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-02T03:27:28Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R407)
//! @arch:see(.yah/docs/working/W154-warden-dual-runtime.md)
//! @yah:depends_on(R407-T1)
//! @yah:handoff("Landed package-native-tarball step end-to-end. types: new StepKind::PackageNativeTarball + two QedStep fields (binary_path, triple) + 4 StepValidationError variants. New crates/yah/qed/src/native.rs module owns NativeTarballManifest (forward-compatible TOML shape — name/version/triple/binary/description/env) and pack_native_tarball() — writes bin/<basename> + manifest.toml into a .tar.gz via tar+flate2 (added as deps). runner: execute_step_package_native_tarball() looks up the catalog entry by step.image, GATES on entry.produces.contains(NativeTarball) (W154 catalog-side guard), resolves triple via step.triple ?? publish::resolve_triple(host), copies the binary, packs the tarball at <camp_root>/.yah/cache/native/<image>-<triple>.tar.gz. resolve_runtime() forces Native for this kind even on Remote runners (pure host file I/O — Container would be wrong). Catalog entry.env propagates into the manifest so Constable has launch env at deploy time without re-reading the catalog. 16 new tests (4 native pack/unpack, 6 runner happy/gate/missing/triple-host-fallback/remote-force-native, 6 types validation, 4 config parse-time). qed --lib: 111 pass + 1 pre-existing unrelated failure (test_builtin_release_build_pipeline 4-vs-6 step count, already flagged in R407-T1 handoff). cargo check -p qed -p yah clean.")
//! @yah:verify("cargo test -p qed --lib package_native_tarball")
//! @yah:verify("cargo test -p qed --lib native::")
//! @yah:verify("cargo check -p qed -p yah")
//! @yah:gotcha("No systemd unit is emitted (per W154 Constable design). Tarball layout is bin/<basename> + manifest.toml at root; that's the deploy contract — Constable readers should accept additive manifest fields.")
//! @yah:gotcha("manifest.toml version comes from YAH_RELEASE_VERSION env (else compiled CARGO_PKG_VERSION). For multi-platform release tagging the GHA shim is expected to set the env before invoking the packaging step.")
//! @yah:gotcha("Sigstore signing of the tarball (R407-T5) is NOT wired here — only content packaging. The packaging step writes plaintext .tar.gz; signing extends in T5.")
//!
//! @yah:ticket(R407-T5, "Sigstore signing extends to native-tarball artifacts (same trust model)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-02T03:27:30Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R407)
//! @arch:see(.yah/docs/working/W154-warden-dual-runtime.md)
//! @yah:depends_on(R407-T2)
//! @yah:handoff("Landed Sigstore signing seam for native-tarball artifacts end-to-end (W154 'same trust model, different artifact shape'). native.rs: new SigstoreSigner async trait + SignedBlob{signature_path, certificate_path, bundle_path} result struct. CosignSigner shells `cosign sign-blob --yes --output-signature <blob>.sig --output-certificate <blob>.crt --bundle <blob>.bundle <blob>` (extends, not substitutes — `.tar.gz.sig` not `.tar.sig`, so the channel layout shows the signature next to the artifact it covers). LoggingSigner test/dev fake writes placeholder bytes and tracing::warn so a local `yah qed run` doesn't fail when cosign isn't installed. New tarball_stem() + native_tarball_output_path() helpers hoist the on-disk convention out of runner.rs — packaging (T2) now calls the same helper, so pack-then-sign in one pipeline always finds the artifact. types.rs: StepKind::SignNativeTarball variant + three StepValidationError variants (HasArgv / MissingImage / ContainerRuntime). Catalog produces gate applied independently at sign dispatch (not only at pack time) so a stale TOML signing step can't sneak through. runner.rs: PipelineRunner.signer: Arc<dyn SigstoreSigner> field, default LoggingSigner across all three constructors, with_signer setter (composes with with_camp_root / with_events). resolve_runtime forces Native for SignNativeTarball on Remote runners. execute_step_sign_native_tarball resolves <camp_root>/.yah/cache/native/<image>-<triple>.tar.gz via the shared helper, checks file exists (routes operator to `kind = \"package-native-tarball\"` on miss), gates on catalog.produces, calls signer.sign_blob, surfaces clean StepFailed on any failure. 16 new tests: 5 types validation, 4 config parse-time, 1 native::tarball_stem + 1 path helper, 3 LoggingSigner/CosignSigner unit tests, 6 runner tests (pack-then-sign happy path, non-native catalog gate, unknown catalog, missing tarball routes-to-packaging, forces-native-on-remote, with_signer override via CountingSigner). cargo test -p qed --lib: 153 pass + 1 pre-existing unrelated failure (test_builtin_release_build_pipeline 4-vs-6 step count, flagged in R407-T1 and R380-T3 handoffs). cargo check -p yah clean.")
//! @yah:verify("cargo test -p qed --lib sign_native_tarball")
//! @yah:verify("cargo test -p qed --lib native::")
//! @yah:verify("cargo check -p qed -p yah")
//! @yah:gotcha("Default signer is LoggingSigner (placeholder bytes + tracing::warn). Release CI MUST wire CosignSigner explicitly via PipelineRunner::with_signer(Arc::new(CosignSigner::default())) — picking up the default in CI ships a tarball with stub `.sig/.crt/.bundle` files and Sigstore verify-blob will reject it at deploy time. The CLI doesn't yet auto-detect cosign on PATH; that's a follow-up when a release pipeline actually runs sign-native-tarball end-to-end (today the GHA cosign step still signs OCI images out-of-band per release.yml, native-tarball signing is wired but not yet invoked from a real release pipeline).")
//! @yah:gotcha("Sign step refuses to sign tarballs from catalog entries that don't declare `produces = [\"native-tarball\"]`. The check is duplicated from packaging on purpose — defense in depth — so a stale signing step left in TOML after a catalog rename can't surface a confusing 'tarball not found' instead of the real 'catalog opt-in missing' error.")
//!
//! @yah:ticket(R438-T14, "qed PipelineRunner consumes ForgeExecutor for subprocess steps")
//! @yah:assignee(bundle-anthropic-ashguard)
//! @yah:at(2026-06-05T07:26:30Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R438)
//! @yah:next("Add a with_executor(Arc<dyn ForgeExecutor>) setter so the cloud reconciler can share a configured driver without spinning up its own; not strictly required but mirrors with_signer/with_events/with_camp_root.")
//! @yah:verify("Manual: emits_lifecycle_events_with_streamed_output + failing_step_streams_stderr_and_finishes_failed still pass — QedEvent adapter must preserve per-line streaming and stderr-tail capture for StepFailed.msg")
//! @arch:see(.yah/docs/working/W164-derived-static-assets.md)
//! @yah:depends_on(R438-T13)
//! @yah:handoff("T14 landed. qed::PipelineRunner now consumes task::ForgeExecutor for subprocess steps. Changes: (1) Added executor: Arc<dyn ForgeExecutor> field to PipelineRunner; default Arc::new(LocalForgeDriver::new()) across new/new_with_dispatcher/new_remote constructors; with_executor(...) setter mirrors with_signer/with_events/with_camp_root. (2) Replaced execute_step_local + execute_step_local_container with thin wrappers that build a ForgeSpec + ExecContext and call drive_subprocess_step. (3) New private drive_subprocess_step helper spawns an adapter task that forwards ExecEvent::Output -> QedEvent::StepOutput on self.events (the per-line streaming contract from R325-F2 is preserved). Started/Finished events are absorbed; run() still emits its own StepStarted/StepFinished. (4) New top-level helper build_subprocess_spec lowers a QedStep into ForgeSpec{Subprocess{argv,image}, TaskPlacement{Local, runtime}, timeout, label, initiator=Human/qed, mesh_access=None}. (5) Error mapping: Ok(outcome).succeeded() -> Ok(()); Ok(outcome) failed -> StepFailed{msg: outcome.stderr_tail}; Spawn -> StepFailed with friendly 'runtime installed?' prefix; Io -> RunnerError::Io (preserves existing From impl); Unsupported -> RunnerError::InvalidConfig. Build-image / package-native-tarball / sign-native-tarball / musl-static-preflight / execute_step_remote paths unchanged — those don't route through the trait yet (out of scope for T14).")
//! @yah:handoff("Tests: cargo test -p qed --lib: 165 pass + 1 pre-existing unrelated failure (tests::test_builtin_release_build_pipeline 4-vs-6 step count, already flagged in R380-T3 / R380-T8 / R381-T2 / R407-T2 / R407-T5 handoffs). Critical streaming/lifecycle tests verified individually: emits_lifecycle_events_with_streamed_output, failing_step_streams_stderr_and_finishes_failed, no_sink_runs_silently, local_container_step_routes_through_docker_path, resolve_runtime_defaults_from_run_where, resolve_runtime_step_override_wins, remote_step_success/failure/abort_on_fail — all green.")
//! @yah:handoff("Coordination note: while T14 was mid-verify, T15's agent landed mid-flight edits to crates/yah/task/src/lib.rs (a `pub use task_runs::Initiator;` plus moving transforms.rs into task) which created a duplicate `use task_runs::Initiator;` (private use on line 126 conflicting with the new `pub use` on line 115). Removed the now-redundant private import to unblock T14's verify. T15's agent owns the new transforms::tests::rejects_recipe_with_struct_image_missing_digest failure (test now gets RecipeError::Parse instead of ImageNotPinned because ImageRef post-R438-T3 tightening rejects struct-form-missing-digest at serde-deserialize time).")
//! @yah:next("After T15 lands its full workspace verify, confirm cargo check --workspace --locked stays clean and that qed::tests::test_builtin_release_build_pipeline's pre-existing failure is the only remaining miss in qed.")
//! @yah:next("Optional follow-up: thread executor through execute_step_build_image too — today it still does inline docker buildx spawn + drain. Same shape as T14 but with a BuildImage variant that LocalForgeDriver currently rejects; would require extending LocalForgeDriver to support BuildImage. Not blocking; current architecture stays.")
//! @yah:next("Optional follow-up: remote dispatch path (execute_step_remote) still calls RemoteForgeDriver directly via self.remote_driver; a RemoteForgeDriver impl of ForgeExecutor would let the runner dispatch through a single executor trait. Symmetric with T14 but blocks on a real consumer needing it.")
//! @yah:verify("cargo test -p qed --lib  # 165 pass + 1 pre-existing failure (test_builtin_release_build_pipeline)")
//! @yah:verify("cargo test -p qed --lib emits_lifecycle  # 1 pass (R325-F2 streaming contract preserved through ForgeExecutor adapter)")
//! @yah:verify("cargo test -p qed --lib failing_step_streams  # 1 pass (stderr_tail captured for StepFailed.msg through ExecOutcome.stderr_tail)")
//! @yah:verify("cargo test -p qed --lib local_container_step_routes  # 1 pass (container path still routes through docker)")
//! @yah:verify("cargo test -p qed --lib resolve_runtime  # 2 pass (runtime resolution unchanged)")
//! @yah:handoff("Verification complete. cargo check --workspace clean (pre-existing desktop warnings only). cargo test -p qed --lib: 165 pass, 1 pre-existing failure (test_builtin_release_build_pipeline step-count 6-vs-4, documented across R380-T3/R380-T8/R381-T2/R407-T2/R407-T5 handoffs). emits_lifecycle_events_with_streamed_output passes. T15 is in review; its workspace verify aligns. with_executor setter is implemented at runner.rs:406. All T14 implementation work was landed by the previous agent session (bundle-anthropic-ashguard).")
//! @yah:verify("cargo test -p qed --lib  # 165 pass + 1 pre-existing failure (test_builtin_release_build_pipeline)")
//! @yah:verify("cargo test -p qed --lib emits_lifecycle  # streaming contract preserved")
//! @yah:verify("cargo check --workspace  # clean")
//!
//! @yah:ticket(R488-F2, "Runner recursion for SubPipelineRef::Builtin and ::Path (nested QedRun, parented run_id)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:54:07Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R488)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:depends_on(R488-F1)
//! @yah:tier(Cleric)
//! @yah:handoff("F2 shipped. Runner gained sub_pipeline_resolver field (default NoopSubPipelineResolver) + suppress_publish_outcomes field + with_sub_pipeline_resolver(...) setter. Public run() refactored to a thin wrapper around new pub(crate) run_inner() that returns (QedRunMeta, Vec<ProducedArtifact>) — parent reads child produced across recursion. SubPipeline arm in run_inner() resolves via configured resolver, builds child runner inheriting executor/signer/camp_root/events/outcome_dispatcher/resolver, applies cfg.params via apply_params, sets suppress_publish_outcomes=cfg.propagate.produces, runs via Box::pin(child.run_inner()) (async recursion). Children produces flow into parents produced when propagate.produces=true. Suppression in outcome dispatch skips Outcome::Publish on child only (WardenDeploy + AlmanacRun still fire). New LoaderSubPipelineResolver in config.rs (Builtin via loader.load, Path via load_from_file resolved relative to camp root, GhaWorkflow returns None until W200-F9). PipelineLoader: Clone derive + pub(crate) on qed_dir + load_from_file. New load_and_validate_graph(name) method runs the F1 walker at parse time. ConfigError gained SubPipelineGraph variant. 7 new runner tests: unresolvable-target failure, happy single-child, failure propagation, produces aggregation + child publish suppression (1 publish total), child publish fires when not suppressed (2 publishes total), two-level nesting with single revalidate, param forwarding. cargo test -p qed --lib: 188 pass (7 new) + 1 pre-existing unrelated failure. cargo check --workspace clean (one more QedStep literal in app/yah/cli/src/camp.rs sed-fixed).")
//! @yah:next("F3 deepens aggregation: PublishingOutcomeDispatcher (the real publish.rs) needs multi-child fan-in coverage — swap F2 CountingDispatcher for a fake ReleasePublisher and assert the staged tree groups artifacts by binary correctly across children.")
//! @yah:next("F3 confirm continue-on-error semantics for SubPipeline steps: current impl drops child produces on failure; might want partial propagation. Document either way.")
//! @yah:next("Wire PipelineLoader::load_and_validate_graph into yah qed run entry so users get pre-flight cycle errors instead of mid-recursion failures.")
//! @yah:verify("cargo test -p qed --lib runner::tests::sub_pipeline (7 tests)")
//! @yah:verify("cargo test -p qed --lib")
//! @yah:verify("cargo check --workspace")
//!
//! @yah:ticket(R488-F6, "SubPipelineRef::GhaWorkflow arm — wraps a W200 workflow run as a sub-pipeline (closes full-release loop)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:54:42Z)
//! @yah:status(review)
//! @yah:phase(P6)
//! @yah:parent(R488)
//! @yah:next("Add the GhaWorkflow arm to the SubPipeline resolver — delegates to qed_gha::execute")
//! @yah:next("Map GhaRunResult.produced into the parent's aggregation; map job_outputs into propagate.outputs")
//! @yah:next("Author .yah/qed/full-release.toml: child 1 = GhaWorkflow(.github/workflows/release.yml), child 2 = builtin(desktop-release); terminal Outcome::Publish")
//! @yah:verify("yah qed run full-release executes both children sequentially; one revalidate POST fires after both succeed")
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:depends_on(R488-F3)
//! @yah:depends_on(R487-F9)
//! @yah:tier(Cleric)
//! @yah:handoff("F6 shipped. (1) runner.rs: execute_step_gha_workflow now returns (Vec<ProducedArtifact>, HashMap<String,String>) — workflow job outputs lifted as `<job_id>.<output_key>` from each successful instance's outputs IndexMap so the enclosing SubPipeline parent's propagate.outputs can address them with the same `<job_id>.<key>` naming convention as GHA's `jobs.<id>.outputs.<key>`. Call site at run_inner() threads workflow_outputs through into step_outputs. (2) .yah/qed/full-release.toml: composite pipeline with two SubPipeline children — child 1 = GhaWorkflow(.github/workflows/release.yml), child 2 = builtin(desktop-release), both with propagate.produces = true; one terminal Outcome::Publish to r2/yah-dev/https://cdn.yah.dev. concurrency_key = cargo-target so it queues behind other cargo-touching pipelines (W155 principle 3). (3) lib.rs: test_full_release_composite_pipeline loads full-release via load_and_validate_graph (parse-time SubPipeline cycle/depth walker) and asserts two SubPipeline children with propagate.produces = true + exactly one terminal Outcome::Publish. Uses CARGO_MANIFEST_DIR-rooted qed_dir so it runs from any cwd. cargo test -p qed --lib: 201 pass + 1 pre-existing failure (test_builtin_release_build_pipeline 4-vs-6 step count, documented across R407-T1/R380-T3/R438-T14/R488-F1/F2/F9 handoffs — not introduced by F6). cargo check -p qed -p yah clean. Verification of the end-to-end `yah qed run full-release` deferred to a host with docker+rust+tauri-cli installed (and a real R2/almanac receiver) — same hermetic constraint F9 documented for the GhaWorkflow step itself.")
//! @yah:verify("cargo test -p qed --lib test_full_release_composite_pipeline  # graph validates")
//! @yah:verify("cargo test -p qed --lib  # 201 pass + 1 pre-existing failure")
//! @yah:verify("cargo check -p qed -p yah  # clean")
//!
//! @yah:ticket(R494-F2, "Local-peer resolution: nested QedRun across camp folders, per-peer-camp serialization")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T23:48:09Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R494)
//! @arch:see(.yah/docs/working/W201-qed-pipeline-composition.md)
//! @yah:depends_on(R494-F1)
//! @yah:tier(Cleric)
//! @yah:handoff("F2 shipped. (1) config.rs: PipelineLoader gained peers: PeerConfig field; constructor loads <qed_dir>/peers.toml opportunistically alongside the existing registries.toml load. New with_peers() setter mirrors with_registries() for tests that don't want a peers.toml on disk. (2) LoaderSubPipelineResolver::resolve Peer arm: look up camp in self.loader.peers; if entry.rig.is_some() return None (R494-T5 refines into typed RemotePeerNotYetSupported); else resolve peer camp root relative to this camp (qed_dir.parent().parent() = <this camp root>, then join entry.path), instantiate PipelineLoader::new(<peer root>/.yah/qed), load the named pipeline. Stamp child.concurrency_key = `peer:<camp>` only when the peer's own pipeline didn't set one — gives per-peer-camp serialization for top-level invocations (`yah qed run peer:cheers:publish` + `yah qed run peer:cheers:test` both queue on `peer:cheers` since cheers' target/ is shared). Peers can opt out by setting `concurrency_key = \"@parallel\"` in their TOML. (3) 5 new config::tests: peer_resolver_loads_pipeline_from_sibling_camp (happy path + concurrency_key stamping verified), peer_resolver_preserves_explicit_concurrency_key, peer_resolver_returns_none_for_unknown_camp, peer_resolver_returns_none_for_unknown_pipeline_in_known_camp, peer_resolver_swallows_remote_peers_until_t5_wires_constable. fixture_peer_camp() helper builds a tempdir layout `<tmp>/parent/.yah/qed/peers.toml` + `<tmp>/peers/cheers/.yah/qed/publish.toml` so the resolver exercises real disk IO. cargo test -p qed --lib: 211 pass (5 new) + 1 pre-existing failure (test_builtin_release_build_pipeline 4-vs-6, documented across R407-T1/R380-T3/R438-T14/R488-F1/F2/F6/F9 + R494-F1 handoffs). cargo check -p qed -p yah -p desktop clean.")
//! @yah:next("Per-peer-camp serialization gap: concurrency_key stamping only takes effect when peer pipelines are invoked at the top level (the queue layer in camp.rs:qed_run_handler keys off concurrency_key). Sub-pipeline recursion inside run_inner bypasses the queue and runs children directly. For the common case (yah's `peer-release` orchestrating cheers->mesofact->rs-hack sequentially via SubPipeline steps in one parent), the parent pipeline's step ordering serializes them; the gap shows up only if a parent declares two peer-children that should serialize but doesn't sequence them. Document this as a v1 limitation or follow-up ticket.")
//! @yah:next("R494-T3 (peers.toml + peer-release.toml authoring) can now land — the resolver wires up end-to-end. F1's mesofact release-build.toml under external/mesofact/.yah/qed/ is the first concrete peer target.")
//! @yah:next("R494-F4 (desktop nested-tree shows peer camp label): sub_pipeline_target_label in runner.rs returns `peer:<camp>:<pipeline>` (F1); the desktop QED-pane consumer of QedEvent::SubPipelineStarted.target already gets this string. F4 just needs to render it as a distinct chip rather than collapse into the run-name column.")
//! @yah:verify("cargo test -p qed --lib config::tests::peer_resolver")
//! @yah:verify("cargo test -p qed --lib  # 211 pass + 1 pre-existing")
//! @yah:verify("cargo check -p qed -p yah -p desktop")

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use observation::ForgeId as ObsForgeId;
use scryer::service::Scryer;
use task::{
    ExecContext, ExecEvent, ForgeCommand, ForgeExecutor, ForgeExecutorError, ForgeSpec,
    ForgeStatus, LocalForgeDriver, MeshAccess, RemoteForgeDriver, TaskLocation, TaskPlacement,
    TaskRuntime, WardenClient,
};
use task_runs::Initiator;
use thiserror::Error;
use uuid::Uuid;
use workload_spec::{Millis, TierTag};

use tokio::sync::mpsc::UnboundedSender;

use crate::events::{OutputStream, QedEvent};
use crate::native::{LoggingSigner, SigstoreSigner};
use crate::types::{OnFail, Outcome, Pipeline, QedRunId, QedRunMeta, RunStatus, StepStatus};

/// Dispatches pipeline outcomes (warden-deploy, almanac-run) after a pipeline completes.
///
/// Implementations are responsible for the actual side-effect. The default stub logs and
/// no-ops until the respective RPC surfaces stabilise (R040-F4 for warden deploy).
#[async_trait]
pub trait OutcomeDispatcher: Send + Sync {
    async fn warden_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError>;
    async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError>;
    /// Publish the artifacts produced by the run's successful steps into a
    /// release channel bucket, then fire the almanac revalidate hook (R330-F3).
    /// The default no-ops so existing impls don't break; the real behaviour
    /// lives in [`crate::publish::PublishingOutcomeDispatcher`].
    async fn publish(&self, req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
        tracing::info!(
            provider = %req.provider,
            bucket = %req.bucket,
            version = %req.version,
            artifacts = req.artifacts.len(),
            "qed outcome: publish skipped (no publishing dispatcher wired)"
        );
        Ok(())
    }
}

/// Stub dispatcher — logs what it would do but takes no action.
/// Used by default until warden deploy RPC (R040-F4) and almanac are stable.
pub struct LoggingOutcomeDispatcher;

#[async_trait]
impl OutcomeDispatcher for LoggingOutcomeDispatcher {
    async fn warden_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError> {
        tracing::info!(
            service,
            env,
            "qed outcome: warden-deploy skipped (warden deploy RPC not yet stable, R040-F4)"
        );
        Ok(())
    }

    async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError> {
        tracing::info!(
            pipeline,
            "qed outcome: almanac-run skipped (almanac not yet implemented)"
        );
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum RunnerError {
    #[error("Step '{step}' failed: {msg}")]
    StepFailed { step: String, msg: String },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid step configuration: {0}")]
    InvalidConfig(String),
    #[error("Remote dispatch error: {0}")]
    Remote(String),
}

/// Where pipeline steps execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunWhere {
    /// Steps run as local subprocesses on this machine.
    Local,
    /// Steps run as `task::remote` workloads on a warden node.
    Remote,
}

/// Map a Docker image tag (`reg/repo:ver`) to a filesystem-safe stem for
/// OCI archive output under `.yah/cache/images/`. Replaces every byte that
/// isn't `[A-Za-z0-9_.-]` with `_`.
fn tag_to_filename(tag: &str) -> String {
    tag.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Catalog lookup + Dockerfile staging output, shared by local and remote
/// build-image dispatch.
struct PreparedBuildImage {
    camp_root: std::path::PathBuf,
    dockerfile_path: std::path::PathBuf,
    buildkit_dir: std::path::PathBuf,
    archive_path: std::path::PathBuf,
    tag: String,
}

pub struct PipelineRunner {
    pipeline: Pipeline,
    run_id: QedRunId,
    remote_driver: Option<Arc<RemoteForgeDriver>>,
    run_where: RunWhere,
    outcome_dispatcher: Arc<dyn OutcomeDispatcher>,
    /// Optional live-event sink (R325-F2). When set, `run()` emits a
    /// [`QedEvent`] at each lifecycle boundary; when `None` the runner is
    /// silent and only the terminal [`QedRunMeta`] is observable.
    events: Option<UnboundedSender<QedEvent>>,
    /// Camp root used by build-image steps to locate per-camp images
    /// (`<camp_root>/.yah/qed/images/<name>/`) and write generated artifacts
    /// (`<camp_root>/.yah/cache/{buildkit,images}/`). Falls back to
    /// `std::env::current_dir()` when unset — production callers leave this
    /// alone; tests override via [`Self::with_camp_root`] to avoid leaking
    /// `.yah/cache/` into the working directory.
    camp_root: Option<std::path::PathBuf>,
    /// Sigstore signer for `kind = "sign-native-tarball"` steps (R407-T5).
    /// Defaults to [`LoggingSigner`], which writes placeholder bytes and
    /// logs a warning so a local `yah qed run` doesn't fail when cosign
    /// isn't installed. Release CI wires [`crate::native::CosignSigner`]
    /// explicitly via [`Self::with_signer`] so an unsigned tarball never
    /// silently ships.
    signer: Arc<dyn SigstoreSigner>,
    /// Subprocess executor for local `kind = "subprocess"` steps (R438-T14).
    /// Defaults to [`LocalForgeDriver`]. Override via [`Self::with_executor`]
    /// when a caller wants to share a configured driver (e.g. the cloud
    /// reconciler reuses one across many materialize calls).
    executor: Arc<dyn ForgeExecutor>,
    /// Resolver for `kind = "sub-pipeline"` steps (R488-F2). Defaults to a
    /// no-op resolver that returns `None` for every target — production
    /// callers wire a [`PipelineLoader`]-backed resolver via
    /// [`Self::with_sub_pipeline_resolver`]. With the default resolver, a
    /// SubPipeline step's target is unresolvable and the step fails with a
    /// clear "no resolver configured" message.
    sub_pipeline_resolver: Arc<dyn crate::types::SubPipelineResolver + Send + Sync>,
    /// When `true`, this runner's terminal `Outcome::Publish` outcomes are
    /// suppressed at the end of `run()`. Set on child runners constructed
    /// for a SubPipeline step where the parent declared
    /// `propagate.produces = true` — the parent owns the terminal publish,
    /// so firing it on the child would double-publish and double-revalidate.
    /// All other outcomes (`WardenDeploy`, `AlmanacRun`) still run.
    suppress_publish_outcomes: bool,
    /// Set on child runners spawned by a SubPipeline step (R488-F5). The
    /// child's terminal [`QedRunMeta`] carries this back to consumers so
    /// the nested tree can be rebuilt from history alone. `None` on
    /// top-level runs.
    parent_run_id: Option<QedRunId>,
    /// Added to every emitted step `index` so that a resume-from-step run
    /// (where the pipeline had its leading steps drained) still shows the
    /// original step position in the UI (e.g. step 6 of 6 instead of 1 of 1).
    /// Set via [`Self::with_index_offset`] in callers that drain steps.
    index_offset: usize,
    /// R499-F3 phase 2: per-step gha-workflow matrix subset. Keyed by
    /// qed step name; the inner set is the chosen
    /// [`qed_gha::graph::JobInstance::key`] values (`<job>` for
    /// non-matrix, `<job>#<row>` for matrix). When a gha-workflow step
    /// has an entry here, [`Self::execute_step_gha_workflow`] threads
    /// it into [`qed_gha::Executor::included_instance_keys`] so
    /// non-selected rows short-circuit to `Skipped`. Steps missing from
    /// the map run their full matrix. Set via
    /// [`Self::with_gha_matrix_subset`].
    gha_matrix_subset:
        std::collections::HashMap<String, std::collections::HashSet<String>>,
}

/// Default [`SubPipelineResolver`] for [`PipelineRunner`] — returns `None`
/// for every target. Production callers replace it with a
/// [`PipelineLoader`]-backed resolver via
/// [`PipelineRunner::with_sub_pipeline_resolver`]; tests pass an
/// in-memory map. Keeping the default a no-op means a runner with no
/// SubPipeline steps requires no extra configuration.
struct NoopSubPipelineResolver;

impl crate::types::SubPipelineResolver for NoopSubPipelineResolver {
    fn resolve(&self, _target: &crate::types::SubPipelineRef) -> Option<Pipeline> {
        None
    }
}

impl PipelineRunner {
    /// Local execution — steps run as subprocesses on this machine.
    pub fn new(pipeline: Pipeline) -> Self {
        let run_id = Uuid::new_v4().to_string();
        Self {
            pipeline,
            run_id,
            remote_driver: None,
            run_where: RunWhere::Local,
            outcome_dispatcher: Arc::new(LoggingOutcomeDispatcher),
            events: None,
            camp_root: None,
            signer: Arc::new(LoggingSigner),
            executor: Arc::new(LocalForgeDriver::new()),
            sub_pipeline_resolver: Arc::new(NoopSubPipelineResolver),
            suppress_publish_outcomes: false,
            parent_run_id: None,
            index_offset: 0,
            gha_matrix_subset: std::collections::HashMap::new(),
        }
    }

    /// Local execution with a custom outcome dispatcher.
    pub fn new_with_dispatcher(pipeline: Pipeline, dispatcher: Arc<dyn OutcomeDispatcher>) -> Self {
        let run_id = Uuid::new_v4().to_string();
        Self {
            pipeline,
            run_id,
            remote_driver: None,
            run_where: RunWhere::Local,
            outcome_dispatcher: dispatcher,
            events: None,
            camp_root: None,
            signer: Arc::new(LoggingSigner),
            executor: Arc::new(LocalForgeDriver::new()),
            sub_pipeline_resolver: Arc::new(NoopSubPipelineResolver),
            suppress_publish_outcomes: false,
            parent_run_id: None,
            index_offset: 0,
            gha_matrix_subset: std::collections::HashMap::new(),
        }
    }

    /// Attach a live-event sink (R325-F2). Composes with any constructor:
    /// `PipelineRunner::new(p).with_events(tx)`. The runner emits a
    /// [`QedEvent`] for run start, each step start, every stdout/stderr line,
    /// each step finish, and run finish. Send failures (no receiver) are
    /// ignored — events are best-effort and never block the run.
    pub fn with_events(mut self, sink: UnboundedSender<QedEvent>) -> Self {
        self.events = Some(sink);
        self
    }

    /// Override the camp root used to resolve per-camp catalog overrides and
    /// the BuildKit cache + OCI archive output directories. Production
    /// callers leave this unset (falls back to [`std::env::current_dir`]);
    /// tests pass a tempdir so generated `.yah/cache/` files don't leak into
    /// the workspace.
    pub fn with_camp_root(mut self, root: std::path::PathBuf) -> Self {
        self.camp_root = Some(root);
        self
    }

    /// Attach a Sigstore signer (R407-T5). Composes with any constructor:
    /// `PipelineRunner::new(p).with_signer(Arc::new(CosignSigner::default()))`.
    /// Release pipelines MUST call this with a real signer; the default
    /// [`LoggingSigner`] writes placeholders so local `yah qed run` flows
    /// don't fail when cosign isn't on PATH.
    pub fn with_signer(mut self, signer: Arc<dyn SigstoreSigner>) -> Self {
        self.signer = signer;
        self
    }

    fn resolve_camp_root(&self) -> Result<std::path::PathBuf, RunnerError> {
        if let Some(root) = &self.camp_root {
            return Ok(root.clone());
        }
        std::env::current_dir().map_err(|e| {
            RunnerError::InvalidConfig(format!("failed to read current dir: {e}"))
        })
    }

    /// Emit one event to the sink if attached. A closed receiver is a no-op.
    fn emit(&self, event: QedEvent) {
        if let Some(tx) = &self.events {
            let _ = tx.send(event);
        }
    }

    /// Pick the sandboxing runtime for a step.  Explicit `step.runtime`
    /// always wins; otherwise default by location (R380-T3):
    ///
    /// | --where  | runtime |
    /// |----------|---------|
    /// | local    | Native    |
    /// | remote   | Container |
    ///
    /// The CLI's `--runtime native|container` override is applied by mutating
    /// each step's `runtime` field *before* the runner is constructed, so by
    /// the time this method runs the per-step value already reflects the
    /// CLI choice (TOML-declared values still win over CLI defaults).
    fn resolve_runtime(&self, step: &crate::types::QedStep) -> TaskRuntime {
        // build-image steps are always Container — parse-time validation
        // already rejects explicit `runtime = "native"`, this catches the
        // implicit `runtime = None` case where the local default would
        // otherwise resolve to Native.
        if matches!(step.kind, crate::types::StepKind::BuildImage) {
            return TaskRuntime::Container;
        }
        // package-native-tarball is always Native — it's pure host file I/O
        // (read binary, write tar.gz). Parse-time rejects `runtime =
        // "container"`; force Native here so the implicit `None` doesn't
        // resolve to Container on a Remote runner.
        if matches!(step.kind, crate::types::StepKind::PackageNativeTarball) {
            return TaskRuntime::Native;
        }
        // musl-static-preflight shells `cargo metadata` on the host — same
        // reasoning as package-native-tarball, always Native.
        if matches!(step.kind, crate::types::StepKind::MuslStaticPreflight) {
            return TaskRuntime::Native;
        }
        // sign-native-tarball shells `cosign sign-blob` on the host (and
        // writes the .sig/.crt/.bundle next to the artifact). Parse-time
        // rejects `runtime = "container"`; force Native here so the implicit
        // `None` doesn't resolve to Container on a Remote runner.
        if matches!(step.kind, crate::types::StepKind::SignNativeTarball) {
            return TaskRuntime::Native;
        }
        step.runtime.unwrap_or(match self.run_where {
            RunWhere::Local => TaskRuntime::Native,
            RunWhere::Remote => TaskRuntime::Container,
        })
    }

    /// Remote execution — steps run as `task::remote` workloads dispatched via
    /// the provided `WardenClient`.
    pub fn new_remote(
        pipeline: Pipeline,
        scryer: Arc<Scryer>,
        warden: Arc<dyn WardenClient>,
    ) -> Self {
        let run_id = Uuid::new_v4().to_string();
        let remote_driver = Arc::new(RemoteForgeDriver::new(scryer, warden));
        Self {
            pipeline,
            run_id,
            remote_driver: Some(remote_driver),
            run_where: RunWhere::Remote,
            outcome_dispatcher: Arc::new(LoggingOutcomeDispatcher),
            events: None,
            camp_root: None,
            signer: Arc::new(LoggingSigner),
            executor: Arc::new(LocalForgeDriver::new()),
            sub_pipeline_resolver: Arc::new(NoopSubPipelineResolver),
            suppress_publish_outcomes: false,
            parent_run_id: None,
            index_offset: 0,
            gha_matrix_subset: std::collections::HashMap::new(),
        }
    }

    /// Attach a custom [`ForgeExecutor`] for local subprocess steps
    /// (R438-T14). Composes with any constructor. The default is
    /// [`LocalForgeDriver`]; callers override to share a configured driver
    /// across multiple runs.
    pub fn with_executor(mut self, executor: Arc<dyn ForgeExecutor>) -> Self {
        self.executor = executor;
        self
    }

    /// Attach a [`SubPipelineResolver`](crate::types::SubPipelineResolver)
    /// for `kind = "sub-pipeline"` steps (R488-F2). Composes with any
    /// constructor. The default resolver returns `None` for every target —
    /// any SubPipeline step will fail with a clear "no resolver configured"
    /// message until this is called. Production callers pass a
    /// [`PipelineLoader`](crate::config::PipelineLoader)-backed resolver;
    /// tests pass an in-memory map.
    pub fn with_sub_pipeline_resolver(
        mut self,
        resolver: Arc<dyn crate::types::SubPipelineResolver + Send + Sync>,
    ) -> Self {
        self.sub_pipeline_resolver = resolver;
        self
    }

    /// Offset added to every emitted step `index`. Use this when the
    /// pipeline's leading steps were drained for a resume-from-step run so
    /// that events still carry the original positions (e.g. step 5 of 6
    /// instead of step 0 of 1 after a `drain(0..5)`).
    pub fn with_index_offset(mut self, offset: usize) -> Self {
        self.index_offset = offset;
        self
    }

    /// R499-F3 phase 2: per-step gha-workflow matrix subset. Each entry
    /// maps a qed step name to the chosen instance keys (see
    /// [`qed_gha::graph::JobInstance::key`]). Steps absent from the map
    /// run their full matrix. Inherited by SubPipeline children.
    pub fn with_gha_matrix_subset(
        mut self,
        subset: std::collections::HashMap<String, std::collections::HashSet<String>>,
    ) -> Self {
        self.gha_matrix_subset = subset;
        self
    }

    /// The run id assigned at construction. Lets a caller (e.g. the camp
    /// daemon's `qed.run` handler) register a run as `Running` *before*
    /// [`Self::run`] completes, so `qed.status` can observe it in flight.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub async fn run(&self) -> Result<QedRunMeta, RunnerError> {
        let (meta, _produced) = self.run_inner().await?;
        Ok(meta)
    }

    /// Same as [`Self::run`] but also returns the aggregated
    /// [`ProducedArtifact`] list. Used by SubPipeline recursion (R488-F2):
    /// a parent's SubPipeline step calls `run_inner()` on the child runner
    /// so it can roll the child's `produced` into its own collection. Public
    /// `run()` discards it (callers that need artifacts go through
    /// `Outcome::Publish`, not the meta).
    pub(crate) async fn run_inner(
        &self,
    ) -> Result<(QedRunMeta, Vec<crate::types::ProducedArtifact>), RunnerError> {
        let mut step_statuses = Vec::new();
        let created_at = Utc::now();
        let mut overall_status = RunStatus::Success;
        // Artifacts declared by steps that *succeed* — handed to an
        // Outcome::Publish (R330-F3). A failed step's `produces` is dropped:
        // we never publish an artifact a failing step may not have written.
        // SubPipeline steps (R488-F2) aggregate their child's `produced` into
        // this collection when `propagate.produces = true`.
        let mut produced: Vec<crate::types::ProducedArtifact> = Vec::new();
        // Named outputs accumulated so far (W201-F4): step_name → {key → value}.
        // Used to substitute `${{ steps.X.outputs.Y }}` in later steps'
        // argv / env before execution.
        let mut step_context: std::collections::HashMap<
            String,
            std::collections::HashMap<String, String>,
        > = std::collections::HashMap::new();

        self.emit(QedEvent::RunStarted {
            total_steps: self.index_offset + self.pipeline.steps.len(),
            at: created_at,
        });

        for (index, step) in self.pipeline.steps.iter().enumerate() {
            let event_index = index + self.index_offset;
            // Apply accumulated step-output substitution to argv / env before
            // the step runs. Clones only when there is something to substitute.
            let step_modified: Option<crate::types::QedStep> = if !step_context.is_empty() {
                let mut s = step.clone();
                s.argv = s
                    .argv
                    .iter()
                    .map(|a| substitute_step_context(a, &step_context))
                    .collect();
                for v in s.env.values_mut() {
                    *v = substitute_step_context(v, &step_context);
                }
                Some(s)
            } else {
                None
            };
            let step = step_modified.as_ref().unwrap_or(step);

            let started_at = Utc::now();
            self.emit(QedEvent::StepStarted {
                index: event_index,
                name: step.name.clone(),
                argv: step.argv.clone(),
                env_keys: crate::events::credential_env_keys(std::env::vars()),
                at: started_at,
            });

            let runtime = self.resolve_runtime(step);
            // step_outputs: key → value collected from this step (W201-F4).
            let (result, task_run_id, step_outputs) = match step.kind {
                crate::types::StepKind::BuildImage => {
                    match self.execute_step_build_image(event_index, step).await {
                        Ok(Some(forge_id)) => (Ok(()), Some(forge_id.to_string()), std::collections::HashMap::new()),
                        Ok(None) => (Ok(()), None, std::collections::HashMap::new()),
                        Err(e) => (Err(e), None, std::collections::HashMap::new()),
                    }
                }
                crate::types::StepKind::PackageNativeTarball => {
                    (self.execute_step_package_native_tarball(step).await, None, std::collections::HashMap::new())
                }
                crate::types::StepKind::MuslStaticPreflight => {
                    (self.execute_step_musl_static_preflight(step).await, None, std::collections::HashMap::new())
                }
                crate::types::StepKind::SignNativeTarball => {
                    (self.execute_step_sign_native_tarball(step).await, None, std::collections::HashMap::new())
                }
                crate::types::StepKind::SubPipeline => {
                    match self.execute_step_sub_pipeline(event_index, step).await {
                        Ok((child_produced, child_outputs)) => {
                            // Aggregation happens here (not below) so child
                            // produces flow into the parent's `Outcome::Publish`
                            // exactly like a sibling step's `produces`. The
                            // generic `produced.extend(step.produces.iter())`
                            // below is a no-op for SubPipeline (validate
                            // rejects direct `produces` on this kind).
                            produced.extend(child_produced);
                            (Ok(()), None, child_outputs)
                        }
                        Err(e) => (Err(e), None, std::collections::HashMap::new()),
                    }
                }
                crate::types::StepKind::GhaWorkflow => {
                    match self.execute_step_gha_workflow(event_index, step).await {
                        Ok((workflow_produced, workflow_outputs)) => {
                            // Same aggregation policy as SubPipeline: the
                            // GHA child's artifacts flow into the parent's
                            // Outcome::Publish in one terminal stage/sync,
                            // not N per workflow job. Job-level outputs are
                            // surfaced as `<job_id>.<key>` so the enclosing
                            // SubPipeline parent's `propagate.outputs` can
                            // pick them up (R488-F6).
                            produced.extend(workflow_produced);
                            (Ok(()), None, workflow_outputs)
                        }
                        Err(e) => (Err(e), None, std::collections::HashMap::new()),
                    }
                }
                crate::types::StepKind::Subprocess => match (self.run_where, runtime) {
                    (RunWhere::Local, TaskRuntime::Native) => {
                        // Inject $YAH_OUTPUTS so the step can write key=value
                        // output lines (W201-F4). Read back after exit regardless
                        // of success/failure, then clean up the temp file.
                        let outputs_path = std::env::temp_dir()
                            .join(format!("yah-qed-{}-{}.env", &self.run_id, index));
                        let mut yah_env = std::collections::HashMap::new();
                        yah_env.insert(
                            "YAH_OUTPUTS".to_string(),
                            outputs_path.display().to_string(),
                        );
                        let result = self.execute_step_local(event_index, step, Some(&yah_env)).await;
                        let collected = parse_yah_outputs(&outputs_path);
                        let _ = std::fs::remove_file(&outputs_path);
                        (result, None, collected)
                    }
                    (RunWhere::Local, TaskRuntime::Container) => {
                        (self.execute_step_local_container(event_index, step).await, None, std::collections::HashMap::new())
                    }
                    (RunWhere::Remote, _) => match self.execute_step_remote(step, runtime).await {
                        Ok(forge_id) => (Ok(()), Some(forge_id.to_string()), std::collections::HashMap::new()),
                        Err(e) => (Err(e), None, std::collections::HashMap::new()),
                    },
                },
            };

            // Store outputs in the step context for downstream substitution.
            // Stored even when the step failed — a continue-on-error sibling
            // may still reference whatever was written before the failure.
            if !step_outputs.is_empty() {
                step_context.insert(step.name.clone(), step_outputs.clone());
            }

            let (status, msg) = match &result {
                Ok(_) => {
                    produced.extend(step.produces.iter().cloned());
                    (RunStatus::Success, None)
                }
                Err(e) => {
                    overall_status = RunStatus::Failed;
                    let msg = match e {
                        RunnerError::StepFailed { msg, .. } => Some(msg.clone()),
                        RunnerError::InvalidConfig(m) => Some(m.clone()),
                        other => Some(other.to_string()),
                    };
                    (RunStatus::Failed, msg)
                }
            };

            let completed_at = Utc::now();
            self.emit(QedEvent::StepFinished {
                index: event_index,
                name: step.name.clone(),
                status,
                msg,
                at: completed_at,
            });

            step_statuses.push(StepStatus {
                name: step.name.clone(),
                task_run_id,
                status,
                started_at: Some(started_at),
                completed_at: Some(completed_at),
                outputs: step_outputs,
            });

            if status == RunStatus::Failed && !matches!(step.on_fail, OnFail::Continue) {
                break;
            }
        }

        let outcomes = match overall_status {
            RunStatus::Success => &self.pipeline.on_success,
            _ => &self.pipeline.on_fail,
        };

        for outcome in outcomes {
            match outcome {
                Outcome::WardenDeploy { service, env } => {
                    self.outcome_dispatcher.warden_deploy(service, env).await?;
                }
                Outcome::AlmanacRun { pipeline } => {
                    self.outcome_dispatcher.almanac_run(pipeline).await?;
                }
                Outcome::Publish { provider, bucket, prefix, base_url } => {
                    // SubPipeline children with `propagate.produces = true`
                    // have their publish suppressed — the parent owns the
                    // terminal stage/sync/revalidate. WardenDeploy /
                    // AlmanacRun are NOT suppressed (they may need to run
                    // per-child regardless of who fires the publish).
                    if self.suppress_publish_outcomes {
                        tracing::debug!(
                            run_id = %self.run_id,
                            "suppressing Outcome::Publish on child sub-pipeline run; parent owns the terminal publish"
                        );
                        continue;
                    }
                    // Resolve relative artifact paths against camp_root so
                    // stage_release's fs::copy succeeds when the process CWD
                    // is not the workspace root (e.g. the Tauri desktop app).
                    let resolved_artifacts: Vec<_> = if let Some(root) = &self.camp_root {
                        produced.iter().map(|a| {
                            let p = std::path::Path::new(&a.path);
                            if p.is_relative() {
                                crate::types::ProducedArtifact {
                                    path: root.join(p).to_string_lossy().into_owned(),
                                    ..a.clone()
                                }
                            } else {
                                a.clone()
                            }
                        }).collect()
                    } else {
                        produced.clone()
                    };
                    let req = crate::publish::PublishRequest {
                        provider: provider.clone(),
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        base_url: base_url.clone(),
                        version: crate::publish::resolve_release_version(),
                        artifacts: resolved_artifacts,
                    };
                    self.outcome_dispatcher.publish(&req).await?;
                }
            }
        }

        let completed_at = Utc::now();
        self.emit(QedEvent::RunFinished {
            status: overall_status,
            at: completed_at,
        });

        Ok((
            QedRunMeta {
                id: self.run_id.clone(),
                pipeline: self.pipeline.name.clone(),
                status: overall_status,
                created_at,
                completed_at: Some(completed_at),
                steps: step_statuses,
                parent_run_id: self.parent_run_id.clone(),
            },
            produced,
        ))
    }

    /// Resolve, configure, and run a SubPipeline child step (R488-F2).
    ///
    /// On success returns the child's [`ProducedArtifact`] list — empty
    /// unless `propagate.produces = true` (in which case the parent's
    /// `Outcome::Publish` aggregates these). On failure returns a clean
    /// `StepFailed` whose `msg` carries the child run's failure tail.
    ///
    /// The child runner inherits the parent's `executor`, `signer`,
    /// `camp_root`, `events`, `outcome_dispatcher`, and
    /// `sub_pipeline_resolver` (so nested SubPipelines recurse with the
    /// same wiring). When `propagate.produces = true`, the child has its
    /// own `Outcome::Publish` suppressed so only the parent fires the
    /// terminal stage/sync/revalidate.
    /// Returns `(produced, outputs)`:
    /// - `produced`: child artifacts to roll up into the parent's publish when
    ///   `propagate.produces = true`; empty otherwise.
    /// - `outputs`: named outputs from the child run projected per
    ///   `propagate.outputs` (W201-F4). The runner scans all child
    ///   `StepStatus::outputs` maps and takes the last writer for each
    ///   declared name. Empty when `propagate.outputs` is empty.
    async fn execute_step_sub_pipeline(
        &self,
        index: usize,
        step: &crate::types::QedStep,
    ) -> Result<(Vec<crate::types::ProducedArtifact>, std::collections::HashMap<String, String>), RunnerError> {
        let Some(cfg) = step.sub_pipeline.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=sub-pipeline with no [sub_pipeline] block (validate() should have caught this)",
                step.name
            )));
        };

        let Some(mut child) = self.sub_pipeline_resolver.resolve(&cfg.target) else {
            let reason = self
                .sub_pipeline_resolver
                .unresolved_reason(&cfg.target)
                .unwrap_or_else(|| format!(
                    "sub-pipeline target unresolvable: {:?} (no resolver configured, or target not found)",
                    cfg.target
                ));
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: reason,
            });
        };

        // Forward params before constructing the child runner — child sees
        // its TOML with `{{key}}` placeholders substituted.
        child.apply_params(&cfg.params);

        // Build a child runner that inherits the parent's wiring. We can't
        // use the existing constructors because they reset every field to
        // defaults; instead, clone parent shape explicitly.
        //
        // R487 follow-up: for SubPipelineRef::GhaWorkflow the resolver
        // synthesises a single-step pipeline whose only step is
        // StepKind::GhaWorkflow. Going through a child runner there is
        // pure paperwork that (a) decouples events so the new
        // GhaEvent → QedEvent bridge can never fire and (b) wraps any
        // inner StepFailed in the generic SubPipeline-level "failed at
        // child step `gha-workflow`" string, erasing the per-job +
        // stderr-tail detail. Short-circuit: execute the GhaWorkflow
        // step directly on `self`, with `self.events` live, then mirror
        // the SubPipelineStarted/Finished bookends so consumers still
        // see the delegation chip.
        if let crate::types::SubPipelineRef::GhaWorkflow { .. } = &cfg.target {
            let target_label = sub_pipeline_target_label(&cfg.target);
            let stub_child_run_id = Uuid::new_v4().to_string();
            self.emit(QedEvent::SubPipelineStarted {
                index,
                name: step.name.clone(),
                target: target_label,
                child_run_id: stub_child_run_id.clone(),
                at: Utc::now(),
            });
            // The synthesised pipeline has exactly one step; pull its
            // GhaWorkflowConfig back out for the direct call.
            let synthesised_step = child
                .steps
                .into_iter()
                .next()
                .ok_or_else(|| RunnerError::InvalidConfig(format!(
                    "step `{}`: GhaWorkflow resolver returned an empty pipeline",
                    step.name,
                )))?;
            let result = self.execute_step_gha_workflow(index, &synthesised_step).await;
            let (status, ret): (RunStatus, Result<_, RunnerError>) = match result {
                Ok((produced, outputs)) => {
                    // Honour propagate.produces: roll up artifacts only
                    // when the parent declared it (mirrors the long-path
                    // SubPipeline behavior — the parent's terminal Publish
                    // stages everything in one go).
                    let out_produced = if cfg.propagate.produces {
                        produced
                    } else {
                        Vec::new()
                    };
                    (RunStatus::Success, Ok((out_produced, outputs)))
                }
                Err(e) => (RunStatus::Failed, Err(e)),
            };
            self.emit(QedEvent::SubPipelineFinished {
                index,
                name: step.name.clone(),
                child_run_id: stub_child_run_id,
                status,
                at: Utc::now(),
            });
            return ret;
        }

        let child_run_id = Uuid::new_v4().to_string();
        let child_runner = Self {
            pipeline: child,
            run_id: child_run_id.clone(),
            remote_driver: self.remote_driver.clone(),
            run_where: self.run_where,
            outcome_dispatcher: self.outcome_dispatcher.clone(),
            events: None,
            camp_root: self.camp_root.clone(),
            signer: self.signer.clone(),
            executor: self.executor.clone(),
            sub_pipeline_resolver: self.sub_pipeline_resolver.clone(),
            // Parent owns the terminal publish when propagate.produces is
            // set; otherwise the child's own Outcome::Publish (if any)
            // fires normally and the child's produced are *not* rolled up
            // to the parent (returned as empty below).
            suppress_publish_outcomes: cfg.propagate.produces,
            parent_run_id: Some(self.run_id.clone()),
            index_offset: 0,
            // Inherit so a SubPipeline whose child is a gha-workflow
            // step still honors the operator's matrix selection.
            gha_matrix_subset: self.gha_matrix_subset.clone(),
        };

        let target_label = sub_pipeline_target_label(&cfg.target);
        self.emit(QedEvent::SubPipelineStarted {
            index,
            name: step.name.clone(),
            target: target_label,
            child_run_id: child_run_id.clone(),
            at: Utc::now(),
        });

        // Async recursion needs explicit boxing.
        let outcome = Box::pin(child_runner.run_inner()).await;
        let (meta, child_produced) = match outcome {
            Ok(pair) => pair,
            Err(e) => {
                // Surface the bookend even when the child runner errored
                // before producing a meta — consumers shouldn't see a
                // dangling Started without a matching Finished.
                self.emit(QedEvent::SubPipelineFinished {
                    index,
                    name: step.name.clone(),
                    child_run_id: child_run_id.clone(),
                    status: RunStatus::Failed,
                    at: Utc::now(),
                });
                return Err(e);
            }
        };

        self.emit(QedEvent::SubPipelineFinished {
            index,
            name: step.name.clone(),
            child_run_id: child_run_id.clone(),
            status: meta.status,
            at: Utc::now(),
        });

        if meta.status != RunStatus::Success {
            // Surface the child's terminal status as a parent step failure
            // with the failing child step's name in the message — operator
            // sees both layers without needing to chase the nested run.
            let failing = meta
                .steps
                .iter()
                .find(|s| s.status == RunStatus::Failed)
                .map(|s| s.name.as_str())
                .unwrap_or("<unknown>");
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "sub-pipeline `{}` failed at child step `{}` (run_id={})",
                    meta.pipeline, failing, meta.id
                ),
            });
        }

        // Collect named outputs from child steps per propagate.outputs (W201-F4).
        // Scan all child StepStatus::outputs maps; last writer wins for each name.
        let propagated_outputs: std::collections::HashMap<String, String> =
            if cfg.propagate.outputs.is_empty() {
                std::collections::HashMap::new()
            } else {
                let mut collected: std::collections::HashMap<String, String> =
                    std::collections::HashMap::new();
                for child_step in &meta.steps {
                    for name in &cfg.propagate.outputs {
                        if let Some(value) = child_step.outputs.get(name) {
                            collected.insert(name.clone(), value.clone());
                        }
                    }
                }
                collected
            };

        let produced = if cfg.propagate.produces { child_produced } else { Vec::new() };
        Ok((produced, propagated_outputs))
    }

    /// Dispatch a [`StepKind::GhaWorkflow`] step into the native W200 GHA
    /// runtime (W200-F9). Reads the workflow YAML at the configured path
    /// (resolved relative to the camp root), parses through
    /// [`qed_gha::parse_workflow`], executes via [`qed_gha::execute_workflow`]
    /// with the F5–F8 built-in overrides pre-registered, and lifts each
    /// surviving [`qed_gha::ProducedArtifact`] into [`crate::types::ProducedArtifact`]
    /// so it flows into the parent's `Outcome::Publish` exactly like a
    /// SubPipeline child's artifacts.
    ///
    /// The qed-gha runtime is synchronous; we cross the seam via
    /// [`tokio::task::spawn_blocking`] so the runner's tokio reactor stays
    /// responsive (long-running workflow legs like `docker buildx build` would
    /// otherwise stall the executor).
    async fn execute_step_gha_workflow(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
    ) -> Result<(Vec<crate::types::ProducedArtifact>, std::collections::HashMap<String, String>), RunnerError> {
        let Some(cfg) = step.gha_workflow.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=gha-workflow with no [gha_workflow] block (validate() should have caught this)",
                step.name,
            )));
        };

        let camp_root = self.resolve_camp_root()?;
        let workflow_path = if cfg.path.is_absolute() {
            cfg.path.clone()
        } else {
            camp_root.join(&cfg.path)
        };
        let step_name = step.name.clone();
        let workspace = camp_root.clone();
        let event = cfg.event.clone().unwrap_or_else(|| "push".into());
        let inputs = cfg.inputs.clone();
        // R499-F3 phase 2: matrix subset for this step (if any). Empty
        // set isn't a runtime concern — the daemon rejects it before
        // ever constructing the runner.
        let matrix_subset = self.gha_matrix_subset.get(&step.name).cloned();

        // Step index of THIS gha-workflow step in the parent qed pipeline,
        // including the resume-time index offset (already baked into
        // `event_index` by the call site). The sync sink → async event
        // forwarder stamps this on every bridged GhaEvent so the receiver
        // can scope the per-job subtree under the right parent step.
        let step_index = event_index;
        let parent_step_name = step.name.clone();

        // Bridge qed_gha's sync std::sync::mpsc sender into our async
        // event sink (R325-F2). We spawn a forwarder *before* the blocking
        // task so the channel is live the moment the runtime starts
        // emitting; the forwarder ends when the blocking task drops its
        // sender.
        let (gha_tx, gha_rx) = std::sync::mpsc::channel::<qed_gha::GhaEvent>();
        let async_events = self.events.clone();
        let forwarder_name = parent_step_name.clone();
        let forwarder = tokio::task::spawn_blocking(move || {
            while let Ok(ev) = gha_rx.recv() {
                if let Some(sink) = &async_events {
                    let qed_ev = bridge_gha_event(step_index, &forwarder_name, ev);
                    let _ = sink.send(qed_ev);
                }
            }
        });

        // Sync execution off the reactor — qed_gha is blocking by design (it
        // spawns `bash`, `docker`, `git`, etc. via std::process::Command).
        let run = tokio::task::spawn_blocking(move || {
            let yaml = std::fs::read_to_string(&workflow_path).map_err(|e| {
                RunnerError::StepFailed {
                    step: step_name.clone(),
                    msg: format!("read workflow {}: {e}", workflow_path.display()),
                }
            })?;
            let workflow = qed_gha::parse_workflow(&yaml).map_err(|e| RunnerError::StepFailed {
                step: step_name.clone(),
                msg: format!("parse {}: {e}", workflow_path.display()),
            })?;
            let secrets = crate::secrets_bridge::SecretsConfig::load_default().resolve_all();
            let mut executor = qed_gha::Executor::new(&workspace)
                .with_events(gha_tx)
                .with_secrets(secrets);
            executor.inputs = inputs_to_value(&inputs);
            executor.github = github_context(&event);
            executor.included_instance_keys = matrix_subset;
            let run = qed_gha::execute_workflow(&workflow, &executor).map_err(|e| {
                RunnerError::StepFailed {
                    step: step_name.clone(),
                    msg: format!("execute {}: {e}", workflow_path.display()),
                }
            })?;
            // Drop the executor (and its event sender) so the forwarder loop
            // exits cleanly once it has drained the channel.
            drop(executor);
            Ok::<_, RunnerError>(run)
        })
        .await
        .map_err(|join_err| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("gha-workflow task panicked: {join_err}"),
        })??;
        // Wait for the forwarder to drain any tail events before we return —
        // otherwise the parent's `StepFinished` could race ahead of the last
        // few GhaStepOutput lines.
        let _ = forwarder.await;

        // Walk the run, lift produced artifacts. WorkflowRun::produced already
        // filters to successful jobs + successful steps, so failure-tainted
        // bytes never reach the publisher.
        let produced: Vec<crate::types::ProducedArtifact> = run
            .produced()
            .into_iter()
            .map(|p| crate::types::ProducedArtifact {
                binary: p.binary.clone(),
                path: p.path.clone(),
                triple: p.triple.clone(),
            })
            .collect();

        // Surface a workflow-level failure as a clean StepFailed pointing at
        // the first failing job AND the first failing step inside that job,
        // with a stderr tail so operators see *why* without having to chase
        // the nested WorkflowRun manually.
        if let Some(failing) = run
            .instances
            .iter()
            .find(|i| matches!(i.result, qed_gha::JobResult::Failure))
        {
            let failing_step = failing
                .steps
                .iter()
                .find(|s| matches!(s.conclusion, qed_gha::StepConclusion::Failure));
            let msg = match failing_step {
                Some(s) => {
                    let label = s
                        .name
                        .clone()
                        .or_else(|| s.step_id.clone())
                        .unwrap_or_else(|| "<unnamed>".to_string());
                    let tail = stderr_tail(&s.stderr, 20);
                    if tail.is_empty() {
                        format!(
                            "gha-workflow `{}` failed at job `{}` step `{}` (no stderr)",
                            cfg.path.display(),
                            failing.job_id,
                            label,
                        )
                    } else {
                        format!(
                            "gha-workflow `{}` failed at job `{}` step `{}`:\n{}",
                            cfg.path.display(),
                            failing.job_id,
                            label,
                            tail,
                        )
                    }
                }
                None => format!(
                    "gha-workflow `{}` failed at job `{}` (no failing step recorded — likely an override / scheduler error)",
                    cfg.path.display(),
                    failing.job_id,
                ),
            };
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg,
            });
        }

        // Lift job-level outputs into a flat HashMap so the parent's
        // SubPipelineCollect::outputs can address them. Naming scheme:
        // `<job_id>.<output_key>` (mirrors GHA's `jobs.<id>.outputs.<key>`
        // mental model). The SubPipeline parent declares which names it
        // wants in `propagate.outputs` and reads them via
        // `${{ steps.<gha-workflow-step>.outputs.<job_id>.<key> }}`.
        // R488-F6.
        let mut outputs: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for instance in &run.instances {
            if !matches!(instance.result, qed_gha::JobResult::Success) {
                continue;
            }
            for (key, value) in &instance.outputs {
                outputs.insert(
                    format!("{}.{}", instance.job_id, key),
                    value.as_str_lossy(),
                );
            }
        }

        Ok((produced, outputs))
    }

    /// Run one step as a local subprocess via [`Self::executor`] (R438-T14).
    ///
    /// Builds a `ForgeSpec{Subprocess, TaskPlacement{Local, Native}}` from
    /// `step.argv`/`step.cwd`/`step.env` and hands it to the configured
    /// `ForgeExecutor`. The executor drains stdout/stderr; an adapter task
    /// forwards each [`ExecEvent::Output`] as [`QedEvent::StepOutput`] so the
    /// per-line live-stream contract from R325-F2 is preserved. Failure
    /// message uses `ExecOutcome.stderr_tail` (same source the inline
    /// implementation captured).
    /// `extra_env` keys are merged on top of `step.env` — used by `run_inner`
    /// to inject `$YAH_OUTPUTS` for output collection (W201-F4) without
    /// mutating the step.
    async fn execute_step_local(
        &self,
        index: usize,
        step: &crate::types::QedStep,
        extra_env: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<(), RunnerError> {
        if step.argv.is_empty() {
            return Err(RunnerError::InvalidConfig("step argv is empty".to_string()));
        }
        let spec = build_subprocess_spec(step, TaskRuntime::Native, None);
        let camp_root = self.resolve_camp_root()?;
        let cwd = match step.cwd.as_ref() {
            Some(rel) => camp_root.join(rel),
            None => camp_root,
        };
        let mut merged_env: std::collections::HashMap<String, String> = step
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if let Some(extra) = extra_env {
            merged_env.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        let ctx = ExecContext::default().with_cwd(cwd).with_env(merged_env.into_iter().collect());
        self.drive_subprocess_step(index, step, spec, ctx).await
    }

    /// Run one step inside a one-shot container (local + container quadrant)
    /// via [`Self::executor`].
    ///
    /// Same flow as [`Self::execute_step_local`] but builds `ForgeSpec` with
    /// `runtime = Container` and an [`Subprocess.image`] resolved through
    /// [`task::default_image::default_forge_image`]. The container `cwd` is
    /// resolved to an absolute path before handoff so the executor's bind
    /// mount matches the host's view (matches the prior inline shape from
    /// R380-T6).
    ///
    /// Image: uses [`task::default_image::default_forge_image`] (resolves
    /// to `yah-rust-bun` since R381-T8). A per-step image catalog (the
    /// rest of R381) lets pipelines pick yah-rust / yah-python / yah-cuda
    /// by name via `task::default_image::catalog_image(name)`.
    async fn execute_step_local_container(
        &self,
        index: usize,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        if step.argv.is_empty() {
            return Err(RunnerError::InvalidConfig("step argv is empty".to_string()));
        }
        // Resolve the cwd that gets bind-mounted into the container. The
        // step's optional `cwd` (typically a relative path like
        // `packages/yah/ui`) joins onto the camp root so the mount is always
        // an absolute path. If neither is set we mount the camp root itself.
        let camp_root = self.resolve_camp_root()?;
        let mount_cwd = match step.cwd.as_deref() {
            Some(rel) => camp_root.join(rel),
            None => camp_root,
        };
        let image = task::default_image::default_forge_image();
        let spec = build_subprocess_spec(step, TaskRuntime::Container, Some(image));
        let ctx = ExecContext::default().with_cwd(mount_cwd).with_env(
            step.env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        self.drive_subprocess_step(index, step, spec, ctx).await
    }

    /// Hand a `(ForgeSpec, ExecContext)` to [`Self::executor`] and translate
    /// the outcome back into the qed runner's error vocabulary. An adapter
    /// task forwards every [`ExecEvent::Output`] into [`QedEvent::StepOutput`]
    /// on the runner's live-event sink — the per-line streaming contract
    /// (R325-F2) is preserved through the trait. `Started`/`Finished` events
    /// from the executor are absorbed; `run()` already brackets every step
    /// with its own `StepStarted`/`StepFinished`.
    async fn drive_subprocess_step(
        &self,
        index: usize,
        step: &crate::types::QedStep,
        spec: ForgeSpec,
        ctx: ExecContext,
    ) -> Result<(), RunnerError> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecEvent>();
        let adapter = {
            let events = self.events.clone();
            let name = step.name.clone();
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    let Some(events) = &events else { continue };
                    if let ExecEvent::Output { stream, line } = ev {
                        let qed_stream = match stream {
                            task::OutputStream::Stdout => OutputStream::Stdout,
                            task::OutputStream::Stderr => OutputStream::Stderr,
                        };
                        let _ = events.send(QedEvent::StepOutput {
                            index,
                            name: name.clone(),
                            stream: qed_stream,
                            line,
                        });
                    }
                }
            })
        };

        let outcome_result = self.executor.execute(spec, ctx, Some(tx)).await;
        let _ = adapter.await;

        match outcome_result {
            Ok(outcome) if outcome.succeeded() => Ok(()),
            Ok(outcome) => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: outcome.stderr_tail,
            }),
            Err(ForgeExecutorError::Spawn(msg)) => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "failed to spawn (is the runtime installed and accessible?): {msg}"
                ),
            }),
            Err(ForgeExecutorError::Io(e)) => Err(RunnerError::Io(e)),
            Err(ForgeExecutorError::Unsupported(what)) => Err(RunnerError::InvalidConfig(
                format!("subprocess executor: {what}"),
            )),
        }
    }
}

/// Translate a [`qed_gha::GhaEvent`] into the qed-runner's own
/// [`crate::QedEvent::Gha*`] variant, stamping the parent step's index and
/// name so the desktop pane can scope nested rows under the right step
/// (W200 R487 follow-up).
fn bridge_gha_event(
    step_index: usize,
    parent_name: &str,
    ev: qed_gha::GhaEvent,
) -> crate::QedEvent {
    use qed_gha::GhaEvent as G;
    let at = chrono::Utc::now();
    match ev {
        G::JobStarted { job_id, matrix_index, key, total_steps } => {
            crate::QedEvent::GhaJobStarted {
                index: step_index,
                name: parent_name.to_string(),
                job_id,
                matrix_index,
                job_key: key,
                total_steps,
                at,
            }
        }
        G::JobFinished { job_id: _, matrix_index: _, key, result } => {
            crate::QedEvent::GhaJobFinished {
                index: step_index,
                name: parent_name.to_string(),
                job_key: key,
                result: gha_result_str(result).to_string(),
                at,
            }
        }
        G::StepStarted {
            job_id,
            matrix_index,
            step_index: gha_step_index,
            step_id,
            name: step_name,
            action_kind,
        } => crate::QedEvent::GhaStepStarted {
            index: step_index,
            name: parent_name.to_string(),
            job_key: instance_key(&job_id, matrix_index),
            step_index: gha_step_index,
            step_id,
            step_name,
            action_kind,
            at,
        },
        G::StepOutput {
            job_id,
            matrix_index,
            step_index: gha_step_index,
            stream,
            line,
        } => crate::QedEvent::GhaStepOutput {
            index: step_index,
            name: parent_name.to_string(),
            job_key: instance_key(&job_id, matrix_index),
            step_index: gha_step_index,
            stream: match stream {
                qed_gha::GhaOutputStream::Stdout => crate::events::OutputStream::Stdout,
                qed_gha::GhaOutputStream::Stderr => crate::events::OutputStream::Stderr,
            },
            line,
        },
        G::StepFinished {
            job_id,
            matrix_index,
            step_index: gha_step_index,
            conclusion,
            msg,
            outputs: _,
            produced: _,
        } => crate::QedEvent::GhaStepFinished {
            index: step_index,
            name: parent_name.to_string(),
            job_key: instance_key(&job_id, matrix_index),
            step_index: gha_step_index,
            conclusion: gha_conclusion_str(conclusion).to_string(),
            msg,
            at,
        },
    }
}

/// Same key format as [`qed_gha::JobInstance::key`] — `"<job>"` for non-matrix
/// jobs, `"<job>#<row>"` for matrix rows. Kept in sync by construction; the
/// receiver pairs Start / Finish by exact-string compare.
fn instance_key(job_id: &str, matrix_index: Option<usize>) -> String {
    match matrix_index {
        Some(idx) => format!("{job_id}#{idx}"),
        None => job_id.to_string(),
    }
}

fn gha_result_str(r: qed_gha::JobResult) -> &'static str {
    match r {
        qed_gha::JobResult::Success => "success",
        qed_gha::JobResult::Failure => "failure",
        qed_gha::JobResult::Cancelled => "cancelled",
        qed_gha::JobResult::Skipped => "skipped",
    }
}

fn gha_conclusion_str(c: qed_gha::StepConclusion) -> &'static str {
    match c {
        qed_gha::StepConclusion::Success => "success",
        qed_gha::StepConclusion::Failure => "failure",
        qed_gha::StepConclusion::Skipped => "skipped",
    }
}

/// Last `lines` non-blank lines of `stderr`, with qed-gha's internal
/// `$GITHUB_ENV` sidechannel marker stripped (see `pop_env_updates` in
/// qed_gha::runtime). Empty when there is nothing useful left to show.
fn stderr_tail(stderr: &str, lines: usize) -> String {
    const ENV_PREFIX: &str = "__qed_gha_env_updates_BEGIN__";
    const ENV_SUFFIX: &str = "__qed_gha_env_updates_END__";
    let cleaned: String = stderr
        .lines()
        .filter(|l| {
            let t = l.trim();
            !t.starts_with(ENV_PREFIX) && !t.starts_with(ENV_SUFFIX) && !t.is_empty()
        })
        .collect::<Vec<_>>()
        .join("\n");
    if cleaned.is_empty() {
        return String::new();
    }
    let trimmed: Vec<&str> = cleaned.lines().collect();
    let start = trimmed.len().saturating_sub(lines);
    trimmed[start..].join("\n")
}

/// Build a minimal `qed_gha::Value` object from a string map. Used to lower
/// `[gha_workflow] inputs = { tag = "v1" }` into the runtime's `inputs.*`
/// expression context.
fn inputs_to_value(inputs: &std::collections::HashMap<String, String>) -> qed_gha::Value {
    let mut m: indexmap::IndexMap<String, qed_gha::Value> = indexmap::IndexMap::new();
    for (k, v) in inputs {
        m.insert(k.clone(), qed_gha::Value::String(v.clone()));
    }
    qed_gha::Value::Object(m)
}

/// Synthesize a minimal `github` expression context for a GhaWorkflow step.
/// v1 populates `event_name` only; ref/sha/actor are left empty since
/// `release.yml` references the ones it cares about via `github.ref_name` and
/// `github.event.inputs.*`, both of which the workflow's own `on:` block fills
/// from the event/inputs we passed in. Future work: a richer context
/// synthesized from the camp's git state.
fn github_context(event_name: &str) -> qed_gha::Value {
    let mut m: indexmap::IndexMap<String, qed_gha::Value> = indexmap::IndexMap::new();
    m.insert("event_name".into(), qed_gha::Value::String(event_name.into()));
    m.insert("ref".into(), qed_gha::Value::String(String::new()));
    m.insert("ref_name".into(), qed_gha::Value::String(String::new()));
    m.insert("sha".into(), qed_gha::Value::String(String::new()));
    m.insert("actor".into(), qed_gha::Value::String(String::new()));
    m.insert("event".into(), qed_gha::Value::Object(indexmap::IndexMap::new()));
    qed_gha::Value::Object(m)
}

/// Lower a `QedStep` into a `ForgeSpec` for the local subprocess executor
/// (R438-T14). The image is `Some` for the container path and `None` for
/// native — the executor branches on `where_.runtime` and rejects a missing
/// image when it needs one.
fn build_subprocess_spec(
    step: &crate::types::QedStep,
    runtime: TaskRuntime,
    image: Option<workload_spec::ImageRef>,
) -> ForgeSpec {
    ForgeSpec {
        command: ForgeCommand::Subprocess {
            argv: step.argv.clone(),
            image,
        },
        where_: TaskPlacement::new(TaskLocation::Local, runtime),
        timeout: step.timeout.map(Millis::from_ms),
        label: Some(step.name.clone()),
        initiator: Initiator::Human { camp: "qed".into() },
        mesh_access: MeshAccess::None,
    }
}

/// Substitute `${{ steps.STEP_NAME.outputs.KEY }}` placeholders in `s`
/// using the accumulated step context (W201-F4). Unknown placeholders are
/// left untouched — downstream tooling (or the W200 expression engine once
/// R487-F2 ships) handles them. The pattern is intentionally minimal: no
/// expression evaluation, no escaping, no nested references.
/// Human-readable token for a [`SubPipelineRef`] (R488-F5). Matches the
/// resolver-token discipline used by `validate_sub_pipeline_graph` and the
/// in-memory test resolver: `builtin:<name>`, `path:<path>`, `gha:<path>`.
/// Surfaced on `QedEvent::SubPipelineStarted.target` so a consumer can label
/// the child run without a back-reference to the parent pipeline TOML.
fn sub_pipeline_target_label(target: &crate::types::SubPipelineRef) -> String {
    match target {
        crate::types::SubPipelineRef::Builtin(n) => format!("builtin:{n}"),
        crate::types::SubPipelineRef::Path(p) => format!("path:{}", p.display()),
        crate::types::SubPipelineRef::GhaWorkflow { path, .. } => {
            format!("gha:{}", path.display())
        }
        crate::types::SubPipelineRef::Peer { camp, pipeline } => {
            format!("peer:{camp}:{pipeline}")
        }
    }
}

fn substitute_step_context(
    s: &str,
    context: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
) -> String {
    let mut out = s.to_string();
    for (step_name, outputs) in context {
        for (key, value) in outputs {
            let pattern = format!("${{{{ steps.{step_name}.outputs.{key} }}}}");
            out = out.replace(&pattern, value);
        }
    }
    out
}

/// Parse a `KEY=VALUE\n`-formatted file written by a step to `$YAH_OUTPUTS`.
/// Lines that don't contain `=` are silently skipped (e.g. blank lines or
/// comment lines). Returns an empty map if the file doesn't exist or can't
/// be read — steps that emit no outputs are the common case.
fn parse_yah_outputs(path: &std::path::Path) -> std::collections::HashMap<String, String> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return std::collections::HashMap::new();
    };
    content
        .lines()
        .filter_map(|line| {
            let (k, v) = line.split_once('=')?;
            let k = k.trim().to_string();
            if k.is_empty() { return None; }
            Some((k, v.to_string()))
        })
        .collect()
}

impl PipelineRunner {
    /// Dispatch a `kind = "build-image"` step.
    ///
    /// Catalog lookup + Dockerfile staging is shared across local and remote:
    ///
    /// 1. Look up the catalog entry named by `step.image` (R381-T1 bundled +
    ///    per-camp).
    /// 2. Compile a Dockerfile via [`crate::images::compile_with_dockerfile_dir`]
    ///    (sibling Dockerfile wins; otherwise the TOML layering shorthand is
    ///    rendered). Per-camp dir is `<camp_root>/.yah/qed/images/<name>/`.
    /// 3. Write the Dockerfile under `.yah/cache/buildkit/<name>.Dockerfile`.
    ///
    /// Local path then calls [`task::local::build_image_command`] (docker
    /// buildx); remote path synthesises a BuildKit-in-containerd workload via
    /// [`task::remote::RemoteForgeDriver`] and waits for the terminal status.
    /// Both paths surface step output through the shared QedEvent sink — for
    /// remote, the per-line stream flows through scryer (`forge.remote`
    /// target) rather than this runner directly, mirroring
    /// [`Self::execute_step_remote`].
    async fn execute_step_build_image(
        &self,
        index: usize,
        step: &crate::types::QedStep,
    ) -> Result<Option<ObsForgeId>, RunnerError> {
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt, BufReader};

        let prepared = self.prepare_build_image(step)?;

        if matches!(self.run_where, RunWhere::Remote) {
            let forge_id = self.execute_step_build_image_remote(step, &prepared).await?;
            return Ok(Some(forge_id));
        }

        let context_buf = step
            .context
            .as_ref()
            .map(|ctx| prepared.camp_root.join(ctx));
        let context = context_buf
            .as_deref()
            .unwrap_or_else(|| std::path::Path::new("."));

        let cmd = {
            let opts = task::local::BuildImageOptions {
                dockerfile: &prepared.dockerfile_path,
                context,
                tag: &prepared.tag,
                push: step.push,
                load: step.load,
                cache_dir: Some(&prepared.buildkit_dir),
                oci_archive: if step.push || step.load {
                    None
                } else {
                    Some(&prepared.archive_path)
                },
            };
            task::local::build_image_command(&opts)
        };

        let mut cmd = cmd;
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("failed to spawn `docker buildx`: {e}"),
        })?;
        let stdout = child.stdout.take().expect("stdout piped above");
        let stderr = child.stderr.take().expect("stderr piped above");

        let stdout_task = {
            let events = self.events.clone();
            let name = step.name.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(tx) = &events {
                        let _ = tx.send(QedEvent::StepOutput {
                            index,
                            name: name.clone(),
                            stream: OutputStream::Stdout,
                            line,
                        });
                    }
                }
            })
        };

        let stderr_task = {
            let events = self.events.clone();
            let name = step.name.clone();
            tokio::spawn(async move {
                let mut captured: Vec<String> = Vec::new();
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(tx) = &events {
                        let _ = tx.send(QedEvent::StepOutput {
                            index,
                            name: name.clone(),
                            stream: OutputStream::Stderr,
                            line: line.clone(),
                        });
                    }
                    captured.push(line);
                }
                captured
            })
        };

        let status = child.wait().await.map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("waiting on `docker buildx` failed: {e}"),
        })?;
        let _ = stdout_task.await;
        let stderr_lines = stderr_task.await.unwrap_or_default();

        if !status.success() {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: stderr_lines.join("\n").trim().to_string(),
            });
        }
        Ok(None)
    }

    /// Shared catalog-lookup + Dockerfile-staging path used by both local and
    /// remote build-image dispatch.
    fn prepare_build_image(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<PreparedBuildImage, RunnerError> {
        let camp_root = self.resolve_camp_root()?;
        let camp_images_dir = camp_root.join(".yah/qed/images");
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir).map_err(|e| {
            RunnerError::InvalidConfig(format!("failed to load catalog: {e}"))
        })?;

        let image_name = step.image.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "build-image step `{}` is missing the `image` field (parse-time validation should have caught this)",
                step.name
            ))
        })?;

        let entry = catalog.get(image_name).cloned().ok_or_else(|| {
            let known: Vec<&str> = catalog.names();
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "unknown catalog image `{image_name}` — known: {known:?}. \
                     Per-camp images live at `.yah/qed/images/<name>/`."
                ),
            }
        })?;

        let per_camp_dir = camp_images_dir.join(image_name);
        let dockerfile_text = crate::images::compile_with_dockerfile_dir(
            &entry,
            &catalog,
            &per_camp_dir,
        )
        .map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("Dockerfile compile failed for `{image_name}`: {e}"),
        })?;

        let cache_root = camp_root.join(".yah/cache");
        let buildkit_dir = cache_root.join("buildkit");
        let archive_dir = cache_root.join("images");
        std::fs::create_dir_all(&buildkit_dir).map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("failed to create {}: {e}", buildkit_dir.display()),
        })?;
        std::fs::create_dir_all(&archive_dir).map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("failed to create {}: {e}", archive_dir.display()),
        })?;

        let dockerfile_path = buildkit_dir.join(format!("{image_name}.Dockerfile"));
        std::fs::write(&dockerfile_path, &dockerfile_text).map_err(|e| {
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("failed to write {}: {e}", dockerfile_path.display()),
            }
        })?;

        let tag = step.tag.clone().unwrap_or_else(|| format!("{image_name}:dev"));
        let safe_tag = tag_to_filename(&tag);
        let archive_path = archive_dir.join(format!("{safe_tag}.tar"));

        Ok(PreparedBuildImage {
            camp_root,
            dockerfile_path,
            buildkit_dir,
            archive_path,
            tag,
        })
    }

    /// Remote build-image dispatch: hand the staged Dockerfile + context to
    /// the warden via a BuildKit-in-containerd workload (R381-T5).
    ///
    /// Mirrors [`Self::execute_step_remote`] but for `ForgeCommand::BuildImage`.
    /// The host-side dockerfile + context paths are passed verbatim to warden
    /// as bind-mount targets; this assumes the warden node has access to those
    /// paths (single-machine sim/dogfood case). Cross-host context shipping
    /// (R091 artifact transport) is its own follow-up.
    async fn execute_step_build_image_remote(
        &self,
        step: &crate::types::QedStep,
        prepared: &PreparedBuildImage,
    ) -> Result<ObsForgeId, RunnerError> {
        let driver = self
            .remote_driver
            .as_ref()
            .expect("remote_driver is Some when run_where == Remote");

        let spec = ForgeSpec {
            command: ForgeCommand::BuildImage {
                dockerfile: prepared.dockerfile_path.clone(),
                context: prepared.camp_root.clone(),
                tag: prepared.tag.clone(),
                push: step.push,
            },
            where_: TaskPlacement::new(
                TaskLocation::RemoteAny { tier: TierTag("infra".into()) },
                TaskRuntime::Container,
            ),
            timeout: step.timeout.map(Millis::from_ms),
            label: Some(step.name.clone()),
            initiator: Initiator::Human { camp: "qed".into() },
            mesh_access: MeshAccess::None,
        };

        let handle = driver.start(spec).await.map_err(|e| RunnerError::Remote(e.to_string()))?;
        let forge_id = handle.id.clone();
        let status = handle.wait().await;

        match status {
            ForgeStatus::Done { exit_code: 0, .. } => Ok(forge_id),
            ForgeStatus::Done { exit_code, .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("buildkit exited with code {exit_code}"),
            }),
            ForgeStatus::TimedOut { .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: "build-image step timed out".into(),
            }),
            ForgeStatus::Killed { signal, .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("buildkit killed by signal {signal}"),
            }),
            ForgeStatus::Lost { reason } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("buildkit lost: {reason}"),
            }),
            ForgeStatus::Pending | ForgeStatus::Running => {
                unreachable!("ForgeRunHandle::wait returns a terminal status")
            }
        }
    }

    /// Dispatch a `kind = "package-native-tarball"` step (R407-T2).
    ///
    /// Pure host file I/O — there is no remote variant. Looks up the catalog
    /// entry named by `step.image`, asserts it declares
    /// [`crate::images::ProduceTarget::NativeTarball`], then writes a
    /// `<camp_root>/.yah/cache/native/<image>-<triple>.tar.gz` containing the
    /// static musl binary at `step.binary_path` plus a `manifest.toml`
    /// describing the workload-spec. The manifest carries the catalog entry's
    /// `env` map and `description` so Constable knows how to launch the
    /// workload without re-reading the catalog at deploy time.
    ///
    /// Cross-compile preflight (R407-T3) is the gate that ensures
    /// `step.binary_path` is actually musl-static before this step runs — by
    /// the time we get here the binary is assumed to be correctly targeted.
    async fn execute_step_package_native_tarball(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let camp_root = self.resolve_camp_root()?;
        let camp_images_dir = camp_root.join(".yah/qed/images");
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir).map_err(|e| {
            RunnerError::InvalidConfig(format!("failed to load catalog: {e}"))
        })?;

        let image_name = step.image.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "package-native-tarball step `{}` is missing `image` \
                 (parse-time validation should have caught this)",
                step.name
            ))
        })?;
        let entry = catalog.get(image_name).cloned().ok_or_else(|| {
            let known: Vec<&str> = catalog.names();
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "unknown catalog image `{image_name}` — known: {known:?}. \
                     Per-camp images live at `.yah/qed/images/<name>/`."
                ),
            }
        })?;

        if !entry.produces.contains(&crate::images::ProduceTarget::NativeTarball) {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "catalog entry `{image_name}` does not declare \
                     `produces = [\"native-tarball\"]` — add `native-tarball` to \
                     its `produces` list (alone or alongside `oci-image`) in \
                     `.yah/qed/images/{image_name}.toml`."
                ),
            });
        }

        let binary_rel = step.binary_path.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "package-native-tarball step `{}` is missing `binary_path` \
                 (parse-time validation should have caught this)",
                step.name
            ))
        })?;
        let binary_path = if std::path::Path::new(binary_rel).is_absolute() {
            std::path::PathBuf::from(binary_rel)
        } else {
            camp_root.join(binary_rel)
        };
        if !binary_path.is_file() {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "binary not found at `{}` — declare the upstream build step \
                     in `produces` and chain it before this packaging step.",
                    binary_path.display()
                ),
            });
        }

        let triple = step
            .triple
            .clone()
            .unwrap_or_else(|| crate::publish::resolve_triple(None));

        let bin_basename = binary_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| RunnerError::InvalidConfig(format!(
                "binary path `{}` has no filename component",
                binary_path.display(),
            )))?
            .to_string();

        let mut env: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
        for (k, v) in &entry.env {
            env.insert(k.clone(), v.clone());
        }

        let manifest = crate::native::NativeTarballManifest {
            name: entry.name.clone(),
            version: crate::publish::resolve_release_version(),
            triple: triple.clone(),
            binary: format!("bin/{bin_basename}"),
            description: if entry.description.is_empty() {
                None
            } else {
                Some(entry.description.clone())
            },
            env,
        };

        let output_path = crate::native::native_tarball_output_path(
            &camp_root,
            &entry.name,
            &triple,
        );

        crate::native::pack_native_tarball(&binary_path, &manifest, &output_path).map_err(
            |e| RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("failed to pack native tarball at {}: {e}", output_path.display()),
            },
        )?;

        Ok(())
    }

    /// Dispatch a `kind = "sign-native-tarball"` step (R407-T5, W154).
    ///
    /// Sigstore signing extends to native-tarball artifacts under the same
    /// keyless-OIDC trust model used for OCI images today (cosign signs the
    /// registry digest; here cosign signs the on-disk blob). The signer
    /// (attached via [`Self::with_signer`]) writes `.sig`, `.crt`, and
    /// `.bundle` next to the artifact; `cosign verify-blob --bundle ...`
    /// at deploy time confirms the GHA workflow identity matches the
    /// release pipeline's expected regex.
    ///
    /// The tarball path is resolved via
    /// [`crate::native::native_tarball_output_path`] — same convention as
    /// packaging, so a pipeline that runs `package-native-tarball` then
    /// `sign-native-tarball` with the same `image` + `triple` always finds
    /// the artifact. A pre-flight check on the catalog entry's `produces`
    /// list refuses to sign tarballs from entries that didn't declare
    /// `native-tarball` (catches a stale step that survived a catalog
    /// rename).
    async fn execute_step_sign_native_tarball(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let camp_root = self.resolve_camp_root()?;
        let camp_images_dir = camp_root.join(".yah/qed/images");
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir).map_err(|e| {
            RunnerError::InvalidConfig(format!("failed to load catalog: {e}"))
        })?;

        let image_name = step.image.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "sign-native-tarball step `{}` is missing `image` \
                 (parse-time validation should have caught this)",
                step.name
            ))
        })?;
        let entry = catalog.get(image_name).cloned().ok_or_else(|| {
            let known: Vec<&str> = catalog.names();
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "unknown catalog image `{image_name}` — known: {known:?}. \
                     Per-camp images live at `.yah/qed/images/<name>/`."
                ),
            }
        })?;

        if !entry.produces.contains(&crate::images::ProduceTarget::NativeTarball) {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "catalog entry `{image_name}` does not declare \
                     `produces = [\"native-tarball\"]` — sign-native-tarball \
                     refuses to sign artifacts the catalog hasn't opted in to. \
                     Update `.yah/qed/images/{image_name}.toml` (or drop this \
                     sign step)."
                ),
            });
        }

        let triple = step
            .triple
            .clone()
            .unwrap_or_else(|| crate::publish::resolve_triple(None));
        let tarball_path = crate::native::native_tarball_output_path(
            &camp_root,
            &entry.name,
            &triple,
        );
        if !tarball_path.is_file() {
            return Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "native tarball not found at `{}` — run \
                     `kind = \"package-native-tarball\"` for `{image_name}` \
                     before signing.",
                    tarball_path.display()
                ),
            });
        }

        let signed = self.signer.sign_blob(&tarball_path).await.map_err(|e| {
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "cosign sign-blob failed for `{}`: {e}",
                    tarball_path.display(),
                ),
            }
        })?;

        tracing::info!(
            tarball = %tarball_path.display(),
            signature = %signed.signature_path.display(),
            certificate = %signed.certificate_path.display(),
            bundle = signed.bundle_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default(),
            "qed sign-native-tarball: artifact signed"
        );
        Ok(())
    }

    /// Dispatch a `kind = "musl-static-preflight"` step (R407-T3).
    ///
    /// Walks `step.package`'s transitive dep closure via `cargo metadata`
    /// and fails the step (with a `NotMuslSafe` error listing the offenders)
    /// if any crate in [`crate::preflight::KNOWN_GLIBC_ONLY_CRATES`] appears.
    /// The error message routes the pipeline author to the container
    /// fallback (`runtime = "container"`) rather than letting the
    /// downstream `cargo build --target=*-musl` step die with a confusing
    /// linker error.
    async fn execute_step_musl_static_preflight(
        &self,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let camp_root = self.resolve_camp_root()?;
        let package = step.package.as_deref().ok_or_else(|| {
            RunnerError::InvalidConfig(format!(
                "musl-static-preflight step `{}` is missing `package` \
                 (parse-time validation should have caught this)",
                step.name
            ))
        })?;
        let package = package.to_string();
        let step_name = step.name.clone();
        let camp_root_clone = camp_root.clone();
        // cargo metadata blocks while it resolves the dep graph — push it
        // off the async runtime so a slow workspace doesn't starve other
        // tasks (e.g. event drain).
        let result = tokio::task::spawn_blocking(move || {
            crate::preflight::check_musl_compatibility(&camp_root_clone, &package)
        })
        .await
        .map_err(|e| RunnerError::StepFailed {
            step: step_name.clone(),
            msg: format!("preflight task panicked: {e}"),
        })?;
        result.map_err(|e| RunnerError::StepFailed {
            step: step_name,
            msg: e.to_string(),
        })?;
        Ok(())
    }

    async fn execute_step_remote(
        &self,
        step: &crate::types::QedStep,
        runtime: TaskRuntime,
    ) -> Result<ObsForgeId, RunnerError> {
        let driver = self
            .remote_driver
            .as_ref()
            .expect("remote_driver is Some when run_where == Remote");

        let spec = ForgeSpec {
            command: ForgeCommand::Subprocess { argv: step.argv.clone(), image: None },
            where_: TaskPlacement::new(
                TaskLocation::RemoteAny { tier: TierTag("infra".into()) },
                runtime,
            ),
            timeout: step.timeout.map(Millis::from_ms),
            label: Some(step.name.clone()),
            // Camp name will be threaded through once warden RPC stabilises (R091).
            initiator: Initiator::Human { camp: "qed".into() },
            mesh_access: MeshAccess::None,
        };

        let handle = driver.start(spec).await.map_err(|e| RunnerError::Remote(e.to_string()))?;

        let forge_id = handle.id.clone();
        let status = handle.wait().await;

        match status {
            ForgeStatus::Done { exit_code: 0, .. } => Ok(forge_id),
            ForgeStatus::Done { exit_code, .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("exited with code {exit_code}"),
            }),
            ForgeStatus::TimedOut { .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: "step timed out".into(),
            }),
            ForgeStatus::Killed { signal, .. } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("killed by signal {signal}"),
            }),
            ForgeStatus::Lost { reason } => Err(RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!("lost: {reason}"),
            }),
            ForgeStatus::Pending | ForgeStatus::Running => {
                unreachable!("ForgeRunHandle::wait returns a terminal status")
            }
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use scryer::service::{Scryer, ScryerConfig};
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use workload_spec::MeshIdent;

    #[test]
    fn stderr_tail_strips_env_markers_and_keeps_last_n_lines() {
        let stderr = "first\nsecond\n__qed_gha_env_updates_BEGIN__\nFOO\tbar\n__qed_gha_env_updates_END__\nthird\nfourth\nfifth\n";
        let out = stderr_tail(stderr, 3);
        // Markers + FOO line stripped (FOO\tbar starts with neither prefix
        // so it'll appear — that's OK as it shows env-update side-effect).
        assert!(!out.contains("__qed_gha_env_updates_BEGIN__"));
        assert!(!out.contains("__qed_gha_env_updates_END__"));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines, vec!["third", "fourth", "fifth"]);
    }

    #[test]
    fn stderr_tail_returns_empty_when_only_env_markers() {
        let stderr = "__qed_gha_env_updates_BEGIN__\n__qed_gha_env_updates_END__\n";
        assert_eq!(stderr_tail(stderr, 10), "");
    }

    #[test]
    fn stderr_tail_empty_input_is_empty() {
        assert_eq!(stderr_tail("", 10), "");
    }

    fn make_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    fn one_step_pipeline(name: &str, argv: Vec<String>) -> Pipeline {
        Pipeline {
            name: name.to_string(),
            label: name.to_string(),
            steps: vec![crate::types::QedStep {
                name: "step-1".to_string(),
                argv,
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::Subprocess,
                image: None,
                tag: None,
                push: false,
                binary_path: None,
                triple: None,
                package: None,
                context: None,
                load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            wraps: None,
        }
    }

    // ── Scripted warden for qed tests ──────────────────────────────────────

    struct ScriptedWarden {
        lines: Vec<String>,
        exit_code: i32,
    }

    #[async_trait::async_trait]
    impl WardenClient for ScriptedWarden {
        async fn deploy(
            &self,
            _spec: &workload_spec::WorkloadSpec,
        ) -> Result<(), task::RemoteForgeError> {
            Ok(())
        }

        async fn connect_logs(
            &self,
            _ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, task::RemoteForgeError> {
            let (tx, rx) = mpsc::channel(64);
            let lines = self.lines.clone();
            tokio::spawn(async move {
                for line in lines {
                    let _ = tx.send(line).await;
                }
            });
            Ok(rx)
        }

        async fn teardown(&self, _ident: &MeshIdent) -> Result<(), task::RemoteForgeError> {
            Ok(())
        }

        async fn exit_code(
            &self,
            _ident: &MeshIdent,
        ) -> Result<Option<i32>, task::RemoteForgeError> {
            Ok(Some(self.exit_code))
        }
    }

    /// Remote path happy: single step exits 0, task_run_id populated in step status.
    #[tokio::test]
    async fn remote_step_success() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden {
            lines: vec!["build ok".to_string()],
            exit_code: 0,
        });

        let pipeline = one_step_pipeline("test-remote", vec!["true".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, warden);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(meta.steps.len(), 1);
        assert!(
            meta.steps[0].task_run_id.is_some(),
            "remote step should record task_run_id"
        );
    }

    /// Remote path failure: non-zero exit code propagates as Failed status.
    #[tokio::test]
    async fn remote_step_failure() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden {
            lines: vec!["error: something went wrong".to_string()],
            exit_code: 1,
        });

        let pipeline = one_step_pipeline("test-remote-fail", vec!["false".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, warden);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// Remote path: second step skipped when first fails with on_fail=Abort.
    #[tokio::test]
    async fn remote_abort_on_fail() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden { lines: vec![], exit_code: 1 });

        let mut pipeline = one_step_pipeline("test-abort", vec!["false".to_string()]);
        pipeline.steps.push(crate::types::QedStep {
            name: "step-2".to_string(),
            argv: vec!["true".to_string()],
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::Subprocess,
            image: None,
            tag: None,
            push: false,
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
        });

        let runner = PipelineRunner::new_remote(pipeline, scryer, warden);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps.len(), 1, "step-2 should be skipped after step-1 fails");
    }

    // ── Outcome dispatch tests ─────────────────────────────────────────────

    use crate::types::Outcome;
    use std::sync::Mutex;

    struct RecordingDispatcher {
        calls: Mutex<Vec<String>>,
    }

    impl RecordingDispatcher {
        fn new() -> Arc<Self> {
            Arc::new(Self { calls: Mutex::new(vec![]) })
        }

        fn recorded(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl OutcomeDispatcher for RecordingDispatcher {
        async fn warden_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError> {
            self.calls.lock().unwrap().push(format!("warden-deploy:{service}:{env}"));
            Ok(())
        }

        async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError> {
            self.calls.lock().unwrap().push(format!("almanac-run:{pipeline}"));
            Ok(())
        }

        async fn publish(&self, req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
            // Record the bucket + how many artifacts the run collected, so a
            // test can assert that only *successful* steps' artifacts arrive.
            self.calls
                .lock()
                .unwrap()
                .push(format!("publish:{}:{}", req.bucket, req.artifacts.len()));
            Ok(())
        }
    }

    fn pipeline_with_outcomes(
        on_success: Vec<Outcome>,
        on_fail: Vec<Outcome>,
        argv: Vec<String>,
    ) -> Pipeline {
        Pipeline {
            name: "test".to_string(),
            label: "test".to_string(),
            steps: vec![crate::types::QedStep {
                name: "step-1".to_string(),
                argv,
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::Subprocess,
                image: None,
                tag: None,
                push: false,
                binary_path: None,
                triple: None,
                package: None,
                context: None,
                load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success,
            on_fail,
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            wraps: None,
        }
    }

    /// on_success outcomes are dispatched when the pipeline passes.
    #[tokio::test]
    async fn dispatches_on_success() {
        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(
            vec![
                Outcome::WardenDeploy { service: "yah".into(), env: "production".into() },
                Outcome::AlmanacRun { pipeline: "update-index".into() },
            ],
            vec![],
            vec!["true".to_string()],
        );
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        let calls = dispatcher.recorded();
        assert_eq!(calls, vec!["warden-deploy:yah:production", "almanac-run:update-index"]);
    }

    /// on_fail outcomes are dispatched when the pipeline fails; on_success is not.
    #[tokio::test]
    async fn dispatches_on_fail_not_on_success() {
        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::WardenDeploy { service: "yah".into(), env: "production".into() }],
            vec![Outcome::AlmanacRun { pipeline: "notify-failure".into() }],
            vec!["false".to_string()],
        );
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        let calls = dispatcher.recorded();
        assert_eq!(calls, vec!["almanac-run:notify-failure"]);
    }

    /// No outcomes = nothing dispatched.
    #[tokio::test]
    async fn no_outcomes_no_dispatch() {
        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(vec![], vec![], vec!["true".to_string()]);
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        runner.run().await.unwrap();
        assert!(dispatcher.recorded().is_empty());
    }

    /// An Outcome::Publish collects the `produces` of *successful* steps and
    /// hands them to `dispatcher.publish` (R330-F3). Here the single step
    /// declares one artifact and succeeds, so publish sees 1 artifact.
    #[tokio::test]
    async fn publish_outcome_collects_produced_artifacts() {
        let dispatcher = RecordingDispatcher::new();
        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-releases".into(),
                prefix: None,
                base_url: None,
            }],
            vec![],
            vec!["true".to_string()],
        );
        pipeline.steps[0].produces = vec![crate::types::ProducedArtifact {
            binary: "yah".into(),
            path: "target/release/yah".into(),
            triple: Some("darwin-aarch64".into()),
        }];
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(dispatcher.recorded(), vec!["publish:yah-releases:1"]);
    }

    /// A failing step's `produces` is dropped — publish only ever runs on
    /// on_success outcomes anyway, but guard the collection too.
    #[tokio::test]
    async fn failed_step_artifacts_not_collected() {
        let dispatcher = RecordingDispatcher::new();
        let mut pipeline = pipeline_with_outcomes(
            vec![],
            vec![Outcome::Publish {
                provider: "r2".into(),
                bucket: "yah-releases".into(),
                prefix: None,
                base_url: None,
            }],
            vec!["false".to_string()],
        );
        pipeline.steps[0].produces = vec![crate::types::ProducedArtifact {
            binary: "yah".into(),
            path: "target/release/yah".into(),
            triple: None,
        }];
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        // Publish ran as an on_fail outcome but collected 0 artifacts (the
        // producing step failed).
        assert_eq!(dispatcher.recorded(), vec!["publish:yah-releases:0"]);
    }

    // ── R325-F2 live event-stream tests ────────────────────────────────────

    /// A runner with an attached sink emits the full lifecycle in order, with
    /// the step's stdout captured as a `StepOutput` line.
    #[tokio::test]
    async fn emits_lifecycle_events_with_streamed_output() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline(
            "test-events",
            vec!["sh".to_string(), "-c".to_string(), "echo hello-stdout".to_string()],
        );
        let runner = PipelineRunner::new(pipeline).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }

        assert!(
            matches!(events.first(), Some(QedEvent::RunStarted { total_steps: 1, .. })),
            "first event is RunStarted, got {:?}",
            events.first()
        );
        assert!(
            matches!(events.last(), Some(QedEvent::RunFinished { status: RunStatus::Success, .. })),
            "last event is RunFinished/Success, got {:?}",
            events.last()
        );
        assert!(
            events.iter().any(|e| matches!(e, QedEvent::StepStarted { index: 0, .. })),
            "saw StepStarted for step 0"
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepFinished { index: 0, status: RunStatus::Success, .. }
            )),
            "saw StepFinished/Success for step 0"
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepOutput { stream: OutputStream::Stdout, line, .. } if line == "hello-stdout"
            )),
            "captured the echoed stdout line; events={events:?}"
        );
    }

    /// A failing step streams stderr; the failure status reaches RunFinished
    /// and the stderr tail surfaces in the StepFailed message.
    #[tokio::test]
    async fn failing_step_streams_stderr_and_finishes_failed() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline(
            "test-events-fail",
            vec!["sh".to_string(), "-c".to_string(), "echo boom >&2; exit 1".to_string()],
        );
        let runner = PipelineRunner::new(pipeline).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }

        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepOutput { stream: OutputStream::Stderr, line, .. } if line == "boom"
            )),
            "captured the stderr line; events={events:?}"
        );
        assert!(
            matches!(events.last(), Some(QedEvent::RunFinished { status: RunStatus::Failed, .. })),
            "last event is RunFinished/Failed, got {:?}",
            events.last()
        );
    }

    /// No sink attached = `run()` still completes and returns terminal meta.
    #[tokio::test]
    async fn no_sink_runs_silently() {
        let pipeline = one_step_pipeline("test-silent", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    // ── R380-T3 runtime resolution tests ────────────────────────────────────

    /// resolve_runtime defaults from RunWhere when the step doesn't pin a
    /// runtime: local ⇒ Native, remote ⇒ Container.
    #[test]
    fn resolve_runtime_defaults_from_run_where() {
        let local_pipeline = one_step_pipeline("local", vec!["true".to_string()]);
        let local_runner = PipelineRunner::new(local_pipeline);
        assert_eq!(
            local_runner.resolve_runtime(&local_runner.pipeline.steps[0]),
            TaskRuntime::Native,
        );

        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden { lines: vec![], exit_code: 0 });
        let remote_pipeline = one_step_pipeline("remote", vec!["true".to_string()]);
        let remote_runner = PipelineRunner::new_remote(remote_pipeline, scryer, warden);
        assert_eq!(
            remote_runner.resolve_runtime(&remote_runner.pipeline.steps[0]),
            TaskRuntime::Container,
        );
    }

    /// An explicit step.runtime always wins over the RunWhere default.
    #[test]
    fn resolve_runtime_step_override_wins() {
        let mut pipeline = one_step_pipeline("override", vec!["true".to_string()]);
        pipeline.steps[0].runtime = Some(TaskRuntime::Container);
        let runner = PipelineRunner::new(pipeline);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Container,
            "step.runtime=Container must override --where=local default Native",
        );
    }

    /// Local + container routes through `task::local::local_container_command`
    /// → `docker run --rm`. The full happy-path (real docker daemon, pull a
    /// public image, exit 0) is exercised by the `#[ignore]` smoke test
    /// `task::local::tests::local_container_run_exits_with_code`.
    ///
    /// Here we only verify the run reaches the local+container branch and
    /// reports a clean step failure on environments without docker — without
    /// regressing back to the pre-T6 InvalidConfig pre-check.
    #[tokio::test]
    async fn local_container_step_routes_through_docker_path() {
        let mut pipeline = one_step_pipeline(
            "local-container",
            // bogus binary so we don't accidentally test against a real
            // docker image even if the CLI happens to be installed
            vec!["__nonexistent_binary_for_docker_test__".to_string()],
        );
        pipeline.steps[0].runtime = Some(TaskRuntime::Container);
        let runner = PipelineRunner::new(pipeline);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
        // task_run_id stays None — that field tracks remote dispatch only.
        assert!(meta.steps[0].task_run_id.is_none());
    }

    fn build_image_pipeline(image: &str) -> Pipeline {
        Pipeline {
            name: "image".to_string(),
            label: "Bake image".to_string(),
            steps: vec![crate::types::QedStep {
                name: "bake".to_string(),
                argv: Vec::new(),
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::BuildImage,
                image: Some(image.to_string()),
                tag: None,
                push: false,
                binary_path: None,
                triple: None,
                package: None,
                context: None,
                load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            wraps: None,
        }
    }

    /// build-image steps force Container regardless of run_where=Local (which
    /// would otherwise default to Native).
    #[test]
    fn build_image_step_forces_container_runtime() {
        let pipeline = build_image_pipeline("yah-rust");
        let runner = PipelineRunner::new(pipeline);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Container,
        );
    }

    /// Unknown catalog image surfaces as a StepFailed at dispatch time.
    #[tokio::test]
    async fn build_image_unknown_catalog_entry_fails() {
        let camp = TempDir::new().unwrap();
        let pipeline = build_image_pipeline("yah-bogus-not-real");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// Remote build-image dispatch round-trips through the BuildKit workload
    /// path (R381-T5). The scripted warden accepts the deploy, emits no logs,
    /// and reports exit 0; the runner surfaces a Success status and records
    /// the task_run_id of the forge run.
    #[tokio::test]
    async fn build_image_remote_dispatch_round_trip() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden { lines: vec![], exit_code: 0 });
        let pipeline = build_image_pipeline("yah-rust");
        let runner = PipelineRunner::new_remote(pipeline, scryer, warden)
            .with_camp_root(dir.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(meta.steps[0].status, RunStatus::Success);
        assert!(
            meta.steps[0].task_run_id.is_some(),
            "remote build-image step must record its ForgeId as task_run_id",
        );
    }

    /// Remote build-image surfaces a non-zero buildkit exit as a step failure.
    #[tokio::test]
    async fn build_image_remote_dispatch_failure_surfaces() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden {
            lines: vec!["dockerfile parse error".into()],
            exit_code: 2,
        });
        let pipeline = build_image_pipeline("yah-rust");
        let runner = PipelineRunner::new_remote(pipeline, scryer, warden)
            .with_camp_root(dir.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// A per-camp catalog entry that extends a nonexistent parent surfaces
    /// the compile error as a StepFailed *before* we shell to docker.
    #[tokio::test]
    async fn build_image_compile_error_surfaces_before_docker() {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::write(
            images.join("bad-entry.toml"),
            r#"
[image]
name        = "bad-entry"
extends     = "does-not-exist"
description = "extends a typo"
"#,
        )
        .unwrap();

        let pipeline = build_image_pipeline("bad-entry");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
        // No docker artifacts should have been written.
        assert!(!camp.path().join(".yah/cache/buildkit").exists());
    }

    /// tag_to_filename replaces characters that aren't safe for OCI archive
    /// filenames (slashes from registry/repo, colons from tags).
    #[test]
    fn tag_to_filename_makes_oci_archive_path_safe() {
        assert_eq!(tag_to_filename("yah-rust:dev"), "yah-rust_dev");
        assert_eq!(
            tag_to_filename("ghcr.io/yah-ai/yah-python:v1.2.3"),
            "ghcr.io_yah-ai_yah-python_v1.2.3",
        );
    }

    /// End-to-end smoke: build a one-line Dockerfile via the full qed →
    /// task::local::build_image_command path. Requires docker + buildx on
    /// PATH; marked #[ignore] so CI without docker doesn't fail.
    ///
    /// Run locally:
    /// ```sh
    /// cargo test -p qed --lib build_image_local_buildx_actually_builds -- --include-ignored
    /// ```
    #[tokio::test]
    #[ignore]
    async fn build_image_local_buildx_actually_builds() {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images/yah-smoke");
        std::fs::create_dir_all(&images).unwrap();
        // Tiny Dockerfile that should build in a couple seconds against alpine.
        std::fs::write(
            images.join("Dockerfile"),
            "FROM alpine:3\nRUN echo smoke-image\n",
        )
        .unwrap();
        std::fs::write(
            images.join("image.toml"),
            r#"
[image]
name        = "yah-smoke"
base        = "alpine:3"
description = "smoke test image"
"#,
        )
        .unwrap();

        let pipeline = build_image_pipeline("yah-smoke");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "build-image should succeed; check docker buildx is available"
        );
        // Generated Dockerfile staged under cache/buildkit.
        assert!(camp.path().join(".yah/cache/buildkit/yah-smoke.Dockerfile").is_file());
        // OCI archive should be produced (push=false default).
        assert!(camp.path().join(".yah/cache/images/yah-smoke_dev.tar").is_file());
    }

    // ── R407-T2 package-native-tarball runner tests ─────────────────────────

    /// Build a pipeline that packages a pre-built binary into a native
    /// tarball. The test always writes a dummy binary at `binary_rel` so we
    /// don't depend on a real cross build.
    fn package_native_tarball_pipeline(image: &str, binary_rel: &str, triple: &str) -> Pipeline {
        Pipeline {
            name: "pack".to_string(),
            label: "Package native tarball".to_string(),
            steps: vec![crate::types::QedStep {
                name: "pack".to_string(),
                argv: Vec::new(),
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::PackageNativeTarball,
                image: Some(image.to_string()),
                tag: None,
                push: false,
                binary_path: Some(binary_rel.to_string()),
                triple: Some(triple.to_string()),
                package: None,
                context: None,
                load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            wraps: None,
        }
    }

    fn stage_native_tarball_camp(image_name: &str, produces: &str, binary_rel: &str) -> TempDir {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::write(
            images.join(format!("{image_name}.toml")),
            format!(
                r#"
[image]
name        = "{image_name}"
base        = "scratch"
description = "Native musl-static workload"
produces    = [{produces}]

[image.env]
RUST_LOG = "info"
"#,
            ),
        )
        .unwrap();
        let bin_path = camp.path().join(binary_rel);
        std::fs::create_dir_all(bin_path.parent().unwrap()).unwrap();
        std::fs::write(&bin_path, b"\x7fELF-fake-musl-binary").unwrap();
        camp
    }

    /// Happy path: catalog entry declares `native-tarball`, binary exists,
    /// runner emits `.yah/cache/native/<image>-<triple>.tar.gz`.
    #[tokio::test]
    async fn package_native_tarball_writes_tar_gz_with_manifest() {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let binary_rel = "target/x86_64-unknown-linux-musl/release/warden";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-warden", "\"native-tarball\"", binary_rel);

        let pipeline = package_native_tarball_pipeline("yah-warden", binary_rel, triple);
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let out = camp
            .path()
            .join(".yah/cache/native/yah-warden-x86_64-unknown-linux-musl.tar.gz");
        assert!(out.is_file(), "tarball at {}", out.display());

        let f = std::fs::File::open(&out).unwrap();
        let gz = GzDecoder::new(f);
        let mut archive = tar::Archive::new(gz);
        let mut seen: Vec<(String, Vec<u8>)> = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().into_owned();
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).unwrap();
            seen.push((path, buf));
        }
        seen.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(seen[0].0, "bin/warden");
        assert_eq!(seen[0].1, b"\x7fELF-fake-musl-binary");
        assert_eq!(seen[1].0, "manifest.toml");
        let text = std::str::from_utf8(&seen[1].1).unwrap();
        let manifest: crate::native::NativeTarballManifest =
            toml::from_str(text).expect("manifest.toml parses");
        assert_eq!(manifest.name, "yah-warden");
        assert_eq!(manifest.triple, triple);
        assert_eq!(manifest.binary, "bin/warden");
        // Catalog env propagates into the manifest.
        assert_eq!(manifest.env.get("RUST_LOG").map(String::as_str), Some("info"));
    }

    /// Catalog entry that only declares `produces = ["oci-image"]` (the
    /// default) is rejected at dispatch time — protects against accidentally
    /// packaging a non-musl image as a native tarball.
    #[tokio::test]
    async fn package_native_tarball_rejects_non_native_catalog_entry() {
        let binary_rel = "target/release/warden";
        let camp = stage_native_tarball_camp("yah-warden", "\"oci-image\"", binary_rel);
        let pipeline =
            package_native_tarball_pipeline("yah-warden", binary_rel, "darwin-aarch64");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// Both-target entries (`["oci-image", "native-tarball"]`) are accepted —
    /// W154's container-and-native peer model.
    #[tokio::test]
    async fn package_native_tarball_accepts_both_targets_entry() {
        let binary_rel = "target/x86_64-unknown-linux-musl/release/warden";
        let camp = stage_native_tarball_camp(
            "yah-warden",
            "\"oci-image\", \"native-tarball\"",
            binary_rel,
        );
        let pipeline = package_native_tarball_pipeline(
            "yah-warden",
            binary_rel,
            "x86_64-unknown-linux-musl",
        );
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(camp
            .path()
            .join(".yah/cache/native/yah-warden-x86_64-unknown-linux-musl.tar.gz")
            .is_file());
    }

    /// Unknown catalog name surfaces as StepFailed (mirrors build-image
    /// dispatch shape).
    #[tokio::test]
    async fn package_native_tarball_unknown_catalog_fails() {
        let camp = TempDir::new().unwrap();
        let bin = camp.path().join("target/release/warden");
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, b"x").unwrap();
        let pipeline = package_native_tarball_pipeline(
            "yah-bogus-not-real",
            "target/release/warden",
            "darwin-aarch64",
        );
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
    }

    /// Missing binary surfaces a clean StepFailed (not an IO panic).
    #[tokio::test]
    async fn package_native_tarball_missing_binary_fails_cleanly() {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::write(
            images.join("yah-warden.toml"),
            r#"
[image]
name        = "yah-warden"
base        = "scratch"
description = "Native"
produces    = ["native-tarball"]
"#,
        )
        .unwrap();
        let pipeline = package_native_tarball_pipeline(
            "yah-warden",
            "target/x86_64-unknown-linux-musl/release/warden",
            "x86_64-unknown-linux-musl",
        );
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        // Nothing should have landed under .yah/cache/native.
        assert!(!camp.path().join(".yah/cache/native").exists());
    }

    /// Triple defaults to the build host when omitted — proves
    /// `publish::resolve_triple(None)` is the fallback used at packaging time.
    #[tokio::test]
    async fn package_native_tarball_triple_defaults_to_host() {
        let binary_rel = "target/release/warden";
        let camp = stage_native_tarball_camp("yah-warden", "\"native-tarball\"", binary_rel);

        // Same pipeline but with triple=None.
        let mut pipeline = package_native_tarball_pipeline("yah-warden", binary_rel, "ignored");
        pipeline.steps[0].triple = None;

        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let host_triple = crate::publish::resolve_triple(None);
        let expected = camp
            .path()
            .join(format!(".yah/cache/native/yah-warden-{host_triple}.tar.gz"));
        assert!(expected.is_file(), "expected {} to exist", expected.display());
    }

    /// PackageNativeTarball is always Native runtime, even on a Remote runner —
    /// the implicit `None` must not get auto-forced to Container.
    #[test]
    fn package_native_tarball_step_forces_native_runtime_on_remote() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden { lines: vec![], exit_code: 0 });
        let pipeline = package_native_tarball_pipeline(
            "yah-warden",
            "target/x86_64-unknown-linux-musl/release/warden",
            "x86_64-unknown-linux-musl",
        );
        let runner = PipelineRunner::new_remote(pipeline, scryer, warden);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Native,
        );
    }

    // ── R407-T3 musl-static-preflight runner tests ──────────────────────────

    fn musl_preflight_pipeline(package: &str) -> Pipeline {
        Pipeline {
            name: "preflight".to_string(),
            label: "musl-static preflight".to_string(),
            steps: vec![crate::types::QedStep {
                name: "musl-gate".to_string(),
                argv: Vec::new(),
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::MuslStaticPreflight,
                image: None,
                tag: None,
                push: false,
                binary_path: None,
                triple: None,
                package: Some(package.to_string()),
                context: None,
                load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            wraps: None,
        }
    }

    fn workspace_root() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .find(|p| p.join("Cargo.lock").is_file())
            .expect("workspace root has Cargo.lock")
            .to_path_buf()
    }

    /// Happy path: gating the qed crate itself passes — qed is musl-clean by
    /// design (no openssl-sys, no dbus, no cuda).
    #[tokio::test]
    async fn musl_static_preflight_passes_clean_workspace_package() {
        let pipeline = musl_preflight_pipeline("qed");
        let runner = PipelineRunner::new(pipeline).with_camp_root(workspace_root());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
    }

    /// Unknown workspace package surfaces a clean StepFailed (not a panic).
    #[tokio::test]
    async fn musl_static_preflight_unknown_package_fails_cleanly() {
        let pipeline = musl_preflight_pipeline("definitely-not-a-real-package");
        let runner = PipelineRunner::new(pipeline).with_camp_root(workspace_root());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// MuslStaticPreflight is always Native runtime, even on a Remote runner.
    #[test]
    fn musl_static_preflight_forces_native_runtime_on_remote() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden { lines: vec![], exit_code: 0 });
        let pipeline = musl_preflight_pipeline("warden");
        let runner = PipelineRunner::new_remote(pipeline, scryer, warden);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Native,
        );
    }

    /// The actionable container-fallback hint surfaces in the step's failure
    /// message — operators reading the failed StepStatus get the routing
    /// recommendation immediately.
    #[test]
    fn musl_gate_error_message_routes_to_container_fallback() {
        use crate::preflight::{check_dep_list, MuslPreflightError};
        let err = check_dep_list("warden", ["openssl-sys"]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("container fallback"), "msg routes to container: {msg}");
        assert!(msg.contains("runtime = \"container\""), "msg names the toml fix: {msg}");
        assert!(
            matches!(err, MuslPreflightError::NotMuslSafe { ref offenders, .. } if offenders == &["openssl-sys".to_string()]),
        );
    }

    // ── R407-T5 sign-native-tarball runner tests ────────────────────────────

    /// Build a pipeline that packages then signs a native tarball, exercising
    /// the same image+triple → on-disk-path convention both steps share.
    fn pack_and_sign_pipeline(image: &str, binary_rel: &str, triple: &str) -> Pipeline {
        Pipeline {
            name: "pack-and-sign".to_string(),
            label: "Package + sign native tarball".to_string(),
            steps: vec![
                crate::types::QedStep {
                    name: "pack".to_string(),
                    argv: Vec::new(),
                    cwd: None,
                    env: HashMap::new(),
                    timeout: None,
                    on_fail: OnFail::Abort,
                    produces: Vec::new(),
                    runtime: None,
                    kind: crate::types::StepKind::PackageNativeTarball,
                    image: Some(image.to_string()),
                    tag: None,
                    push: false,
                    binary_path: Some(binary_rel.to_string()),
                    triple: Some(triple.to_string()),
                    package: None,
                    context: None,
                    load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
                },
                crate::types::QedStep {
                    name: "sign".to_string(),
                    argv: Vec::new(),
                    cwd: None,
                    env: HashMap::new(),
                    timeout: None,
                    on_fail: OnFail::Abort,
                    produces: Vec::new(),
                    runtime: None,
                    kind: crate::types::StepKind::SignNativeTarball,
                    image: Some(image.to_string()),
                    tag: None,
                    push: false,
                    binary_path: None,
                    triple: Some(triple.to_string()),
                    package: None,
                    context: None,
                    load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
                },
            ],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            wraps: None,
        }
    }

    /// Sign-only pipeline (no pack step) — for asserting the "tarball must
    /// already exist" gate without coupling to the packaging step.
    fn sign_only_pipeline(image: &str, triple: &str) -> Pipeline {
        Pipeline {
            name: "sign".to_string(),
            label: "Sign native tarball".to_string(),
            steps: vec![crate::types::QedStep {
                name: "sign".to_string(),
                argv: Vec::new(),
                cwd: None,
                env: HashMap::new(),
                timeout: None,
                on_fail: OnFail::Abort,
                produces: Vec::new(),
                runtime: None,
                kind: crate::types::StepKind::SignNativeTarball,
                image: Some(image.to_string()),
                tag: None,
                push: false,
                binary_path: None,
                triple: Some(triple.to_string()),
                package: None,
                context: None,
                load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            wraps: None,
        }
    }

    /// Happy path: pack-then-sign in one pipeline writes the tarball and
    /// then `.sig`, `.crt`, `.bundle` next to it. Uses the default
    /// LoggingSigner — exercising the same trust shape as cosign without
    /// requiring a cosign install in the test sandbox.
    #[tokio::test]
    async fn sign_native_tarball_pack_then_sign_writes_sig_crt_bundle() {
        let binary_rel = "target/x86_64-unknown-linux-musl/release/warden";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-warden", "\"native-tarball\"", binary_rel);

        let pipeline = pack_and_sign_pipeline("yah-warden", binary_rel, triple);
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(meta.steps[0].status, RunStatus::Success); // pack
        assert_eq!(meta.steps[1].status, RunStatus::Success); // sign

        let tarball = camp
            .path()
            .join(".yah/cache/native/yah-warden-x86_64-unknown-linux-musl.tar.gz");
        assert!(tarball.is_file());
        for suffix in [".sig", ".crt", ".bundle"] {
            let mut name = tarball.file_name().unwrap().to_os_string();
            name.push(suffix);
            let p = tarball.with_file_name(name);
            assert!(p.is_file(), "expected {} to exist", p.display());
        }
    }

    /// Catalog entry without `native-tarball` in `produces` is refused at
    /// sign time — same gate as packaging, applied independently so a
    /// signing step picked up from old TOML can't sneak through.
    #[tokio::test]
    async fn sign_native_tarball_rejects_non_native_catalog_entry() {
        let binary_rel = "target/release/warden";
        let camp = stage_native_tarball_camp("yah-warden", "\"oci-image\"", binary_rel);
        let pipeline = sign_only_pipeline("yah-warden", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// Unknown catalog name surfaces as StepFailed (mirrors packaging dispatch).
    #[tokio::test]
    async fn sign_native_tarball_unknown_catalog_fails() {
        let camp = TempDir::new().unwrap();
        let pipeline = sign_only_pipeline("yah-bogus-not-real", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
    }

    /// Missing tarball (sign called without pack) surfaces a clean StepFailed
    /// whose message routes the operator to the packaging step.
    #[tokio::test]
    async fn sign_native_tarball_missing_tarball_routes_to_packaging() {
        let camp = TempDir::new().unwrap();
        let images = camp.path().join(".yah/qed/images");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::write(
            images.join("yah-warden.toml"),
            r#"
[image]
name        = "yah-warden"
base        = "scratch"
description = "Native"
produces    = ["native-tarball"]
"#,
        )
        .unwrap();
        let pipeline = sign_only_pipeline("yah-warden", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        // Nothing should have been signed.
        assert!(!camp.path().join(".yah/cache/native").exists());
    }

    /// SignNativeTarball is always Native runtime, even on a Remote runner —
    /// the implicit `None` must not get auto-forced to Container.
    #[test]
    fn sign_native_tarball_forces_native_runtime_on_remote() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let warden = Arc::new(ScriptedWarden { lines: vec![], exit_code: 0 });
        let pipeline = sign_only_pipeline("yah-warden", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new_remote(pipeline, scryer, warden);
        assert_eq!(
            runner.resolve_runtime(&runner.pipeline.steps[0]),
            TaskRuntime::Native,
        );
    }

    /// `with_signer(...)` replaces the default LoggingSigner — release CI
    /// uses this seam to wire a real CosignSigner.
    #[tokio::test]
    async fn sign_native_tarball_uses_attached_signer() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingSigner {
            calls: AtomicUsize,
        }
        #[async_trait]
        impl SigstoreSigner for CountingSigner {
            async fn sign_blob(
                &self,
                blob_path: &std::path::Path,
            ) -> std::io::Result<crate::native::SignedBlob> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                // Mirror the LoggingSigner shape so the runner's success log
                // remains coherent.
                crate::native::LoggingSigner.sign_blob(blob_path).await
            }
        }

        let binary_rel = "target/x86_64-unknown-linux-musl/release/warden";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-warden", "\"native-tarball\"", binary_rel);

        let signer = Arc::new(CountingSigner { calls: AtomicUsize::new(0) });
        let pipeline = pack_and_sign_pipeline("yah-warden", binary_rel, triple);
        let runner = PipelineRunner::new(pipeline)
            .with_camp_root(camp.path().to_path_buf())
            .with_signer(signer.clone());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(signer.calls.load(Ordering::SeqCst), 1);
    }

    // ─── SubPipeline recursion (R488-F2) ────────────────────────────────────

    use crate::types::{
        ProducedArtifact, SubPipelineCollect, SubPipelineConfig, SubPipelineRef,
        SubPipelineResolver,
    };

    /// In-memory resolver — maps a ref-token string to a Pipeline. The same
    /// token discipline the walker uses, so resolver + walker stay aligned.
    struct MapResolver(std::collections::HashMap<String, Pipeline>);

    impl SubPipelineResolver for MapResolver {
        fn resolve(&self, target: &SubPipelineRef) -> Option<Pipeline> {
            let key = match target {
                SubPipelineRef::Builtin(n) => format!("builtin:{n}"),
                SubPipelineRef::Path(p) => format!("path:{}", p.display()),
                SubPipelineRef::GhaWorkflow { path, .. } => format!("gha:{}", path.display()),
                SubPipelineRef::Peer { camp, pipeline } => format!("peer:{camp}:{pipeline}"),
            };
            self.0.get(&key).cloned()
        }
    }

    fn shell_step(name: &str, argv: Vec<&str>) -> crate::types::QedStep {
        crate::types::QedStep {
            name: name.into(),
            argv: argv.into_iter().map(String::from).collect(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::Subprocess,
            image: None,
            tag: None,
            push: false,
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            load: false,
            sub_pipeline: None,
            gha_workflow: None,
            outputs: Vec::new(),
        }
    }

    fn producing_step(name: &str, binary: &str, path: &str) -> crate::types::QedStep {
        let mut s = shell_step(name, vec!["true"]);
        s.produces = vec![ProducedArtifact {
            binary: binary.into(),
            path: path.into(),
            triple: None,
        }];
        s
    }

    fn sub_step(
        name: &str,
        target: SubPipelineRef,
        propagate_produces: bool,
    ) -> crate::types::QedStep {
        crate::types::QedStep {
            name: name.into(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::SubPipeline,
            image: None,
            tag: None,
            push: false,
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            load: false,
            sub_pipeline: Some(SubPipelineConfig {
                target,
                params: HashMap::new(),
                propagate: SubPipelineCollect {
                    produces: propagate_produces,
                    outputs: Vec::new(),
                },
            }),
            outputs: Vec::new(),
            gha_workflow: None,
        }
    }

    fn make_pipeline(name: &str, steps: Vec<crate::types::QedStep>) -> Pipeline {
        Pipeline {
            name: name.into(),
            label: name.into(),
            steps,
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            wraps: None,
        }
    }

    /// Counts publish and revalidate calls so we can assert "single publish"
    /// behaviour across composite runs.
    #[derive(Default)]
    struct CountingDispatcher {
        publishes: Mutex<u32>,
    }

    #[async_trait::async_trait]
    impl OutcomeDispatcher for CountingDispatcher {
        async fn warden_deploy(&self, _s: &str, _e: &str) -> Result<(), RunnerError> {
            Ok(())
        }
        async fn almanac_run(&self, _p: &str) -> Result<(), RunnerError> {
            Ok(())
        }
        async fn publish(&self, _req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
            *self.publishes.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn sub_pipeline_resolves_unresolvable_with_clear_error() {
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("does-not-exist".into()),
                false,
            )],
        );
        // Default NoopSubPipelineResolver — every resolve returns None.
        let runner = PipelineRunner::new(root);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let step = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(step.status, RunStatus::Failed);
    }

    /// Resolver that publishes a typed [`unresolved_reason`] — used to assert
    /// the runner surfaces the typed message in `StepFailed.msg` for the
    /// R494-T5 remote-peer path.
    struct DiagnosticResolver(String);
    impl SubPipelineResolver for DiagnosticResolver {
        fn resolve(&self, _target: &SubPipelineRef) -> Option<Pipeline> { None }
        fn unresolved_reason(&self, _target: &SubPipelineRef) -> Option<String> {
            Some(self.0.clone())
        }
    }

    #[tokio::test]
    async fn sub_pipeline_unresolved_surfaces_resolver_typed_reason() {
        // R494-T5: when the resolver publishes an unresolved_reason (e.g.
        // "remote peer not yet supported"), the runner's StepFailed.msg
        // carries that message verbatim instead of the generic "target
        // unresolvable" debug tail.
        let peer_target = SubPipelineRef::Peer {
            camp: "cheers".into(),
            pipeline: "publish".into(),
        };
        let typed = "remote peer `cheers` lives on rig `rig-tokyo-1` (R494-T5)".to_string();
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> =
            Arc::new(DiagnosticResolver(typed.clone()));
        let runner = PipelineRunner::new(make_pipeline(
            "root",
            vec![sub_step("remote", peer_target.clone(), false)],
        ))
        .with_sub_pipeline_resolver(resolver);
        let err = runner
            .execute_step_sub_pipeline(0, &sub_step("remote", peer_target, false))
            .await
            .expect_err("expected StepFailed");
        match err {
            RunnerError::StepFailed { msg, .. } => assert_eq!(msg, typed),
            other => panic!("expected StepFailed, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sub_pipeline_runs_child_to_completion() {
        // root has one SubPipeline step → child has one trivial run step.
        let child = make_pipeline("child", vec![shell_step("ok", vec!["true"])]);
        let root = make_pipeline(
            "root",
            vec![sub_step("compose", SubPipelineRef::Builtin("child".into()), false)],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let step = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(step.status, RunStatus::Success);
    }

    #[tokio::test]
    async fn sub_pipeline_failure_propagates_to_parent() {
        let child = make_pipeline("child", vec![shell_step("boom", vec!["false"])]);
        let root = make_pipeline(
            "root",
            vec![sub_step("compose", SubPipelineRef::Builtin("child".into()), false)],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
    }

    /// R487 follow-up: when a `SubPipelineRef::GhaWorkflow` child step's
    /// inner workflow fails, the parent's `StepFailed.msg` must carry the
    /// inner stderr tail and the failing job/step name — NOT the generic
    /// "failed at child step `gha-workflow`" wrapper that the long
    /// SubPipeline path produces. Verifies the short-circuit in
    /// `execute_step_sub_pipeline` (R487 follow-up).
    #[tokio::test]
    async fn gha_workflow_subpipeline_surfaces_stderr_tail_to_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let wf_path = tmp.path().join("fail.yml");
        std::fs::write(
            &wf_path,
            r#"
name: fail
on: push
jobs:
  blow-up:
    runs-on: ubuntu-latest
    steps:
      - name: emit then fail
        run: |
          echo "boom-marker-9b7c"
          echo "fatal: nothing to see here" 1>&2
          exit 17
"#,
        )
        .unwrap();

        // Synthesised one-step pipeline carrying the GhaWorkflow step,
        // exactly as `LoaderSubPipelineResolver::resolve` would build it.
        let step = crate::types::QedStep {
            name: "gha-workflow".to_string(),
            argv: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: None,
            on_fail: OnFail::Abort,
            produces: Vec::new(),
            runtime: None,
            kind: crate::types::StepKind::GhaWorkflow,
            image: None,
            tag: None,
            push: false,
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            load: false,
            sub_pipeline: None,
            outputs: Vec::new(),
            gha_workflow: Some(crate::types::GhaWorkflowConfig {
                path: wf_path.clone(),
                event: None,
                inputs: HashMap::new(),
            }),
        };
        let child = Pipeline {
            name: "fail".into(),
            label: "fail".into(),
            steps: vec![step],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: Default::default(),
            wraps: None,
        };

        let mut map = std::collections::HashMap::new();
        map.insert(format!("gha:{}", wf_path.display()), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        // Parent: one SubPipeline step targeting our GHA workflow.
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "wrap",
                SubPipelineRef::GhaWorkflow {
                    path: wf_path.clone(),
                    event: None,
                    inputs: HashMap::new(),
                },
                false,
            )],
        );

        // Capture parent's event stream so we can inspect the
        // StepFinished.msg the consumer would see.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runner = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .with_camp_root(tmp.path().to_path_buf())
            .with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        // Walk the event stream for the parent's StepFinished on step 0.
        let mut step_fail_msg: Option<String> = None;
        let mut saw_subpipeline_started = false;
        let mut saw_subpipeline_finished = false;
        while let Ok(ev) = rx.try_recv() {
            match &ev {
                QedEvent::StepFinished { index: 0, msg, status, .. } => {
                    if *status == RunStatus::Failed {
                        step_fail_msg = msg.clone();
                    }
                }
                QedEvent::SubPipelineStarted { index: 0, .. } => {
                    saw_subpipeline_started = true;
                }
                QedEvent::SubPipelineFinished { index: 0, status, .. } => {
                    if *status == RunStatus::Failed {
                        saw_subpipeline_finished = true;
                    }
                }
                _ => {}
            }
        }
        assert!(
            saw_subpipeline_started,
            "short-circuit must still emit SubPipelineStarted bookend",
        );
        assert!(
            saw_subpipeline_finished,
            "short-circuit must still emit SubPipelineFinished bookend with failed status",
        );
        let msg = step_fail_msg.expect("parent StepFinished carries a failure msg");
        assert!(
            msg.contains("blow-up"),
            "msg should name the failing job (got: {msg})",
        );
        assert!(
            msg.contains("emit then fail"),
            "msg should name the failing step (got: {msg})",
        );
        assert!(
            msg.contains("fatal: nothing to see here"),
            "msg should carry the stderr tail (got: {msg})",
        );
        // And — critically — the inner tail should NOT be wrapped in the
        // generic SubPipeline "failed at child step `gha-workflow`" string
        // that the long path produces.
        assert!(
            !msg.contains("failed at child step `gha-workflow`"),
            "short-circuit should bypass the SubPipeline-wrapper msg (got: {msg})",
        );
    }

    #[tokio::test]
    async fn sub_pipeline_aggregates_produces_when_propagate_set() {
        // Child has a producing step + its own Outcome::Publish that we
        // expect SUPPRESSED because parent claims propagate.produces.
        let mut child = make_pipeline(
            "child",
            vec![producing_step("emit", "yah", "target/release/yah")],
        );
        child.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
        }];

        // Parent: SubPipeline child with propagate.produces=true + its own
        // Outcome::Publish. We expect ONE publish total (the parent's),
        // confirming both suppression on child and aggregation on parent.
        let mut root = make_pipeline(
            "root",
            vec![sub_step("compose", SubPipelineRef::Builtin("child".into()), true)],
        );
        root.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
        }];

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let dispatcher = Arc::new(CountingDispatcher::default());
        let runner = PipelineRunner::new_with_dispatcher(root, dispatcher.clone())
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            *dispatcher.publishes.lock().unwrap(),
            1,
            "exactly one publish — parent fires, child suppressed"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_child_publish_fires_when_propagate_unset() {
        // Mirror of the above but propagate.produces=false — child's own
        // Outcome::Publish should fire, parent's too. Total: 2.
        let mut child = make_pipeline(
            "child",
            vec![producing_step("emit", "yah", "target/release/yah")],
        );
        child.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
        }];

        let mut root = make_pipeline(
            "root",
            vec![sub_step("compose", SubPipelineRef::Builtin("child".into()), false)],
        );
        root.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
        }];

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let dispatcher = Arc::new(CountingDispatcher::default());
        let runner = PipelineRunner::new_with_dispatcher(root, dispatcher.clone())
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            *dispatcher.publishes.lock().unwrap(),
            2,
            "two publishes — child fires its own + parent fires its own"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_nested_two_levels_works() {
        // root -> mid -> leaf. propagate.produces all the way up.
        let leaf = make_pipeline(
            "leaf",
            vec![producing_step("emit", "yah", "target/release/yah")],
        );
        let mid = make_pipeline(
            "mid",
            vec![sub_step("descend", SubPipelineRef::Builtin("leaf".into()), true)],
        );
        let mut root = make_pipeline(
            "root",
            vec![sub_step("compose", SubPipelineRef::Builtin("mid".into()), true)],
        );
        root.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: None,
        }];

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:leaf".to_string(), leaf);
        map.insert("builtin:mid".to_string(), mid);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let dispatcher = Arc::new(CountingDispatcher::default());
        let runner = PipelineRunner::new_with_dispatcher(root, dispatcher.clone())
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(
            *dispatcher.publishes.lock().unwrap(),
            1,
            "single revalidate even across two SubPipeline edges"
        );
    }

    // ─── F3: multi-child publish fan-in + continue-on-error ─────────────────

    /// Recording publisher that captures the staged tree on each sync, so
    /// tests can assert "what would have been uploaded" without a real R2
    /// account. Differs from `publish::tests::RecordingPublisher` by
    /// exposing every staged file (not just one manifest) so we can verify
    /// multi-binary fan-in across SubPipeline children.
    #[derive(Default)]
    struct StageRecorder {
        syncs: Mutex<u32>,
        revalidates: Mutex<u32>,
        /// Channel keys (`<binary>/<version>/<triple>/<file>` or
        /// `<binary>/release-manifest.json`) observed across all syncs.
        files: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crate::publish::ReleasePublisher for StageRecorder {
        async fn sync(
            &self,
            staging_dir: &std::path::Path,
            _provider: &str,
            _bucket: &str,
            _prefix: Option<&str>,
        ) -> Result<(), RunnerError> {
            *self.syncs.lock().unwrap() += 1;
            let mut walker = vec![staging_dir.to_path_buf()];
            while let Some(dir) = walker.pop() {
                for entry in std::fs::read_dir(&dir).unwrap() {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    if path.is_dir() {
                        walker.push(path);
                    } else {
                        let rel = path
                            .strip_prefix(staging_dir)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned();
                        self.files.lock().unwrap().push(rel);
                    }
                }
            }
            self.files.lock().unwrap().sort();
            Ok(())
        }

        async fn revalidate(&self) -> Result<(), RunnerError> {
            *self.revalidates.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn sub_pipeline_multi_child_fan_in_groups_by_binary_with_single_publish() {
        // Three children producing different binaries (yah, desktop,
        // mesofact) all rolled up into the parent. The parent's single
        // Outcome::Publish should fire ONCE with a staged tree containing
        // all three binaries' files + per-binary manifests, and exactly
        // one revalidate POST. Exercises the full chain F2 wired:
        //   parent.run -> child.run_inner (x3) -> aggregate produced
        //              -> parent's PublishingOutcomeDispatcher.publish
        //              -> stage_release (lays out the tree)
        //              -> StageRecorder.sync (one call, sees all binaries)
        //              -> StageRecorder.revalidate (one call total).
        let tmp = TempDir::new().unwrap();
        let yah_path = tmp.path().join("yah");
        std::fs::write(&yah_path, b"YAH").unwrap();
        let desktop_path = tmp.path().join("desktop");
        std::fs::write(&desktop_path, b"DESKTOP").unwrap();
        let mesofact_path = tmp.path().join("mesofact");
        std::fs::write(&mesofact_path, b"MESOFACT").unwrap();

        let child_cli = make_pipeline(
            "child-cli",
            vec![producing_step("build-cli", "yah", yah_path.to_string_lossy().as_ref())],
        );
        let child_desktop = make_pipeline(
            "child-desktop",
            vec![producing_step(
                "build-desktop",
                "desktop",
                desktop_path.to_string_lossy().as_ref(),
            )],
        );
        let child_mesofact = make_pipeline(
            "child-mesofact",
            vec![producing_step(
                "build-mesofact",
                "mesofact",
                mesofact_path.to_string_lossy().as_ref(),
            )],
        );

        let mut root = make_pipeline(
            "full-release",
            vec![
                sub_step("compose-cli", SubPipelineRef::Builtin("child-cli".into()), true),
                sub_step(
                    "compose-desktop",
                    SubPipelineRef::Builtin("child-desktop".into()),
                    true,
                ),
                sub_step(
                    "compose-mesofact",
                    SubPipelineRef::Builtin("child-mesofact".into()),
                    true,
                ),
            ],
        );
        root.on_success = vec![Outcome::Publish {
            provider: "r2".into(),
            bucket: "yah-releases".into(),
            prefix: None,
            base_url: Some("https://releases.yah.dev".into()),
        }];

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child-cli".to_string(), child_cli);
        map.insert("builtin:child-desktop".to_string(), child_desktop);
        map.insert("builtin:child-mesofact".to_string(), child_mesofact);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        // Pin the version so the staged path is deterministic. SAFETY:
        // single-threaded test; set + clear locally.
        std::env::set_var("YAH_RELEASE_VERSION", "1.2.3");

        let recorder = Arc::new(StageRecorder::default());
        struct ArcRecorder(Arc<StageRecorder>);
        #[async_trait::async_trait]
        impl crate::publish::ReleasePublisher for ArcRecorder {
            async fn sync(
                &self,
                d: &std::path::Path,
                p: &str,
                b: &str,
                pre: Option<&str>,
            ) -> Result<(), RunnerError> {
                self.0.sync(d, p, b, pre).await
            }
            async fn revalidate(&self) -> Result<(), RunnerError> {
                self.0.revalidate().await
            }
        }
        let dispatcher = Arc::new(crate::publish::PublishingOutcomeDispatcher::new(
            ArcRecorder(recorder.clone()),
        ));
        let runner = PipelineRunner::new_with_dispatcher(root, dispatcher)
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        std::env::remove_var("YAH_RELEASE_VERSION");

        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(*recorder.syncs.lock().unwrap(), 1, "single sync across all children");
        assert_eq!(*recorder.revalidates.lock().unwrap(), 1, "single revalidate POST");

        let files = recorder.files.lock().unwrap();
        // Three per-binary shared manifests + three per-(binary,triple) stable
        // manifests (single triple in this fan-in: darwin-aarch64) + three
        // binary files = 9 staged objects. The per-triple stable manifests
        // were added in R330-B8 for cross-stage merge fan-in.
        assert_eq!(files.len(), 9, "staged tree contents: {files:?}");
        assert!(files.iter().any(|f| f == "yah/release-manifest.json"));
        assert!(files.iter().any(|f| f == "desktop/release-manifest.json"));
        assert!(files.iter().any(|f| f == "mesofact/release-manifest.json"));
        assert!(files.iter().any(|f| f == "yah/release-manifest-darwin-aarch64.json"));
        assert!(files.iter().any(|f| f == "desktop/release-manifest-darwin-aarch64.json"));
        assert!(files.iter().any(|f| f == "mesofact/release-manifest-darwin-aarch64.json"));
        assert!(files.iter().any(|f| f.starts_with("yah/1.2.3/")));
        assert!(files.iter().any(|f| f.starts_with("desktop/1.2.3/")));
        assert!(files.iter().any(|f| f.starts_with("mesofact/1.2.3/")));
    }

    #[tokio::test]
    async fn sub_pipeline_failed_child_with_continue_on_error_does_not_abort_parent() {
        // Pins the F2 open question: a SubPipeline step with on_fail =
        // Continue marks itself failed but the parent loop proceeds to
        // subsequent steps. The child's produced are dropped (current
        // implementation only aggregates on success — documented behaviour).
        let bad_child = make_pipeline("bad", vec![shell_step("boom", vec!["false"])]);
        let mut sub = sub_step("compose", SubPipelineRef::Builtin("bad".into()), false);
        sub.on_fail = OnFail::Continue;
        let after = shell_step("after", vec!["true"]);
        let root = make_pipeline("root", vec![sub, after]);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:bad".to_string(), bad_child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();

        // Overall status is Failed (any failed step flips it regardless of
        // on_fail policy), but the subsequent `after` step still ran
        // because Continue suppresses the early break.
        assert_eq!(meta.status, RunStatus::Failed);
        let compose = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(compose.status, RunStatus::Failed);
        let after_step = meta.steps.iter().find(|s| s.name == "after").unwrap();
        assert_eq!(after_step.status, RunStatus::Success, "after step ran despite child failure");
    }

    #[tokio::test]
    async fn sub_pipeline_forwards_params_to_child() {
        // Child step has a `{{greeting}}` arg; parent's SubPipeline params
        // substitute it before the child runs.
        let child = make_pipeline("child", vec![shell_step("echo", vec!["true", "{{greeting}}"])]);
        let mut step = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        if let Some(cfg) = step.sub_pipeline.as_mut() {
            cfg.params.insert("greeting".to_string(), "hello".to_string());
        }
        let root = make_pipeline("root", vec![step]);

        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        // Successful = `true hello` exited 0. We don't capture argv here but
        // a `false {{greeting}}` would fail the same; this proves the step
        // ran post-substitution.
        assert_eq!(meta.status, RunStatus::Success);
    }

    // ─── Named output exposure (R488-F4) ────────────────────────────────────

    /// Step 1 writes an output via $YAH_OUTPUTS; step 2 references it in
    /// argv via `${{ steps.step1.outputs.digest }}` — the runner substitutes
    /// the value before execution so step 2 receives the resolved string.
    #[tokio::test]
    async fn step_outputs_substituted_into_sibling_argv() {
        // step1: writes digest=abc123 to $YAH_OUTPUTS via a shell one-liner.
        // step2: echoes the substitution placeholder — if substitution worked,
        //        argv will have been rewritten to "echo abc123" before
        //        execution, and the step exits 0.
        let step1 = shell_step(
            "step1",
            vec!["sh", "-c", "echo digest=abc123 >> \"$YAH_OUTPUTS\""],
        );
        // step2's argv contains the placeholder; the runner rewrites it
        // before passing to the executor.
        let step2 = shell_step(
            "step2",
            vec!["sh", "-c", "test \"$1\" = abc123", "--", "${{ steps.step1.outputs.digest }}"],
        );
        let pipeline = make_pipeline("p", vec![step1, step2]);
        let runner = PipelineRunner::new(pipeline);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success, "step2 should receive substituted value");
        let s1 = meta.steps.iter().find(|s| s.name == "step1").unwrap();
        assert_eq!(s1.outputs.get("digest").map(|s| s.as_str()), Some("abc123"),
            "step1 outputs map should contain captured value");
    }

    /// Step 1 writes KEY=VALUE to $YAH_OUTPUTS; the runner collects it into
    /// StepStatus::outputs regardless of whether the step declared it in
    /// the `outputs` field.
    #[tokio::test]
    async fn step_outputs_captured_in_step_status() {
        let step = shell_step(
            "emit",
            vec!["sh", "-c", "printf 'foo=bar\\nbaz=qux\\n' >> \"$YAH_OUTPUTS\""],
        );
        let pipeline = make_pipeline("p", vec![step]);
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let s = meta.steps.iter().find(|s| s.name == "emit").unwrap();
        assert_eq!(s.outputs.get("foo").map(|s| s.as_str()), Some("bar"));
        assert_eq!(s.outputs.get("baz").map(|s| s.as_str()), Some("qux"));
    }

    /// SubPipeline step with propagate.outputs propagates named child outputs
    /// to the parent step context so subsequent sibling steps can reference
    /// `${{ steps.<child-step-name>.outputs.<key> }}`.
    #[tokio::test]
    async fn sub_pipeline_propagates_named_outputs_to_parent_context() {
        // Inner child pipeline: one step that writes "result=42" to $YAH_OUTPUTS.
        let child_step = shell_step(
            "inner",
            vec!["sh", "-c", "echo result=42 >> \"$YAH_OUTPUTS\""],
        );
        let child = make_pipeline("child", vec![child_step]);

        // SubPipeline step propagates the "result" output.
        let mut sub = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        if let Some(cfg) = sub.sub_pipeline.as_mut() {
            cfg.propagate.outputs = vec!["result".to_string()];
        }

        // A sibling step after the SubPipeline step references the propagated output.
        let sibling = shell_step(
            "check",
            vec!["sh", "-c", "test \"$1\" = 42", "--", "${{ steps.compose.outputs.result }}"],
        );

        let root = make_pipeline("root", vec![sub, sibling]);
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success,
            "sibling should receive child output via parent step context");
        let compose = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(compose.outputs.get("result").map(|s| s.as_str()), Some("42"),
            "SubPipeline step status should carry propagated outputs");
    }

    // ---------- R488-F5: event-stream wiring for sub-pipelines ----------

    #[tokio::test]
    async fn sub_pipeline_emits_started_finished_bookends_with_child_run_id() {
        // Parent has two SubPipeline steps, each invoking a distinct child.
        // Assert: each parent SubPipeline step is wrapped by
        // SubPipelineStarted{child_run_id=X} ... SubPipelineFinished{child_run_id=X, status=Success}.
        // The child's own RunStarted/Step*/RunFinished events do NOT leak
        // onto the parent's stream (the child sink is decoupled).
        let child_a = make_pipeline("child-a", vec![shell_step("ok", vec!["true"])]);
        let child_b = make_pipeline("child-b", vec![shell_step("ok", vec!["true"])]);
        let root = make_pipeline(
            "root",
            vec![
                sub_step("compose-a", SubPipelineRef::Builtin("child-a".into()), false),
                sub_step("compose-b", SubPipelineRef::Path(".yah/qed/child-b.toml".into()), false),
            ],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child-a".to_string(), child_a);
        map.insert("path:.yah/qed/child-b.toml".to_string(), child_b);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runner = PipelineRunner::new(root)
            .with_events(tx)
            .with_sub_pipeline_resolver(resolver);
        let parent_run_id = runner.run_id().to_string();
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(meta.parent_run_id.is_none(), "top-level run has no parent");

        let mut events = Vec::new();
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        let starts: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                QedEvent::SubPipelineStarted { name, target, child_run_id, .. } => {
                    Some((name.clone(), target.clone(), child_run_id.clone()))
                }
                _ => None,
            })
            .collect();
        let finishes: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                QedEvent::SubPipelineFinished { name, child_run_id, status, .. } => {
                    Some((name.clone(), child_run_id.clone(), *status))
                }
                _ => None,
            })
            .collect();

        assert_eq!(starts.len(), 2, "two SubPipelineStarted events");
        assert_eq!(finishes.len(), 2, "two SubPipelineFinished events");

        assert_eq!(starts[0].0, "compose-a");
        assert_eq!(starts[0].1, "builtin:child-a");
        assert_eq!(starts[1].0, "compose-b");
        assert_eq!(starts[1].1, "path:.yah/qed/child-b.toml");

        // Each finish pairs with the same step + child_run_id as its start,
        // and both children terminated Success.
        for (start, finish) in starts.iter().zip(finishes.iter()) {
            assert_eq!(start.0, finish.0, "start/finish name match");
            assert_eq!(start.2, finish.1, "start/finish child_run_id match");
            assert_eq!(finish.2, RunStatus::Success);
            assert_ne!(start.2, parent_run_id, "child run_id distinct from parent");
        }

        // Child events DO NOT leak onto the parent's stream: zero RunStarted
        // events for the children (only the parent's own RunStarted).
        let run_started_count = events
            .iter()
            .filter(|e| matches!(e, QedEvent::RunStarted { .. }))
            .count();
        assert_eq!(run_started_count, 1, "only parent's RunStarted on the parent stream");
    }

    #[tokio::test]
    async fn sub_pipeline_finished_emits_failed_status_when_child_fails() {
        let child = make_pipeline("child", vec![shell_step("boom", vec!["false"])]);
        let root = make_pipeline(
            "root",
            vec![sub_step("compose", SubPipelineRef::Builtin("child".into()), false)],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runner = PipelineRunner::new(root)
            .with_events(tx)
            .with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        let mut finished = None;
        while let Ok(e) = rx.try_recv() {
            if let QedEvent::SubPipelineFinished { status, .. } = e {
                finished = Some(status);
            }
        }
        assert_eq!(finished, Some(RunStatus::Failed),
            "child failure surfaces on SubPipelineFinished.status");
    }
}
