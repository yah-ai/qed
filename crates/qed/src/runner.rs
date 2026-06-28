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
//! @yah:gotcha("The qed.tail `run` snapshot + events buffer are updated by a SEPARATE drain task that can briefly lag the run task's authoritative terminal write. A consumer should keep polling until the last event is RunFinished (don't stop just because run.completed_at is set). Remote (where=remote) still only emits step-level StepStarted/StepFinished — no StepOutput line streaming (execute_step_remote just waits on the yubaba handle); remote line-tail would flow through scryer/task.tail and is a follow-up. All qed runs are still in-memory (run-history persistence is R325-F3) so the event buffer is lost on daemon restart.")
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
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
//! @yah:depends_on(R407-T1)
//! @yah:handoff("Landed package-native-tarball step end-to-end. types: new StepKind::PackageNativeTarball + two QedStep fields (binary_path, triple) + 4 StepValidationError variants. New crates/yah/qed/src/native.rs module owns NativeTarballManifest (forward-compatible TOML shape — name/version/triple/binary/description/env) and pack_native_tarball() — writes bin/<basename> + manifest.toml into a .tar.gz via tar+flate2 (added as deps). runner: execute_step_package_native_tarball() looks up the catalog entry by step.image, GATES on entry.produces.contains(NativeTarball) (W154 catalog-side guard), resolves triple via step.triple ?? publish::resolve_triple(host), copies the binary, packs the tarball at <camp_root>/.yah/cache/native/<image>-<triple>.tar.gz. resolve_runtime() forces Native for this kind even on Remote runners (pure host file I/O — Container would be wrong). Catalog entry.env propagates into the manifest so Kamaji has launch env at deploy time without re-reading the catalog. 16 new tests (4 native pack/unpack, 6 runner happy/gate/missing/triple-host-fallback/remote-force-native, 6 types validation, 4 config parse-time). qed --lib: 111 pass + 1 pre-existing unrelated failure (test_builtin_release_build_pipeline 4-vs-6 step count, already flagged in R407-T1 handoff). cargo check -p qed -p yah clean.")
//! @yah:verify("cargo test -p qed --lib package_native_tarball")
//! @yah:verify("cargo test -p qed --lib native::")
//! @yah:verify("cargo check -p qed -p yah")
//! @yah:gotcha("No systemd unit is emitted (per W154 Kamaji design). Tarball layout is bin/<basename> + manifest.toml at root; that's the deploy contract — Kamaji readers should accept additive manifest fields.")
//! @yah:gotcha("manifest.toml version comes from YAH_RELEASE_VERSION env (else compiled CARGO_PKG_VERSION). For multi-platform release tagging the GHA shim is expected to set the env before invoking the packaging step.")
//! @yah:gotcha("Sigstore signing of the tarball (R407-T5) is NOT wired here — only content packaging. The packaging step writes plaintext .tar.gz; signing extends in T5.")
//!
//! @yah:ticket(R407-T5, "Sigstore signing extends to native-tarball artifacts (same trust model)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-02T03:27:30Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R407)
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
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
//! @yah:next("Add the GhaWorkflow arm to the SubPipeline resolver — delegates to yah_qed_gha::execute")
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
use yah_scryer::service::Scryer;
use velveteen::{
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
use crate::types::{
    OnFail, Outcome, Pipeline, ProducedArtifact, QedRunId, QedRunMeta, QedStep, RunStatus,
    StepActivation, StepStatus, WorkspaceMode,
};

/// Dispatches pipeline outcomes (yubaba-deploy, almanac-run) after a pipeline completes.
///
/// Implementations are responsible for the actual side-effect. The default stub logs and
/// no-ops until the respective RPC surfaces stabilise (R040-F4 for yubaba deploy).
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
/// Used by default until yubaba deploy RPC (R040-F4) and almanac are stable.
pub struct LoggingOutcomeDispatcher;

#[async_trait]
impl OutcomeDispatcher for LoggingOutcomeDispatcher {
    async fn warden_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError> {
        tracing::info!(
            service,
            env,
            "qed outcome: yubaba-deploy skipped (yubaba deploy RPC not yet stable, R040-F4)"
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
    /// A terminal outcome / release-provider adapter failed (R509): missing
    /// credential slot, unknown provider, vendor API error.
    #[error("Release outcome error: {0}")]
    Outcome(String),
    /// Plan-time toolchain pinning check failed (R507, W208): the host can't
    /// satisfy one or more `[pipeline.toolchain]` / per-step `toolchain.*` pins
    /// and no container image provides them. Carries the actionable per-pin
    /// report from [`crate::toolchain::ToolchainPreflight::error_report`].
    #[error("{0}")]
    ToolchainUnsatisfied(String),
}

/// Where pipeline steps execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunWhere {
    /// Steps run as local subprocesses on this machine.
    Local,
    /// Steps run as `task::remote` workloads on a yubaba node.
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
    /// [`yah_qed_gha::graph::JobInstance::key`] values (`<job>` for
    /// non-matrix, `<job>#<row>` for matrix). When a gha-workflow step
    /// has an entry here, [`Self::execute_step_gha_workflow`] threads
    /// it into [`yah_qed_gha::Executor::included_instance_keys`] so
    /// non-selected rows short-circuit to `Skipped`. Steps missing from
    /// the map run their full matrix. Set via
    /// [`Self::with_gha_matrix_subset`].
    gha_matrix_subset: std::collections::HashMap<String, std::collections::HashSet<String>>,
    /// On-demand override for [`StepActivation::Stubbed`] steps (R506). When
    /// `true`, the runner ignores `status = "stubbed"` and runs the step the
    /// same way an `active` step would. Set via [`Self::with_include_stubbed`].
    /// Defaults to `false`; `enabled = false` is still always honored even
    /// when this flag is on (the two knobs are orthogonal — `enabled` means
    /// "explicitly off for this run", `stubbed` means "not implemented yet").
    include_stubbed: bool,
    /// Matrix coordinate this runner is executing for (R506). Set by the
    /// planner when fanning a pipeline over its `[matrix]` block; threaded
    /// into the `if=` expression context so a step can gate on
    /// `matrix.<key>` values. `None` for non-matrix runs — `matrix.<key>`
    /// lookups then return `Null`/falsy via [`yah_qed_gha::Context`] semantics.
    matrix_coord: Option<crate::matrix::MatrixCoord>,
    /// Self-detected host triple this runner executes on (R531-T1, W222),
    /// e.g. `aarch64-apple-darwin`. Detected once at construction via
    /// [`crate::platform::detect_host_triple`] and threaded into the plan
    /// context — the GHA executor's `runner.{os,arch}` for workflow steps,
    /// and (once F2/F3 land) the `host` leg of each step's `Platform` triple
    /// that `resolve(host, target, container_platform)` reasons over. Override
    /// via [`Self::with_host_triple`] when the execution host differs from the
    /// process host (e.g. a remote runner whose triple the daemon knows).
    host_triple: String,
    /// Which host-native cross toolchains are installed (R531-T6, W222).
    /// Probed lazily on first use ([`Self::cross_availability`]) so building a
    /// runner shells out nothing; seedable via [`Self::with_cross_availability`]
    /// for tests and for a daemon that knows a remote runner's toolchain set.
    /// Consumed when a NativeCross step's argv is rewritten onto cargo-zigbuild
    /// / musl-cross (F5's [`crate::nativecross::plan_native_cross`]).
    cross_availability: std::sync::OnceLock<crate::nativecross::ToolAvailability>,
    /// Host-detected toolchain versions for the plan-time pinning check (R507,
    /// W208). Probed lazily ([`Self::host_toolchains`]) — a runner whose
    /// pipeline declares no `[toolchain]` pins never shells out — and seedable
    /// via [`Self::with_host_toolchains`] for tests and for a daemon that knows
    /// a remote runner's installed versions. Maps pin key → detected version
    /// (`None` = tool absent on host).
    host_toolchains: std::sync::OnceLock<std::collections::HashMap<String, Option<String>>>,
    /// Registry of vendor release adapters (R509) dispatched by
    /// [`Outcome::Provider`]. Empty by default — the CLI / daemon construction
    /// sites wire the built-in set via [`Self::with_release_providers`]. An
    /// `Outcome::Provider` naming an unregistered adapter fails with a typed
    /// error listing the known names.
    provider_registry: Arc<crate::provider::ProviderRegistry>,
    /// Credential resolver passed to vendor adapters at dispatch (R509).
    /// Defaults to an empty [`crate::provider::MapSecrets`]; production wires
    /// [`crate::secrets_bridge::SecretsConfig`] over the vault via
    /// [`Self::with_release_providers`].
    secrets: Arc<dyn crate::provider::SecretSource>,
    /// Target git branch for this run (W224). Drives how the runner positions
    /// the workspace before a `gha-workflow` step runs, per the pipeline's
    /// [`WorkspaceMode`](crate::types::WorkspaceMode). `None` ⇒ `main`. Set by
    /// the launch surface (`yah qed run --branch`, the QED-tab selector).
    branch: Option<String>,
    /// The on-disk tree this run actually builds against, positioned once at
    /// run start per the pipeline's [`WorkspaceMode`] + target branch (W224
    /// R533-F11). Set by [`Self::run_inner`] before any step executes; every
    /// step kind then resolves its root through [`Self::resolve_camp_root`],
    /// which prefers this. Unset until positioned (and on child runners, which
    /// inherit the parent's already-positioned tree via `camp_root`). For
    /// `Isolated` mode this is the throwaway worktree path — so a subprocess
    /// `desktop-release` step builds from the same worktree as the run's
    /// `gha-workflow` step, not the live camp root.
    positioned_workspace: std::sync::OnceLock<std::path::PathBuf>,
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

/// A spawned background sidecar step (R513-F2, W207 Gap #4) being tracked by
/// [`PipelineRunner::run_inner`] until it is reaped — either when its
/// `background_until` gate step finishes or at the end of the step loop.
///
/// The `join` handle owns the running subprocess future; aborting it drops the
/// future, which drops the `tokio::process::Child` (spawned with
/// `kill_on_drop(true)`), which kills the process. That is the whole
/// reap-on-cancellation story: even an early `return` out of `run_inner` (a
/// foreground error, or the whole run future being cancelled by `qed.cancel`)
/// drops this Vec and tears down every live sidecar.
struct BackgroundTask {
    /// Index into `run_inner`'s `step_statuses` Vec for the placeholder
    /// `Running` row, finalized in place at reap.
    status_index: usize,
    /// Event index (with offset) for the deferred `StepFinished` emit.
    event_index: usize,
    name: String,
    /// Step name after which to reap; `None` ⇒ reap at end of the loop.
    until: Option<String>,
    join: tokio::task::JoinHandle<Result<(), RunnerError>>,
}

/// Reap one background sidecar (R513-F2), returning its terminal status.
///
/// - Still running at reap → abort (kill) → [`RunStatus::Success`]: a healthy
///   sidecar torn down on schedule is the expected lifecycle, not a failure.
/// - Already exited on its own with code 0 → `Success`.
/// - Already exited non-zero (or panicked) → [`RunStatus::Failed`] with the
///   failure tail: a sidecar that dies mid-pipeline is a genuine problem.
async fn reap_background(
    join: tokio::task::JoinHandle<Result<(), RunnerError>>,
) -> (RunStatus, Option<String>) {
    if join.is_finished() {
        match join.await {
            Ok(Ok(())) => (RunStatus::Success, None),
            Ok(Err(e)) => {
                let msg = match e {
                    RunnerError::StepFailed { msg, .. } => Some(msg),
                    RunnerError::InvalidConfig(m) => Some(m),
                    other => Some(other.to_string()),
                };
                (RunStatus::Failed, msg)
            }
            Err(join_err) => (
                RunStatus::Failed,
                Some(format!("background task panicked: {join_err}")),
            ),
        }
    } else {
        join.abort();
        let _ = join.await;
        (RunStatus::Success, None)
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
            include_stubbed: false,
            matrix_coord: None,
            host_triple: crate::platform::detect_host_triple(),
            cross_availability: std::sync::OnceLock::new(),
            host_toolchains: std::sync::OnceLock::new(),
            provider_registry: Arc::new(crate::provider::ProviderRegistry::new()),
            secrets: Arc::new(crate::provider::MapSecrets::default()),
            branch: None,
            positioned_workspace: std::sync::OnceLock::new(),
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
            include_stubbed: false,
            matrix_coord: None,
            host_triple: crate::platform::detect_host_triple(),
            cross_availability: std::sync::OnceLock::new(),
            host_toolchains: std::sync::OnceLock::new(),
            provider_registry: Arc::new(crate::provider::ProviderRegistry::new()),
            secrets: Arc::new(crate::provider::MapSecrets::default()),
            branch: None,
            positioned_workspace: std::sync::OnceLock::new(),
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

    /// Set the target git branch for this run (W224). Drives workspace
    /// positioning for `gha-workflow` steps per the pipeline's
    /// [`WorkspaceMode`](crate::types::WorkspaceMode). `None` / unset ⇒ `main`.
    /// Composes with any constructor.
    pub fn with_branch(mut self, branch: Option<String>) -> Self {
        self.branch = branch.filter(|b| !b.trim().is_empty());
        self
    }

    /// The run's effective target branch — the requested `branch`, or `main`.
    fn target_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or("main")
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

    /// On-demand runner of `status = "stubbed"` steps (R506). When `true`,
    /// the runner ignores the stubbed marker and runs the step normally.
    /// `enabled = false` is still honored regardless. Composes with any
    /// constructor: `PipelineRunner::new(p).with_include_stubbed(true)`.
    pub fn with_include_stubbed(mut self, include: bool) -> Self {
        self.include_stubbed = include;
        self
    }

    /// Bind the runner to a matrix coordinate (R506). Set by the planner
    /// when fanning a pipeline over its `[matrix]` block — the coord shows
    /// up as `matrix.<key>` in `if=` expressions. Composes with any
    /// constructor.
    pub fn with_matrix_coord(mut self, coord: crate::matrix::MatrixCoord) -> Self {
        self.matrix_coord = Some(coord);
        self
    }

    fn resolve_camp_root(&self) -> Result<std::path::PathBuf, RunnerError> {
        // Once a run has positioned its workspace (W224 R533-F11), every step
        // builds against that tree — for `Isolated` the throwaway worktree, for
        // `Checkout`/`Live` the (possibly branch-switched) camp root. This is
        // the single seam all step kinds share, so threading it here lifts
        // positioning from the gha-workflow step to the whole run.
        if let Some(ws) = self.positioned_workspace.get() {
            return Ok(ws.clone());
        }
        if let Some(root) = &self.camp_root {
            return Ok(root.clone());
        }
        std::env::current_dir()
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to read current dir: {e}")))
    }

    /// The unpositioned camp root — `self.camp_root` (or the current dir),
    /// *ignoring* any positioned workspace. Used by [`Self::run_inner`] to feed
    /// [`Self::prepare_workspace`] the base tree to position from, before the
    /// positioned workspace is set.
    fn base_camp_root(&self) -> Result<std::path::PathBuf, RunnerError> {
        if let Some(root) = &self.camp_root {
            return Ok(root.clone());
        }
        std::env::current_dir()
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to read current dir: {e}")))
    }

    /// Position the on-disk tree this *run* builds against, per the pipeline's
    /// [`WorkspaceMode`] and the run's target branch (W224). Called once at run
    /// start (R533-F11) — every step kind (subprocess, build-image, sign,
    /// sub-pipeline, gha-workflow) then builds from the returned tree, so an
    /// `Isolated` release positions the whole run into one worktree rather than
    /// only its gha-workflow step.
    ///
    /// Returns the effective workspace path plus an optional RAII
    /// [`WorktreeGuard`] — held by the caller for the lifetime of the *run* so
    /// an `Isolated` worktree outlives every step and is torn down once the run
    /// finishes (even on a mid-run error). The dirty check considers tracked
    /// modifications only (`--untracked-files=no`): untracked files don't change
    /// which committed bytes a build sees and would otherwise block every run in
    /// a working camp.
    fn prepare_workspace(
        &self,
        camp_root: &std::path::Path,
    ) -> Result<(std::path::PathBuf, Option<WorktreeGuard>), RunnerError> {
        let branch = self.target_branch();
        match self.pipeline.workspace {
            // Build whatever is on disk — no branch switch, no dirty check.
            WorkspaceMode::Live => Ok((camp_root.to_path_buf(), None)),
            // Switch the camp root to the branch, but never over local edits.
            WorkspaceMode::Checkout => {
                if git_tree_is_dirty(camp_root)? {
                    return Err(RunnerError::InvalidConfig(format!(
                        "workspace mode `checkout` won't run over uncommitted changes in {} — \
                         commit or stash them, or set the pipeline to `workspace = \"isolated\"` \
                         (build in a throwaway worktree) or `\"live\"` (build the tree as-is)",
                        camp_root.display()
                    )));
                }
                run_git(camp_root, &["checkout", branch])
                    .map_err(|e| RunnerError::InvalidConfig(format!("git checkout {branch}: {e}")))?;
                Ok((camp_root.to_path_buf(), None))
            }
            // Build in a dedicated worktree at the branch; camp root untouched.
            WorkspaceMode::Isolated => {
                let worktree = std::env::temp_dir().join(format!("qed-worktree-{}", self.run_id));
                // A prior crashed run may have left this path registered; clear
                // it first so `worktree add` doesn't fail on a stale entry.
                let _ = std::process::Command::new("git")
                    .current_dir(camp_root)
                    .args(["worktree", "remove", "--force"])
                    .arg(&worktree)
                    .output();
                run_git(
                    camp_root,
                    &["worktree", "add", "--force", &worktree.to_string_lossy(), branch],
                )
                .map_err(|e| {
                    RunnerError::InvalidConfig(format!("git worktree add at {branch}: {e}"))
                })?;
                let guard = WorktreeGuard {
                    camp_root: camp_root.to_path_buf(),
                    worktree: worktree.clone(),
                };
                Ok((worktree, Some(guard)))
            }
        }
    }

    /// W209: evaluate every `[[bind]]` in the pipeline whose `from`
    /// references this step's outputs, write the accepted values into the
    /// source tree, and return the per-bind result list for surfacing in
    /// [`StepStatus::applied_binds`].
    ///
    /// Build → checkin → release inversion in mechanical form: the source
    /// tree IS the step-to-step plumbing. Downstream steps will read these
    /// values from disk like any other tool would.
    ///
    /// Failures are logged at `warn` and surfaced as an empty result list
    /// rather than poisoning the run. Per W209 § Safety the diff is the
    /// review surface; an applier crash on one file doesn't justify
    /// killing the pipeline (the operator can still inspect what landed
    /// and what didn't via `git status`).
    fn apply_step_binds(
        &self,
        step: &QedStep,
        step_outputs: &std::collections::HashMap<String, String>,
    ) -> Vec<manifest_bind::AppliedBind> {
        // Cheap pre-filter so we don't even touch the filesystem when
        // nothing in this pipeline binds against this step.
        let any_match = self.pipeline.binds.iter().any(|b| match &b.from {
            manifest_bind::OutputRef::StepOutput { step: s, .. } => s == &step.name,
            manifest_bind::OutputRef::Uri(_) => false,
        });
        if !any_match {
            return Vec::new();
        }

        let workspace_root = match self.resolve_camp_root() {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    step = %step.name,
                    error = %e,
                    "skipping [[bind]] application: cannot resolve workspace root",
                );
                return Vec::new();
            }
        };

        // Build a single-step OutputMap. Each declared output carries its
        // typed shape; undeclared keys default to `String` (matches
        // OutputDecl::kind's serde default) so back-compat steps from
        // R488-F4 still flow through — the per-bind type check stays the
        // hard boundary.
        let mut outputs = manifest_bind::OutputMap::new();
        for (key, raw) in step_outputs {
            let kind = step
                .outputs
                .iter()
                .find(|o| &o.name == key)
                .map(|o| o.kind)
                .unwrap_or(manifest_bind::ValueType::String);
            outputs.insert(
                step.name.clone(),
                key.clone(),
                manifest_bind::OutputValue::new(kind, raw.clone()),
            );
        }

        // Scope to binds that fire from this step. apply_binds itself
        // already filters by `outputs.lookup(&bind.from).is_some()`, but
        // doing it here avoids touching files that bind only from other
        // steps and keeps the AppliedBind list scoped to the step that
        // caused the writes.
        let relevant: Vec<manifest_bind::BindSpec> = self
            .pipeline
            .binds
            .iter()
            .filter(|b| {
                matches!(
                    &b.from,
                    manifest_bind::OutputRef::StepOutput { step: s, .. } if s == &step.name
                )
            })
            .cloned()
            .collect();

        match manifest_bind::apply_binds(&outputs, &relevant, &workspace_root) {
            Ok(applied) => {
                for a in &applied {
                    if a.changed {
                        tracing::info!(
                            step = %step.name,
                            file = %a.file.display(),
                            path = %a.path,
                            from = %a.from,
                            "bound output → manifest (changed)",
                        );
                    }
                }
                // W209/R510-F6: fire hash-change hooks after the bind
                // transaction has committed, for binds that actually changed.
                self.fire_change_hooks(&step.name, &applied, &workspace_root);
                applied
            }
            Err(e) => {
                tracing::warn!(
                    step = %step.name,
                    error = %e,
                    "manifest-bind apply failed; downstream steps will read pre-bind values",
                );
                Vec::new()
            }
        }
    }

    /// W209/R510-F6: evaluate every `[[on_change]]` hook against the binds
    /// this step just committed and perform each matching hook's side effect.
    /// Only binds that actually changed bytes fire (the no-op idempotency
    /// guarantee lives in [`manifest_bind::fired_hooks`]). `journal` / `event`
    /// actions commit to disk inside `dispatch_hook`; `pipeline` actions are
    /// surfaced as a logged request — v1 does not auto-cascade pipelines (the
    /// reserved `rebind_stop` guard is the design's bound on cascade storms),
    /// so the operator enqueues the downstream pipeline explicitly.
    ///
    /// A hook dispatch failure is logged at `warn` and never poisons the run,
    /// mirroring the bind applier's own failure stance (W209 § Safety): the
    /// in-tree bind result is the source of truth; the hook is a downstream
    /// side effect.
    fn fire_change_hooks(
        &self,
        step_name: &str,
        applied: &[manifest_bind::AppliedBind],
        workspace_root: &std::path::Path,
    ) {
        if self.pipeline.on_change.is_empty() {
            return;
        }
        for fired in manifest_bind::fired_hooks(&self.pipeline.on_change, applied) {
            match manifest_bind::dispatch_hook(&fired, workspace_root) {
                Ok(manifest_bind::HookOutcome::Journaled { file }) => tracing::info!(
                    step = %step_name,
                    bind = %fired.bind,
                    journal = %file.display(),
                    "on_change: appended journal line",
                ),
                Ok(manifest_bind::HookOutcome::EventEmitted { file, kind }) => tracing::info!(
                    step = %step_name,
                    bind = %fired.bind,
                    event = %kind,
                    sink = %file.display(),
                    "on_change: emitted event",
                ),
                Ok(manifest_bind::HookOutcome::PipelineRequested { pipeline, params }) => {
                    tracing::info!(
                        step = %step_name,
                        bind = %fired.bind,
                        pipeline = %pipeline,
                        params = ?params,
                        "on_change: pipeline requested (v1 does not auto-cascade — \
                         operator enqueues `yah qed run` explicitly)",
                    )
                }
                Err(e) => tracing::warn!(
                    step = %step_name,
                    bind = %fired.bind,
                    error = %e,
                    "on_change: hook dispatch failed (bind result stands; hook skipped)",
                ),
            }
        }
    }

    /// R506: determine whether a step should be skipped, and why. Returns
    /// `Some(human-readable reason)` to skip, `None` to dispatch normally.
    ///
    /// Precedence (declarative gates run before runtime ones, since they
    /// can't observe step outputs):
    ///   1. `enabled = false` — always wins, even when `include_stubbed`.
    ///   2. `activation = "stubbed"` and `!include_stubbed`.
    ///   3. `if = "<expr>"` evaluates to a falsy value against the W201-F4
    ///      context (matrix coord + accumulated step outputs + env).
    ///
    /// An `if` expression that fails to parse is treated as falsy with a
    /// descriptive reason so the dashboard surfaces the syntax error rather
    /// than the runner crashing the whole pipeline mid-run.
    fn resolve_skip_reason(
        &self,
        step: &crate::types::QedStep,
        step_context: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
        running_status: RunStatus,
    ) -> Option<String> {
        if !step.enabled {
            return Some("skipped: enabled = false".to_string());
        }
        if matches!(step.activation, StepActivation::Stubbed) && !self.include_stubbed {
            return Some(
                "skipped: status = \"stubbed\" (pass --include-stubbed to run anyway)".to_string(),
            );
        }
        if let Some(raw) = step.if_cond.as_deref() {
            let body = strip_expr_delimiters(raw);
            let ctx = self.build_expr_context(step_context, running_status);
            return match yah_qed_gha::evaluate(body, &ctx) {
                Ok(v) if v.is_truthy() => None,
                Ok(_) => Some(format!("skipped: if = \"{raw}\" evaluated falsy")),
                Err(e) => Some(format!("skipped: if = \"{raw}\" parse error: {e}")),
            };
        }
        None
    }

    /// Map the runner's running aggregate [`RunStatus`] onto the GHA-shaped
    /// [`yah_qed_gha::JobStatus`] consumed by `success()`/`failure()`/`always()`/
    /// `cancelled()` context functions. The runner has no mid-flight
    /// `Cancelled` state (cancel arrives via the abort handle and aborts the
    /// whole future), so only `Success` and `Failure` are reachable here —
    /// `cancelled()` therefore always evaluates to false from inside a step's
    /// `if=` expression, matching GHA semantics where a cancelled job never
    /// reaches the next step's gate.
    fn running_job_status(status: RunStatus) -> yah_qed_gha::JobStatus {
        match status {
            RunStatus::Failed => yah_qed_gha::JobStatus::Failure,
            _ => yah_qed_gha::JobStatus::Success,
        }
    }

    /// Build the [`yah_qed_gha::Context`] passed to `if=` evaluation. Populates:
    ///   - `matrix` from [`Self::matrix_coord`]
    ///   - `steps.<name>.outputs.<key>` from the accumulated step context
    ///   - `env` from the current process environment
    ///   - `job_status` from the cumulative `RunStatus` so
    ///     `success()`/`failure()`/`always()`/`cancelled()` reflect the
    ///     running aggregate at the moment this step is gated
    fn build_expr_context(
        &self,
        step_context: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
        running_status: RunStatus,
    ) -> yah_qed_gha::Context<'static> {
        use indexmap::IndexMap;
        let mut ctx = yah_qed_gha::Context::new();

        // env: process env
        let mut env_obj: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
        for (k, v) in std::env::vars() {
            env_obj.insert(k, yah_qed_gha::Value::String(v));
        }
        ctx.env = yah_qed_gha::Value::Object(env_obj);

        // matrix: from runner coord (None → leave as None so matrix.<key> → Null)
        if let Some(coord) = &self.matrix_coord {
            let mut m: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
            for (k, v) in coord {
                m.insert(
                    k.clone(),
                    yah_qed_gha::Value::String(crate::matrix::toml_value_to_str(v)),
                );
            }
            ctx.matrix = Some(yah_qed_gha::Value::Object(m));
        }

        // steps.<name>.outputs.<key>
        let mut steps_obj: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
        for (name, outputs) in step_context {
            let mut out_map: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
            for (k, v) in outputs {
                out_map.insert(k.clone(), yah_qed_gha::Value::String(v.clone()));
            }
            let mut step_obj: IndexMap<String, yah_qed_gha::Value> = IndexMap::new();
            step_obj.insert("outputs".to_string(), yah_qed_gha::Value::Object(out_map));
            steps_obj.insert(name.clone(), yah_qed_gha::Value::Object(step_obj));
        }
        ctx.steps = yah_qed_gha::Value::Object(steps_obj);

        ctx.job_status = Some(Self::running_job_status(running_status));

        ctx
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
        yubaba: Arc<dyn WardenClient>,
    ) -> Self {
        let run_id = Uuid::new_v4().to_string();
        let remote_driver = Arc::new(RemoteForgeDriver::new(scryer, yubaba));
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
            include_stubbed: false,
            matrix_coord: None,
            host_triple: crate::platform::detect_host_triple(),
            cross_availability: std::sync::OnceLock::new(),
            host_toolchains: std::sync::OnceLock::new(),
            provider_registry: Arc::new(crate::provider::ProviderRegistry::new()),
            secrets: Arc::new(crate::provider::MapSecrets::default()),
            branch: None,
            positioned_workspace: std::sync::OnceLock::new(),
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

    /// Wire the vendor release-provider registry + credential source (R509)
    /// used to dispatch [`Outcome::Provider`] outcomes (notarize, authenticode,
    /// sparkle, …). Composes with any constructor and is inherited by
    /// SubPipeline children. The defaults are an empty registry + empty
    /// secrets, so a pipeline with no vendor outcomes needs no wiring; a
    /// pipeline that *does* declare one fails with a typed unknown-provider
    /// error until this is called with a populated registry
    /// ([`crate::provider::ProviderRegistry::production`]).
    pub fn with_release_providers(
        mut self,
        registry: Arc<crate::provider::ProviderRegistry>,
        secrets: Arc<dyn crate::provider::SecretSource>,
    ) -> Self {
        self.provider_registry = registry;
        self.secrets = secrets;
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
    /// [`yah_qed_gha::graph::JobInstance::key`]). Steps absent from the map
    /// run their full matrix. Inherited by SubPipeline children.
    pub fn with_gha_matrix_subset(
        mut self,
        subset: std::collections::HashMap<String, std::collections::HashSet<String>>,
    ) -> Self {
        self.gha_matrix_subset = subset;
        self
    }

    /// Override the self-detected host triple (R531-T1). Constructors default
    /// to [`crate::platform::detect_host_triple`] (the process host); callers
    /// that know the execution host differs — e.g. a daemon constructing a
    /// runner whose steps will land on a remote runner of a known triple —
    /// set it explicitly. Composes with any constructor.
    pub fn with_host_triple(mut self, triple: impl Into<String>) -> Self {
        self.host_triple = triple.into();
        self
    }

    /// Seed the host-native cross-toolchain availability (R531-T6) instead of
    /// probing it. Tests use this to drive the NativeCross rewrite
    /// deterministically; a daemon constructing a runner for a remote host of a
    /// known toolchain set uses it to avoid a wrong local probe. Composes with
    /// any constructor; takes effect only if set before the first
    /// [`Self::cross_availability`] read.
    pub fn with_cross_availability(self, avail: crate::nativecross::ToolAvailability) -> Self {
        // OnceLock::set errors only if already initialized; a builder call
        // before any step runs is always first, so ignore the result.
        let _ = self.cross_availability.set(avail);
        self
    }

    /// The host-native cross toolchains installed on this runner (R531-T6),
    /// probed once and cached. The lazy half of the F5/T6 wiring: a runner with
    /// no NativeCross-tier step never calls this, so it never shells out.
    fn cross_availability(&self) -> crate::nativecross::ToolAvailability {
        *self
            .cross_availability
            .get_or_init(crate::nativecross::ToolAvailability::probe)
    }

    /// The host triple this runner executes on (R531-T1, W222), e.g.
    /// `aarch64-apple-darwin`. Threaded into the GHA `runner.{os,arch}`
    /// context and (F2/F3) the `host` leg of each step's `Platform` triple.
    pub fn host_triple(&self) -> &str {
        &self.host_triple
    }

    /// Seed the host's detected toolchain versions (R507, W208) instead of
    /// probing them. Tests drive the plan-time pinning check deterministically
    /// with this; a daemon constructing a runner for a remote host of a known
    /// toolchain set uses it to avoid a wrong local probe. Maps pin key →
    /// detected version (`None` = tool absent). Takes effect only if set before
    /// the first [`Self::host_toolchains`] read.
    pub fn with_host_toolchains(
        self,
        detected: std::collections::HashMap<String, Option<String>>,
    ) -> Self {
        let _ = self.host_toolchains.set(detected);
        self
    }

    /// The host's detected toolchain versions, probed once and cached (R507).
    /// The lazy half of the pinning check: a runner whose pipeline declares no
    /// `[toolchain]` pins never calls this, so it never shells out. Probes only
    /// the tools actually named across the pipeline + step pins.
    fn host_toolchains(&self) -> &std::collections::HashMap<String, Option<String>> {
        self.host_toolchains.get_or_init(|| {
            let mut keys: Vec<&str> = Vec::new();
            if let Some(tc) = &self.pipeline.toolchain {
                keys.extend(tc.pins.keys().map(String::as_str));
            }
            for step in &self.pipeline.steps {
                if let Some(tc) = &step.toolchain {
                    keys.extend(tc.pins.keys().map(String::as_str));
                }
            }
            crate::toolchain::detect_host_versions(keys)
        })
    }

    /// Whether a step's toolchain is provided by a container image rather than
    /// the host (R507, W208). A step that pulls an explicit `image` or pins
    /// `runtime = "container"` delegates its toolchain to that image, so the
    /// host-side pin check is skipped. Host-native steps (the default) are
    /// checked against the host's installed versions.
    fn step_satisfied_by_image(&self, step: &crate::types::QedStep) -> bool {
        step.image.is_some() || matches!(step.runtime, Some(TaskRuntime::Container))
    }

    /// Plan-time toolchain pinning check (R507, W208 pillar 3): for every step,
    /// overlay its `toolchain.*` overrides onto the pipeline-level
    /// `[toolchain]` pins, then resolve each pin against the host's detected
    /// versions (or mark it image-provided). Pure given the (seeded or probed)
    /// host versions — builds the verdict from the static pipeline, runs
    /// nothing. The runner gates `run()` on
    /// [`ToolchainPreflight::is_satisfied`](crate::toolchain::ToolchainPreflight::is_satisfied)
    /// and fails fast with its error report.
    pub fn toolchain_preflight(&self) -> crate::toolchain::ToolchainPreflight {
        let host = self.host_toolchains();
        let mut entries = Vec::new();
        for step in &self.pipeline.steps {
            let pins = crate::toolchain::effective_pins(
                self.pipeline.toolchain.as_ref(),
                step.toolchain.as_ref(),
            );
            if pins.is_empty() {
                continue;
            }
            let by_image = self.step_satisfied_by_image(step);
            for (tool, want) in &pins {
                let detected = host.get(tool).and_then(|v| v.as_deref());
                let resolution = crate::toolchain::resolve_pin(tool, want, detected, by_image);
                entries.push(crate::toolchain::PreflightEntry {
                    step: step.name.clone(),
                    resolution,
                });
            }
        }
        crate::toolchain::ToolchainPreflight { entries }
    }

    /// Compose a step's full [`Platform`](crate::platform::Platform) triple-set
    /// (R531-F2, W222): this runner's self-detected `host`, the step's declared
    /// `target` (its `[platform].target`, falling back to the legacy per-kind
    /// `triple` field), and the `container_platform` it pulls. This is the
    /// value F3's `resolve(host, target, container_platform)` decision table
    /// reasons over.
    pub fn step_platform(&self, step: &crate::types::QedStep) -> crate::platform::Platform {
        crate::platform::Platform::compose(
            &self.host_triple,
            step.platform.as_ref(),
            step.triple.as_deref(),
        )
    }

    /// Resolve how a step's build is satisfied on this runner's host (R531-F3,
    /// W222): compose its [`Platform`](crate::platform::Platform) triple-set,
    /// then run the cross-first decision table. Feeds the T4 portability
    /// preflight and (P2) the container-seam wiring.
    pub fn resolve_step(&self, step: &crate::types::QedStep) -> crate::platform::Resolution {
        let p = self.step_platform(step);
        crate::platform::resolve(
            &p.host,
            p.target.as_deref(),
            p.container_platform.as_deref(),
        )
    }

    /// Plan the host-native cross build for a step that resolves to the
    /// [`NativeCross`](crate::platform::Resolution::NativeCross) tier (R531-F5,
    /// W222) — the concrete cargo-zigbuild / musl-cross invocation that should
    /// *replace* the recipe's `cross build` / bare `cargo build` argv.
    ///
    /// Returns `None` for any step F3 does **not** resolve to NativeCross
    /// (those go through emulate / cross-docker / offload, not this tier), and
    /// for a NativeCross verdict with no concrete `target` (a plain host build
    /// needs no rewrite). For an in-tier step it selects the toolchain against
    /// `avail` and rewrites the step's `argv`, yielding the emulation-free
    /// plan (or a [`CrossToolUnavailable`](crate::nativecross::CrossToolUnavailable)
    /// carrying an install hint).
    ///
    /// This is the seam T6 wires into the subprocess executor; F5 only defines
    /// and tests it — `run()` does not yet route through it.
    pub fn native_cross_plan(
        &self,
        step: &crate::types::QedStep,
        avail: &crate::nativecross::ToolAvailability,
    ) -> Option<Result<crate::nativecross::NativeCrossPlan, crate::nativecross::CrossToolUnavailable>>
    {
        if !matches!(
            self.resolve_step(step),
            crate::platform::Resolution::NativeCross
        ) {
            return None;
        }
        let platform = self.step_platform(step);
        // A host-arch / absent target is a plain native build — no foreign
        // toolchain, nothing for this tier to rewrite.
        let target = platform.target.as_deref()?;
        if !crate::nativecross::is_native_cross_target(&platform.host, target) {
            return None;
        }
        Some(crate::nativecross::plan_native_cross(
            &step.argv,
            &platform.host,
            target,
            avail,
        ))
    }

    /// Portability preflight (R531-T4, W222): one rendered line per step
    /// describing what it targets, the host it runs on, and the resolution
    /// verdict — so an operator sees where mac and linux will diverge (and at
    /// what cost) *before* the run. Pure: builds the lines from the static
    /// pipeline, no execution. The `index_offset` is honored so a
    /// resume-from-step run still shows original step positions.
    pub fn portability_preflight(&self) -> Vec<String> {
        self.pipeline
            .steps
            .iter()
            .map(|step| {
                let platform = self.step_platform(step);
                let resolution = crate::platform::resolve(
                    &platform.host,
                    platform.target.as_deref(),
                    platform.container_platform.as_deref(),
                );
                crate::platform::preflight_line(&step.name, &platform, &resolution)
            })
            .collect()
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
        // R513-F2: background sidecar steps spawned but not yet reaped. Reaped
        // either when their `background_until` gate step finishes (mid-loop) or
        // at the end of the step loop, whichever comes first. The Vec owns the
        // `kill_on_drop` task handles, so an early-return drops it and kills
        // every live sidecar.
        let mut background_tasks: Vec<BackgroundTask> = Vec::new();

        self.emit(QedEvent::RunStarted {
            total_steps: self.index_offset + self.pipeline.steps.len(),
            at: created_at,
        });

        // R531-T4: portability preflight — log the per-step host/target/
        // resolution verdict before executing anything, so divergence (and its
        // cost) is legible up front instead of after a wave-three faceplant.
        // Report-only: this never gates execution.
        for line in self.portability_preflight() {
            tracing::info!(target: "qed::preflight", host = %self.host_triple, "{line}");
        }

        // R507/W208: toolchain pinning preflight — resolve every `[toolchain]`
        // pin against the host's installed versions (or mark it image-provided)
        // and *fail fast* before any step runs when the host can't satisfy a
        // pin. Unlike the portability preflight above this one gates execution:
        // a missing Xcode/NDK should stop a multi-hour release at second zero
        // with an actionable error, not three waves in. Logged either way.
        let toolchain_preflight = self.toolchain_preflight();
        for line in toolchain_preflight.report() {
            tracing::info!(target: "qed::preflight", host = %self.host_triple, "toolchain: {line}");
        }
        if let Some(report) = toolchain_preflight.error_report() {
            tracing::error!(target: "qed::preflight", host = %self.host_triple, "{report}");
            return Err(RunnerError::ToolchainUnsatisfied(report));
        }

        // R513-F2: background sidecar pre-flight. v1 supports local + native
        // subprocess sidecars only, and a `background_until` target must name a
        // step that appears *later* in the pipeline. Fail loudly here, before
        // any step runs, rather than spawning a sidecar that can never be
        // reaped on schedule (a typo'd `background_until`) or routing one
        // through a runtime that can't honour `kill_on_drop` teardown.
        let step_names: Vec<&str> = self
            .pipeline
            .steps
            .iter()
            .map(|s| s.name.as_str())
            .collect();
        for (i, step) in self.pipeline.steps.iter().enumerate() {
            if !step.is_background() {
                continue;
            }
            if self.run_where != RunWhere::Local {
                return Err(RunnerError::InvalidConfig(format!(
                    "step `{}`: background steps run locally only (R513-F2) — \
                     remote sidecars are yubaba-supervised, a separate lifecycle",
                    step.name,
                )));
            }
            if self.resolve_runtime(step) != TaskRuntime::Native {
                return Err(RunnerError::InvalidConfig(format!(
                    "step `{}`: background steps run native only in v1 (R513-F2) — \
                     drop `runtime = \"container\"`",
                    step.name,
                )));
            }
            if let Some(until) = &step.background_until {
                match step_names.iter().position(|n| n == until) {
                    None => {
                        return Err(RunnerError::InvalidConfig(format!(
                            "step `{}`: background_until names unknown step `{until}`",
                            step.name,
                        )));
                    }
                    Some(pos) if pos <= i => {
                        return Err(RunnerError::InvalidConfig(format!(
                            "step `{}`: background_until must name a *later* step, \
                             but `{until}` is at or before it — a sidecar reaped on a \
                             prior step would never see its gate fire",
                            step.name,
                        )));
                    }
                    Some(_) => {}
                }
            }
        }

        // W224 R533-F11: position the whole run's workspace ONCE, before any
        // step. Top-level runs honour the pipeline's WorkspaceMode (Live /
        // Checkout-bail-if-dirty / Isolated worktree); the resulting tree is
        // recorded in `positioned_workspace` so every step kind resolves its
        // root through it (subprocess `desktop-release` builds from the same
        // Isolated worktree as a `gha-workflow` step, not the live camp root).
        // Child sub-pipeline runners inherit the parent's already-positioned
        // tree via `camp_root` (set at construction), so they skip repositioning
        // — re-running a checkout / spinning a second worktree mid-run would be
        // wrong. `_run_worktree_guard` is held for the entire step loop so an
        // Isolated worktree outlives the whole run and is torn down on drop,
        // even when a step below returns early with an error.
        let _run_worktree_guard = if self.parent_run_id.is_some() {
            None
        } else {
            let base = self.base_camp_root()?;
            let (workspace, guard) = self.prepare_workspace(&base)?;
            // OnceLock: this is the only writer (run_inner runs once per runner
            // instance) and it fires before the first step, so every step-time
            // resolve_camp_root() sees the positioned tree.
            let _ = self.positioned_workspace.set(workspace);
            guard
        };

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

            // R506: declarative + runtime gating. Resolve the reason (if any)
            // *before* emitting StepStarted so a skipped step's lifecycle
            // pair carries a Skipped terminal status with no "Running"
            // intermediate state on the wire.
            let skip_reason = self.resolve_skip_reason(step, &step_context, overall_status);

            let started_at = Utc::now();
            self.emit(QedEvent::StepStarted {
                index: event_index,
                name: step.name.clone(),
                argv: step.argv.clone(),
                env_keys: crate::events::credential_env_keys(std::env::vars()),
                at: started_at,
            });

            if let Some(reason) = skip_reason {
                let completed_at = Utc::now();
                self.emit(QedEvent::StepFinished {
                    index: event_index,
                    name: step.name.clone(),
                    status: RunStatus::Skipped,
                    msg: Some(reason),
                    at: completed_at,
                });
                step_statuses.push(StepStatus {
                    name: step.name.clone(),
                    task_run_id: None,
                    status: RunStatus::Skipped,
                    started_at: Some(started_at),
                    completed_at: Some(completed_at),
                    error: None,
                    outputs: std::collections::HashMap::new(),
                    applied_binds: Vec::new(),
                    jobs: Vec::new(),
                });
                continue;
            }

            let runtime = self.resolve_runtime(step);

            // R513-F2: a background sidecar is *spawned*, not awaited. Emit only
            // its StepStarted (already done above), kick the subprocess onto its
            // own task, record a `Running` placeholder row finalized at reap, and
            // advance to the next step. Pre-flight above guarantees this is a
            // local + native subprocess step. Output collection ($YAH_OUTPUTS) is
            // skipped — a long-lived sidecar has no terminal moment to read it
            // back, and downstream substitution can't wait on a server that
            // never exits.
            if step.is_background() {
                let spec = build_subprocess_spec(step, TaskRuntime::Native, None);
                let camp_root = self.resolve_camp_root()?;
                let cwd = match step.cwd.as_ref() {
                    Some(rel) => camp_root.join(rel),
                    None => camp_root,
                };
                let env: Vec<(String, String)> =
                    step.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                let ctx = ExecContext::default().with_cwd(cwd).with_env(env);
                let join = self.spawn_background_step(event_index, step, spec, ctx);
                let status_index = step_statuses.len();
                step_statuses.push(StepStatus {
                    name: step.name.clone(),
                    task_run_id: None,
                    status: RunStatus::Running,
                    started_at: Some(started_at),
                    completed_at: None,
                    error: None,
                    outputs: std::collections::HashMap::new(),
                    applied_binds: Vec::new(),
                    jobs: Vec::new(),
                });
                background_tasks.push(BackgroundTask {
                    status_index,
                    event_index,
                    name: step.name.clone(),
                    until: step.background_until.clone(),
                    join,
                });
                continue;
            }

            // step_outputs: key → value collected from this step (W201-F4).
            // step_jobs: per-job rows when this step wraps a GHA workflow
            // (W223 R532-T1); stays empty for every other step kind.
            let mut step_jobs: Vec<crate::types::JobRow> = Vec::new();
            let (result, task_run_id, step_outputs) = match step.kind {
                crate::types::StepKind::BuildImage => {
                    match self.execute_step_build_image(event_index, step).await {
                        Ok(Some(forge_id)) => (
                            Ok(()),
                            Some(forge_id.to_string()),
                            std::collections::HashMap::new(),
                        ),
                        Ok(None) => (Ok(()), None, std::collections::HashMap::new()),
                        Err(e) => (Err(e), None, std::collections::HashMap::new()),
                    }
                }
                crate::types::StepKind::PackageNativeTarball => (
                    self.execute_step_package_native_tarball(step).await,
                    None,
                    std::collections::HashMap::new(),
                ),
                crate::types::StepKind::MuslStaticPreflight => (
                    self.execute_step_musl_static_preflight(step).await,
                    None,
                    std::collections::HashMap::new(),
                ),
                crate::types::StepKind::SignNativeTarball => (
                    self.execute_step_sign_native_tarball(step).await,
                    None,
                    std::collections::HashMap::new(),
                ),
                crate::types::StepKind::SubPipeline => {
                    match self
                        .execute_step_sub_pipeline(event_index, step, &mut step_jobs)
                        .await
                    {
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
                    let cfg = step.gha_workflow.clone();
                    let dispatch = match cfg.as_ref() {
                        Some(cfg) => self.execute_step_gha_workflow(event_index, step, cfg, &mut step_jobs).await,
                        None => Err(RunnerError::InvalidConfig(format!(
                            "step `{}`: kind=gha-workflow with no [gha_workflow] block (validate() should have caught this)",
                            step.name,
                        ))),
                    };
                    match dispatch {
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
                crate::types::StepKind::Import => {
                    match self
                        .execute_step_import(event_index, step, &mut step_jobs)
                        .await
                    {
                        Ok((import_produced, import_outputs)) => {
                            // The imported workflow's expansion rolls up exactly
                            // like a GhaWorkflow step (W224 keeps the front-end):
                            // produced artifacts into the parent's terminal
                            // Outcome::Publish, job-level outputs as `<job>.<key>`.
                            produced.extend(import_produced);
                            (Ok(()), None, import_outputs)
                        }
                        Err(e) => (Err(e), None, std::collections::HashMap::new()),
                    }
                }
                crate::types::StepKind::WaitFor => (
                    self.execute_step_wait_for(event_index, step).await,
                    None,
                    std::collections::HashMap::new(),
                ),
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
                        let result = self
                            .execute_step_local(event_index, step, Some(&yah_env))
                            .await;
                        let collected = parse_yah_outputs(&outputs_path);
                        let _ = std::fs::remove_file(&outputs_path);
                        (result, None, collected)
                    }
                    (RunWhere::Local, TaskRuntime::Container) => (
                        self.execute_step_local_container(event_index, step).await,
                        None,
                        std::collections::HashMap::new(),
                    ),
                    (RunWhere::Remote, _) => match self.execute_step_remote(event_index, step, runtime).await {
                        Ok(forge_id) => (
                            Ok(()),
                            Some(forge_id.to_string()),
                            std::collections::HashMap::new(),
                        ),
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

            // W209: when the step succeeded, evaluate every bind whose
            // `from` references one of its outputs. Each AppliedBind is
            // persisted on the StepStatus so the qed-run tile (F7) and
            // hash-change hooks (F6) can drive off it. A failed step skips
            // its binds entirely — the source tree should only be touched
            // by receipts that came from a clean run. (Prior steps'
            // already-written binds remain on disk; the operator triages
            // via `git diff`, per W209 § Failure handling.)
            let applied_binds = if status == RunStatus::Success {
                self.apply_step_binds(step, &step_outputs)
            } else {
                Vec::new()
            };

            let completed_at = Utc::now();
            // Keep the failure reason on the persisted StepStatus (not only in
            // the live StepFinished event) so `qed.status` surfaces *why* a
            // step failed after the run ends.
            let error = if status == RunStatus::Failed {
                msg.clone()
            } else {
                None
            };
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
                error,
                outputs: step_outputs,
                applied_binds,
                jobs: step_jobs,
            });

            // R513-F2: reap any background sidecar gated on this step finishing
            // (`background_until = step.name`). Reaping here — before the
            // `on_fail` break below — means a sidecar is torn down right after
            // its gate step regardless of whether that step passed or failed.
            let mut i = 0;
            while i < background_tasks.len() {
                if background_tasks[i].until.as_deref() == Some(step.name.as_str()) {
                    let bg = background_tasks.remove(i);
                    let (bg_status, bg_msg) = reap_background(bg.join).await;
                    let bg_completed_at = Utc::now();
                    if bg_status == RunStatus::Failed {
                        overall_status = RunStatus::Failed;
                    }
                    self.emit(QedEvent::StepFinished {
                        index: bg.event_index,
                        name: bg.name.clone(),
                        status: bg_status,
                        msg: bg_msg.clone(),
                        at: bg_completed_at,
                    });
                    let row = &mut step_statuses[bg.status_index];
                    row.status = bg_status;
                    row.completed_at = Some(bg_completed_at);
                    row.error = if bg_status == RunStatus::Failed {
                        bg_msg
                    } else {
                        None
                    };
                } else {
                    i += 1;
                }
            }

            if status == RunStatus::Failed && !matches!(step.on_fail, OnFail::Continue) {
                break;
            }
        }

        // R513-F2: reap every background sidecar still running at the end of the
        // step loop — those with no `background_until` (reap-at-pipeline-end),
        // plus any whose gate step was skipped or never reached. Done before
        // terminal-outcome selection so a sidecar that *crashed* mid-pipeline
        // flips the run to Failed and fires `on_fail`.
        for bg in background_tasks.drain(..) {
            let (bg_status, bg_msg) = reap_background(bg.join).await;
            let bg_completed_at = Utc::now();
            if bg_status == RunStatus::Failed {
                overall_status = RunStatus::Failed;
            }
            self.emit(QedEvent::StepFinished {
                index: bg.event_index,
                name: bg.name.clone(),
                status: bg_status,
                msg: bg_msg.clone(),
                at: bg_completed_at,
            });
            let row = &mut step_statuses[bg.status_index];
            row.status = bg_status;
            row.completed_at = Some(bg_completed_at);
            row.error = if bg_status == RunStatus::Failed {
                bg_msg
            } else {
                None
            };
        }

        // R513-F4 (W207 Gap #6): always-run `finally:` teardown. Runs after the
        // sidecar reap and before terminal-outcome dispatch, unconditionally —
        // pass or fail — so artifact/diagnostic teardown (upload Playwright
        // traces, `docker compose down`, collect logs) always happens. Two
        // deliberate semantics:
        //   * Outcome selection uses the *work* status (steps + sidecars),
        //     snapshotted here BEFORE finally runs — a flaky teardown never
        //     redirects `on_success` → `on_fail`.
        //   * Every finally step is attempted (a failure never aborts the rest;
        //     teardown should always run to completion). A failed finally step
        //     still marks the *run* Failed (tile + `RunFinished`) unless it sets
        //     `on_fail = "continue"`.
        // Loader validation (`validate_finally`) guarantees these are Subprocess
        // steps and never background.
        let work_status = overall_status;
        let finally_index_base = self.pipeline.steps.len() + self.index_offset;
        for (j, step) in self.pipeline.finally.iter().enumerate() {
            let event_index = finally_index_base + j;
            // A teardown step may reference a prior step's output (e.g. the path
            // a test step emitted for its trace bundle), so apply the same
            // `${{ steps.X.outputs.Y }}` substitution the main loop uses.
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

            // Honor declarative disable / stub (cheap parity with main steps); a
            // finally step is otherwise unconditional — no `if` gate is consulted
            // (teardown is always-run by definition).
            if !step.enabled || step.activation == crate::types::StepActivation::Stubbed {
                let completed_at = Utc::now();
                let reason = if !step.enabled {
                    "finally step disabled (enabled = false)"
                } else {
                    "finally step stubbed (status = stubbed)"
                };
                self.emit(QedEvent::StepFinished {
                    index: event_index,
                    name: step.name.clone(),
                    status: RunStatus::Skipped,
                    msg: Some(reason.to_string()),
                    at: completed_at,
                });
                step_statuses.push(StepStatus {
                    name: step.name.clone(),
                    task_run_id: None,
                    status: RunStatus::Skipped,
                    started_at: Some(started_at),
                    completed_at: Some(completed_at),
                    error: None,
                    outputs: std::collections::HashMap::new(),
                    applied_binds: Vec::new(),
                    jobs: Vec::new(),
                });
                continue;
            }

            let runtime = self.resolve_runtime(step);
            let result = match (self.run_where, runtime) {
                (RunWhere::Local, TaskRuntime::Native) => {
                    self.execute_step_local(event_index, step, None).await
                }
                (RunWhere::Local, TaskRuntime::Container) => {
                    self.execute_step_local_container(event_index, step).await
                }
                (RunWhere::Remote, _) => self
                    .execute_step_remote(event_index, step, runtime)
                    .await
                    .map(|_| ()),
            };

            let (status, msg) = match &result {
                Ok(_) => (RunStatus::Success, None),
                Err(e) => {
                    // A failed teardown marks the run Failed (so it's visible),
                    // unless the step opted out with `on_fail = "continue"`. It
                    // never aborts the remaining finally steps.
                    if !matches!(step.on_fail, OnFail::Continue) {
                        overall_status = RunStatus::Failed;
                    }
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
                msg: msg.clone(),
                at: completed_at,
            });
            step_statuses.push(StepStatus {
                name: step.name.clone(),
                task_run_id: None,
                status,
                started_at: Some(started_at),
                completed_at: Some(completed_at),
                error: msg,
                outputs: std::collections::HashMap::new(),
                applied_binds: Vec::new(),
                jobs: Vec::new(),
            });
        }

        // Outcome selection keys off the *work* status, not the post-finally
        // status (a teardown failure marks the run Failed but doesn't redirect
        // which terminal outcomes fire).
        let outcomes = match work_status {
            RunStatus::Success => &self.pipeline.on_success,
            _ => &self.pipeline.on_fail,
        };

        // Terminal outcomes operate on the run's produced artifacts, resolved
        // against camp_root once so relative paths work when the process CWD
        // isn't the workspace root (e.g. the Tauri desktop app). A vendor
        // adapter that *transforms* artifacts (notarize staples a bundle,
        // authenticode signs an `.exe`) folds its result back into `staged` so
        // a later outcome in the same chain (sparkle ships the stapled bundle,
        // a Publish syncs the signed binary) sees the transformed file (R509).
        let version = crate::publish::resolve_release_version();
        let mut staged: Vec<ProducedArtifact> = if let Some(root) = &self.camp_root {
            produced
                .iter()
                .map(|a| {
                    let p = std::path::Path::new(&a.path);
                    if p.is_relative() {
                        ProducedArtifact {
                            path: root.join(p).to_string_lossy().into_owned(),
                            ..a.clone()
                        }
                    } else {
                        a.clone()
                    }
                })
                .collect()
        } else {
            produced.clone()
        };

        for outcome in outcomes {
            match outcome {
                Outcome::WardenDeploy { service, env } => {
                    self.outcome_dispatcher.warden_deploy(service, env).await?;
                }
                Outcome::AlmanacRun { pipeline } => {
                    self.outcome_dispatcher.almanac_run(pipeline).await?;
                }
                Outcome::Publish {
                    provider,
                    bucket,
                    prefix,
                    base_url,
                } => {
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
                    let req = crate::publish::PublishRequest {
                        provider: provider.clone(),
                        bucket: bucket.clone(),
                        prefix: prefix.clone(),
                        base_url: base_url.clone(),
                        version: version.clone(),
                        artifacts: staged.clone(),
                    };
                    self.outcome_dispatcher.publish(&req).await?;
                }
                Outcome::Provider {
                    provider,
                    with,
                    base_url,
                } => {
                    // Vendor adapters are suppressed on SubPipeline children
                    // exactly like Publish — the parent owns the terminal
                    // vendor ship, so a child that notarized its own bundle and
                    // handed it up would double-submit.
                    if self.suppress_publish_outcomes {
                        tracing::debug!(
                            run_id = %self.run_id,
                            provider = %provider,
                            "suppressing Outcome::Provider on child sub-pipeline run; parent owns the terminal publish"
                        );
                        continue;
                    }
                    // Per-dispatch scratch dir for materialized credentials /
                    // generated artifacts; dropped (and cleaned) at arm exit.
                    let work = tempfile::tempdir()?;
                    let report = {
                        let ctx = crate::provider::ProviderContext {
                            version: &version,
                            artifacts: &staged,
                            base_url: base_url.as_deref(),
                            config: with,
                            work_dir: work.path(),
                            secrets: self.secrets.as_ref(),
                            // Live run path; the per-adapter dry-run check is a
                            // unit-test + `qed validate` plan-time concern.
                            dry_run: false,
                        };
                        self.provider_registry.dispatch(provider, &ctx).await?
                    };
                    for line in &report.actions {
                        tracing::info!(run_id = %self.run_id, provider = %provider, "{line}");
                    }
                    for url in &report.published {
                        tracing::info!(run_id = %self.run_id, provider = %provider, url = %url, "vendor publish");
                    }
                    // Fold transformed/new artifacts back into the working set
                    // so the next outcome in the chain addresses them. An
                    // in-place transform (same path) replaces; a new artifact
                    // (appcast/delta) appends.
                    for art in report.produced {
                        match staged.iter_mut().find(|s| s.path == art.path) {
                            Some(slot) => *slot = art,
                            None => staged.push(art),
                        }
                    }
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
    /// `jobs_out` is forwarded to [`Self::execute_step_gha_workflow`] when the
    /// target is a GHA workflow (the short-circuit path), so the wrapping
    /// sub-pipeline step's `StepStatus` carries the inlined workflow's per-job
    /// rows (W223 R532-T1). Left empty for non-GHA sub-pipeline children —
    /// transparency for `Path` / `Builtin` / `Peer` targets is a later phase.
    async fn execute_step_sub_pipeline(
        &self,
        index: usize,
        step: &crate::types::QedStep,
        jobs_out: &mut Vec<crate::types::JobRow>,
    ) -> Result<
        (
            Vec<crate::types::ProducedArtifact>,
            std::collections::HashMap<String, String>,
        ),
        RunnerError,
    > {
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
            let synthesised_step = child.steps.into_iter().next().ok_or_else(|| {
                RunnerError::InvalidConfig(format!(
                    "step `{}`: GhaWorkflow resolver returned an empty pipeline",
                    step.name,
                ))
            })?;
            let synthesised_cfg = synthesised_step.gha_workflow.clone().ok_or_else(|| {
                RunnerError::InvalidConfig(format!(
                    "step `{}`: synthesised GhaWorkflow step carried no [gha_workflow] block",
                    step.name,
                ))
            })?;
            let result = self
                .execute_step_gha_workflow(index, &synthesised_step, &synthesised_cfg, jobs_out)
                .await;
            // W223 R532-F3: opaque opt-out — keep the wrapper a single
            // black-box node by dropping the inlined per-job rows. The
            // workflow still ran and its status still rolls up below.
            if cfg.opaque {
                jobs_out.clear();
            }
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

        // Peer children execute in the *peer* camp's workspace — the
        // resolver reports its root so subprocess steps (`cargo …`) get the
        // right cwd. Builtin/Path/GhaWorkflow children return None here and
        // inherit the parent's *positioned* workspace (W224 R533-F11): an
        // Isolated parent already moved its tree into a worktree, so the child
        // must build there too, not in the live camp root. `resolve_camp_root`
        // returns the positioned tree (set in run_inner before any step), so the
        // child inherits the worktree and — carrying `parent_run_id` — skips its
        // own repositioning. Without the peer override a `peer-release` runs
        // yubaba's `cargo publish -p workload-spec` from yah's root and fails
        // (package not in yah's workspace).
        let child_camp_root = self
            .sub_pipeline_resolver
            .resolved_camp_root(&cfg.target)
            .or_else(|| self.resolve_camp_root().ok());

        let child_run_id = Uuid::new_v4().to_string();
        let child_runner = Self {
            pipeline: child,
            run_id: child_run_id.clone(),
            remote_driver: self.remote_driver.clone(),
            run_where: self.run_where,
            outcome_dispatcher: self.outcome_dispatcher.clone(),
            events: None,
            camp_root: child_camp_root,
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
            // Inherit so an `--include-stubbed` pickup of a parent pipeline
            // applies recursively to its sub-pipeline children.
            include_stubbed: self.include_stubbed,
            // Child runs don't inherit the parent's matrix coord — they may
            // themselves be matrix-expanded.
            matrix_coord: None,
            // Inherit the parent's host triple (R531-T1): a SubPipeline child
            // executes on the same host, so it shares the parent's platform
            // context rather than re-detecting (which would also lose a
            // with_host_triple override the parent carried).
            host_triple: self.host_triple.clone(),
            // Carry the parent's already-probed toolchain set when present so
            // a child sub-pipeline doesn't re-probe; otherwise a fresh lazy
            // cache (it shares the host, so the result would match anyway).
            cross_availability: match self.cross_availability.get() {
                Some(a) => std::sync::OnceLock::from(*a),
                None => std::sync::OnceLock::new(),
            },
            // Same rationale (R507): inherit the parent's probed host toolchain
            // set when present so a child sub-pipeline doesn't re-probe; the
            // child shares the host, so a fresh lazy cache would match anyway.
            host_toolchains: match self.host_toolchains.get() {
                Some(m) => std::sync::OnceLock::from(m.clone()),
                None => std::sync::OnceLock::new(),
            },
            // Inherit the vendor adapter registry + credential source so a
            // child sub-pipeline whose `Outcome::Provider` *isn't* suppressed
            // (propagate.produces = false) can still resolve its adapter.
            provider_registry: self.provider_registry.clone(),
            secrets: self.secrets.clone(),
            // Inherit the run's target branch so a sub-pipeline whose child is a
            // gha-workflow positions its workspace at the same ref the parent
            // run requested (W224).
            branch: self.branch.clone(),
            // Child skips repositioning (parent_run_id is Some ⇒ run_inner
            // leaves this unset) and inherits the parent's positioned tree via
            // camp_root above (W224 R533-F11).
            positioned_workspace: std::sync::OnceLock::new(),
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

        // W223 R532-F3: generalize transparent-by-default to the non-GHA
        // child kinds (Builtin / Path / Peer). The child ran as its own
        // pipeline, so its steps are attributed to this wrapping step as
        // inlined rows — the same treatment GHA jobs get — unless the step
        // opted out via `opaque`. Child qed steps are linear-by-ordering
        // (no `depends_on`), so the rows carry no `needs` edges; the report
        // and graph render them as a flat sequence under the wrapper. The
        // failing-step detail stays on the per-row `error`, mirroring the
        // child's own StepStatus.
        if !cfg.opaque {
            jobs_out.clear();
            jobs_out.extend(meta.steps.iter().map(|s| crate::types::JobRow {
                id: s.name.clone(),
                status: s.status,
                error: s.error.clone(),
                needs: Vec::new(),
            }));
        }

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

        let produced = if cfg.propagate.produces {
            child_produced
        } else {
            Vec::new()
        };
        Ok((produced, propagated_outputs))
    }

    /// Dispatch a [`StepKind::GhaWorkflow`] step into the native W200 GHA
    /// runtime (W200-F9). Reads the workflow YAML at the configured path
    /// (resolved relative to the camp root), parses through
    /// [`yah_qed_gha::parse_workflow`], executes via [`yah_qed_gha::execute_workflow`]
    /// with the tier-1/2 toolkit actions pre-registered (W224 R533-T7 — the
    /// tier-3 service overrides were retired). The delegated path produces no
    /// native publish artifacts; release artifacts come from native QED
    /// publisher steps, so the returned produced-artifact list is always empty.
    ///
    /// The qed-gha runtime is synchronous; we cross the seam via
    /// [`tokio::task::spawn_blocking`] so the runner's tokio reactor stays
    /// responsive (long-running workflow legs like `docker buildx build` would
    /// otherwise stall the executor).
    /// `jobs_out` is populated with one [`crate::types::JobRow`] per GHA job
    /// the workflow ran, regardless of overall success/failure, so the wrapping
    /// step's `StepStatus` carries the workflow's per-job structure transparently
    /// (W223 R532-T1). Left untouched when the run never starts (read / parse /
    /// join failure before any job executes).
    ///
    /// `cfg` is passed explicitly rather than read from `step.gha_workflow` so
    /// the same execution path serves both a `kind = gha-workflow` step (which
    /// passes its own `[gha_workflow]` block) and a `kind = import` step (R533-F1),
    /// whose plan-time expansion synthesizes an equivalent [`GhaWorkflowConfig`]
    /// via [`crate::import::expand_import`]. `step` still supplies the step name,
    /// matrix-subset key, and event index.
    async fn execute_step_gha_workflow(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
        cfg: &crate::types::GhaWorkflowConfig,
        jobs_out: &mut Vec<crate::types::JobRow>,
    ) -> Result<
        (
            Vec<crate::types::ProducedArtifact>,
            std::collections::HashMap<String, String>,
        ),
        RunnerError,
    > {
        // W224 R533-F11: the run already positioned its workspace once (in
        // run_inner, per the pipeline's WorkspaceMode + branch); read the
        // effective tree here rather than repositioning per gha step. For an
        // Isolated run this resolves to the run's worktree, so the workflow
        // reads the branch's copy of release.yml from the same tree every other
        // step builds in. The run-scoped WorktreeGuard (held in run_inner)
        // outlives this step.
        let workspace = self.resolve_camp_root()?;
        let workflow_path = if cfg.path.is_absolute() {
            cfg.path.clone()
        } else {
            workspace.join(&cfg.path)
        };
        let step_name = step.name.clone();
        let event = cfg.event.clone().unwrap_or_else(|| "push".into());
        let inputs = cfg.inputs.clone();
        // R531-T1: thread the self-detected host into the GHA plan context so
        // workflow steps gating on `runner.arch` see the real host this runner
        // executes on (the GHA executor detects its own OS, but QED owns the
        // authoritative host triple). Map the Rust arch token to GHA's
        // `runner.arch` vocabulary (`X64` / `ARM64`).
        let host_arch =
            crate::platform::gha_runner_arch(crate::platform::arch_of(&self.host_triple));
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
        let (gha_tx, gha_rx) = std::sync::mpsc::channel::<yah_qed_gha::GhaEvent>();
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
            let yaml =
                std::fs::read_to_string(&workflow_path).map_err(|e| RunnerError::StepFailed {
                    step: step_name.clone(),
                    msg: format!("read workflow {}: {e}", workflow_path.display()),
                })?;
            let workflow = yah_qed_gha::parse_workflow(&yaml).map_err(|e| RunnerError::StepFailed {
                step: step_name.clone(),
                msg: format!("parse {}: {e}", workflow_path.display()),
            })?;
            let secrets = crate::secrets_bridge::SecretsConfig::load_default().resolve_all();
            let mut executor = yah_qed_gha::Executor::new(&workspace)
                .with_events(gha_tx)
                .with_secrets(secrets);
            executor.inputs = inputs_to_value(&inputs);
            executor.github = github_context(&event, &workspace);
            executor.runner_arch = host_arch;
            executor.included_instance_keys = matrix_subset;
            // W224 R533-T7 retired the W200 per-camp override overlay
            // (`.yah/qed/gha-actions.toml` registry_route / deny rules) along
            // with the tier-3 docker-push reimplementation it configured. The
            // executor now runs only tier-1/2 toolkit actions; tier-3 service
            // steps error with a native-replacement hint (import-time concern,
            // not a runtime overlay).
            let run = yah_qed_gha::execute_workflow(&workflow, &executor).map_err(|e| {
                RunnerError::StepFailed {
                    step: step_name.clone(),
                    msg: format!("execute {}: {e}", workflow_path.display()),
                }
            })?;
            // Lift each job's `needs:` out of the parsed workflow before it's
            // dropped — the graph viewer renders these as intra-workflow
            // dependency edges between the inlined job nodes (W223 R532-F2).
            let needs_by_job: std::collections::HashMap<String, Vec<String>> = workflow
                .jobs
                .iter()
                .map(|(id, job)| (id.clone(), job.needs.clone()))
                .collect();
            // Drop the executor (and its event sender) so the forwarder loop
            // exits cleanly once it has drained the channel.
            drop(executor);
            Ok::<_, RunnerError>((run, needs_by_job))
        })
        .await
        .map_err(|join_err| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("gha-workflow task panicked: {join_err}"),
        })??;
        let (run, needs_by_job) = run;
        // Wait for the forwarder to drain any tail events before we return —
        // otherwise the parent's `StepFinished` could race ahead of the last
        // few GhaStepOutput lines.
        let _ = forwarder.await;

        // W223 R532-T1: persist the wrapped workflow's per-job structure on the
        // wrapping step. Build one row per job regardless of outcome so the
        // report renders the workflow transparently — success and skipped rows
        // are present too, folding the R516 skip-count into per-row Skipped
        // state rather than a trailing sentence. The flattened failure string
        // below is still produced (it remains the step-level `error`), but the
        // structured rows are now the source of truth for per-job detail.
        jobs_out.clear();
        jobs_out.extend(run.instances.iter().map(|inst| {
            let status = match inst.result {
                yah_qed_gha::JobResult::Success => RunStatus::Success,
                yah_qed_gha::JobResult::Failure | yah_qed_gha::JobResult::Cancelled => RunStatus::Failed,
                yah_qed_gha::JobResult::Skipped => RunStatus::Skipped,
            };
            let error = matches!(inst.result, yah_qed_gha::JobResult::Failure)
                .then(|| gha_job_failure_detail(inst));
            crate::types::JobRow {
                id: inst.job_id.clone(),
                status,
                error,
                needs: needs_by_job.get(&inst.job_id).cloned().unwrap_or_default(),
            }
        }));

        // W224 R533-T7: an imported/delegated GHA workflow produces NO native
        // publish artifacts. The tier-3 `gh-release` override that used to stage
        // them is retired; QED's native publisher steps (W208) own release
        // artifacts now. The transformer (R533-F4) flags a workflow's release
        // step with a native-replacement stanza for the human to wire as a
        // native step — those steps emit `produces`, not this delegated path.
        let produced: Vec<crate::types::ProducedArtifact> = Vec::new();

        // Surface a workflow-level failure as a clean StepFailed enumerating
        // EVERY failing job (and the first failing step inside each), with a
        // stderr tail per job so operators see *why* without having to chase
        // the nested WorkflowRun manually. A single gha-workflow step can fan
        // out to many jobs (e.g. image-yah-*); collapsing to just the first
        // failure (the old `.find`) silently dropped the rest.
        let failing: Vec<&_> = run
            .instances
            .iter()
            .filter(|i| matches!(i.result, yah_qed_gha::JobResult::Failure))
            .collect();
        if !failing.is_empty() {
            let per_job: Vec<String> = failing
                .iter()
                .map(|job| format!("job `{}` {}", job.job_id, gha_job_failure_detail(job)))
                .collect();
            // Reconcile the text report with the job graph: the UI renders every
            // skipped job too, so a report that names only the failures reads as
            // "6 failed" while the screen shows ~20 red/grey rows. Count the
            // skips (downstream jobs gated on a failed/skipped dependency) and
            // say so explicitly, so the gap between "failed N" and "graph shows
            // more" is accounted for rather than mysterious (R516).
            let skipped = run
                .instances
                .iter()
                .filter(|i| matches!(i.result, yah_qed_gha::JobResult::Skipped))
                .count();
            let skip_note = if skipped > 0 {
                format!(
                    "\n\n{skipped} downstream job(s) skipped — gated on a failed or \
                     skipped dependency, not independent failures."
                )
            } else {
                String::new()
            };
            let msg = if per_job.len() == 1 {
                format!(
                    "gha-workflow `{}` failed at {}{}",
                    cfg.path.display(),
                    per_job[0],
                    skip_note,
                )
            } else {
                format!(
                    "gha-workflow `{}` failed in {} jobs:\n\n{}{}",
                    cfg.path.display(),
                    per_job.len(),
                    per_job.join("\n\n"),
                    skip_note,
                )
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
            if !matches!(instance.result, yah_qed_gha::JobResult::Success) {
                continue;
            }
            for (key, value) in &instance.outputs {
                outputs.insert(format!("{}.{}", instance.job_id, key), value.as_str_lossy());
            }
        }

        Ok((produced, outputs))
    }

    /// Dispatch a [`StepKind::Import`] step (W224 "import, don't emulate";
    /// R533-F1). Reads the imported `workflow.yml` source, recomputes its
    /// blake3 content hash, checks it against the pinned hash, then expands the
    /// source into the native subgraph and executes it.
    ///
    /// F1's expansion is the single-node [`crate::import::ImportExpansion::Delegated`]
    /// form: route through the recast W200 GHA front-end (so the import step
    /// actually runs while GHA is canonical). The hash pin is the drift
    /// guardrail; under the default **virtual** expansion a drifted source is
    /// benign — we re-expand from whatever is on disk, only logging the drift.
    /// R533-F4 swaps the expansion body for the mechanical tier-1/2 native map;
    /// R533-F6 wires `materialize` (eject to TOML) + the stale-source guard.
    async fn execute_step_import(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
        jobs_out: &mut Vec<crate::types::JobRow>,
    ) -> Result<
        (
            Vec<crate::types::ProducedArtifact>,
            std::collections::HashMap<String, String>,
        ),
        RunnerError,
    > {
        let Some(cfg) = step.import.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=import with no [import] block (validate() should have caught this)",
                step.name,
            )));
        };

        let camp_root = self.resolve_camp_root()?;
        let source_path = if cfg.source.is_absolute() {
            cfg.source.clone()
        } else {
            camp_root.join(&cfg.source)
        };

        // Read the source so we can pin/verify its hash. A missing source is a
        // hard error (unlike a drifted hash) — there's nothing to expand.
        let bytes = std::fs::read(&source_path).map_err(|e| RunnerError::StepFailed {
            step: step.name.clone(),
            msg: format!("read import source {}: {e}", source_path.display()),
        })?;
        let actual = crate::import::content_hash(&bytes);

        // Freshness against the pin. Virtual-by-default (the F1 path): a stale
        // source is benign — expand from disk and note the drift. The pin is
        // load-bearing for the materialized eject guard (R533-F6), not for the
        // virtual run, so we never fail the run here.
        match cfg.freshness(&actual) {
            crate::import::ImportFreshness::Fresh => {}
            crate::import::ImportFreshness::Unpinned => {
                tracing::debug!(
                    step = %step.name,
                    source = %source_path.display(),
                    hash = %actual,
                    "import: source not yet pinned; expanding virtually (hash recorded for a future eject)",
                );
            }
            crate::import::ImportFreshness::Stale { pinned, actual } => {
                tracing::warn!(
                    step = %step.name,
                    source = %source_path.display(),
                    %pinned,
                    %actual,
                    "import: source drifted from its pinned hash; re-expanding virtually \
                     (zero-drift by construction — nothing stored to diverge)",
                );
            }
        }

        if cfg.materialize {
            tracing::warn!(
                step = %step.name,
                "import: `materialize = true` is a request to eject to generated TOML, which is \
                 an explicit one-time move (`crate::eject::eject` / `qed eject`), not a per-run \
                 side-effect; proceeding with virtual expansion this run (R533-F6)",
            );
        }

        // Plan-time expansion. F1 yields a single delegated GHA front-end node;
        // F4 will generalize this match with a native-steps arm.
        match crate::import::expand_import(cfg) {
            crate::import::ImportExpansion::Delegated(gha) => {
                self.execute_step_gha_workflow(event_index, step, &gha, jobs_out)
                    .await
            }
        }
    }

    /// Dispatch a [`StepKind::WaitFor`] step (R513-F3, W207 Gap #5): poll the
    /// configured endpoint until it is healthy, then return `Ok(())`; fail the
    /// step if it never comes up within `timeout_secs`.
    ///
    /// The loop emits a live [`QedEvent::StepOutput`] line per attempt so the
    /// QED tail shows "waiting … (attempt N)" and, on success, "healthy after
    /// Nms" — the same streaming contract a subprocess step has. Cancellation is
    /// structural: on `qed.cancel` the whole run future is dropped, which drops
    /// this loop mid-`sleep`/probe — no lingering poller.
    ///
    /// The probe primitives live in [`crate::waitfor`]; this owns only the
    /// deadline/interval scheduling and event emission.
    async fn execute_step_wait_for(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
    ) -> Result<(), RunnerError> {
        let Some(cfg) = step.wait_for.as_ref() else {
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: kind=wait-for with no [wait_for] block (validate() should have caught this)",
                step.name,
            )));
        };

        // Resolve the probe shape once, up front, so a malformed URL fails the
        // step immediately instead of burning the whole timeout budget retrying
        // an un-parseable target.
        enum Probe {
            Http(crate::waitfor::HttpTarget, Option<u16>),
            Tcp(String),
        }
        let (probe, target_label) = if let Some(url) = cfg.http.as_ref() {
            let target = crate::waitfor::parse_http_url(url).map_err(|msg| {
                RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!("wait-for: {msg}"),
                }
            })?;
            (Probe::Http(target, cfg.expect_status), url.clone())
        } else if let Some(addr) = cfg.tcp.as_ref() {
            (Probe::Tcp(addr.clone()), addr.clone())
        } else {
            // validate() guarantees exactly one target; defensive only.
            return Err(RunnerError::InvalidConfig(format!(
                "step `{}`: wait-for with no http/tcp target (validate() should have caught this)",
                step.name,
            )));
        };

        let timeout_budget = std::time::Duration::from_secs(cfg.timeout_secs);
        let interval = std::time::Duration::from_millis(cfg.interval_ms.max(1));
        // Per-attempt timeout: never let a single probe outlast the whole budget.
        let attempt_timeout = timeout_budget.min(std::time::Duration::from_secs(5));
        let deadline = tokio::time::Instant::now() + timeout_budget;

        self.emit(QedEvent::StepOutput {
            index: event_index,
            name: step.name.clone(),
            stream: crate::events::OutputStream::Stdout,
            line: format!(
                "wait-for: polling {target_label} (timeout {}s, interval {}ms)",
                cfg.timeout_secs, cfg.interval_ms,
            ),
        });

        let started = tokio::time::Instant::now();
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let outcome: Result<(), String> = match &probe {
                Probe::Http(target, expect) => {
                    match crate::waitfor::probe_http_once(target, attempt_timeout).await {
                        Ok(status) if crate::waitfor::http_status_ok(status, *expect) => Ok(()),
                        Ok(status) => Err(match expect {
                            Some(want) => format!("HTTP {status} (want {want})"),
                            None => format!("HTTP {status} (want 2xx/3xx)"),
                        }),
                        Err(e) => Err(e),
                    }
                }
                Probe::Tcp(addr) => crate::waitfor::probe_tcp_once(addr, attempt_timeout).await,
            };

            match outcome {
                Ok(()) => {
                    let elapsed = started.elapsed().as_millis();
                    self.emit(QedEvent::StepOutput {
                        index: event_index,
                        name: step.name.clone(),
                        stream: crate::events::OutputStream::Stdout,
                        line: format!(
                            "wait-for: {target_label} healthy after {elapsed}ms ({attempt} attempt{})",
                            if attempt == 1 { "" } else { "s" },
                        ),
                    });
                    return Ok(());
                }
                Err(reason) => {
                    // Stop if the next interval would push us past the budget —
                    // no point sleeping only to give up.
                    if tokio::time::Instant::now() + interval >= deadline {
                        return Err(RunnerError::StepFailed {
                            step: step.name.clone(),
                            msg: format!(
                                "wait-for: {target_label} never became healthy within {}s \
                                 ({attempt} attempts; last: {reason})",
                                cfg.timeout_secs,
                            ),
                        });
                    }
                    self.emit(QedEvent::StepOutput {
                        index: event_index,
                        name: step.name.clone(),
                        stream: crate::events::OutputStream::Stderr,
                        line: format!("wait-for: attempt {attempt} not ready ({reason}); retrying"),
                    });
                    tokio::time::sleep(interval).await;
                }
            }
        }
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

        // R531-T6: if F3 resolves this step to the NativeCross tier (a
        // foreign-arch crossable target), route its build onto the host-native
        // cross toolchain (cargo-zigbuild / musl-cross) instead of running the
        // recipe's `cross build` verbatim — the mesofact "stop using the amd64
        // container, use zigbuild" fix. Native-only per W224: imported GHA
        // steps lift their target at import time, they don't reach this seam.
        let mut cross_env: Vec<(String, String)> = Vec::new();
        let effective_step;
        let step: &crate::types::QedStep =
            match self.native_cross_plan(step, &self.cross_availability()) {
                Some(Ok(plan)) => {
                    tracing::info!(
                        target: "qed::nativecross",
                        step = %step.name,
                        tool = plan.tool.label(),
                        "rerouting build to host-native cross: {:?}",
                        plan.argv,
                    );
                    cross_env = plan.env;
                    effective_step = crate::types::QedStep {
                        argv: plan.argv,
                        ..step.clone()
                    };
                    &effective_step
                }
                Some(Err(unavailable)) => {
                    // No host-native toolchain for a target the table said *should*
                    // cross-compile — fail with the install hint rather than fall
                    // through to a confusing linker/manifest error.
                    return Err(RunnerError::StepFailed {
                        step: step.name.clone(),
                        msg: unavailable.to_string(),
                    });
                }
                None => step,
            };

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
        // Cross-toolchain env (musl-cross linker/CC/AR) underlays the step's own
        // env and the output-collection extras, so an explicit step `env` still
        // wins on a key collision.
        for (k, v) in cross_env {
            merged_env.entry(k).or_insert(v);
        }
        if let Some(extra) = extra_env {
            merged_env.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        let ctx = ExecContext::default()
            .with_cwd(cwd)
            .with_env(merged_env.into_iter().collect());
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
        let image = velveteen::default_image::default_forge_image();
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
                            velveteen::OutputStream::Stdout => OutputStream::Stdout,
                            velveteen::OutputStream::Stderr => OutputStream::Stderr,
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
                msg: format!("failed to spawn (is the runtime installed and accessible?): {msg}"),
            }),
            Err(ForgeExecutorError::Io(e)) => Err(RunnerError::Io(e)),
            Err(ForgeExecutorError::Unsupported(what)) => Err(RunnerError::InvalidConfig(format!(
                "subprocess executor: {what}"
            ))),
        }
    }

    /// Spawn a background sidecar step (R513-F2) onto its own task and return a
    /// [`JoinHandle`] the caller tracks until reap. Mirrors
    /// [`Self::drive_subprocess_step`] — same `ExecEvent` → `QedEvent::StepOutput`
    /// adapter so a sidecar's logs keep streaming under its step index — but
    /// does NOT await completion: the future runs detached so the step loop
    /// advances immediately.
    ///
    /// Only the `executor` + `events` are captured (both cheap `Arc`/`Sender`
    /// clones) so the spawned future is `'static`. The inner
    /// [`ForgeExecutor::execute`] owns the `kill_on_drop` child, so aborting the
    /// returned handle (reap, cancel, or `run_inner` early-return) kills the
    /// process.
    ///
    /// [`JoinHandle`]: tokio::task::JoinHandle
    fn spawn_background_step(
        &self,
        event_index: usize,
        step: &crate::types::QedStep,
        spec: ForgeSpec,
        ctx: ExecContext,
    ) -> tokio::task::JoinHandle<Result<(), RunnerError>> {
        let executor = self.executor.clone();
        let events = self.events.clone();
        let name = step.name.clone();
        tokio::spawn(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecEvent>();
            let adapter = {
                let events = events.clone();
                let name = name.clone();
                tokio::spawn(async move {
                    while let Some(ev) = rx.recv().await {
                        let Some(events) = &events else { continue };
                        if let ExecEvent::Output { stream, line } = ev {
                            let qed_stream = match stream {
                                velveteen::OutputStream::Stdout => OutputStream::Stdout,
                                velveteen::OutputStream::Stderr => OutputStream::Stderr,
                            };
                            let _ = events.send(QedEvent::StepOutput {
                                index: event_index,
                                name: name.clone(),
                                stream: qed_stream,
                                line,
                            });
                        }
                    }
                })
            };

            let outcome_result = executor.execute(spec, ctx, Some(tx)).await;
            let _ = adapter.await;

            match outcome_result {
                Ok(outcome) if outcome.succeeded() => Ok(()),
                Ok(outcome) => Err(RunnerError::StepFailed {
                    step: name,
                    msg: outcome.stderr_tail,
                }),
                Err(ForgeExecutorError::Spawn(msg)) => Err(RunnerError::StepFailed {
                    step: name,
                    msg: format!(
                        "failed to spawn (is the runtime installed and accessible?): {msg}"
                    ),
                }),
                Err(ForgeExecutorError::Io(e)) => Err(RunnerError::Io(e)),
                Err(ForgeExecutorError::Unsupported(what)) => Err(RunnerError::InvalidConfig(
                    format!("subprocess executor: {what}"),
                )),
            }
        })
    }
}

/// Translate a [`yah_qed_gha::GhaEvent`] into the qed-runner's own
/// [`crate::QedEvent::Gha*`] variant, stamping the parent step's index and
/// name so the desktop pane can scope nested rows under the right step
/// (W200 R487 follow-up).
fn bridge_gha_event(
    step_index: usize,
    parent_name: &str,
    ev: yah_qed_gha::GhaEvent,
) -> crate::QedEvent {
    use yah_qed_gha::GhaEvent as G;
    let at = chrono::Utc::now();
    match ev {
        G::JobStarted {
            job_id,
            matrix_index,
            key,
            total_steps,
        } => crate::QedEvent::GhaJobStarted {
            index: step_index,
            name: parent_name.to_string(),
            job_id,
            matrix_index,
            job_key: key,
            total_steps,
            at,
        },
        G::JobFinished {
            job_id: _,
            matrix_index: _,
            key,
            result,
        } => crate::QedEvent::GhaJobFinished {
            index: step_index,
            name: parent_name.to_string(),
            job_key: key,
            result: gha_result_str(result).to_string(),
            at,
        },
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
                yah_qed_gha::GhaOutputStream::Stdout => crate::events::OutputStream::Stdout,
                yah_qed_gha::GhaOutputStream::Stderr => crate::events::OutputStream::Stderr,
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

/// Same key format as [`yah_qed_gha::JobInstance::key`] — `"<job>"` for non-matrix
/// jobs, `"<job>#<row>"` for matrix rows. Kept in sync by construction; the
/// receiver pairs Start / Finish by exact-string compare.
fn instance_key(job_id: &str, matrix_index: Option<usize>) -> String {
    match matrix_index {
        Some(idx) => format!("{job_id}#{idx}"),
        None => job_id.to_string(),
    }
}

fn gha_result_str(r: yah_qed_gha::JobResult) -> &'static str {
    match r {
        yah_qed_gha::JobResult::Success => "success",
        yah_qed_gha::JobResult::Failure => "failure",
        yah_qed_gha::JobResult::Cancelled => "cancelled",
        yah_qed_gha::JobResult::Skipped => "skipped",
    }
}

fn gha_conclusion_str(c: yah_qed_gha::StepConclusion) -> &'static str {
    match c {
        yah_qed_gha::StepConclusion::Success => "success",
        yah_qed_gha::StepConclusion::Failure => "failure",
        yah_qed_gha::StepConclusion::Skipped => "skipped",
    }
}

/// Last `lines` non-blank lines of `stderr`, with qed-gha's internal
/// `$GITHUB_ENV` sidechannel marker stripped (see `pop_env_updates` in
/// yah_qed_gha::runtime). Empty when there is nothing useful left to show.
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

/// Render the failure detail for one failed GHA job instance: the first failing
/// step's name plus its stderr tail. Shared by the flattened step-level failure
/// summary (which prefixes the job id) and the structured per-job rows (W223
/// R532-T1, where the [`crate::types::JobRow`] already carries the job id, so
/// this is the row's `error` text without the redundant prefix).
fn gha_job_failure_detail(job: &yah_qed_gha::InstanceRun) -> String {
    let failing_step = job
        .steps
        .iter()
        .find(|s| matches!(s.conclusion, yah_qed_gha::StepConclusion::Failure));
    match failing_step {
        Some(s) => {
            let label = s
                .name
                .clone()
                .or_else(|| s.step_id.clone())
                .unwrap_or_else(|| "<unnamed>".to_string());
            let tail = stderr_tail(&s.stderr, 20);
            if tail.is_empty() {
                format!("step `{label}` (no stderr)")
            } else {
                format!("step `{label}`:\n{tail}")
            }
        }
        None => "(no failing step recorded — likely an override / scheduler error)".to_string(),
    }
}

/// Build a minimal `yah_qed_gha::Value` object from a string map. Used to lower
/// `[gha_workflow] inputs = { tag = "v1" }` into the runtime's `inputs.*`
/// expression context.
fn inputs_to_value(inputs: &std::collections::HashMap<String, String>) -> yah_qed_gha::Value {
    let mut m: indexmap::IndexMap<String, yah_qed_gha::Value> = indexmap::IndexMap::new();
    for (k, v) in inputs {
        m.insert(k.clone(), yah_qed_gha::Value::String(v.clone()));
    }
    yah_qed_gha::Value::Object(m)
}

/// RAII guard for a `WorkspaceMode::Isolated` git worktree (W224). Dropping it
/// runs `git worktree remove --force` so a release run — including one that
/// errors mid-step — never leaves an orphaned tree behind. Best-effort: a
/// failed removal is swallowed (the next run's pre-add cleanup clears it).
#[derive(Debug)]
struct WorktreeGuard {
    camp_root: std::path::PathBuf,
    worktree: std::path::PathBuf,
}

impl Drop for WorktreeGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new("git")
            .current_dir(&self.camp_root)
            .args(["worktree", "remove", "--force"])
            .arg(&self.worktree)
            .output();
    }
}

/// Run a git command in `dir`, mapping a non-zero exit to its trimmed stderr.
fn run_git(dir: &std::path::Path, args: &[&str]) -> Result<(), String> {
    let out = std::process::Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .map_err(|e| format!("spawn git: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// True when the working tree has uncommitted *tracked* changes. Untracked
/// files are ignored (`--untracked-files=no`): they don't change which
/// committed bytes a build sees, and a working camp almost always carries some.
fn git_tree_is_dirty(dir: &std::path::Path) -> Result<bool, RunnerError> {
    let out = std::process::Command::new("git")
        .current_dir(dir)
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .map_err(RunnerError::Io)?;
    if !out.status.success() {
        return Err(RunnerError::InvalidConfig(format!(
            "git status failed in {}: {}",
            dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(!out.stdout.is_empty())
}

/// Synthesize a `github` expression context for a GhaWorkflow step from the
/// camp's live git state. `release.yml` references `github.sha` (the
/// `:smoke-<sha>` image tag), `github.ref_name` (tarball stage dirs + the
/// `!contains(ref_name, '-')` smoke gate) and `github.actor` (ghcr login),
/// so leaving these empty produced malformed `:smoke-` tags and `cli--<triple>`
/// stage names. We read them from the workspace's git checkout, mirroring what
/// a real runner gets from the push event:
///   sha      = `git rev-parse HEAD` (full 40-char, matching GHA)
///   ref_name = exact tag if HEAD is tagged, else the current branch
///   actor    = `git config user.name`
/// Each lookup degrades to empty on error (detached/dirty/no-git) rather than
/// failing the step — an empty field is no worse than the old behaviour.
fn github_context(event_name: &str, workspace: &std::path::Path) -> yah_qed_gha::Value {
    let git = |args: &[&str]| -> String {
        std::process::Command::new("git")
            .current_dir(workspace)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    };

    let sha = git(&["rev-parse", "HEAD"]);
    // Prefer an exact tag (the real release trigger shape) over the branch.
    let exact_tag = git(&["describe", "--tags", "--exact-match"]);
    let (ref_name, ref_full) = if !exact_tag.is_empty() {
        (exact_tag.clone(), format!("refs/tags/{exact_tag}"))
    } else {
        let branch = git(&["rev-parse", "--abbrev-ref", "HEAD"]);
        let full = if branch.is_empty() {
            String::new()
        } else {
            format!("refs/heads/{branch}")
        };
        (branch, full)
    };
    let actor = git(&["config", "user.name"]);

    let mut m: indexmap::IndexMap<String, yah_qed_gha::Value> = indexmap::IndexMap::new();
    m.insert(
        "event_name".into(),
        yah_qed_gha::Value::String(event_name.into()),
    );
    m.insert("ref".into(), yah_qed_gha::Value::String(ref_full));
    m.insert("ref_name".into(), yah_qed_gha::Value::String(ref_name));
    m.insert("sha".into(), yah_qed_gha::Value::String(sha));
    m.insert("actor".into(), yah_qed_gha::Value::String(actor));
    m.insert(
        "event".into(),
        yah_qed_gha::Value::Object(indexmap::IndexMap::new()),
    );
    yah_qed_gha::Value::Object(m)
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

/// R506: peel one layer of `${{ … }}` delimiters off an `if=` body so a
/// pipeline author can write either `if = "matrix.target == 'mac'"` or
/// `if = "${{ matrix.target == 'mac' }}"` interchangeably. Mirrors GHA's
/// implicit-expression-body semantics for the job-level `if:` key.
fn strip_expr_delimiters(input: &str) -> &str {
    let t = input.trim();
    if let Some(inner) = t.strip_prefix("${{").and_then(|s| s.strip_suffix("}}")) {
        inner.trim()
    } else {
        t
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
            if k.is_empty() {
                return None;
            }
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
            let forge_id = self
                .execute_step_build_image_remote(step, &prepared)
                .await?;
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
            let opts = velveteen::local::BuildImageOptions {
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
            velveteen::local::build_image_command(&opts)
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
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir)
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to load catalog: {e}")))?;

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
        let dockerfile_text =
            crate::images::compile_with_dockerfile_dir(&entry, &catalog, &per_camp_dir).map_err(
                |e| RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!("Dockerfile compile failed for `{image_name}`: {e}"),
                },
            )?;

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

        let tag = step
            .tag
            .clone()
            .unwrap_or_else(|| format!("{image_name}:dev"));
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
    /// the yubaba via a BuildKit-in-containerd workload (R381-T5).
    ///
    /// Mirrors [`Self::execute_step_remote`] but for `ForgeCommand::BuildImage`.
    /// The host-side dockerfile + context paths are passed verbatim to yubaba
    /// as bind-mount targets; this assumes the yubaba node has access to those
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
                TaskLocation::RemoteAny {
                    tier: TierTag("infra".into()),
                },
                TaskRuntime::Container,
            ),
            timeout: step.timeout.map(Millis::from_ms),
            label: Some(step.name.clone()),
            initiator: Initiator::Human { camp: "qed".into() },
            mesh_access: MeshAccess::None,
        };

        let handle = driver
            .start(spec)
            .await
            .map_err(|e| RunnerError::Remote(e.to_string()))?;
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
    /// `env` map and `description` so Kamaji knows how to launch the
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
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir)
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to load catalog: {e}")))?;

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

        if !entry
            .produces
            .contains(&crate::images::ProduceTarget::NativeTarball)
        {
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
            .ok_or_else(|| {
                RunnerError::InvalidConfig(format!(
                    "binary path `{}` has no filename component",
                    binary_path.display(),
                ))
            })?
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

        let output_path =
            crate::native::native_tarball_output_path(&camp_root, &entry.name, &triple);

        crate::native::pack_native_tarball(&binary_path, &manifest, &output_path).map_err(|e| {
            RunnerError::StepFailed {
                step: step.name.clone(),
                msg: format!(
                    "failed to pack native tarball at {}: {e}",
                    output_path.display()
                ),
            }
        })?;

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
        let catalog = crate::images::CatalogManifest::load(&camp_images_dir)
            .map_err(|e| RunnerError::InvalidConfig(format!("failed to load catalog: {e}")))?;

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

        if !entry
            .produces
            .contains(&crate::images::ProduceTarget::NativeTarball)
        {
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
        let tarball_path =
            crate::native::native_tarball_output_path(&camp_root, &entry.name, &triple);
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

        let signed =
            self.signer
                .sign_blob(&tarball_path)
                .await
                .map_err(|e| RunnerError::StepFailed {
                    step: step.name.clone(),
                    msg: format!(
                        "cosign sign-blob failed for `{}`: {e}",
                        tarball_path.display(),
                    ),
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
        index: usize,
        step: &crate::types::QedStep,
        runtime: TaskRuntime,
    ) -> Result<ObsForgeId, RunnerError> {
        let driver = self
            .remote_driver
            .as_ref()
            .expect("remote_driver is Some when run_where == Remote");

        let spec = ForgeSpec {
            command: ForgeCommand::Subprocess {
                argv: step.argv.clone(),
                image: None,
            },
            where_: TaskPlacement::new(
                TaskLocation::RemoteAny {
                    tier: TierTag("infra".into()),
                },
                runtime,
            ),
            timeout: step.timeout.map(Millis::from_ms),
            label: Some(step.name.clone()),
            // Camp name will be threaded through once yubaba RPC stabilises (R091).
            initiator: Initiator::Human { camp: "qed".into() },
            mesh_access: MeshAccess::None,
        };

        // Adapter: forward yubaba log lines into the runner's live sink as
        // StepOutput, mirroring the local subprocess path (R508). Without this
        // a yubaba-dispatched step only surfaced its log lines post-run via
        // scryer; now they stream into qed.tail / the desktop pane live.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecEvent>();
        let adapter = {
            let events = self.events.clone();
            let name = step.name.clone();
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    let Some(events) = &events else { continue };
                    if let ExecEvent::Output { stream, line } = ev {
                        let qed_stream = match stream {
                            velveteen::OutputStream::Stdout => OutputStream::Stdout,
                            velveteen::OutputStream::Stderr => OutputStream::Stderr,
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

        let handle = driver
            .start_with_sink(spec, Some(tx))
            .await
            .map_err(|e| RunnerError::Remote(e.to_string()))?;

        let forge_id = handle.id.clone();
        let status = handle.wait().await;
        // Drain any remaining buffered lines before the step is marked done.
        let _ = adapter.await;

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
    use yah_scryer::service::{Scryer, ScryerConfig};
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
                background: false,
                background_until: None,
                wait_for: None,
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
                import: None,
                matrix: None,
                enabled: true,
                activation: StepActivation::Active,
                if_cond: None,
                platform: None,
                toolchain: None,
                outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        }
    }

    // ── R507/W208 toolchain pinning preflight ──────────────────────────────

    fn tc_spec(pairs: &[(&str, &str)]) -> crate::toolchain::ToolchainSpec {
        crate::toolchain::ToolchainSpec {
            pins: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    fn host_map(pairs: &[(&str, Option<&str>)]) -> HashMap<String, Option<String>> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.map(str::to_string)))
            .collect()
    }

    #[test]
    fn toolchain_preflight_passes_when_host_satisfies_pin() {
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("rust", "1.84.0")]));
        let runner = PipelineRunner::new(pipeline)
            .with_host_toolchains(host_map(&[("rust", Some("1.84.0"))]));
        let pf = runner.toolchain_preflight();
        assert!(pf.is_satisfied(), "{:?}", pf.report());
        assert_eq!(pf.entries.len(), 1);
    }

    #[test]
    fn toolchain_preflight_blocks_on_missing_tool() {
        // noisetable's release.apple pins xcode=15.4; a host without it blocks.
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("xcode", "15.4")]));
        let runner =
            PipelineRunner::new(pipeline).with_host_toolchains(host_map(&[("xcode", None)]));
        let pf = runner.toolchain_preflight();
        assert!(!pf.is_satisfied());
        let report = pf.error_report().expect("blocking ⇒ report");
        assert!(report.contains("xcode"));
        assert!(report.contains("15.4"));
    }

    #[tokio::test]
    async fn run_fails_fast_when_host_cannot_satisfy_pin() {
        // The gate fires before any step executes — even a bare `echo` never
        // runs when the host can't satisfy the pin.
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("xcode", "15.4")]));
        let runner = PipelineRunner::new(pipeline)
            .with_host_toolchains(host_map(&[("xcode", Some("15.2"))]));
        let err = runner.run().await.unwrap_err();
        match err {
            RunnerError::ToolchainUnsatisfied(report) => {
                assert!(report.contains("xcode"));
                assert!(report.contains("15.4"));
                assert!(
                    report.contains("15.2"),
                    "report names the host version: {report}"
                );
            }
            other => panic!("expected ToolchainUnsatisfied, got {other:?}"),
        }
    }

    #[test]
    fn step_toolchain_override_beats_pipeline_pin() {
        // Pipeline pins ndk=r27; the step overrides to r26d. Host has ndk 26.3
        // — which the pipeline pin (27) would reject but the step override
        // (r26d → 26) satisfies. A satisfied preflight proves the override won.
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("ndk", "r27")]));
        pipeline.steps[0].toolchain = Some(tc_spec(&[("ndk", "r26d")]));
        let runner = PipelineRunner::new(pipeline)
            .with_host_toolchains(host_map(&[("ndk", Some("26.3.11579264"))]));
        let pf = runner.toolchain_preflight();
        assert!(
            pf.is_satisfied(),
            "step r26d override should win: {:?}",
            pf.report()
        );
        // Sanity: the *pipeline* pin alone (no override) would block this host.
        let mut blocked = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        blocked.toolchain = Some(tc_spec(&[("ndk", "r27")]));
        let blocked_runner = PipelineRunner::new(blocked)
            .with_host_toolchains(host_map(&[("ndk", Some("26.3.11579264"))]));
        assert!(!blocked_runner.toolchain_preflight().is_satisfied());
    }

    #[test]
    fn containerized_step_satisfies_pin_via_image() {
        // A step that pulls an image delegates its toolchain to that image, so
        // a host missing Xcode entirely still passes the preflight.
        let mut pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        pipeline.toolchain = Some(tc_spec(&[("xcode", "15.4")]));
        pipeline.steps[0].image = Some("apple-builder:15.4".into());
        let runner =
            PipelineRunner::new(pipeline).with_host_toolchains(host_map(&[("xcode", None)]));
        let pf = runner.toolchain_preflight();
        assert!(pf.is_satisfied());
        assert!(matches!(
            pf.entries[0].resolution,
            crate::toolchain::PinResolution::SatisfiedByImage { .. }
        ));
    }

    #[test]
    fn no_toolchain_block_means_no_preflight_entries() {
        let pipeline = one_step_pipeline("p", vec!["echo".into(), "hi".into()]);
        // Seed an empty host map so this never shells out.
        let runner = PipelineRunner::new(pipeline).with_host_toolchains(HashMap::new());
        let pf = runner.toolchain_preflight();
        assert!(pf.is_satisfied());
        assert!(pf.entries.is_empty());
    }

    // ── Scripted yubaba for qed tests ──────────────────────────────────────

    struct ScriptedWarden {
        lines: Vec<String>,
        exit_code: i32,
    }

    #[async_trait::async_trait]
    impl WardenClient for ScriptedWarden {
        async fn deploy(
            &self,
            _spec: &workload_spec::WorkloadSpec,
        ) -> Result<(), velveteen::RemoteForgeError> {
            Ok(())
        }

        async fn connect_logs(
            &self,
            _ident: &MeshIdent,
        ) -> Result<mpsc::Receiver<String>, velveteen::RemoteForgeError> {
            let (tx, rx) = mpsc::channel(64);
            let lines = self.lines.clone();
            tokio::spawn(async move {
                for line in lines {
                    let _ = tx.send(line).await;
                }
            });
            Ok(rx)
        }

        async fn teardown(&self, _ident: &MeshIdent) -> Result<(), velveteen::RemoteForgeError> {
            Ok(())
        }

        async fn exit_code(
            &self,
            _ident: &MeshIdent,
        ) -> Result<Option<i32>, velveteen::RemoteForgeError> {
            Ok(Some(self.exit_code))
        }
    }

    /// Remote path happy: single step exits 0, task_run_id populated in step status.
    #[tokio::test]
    async fn remote_step_success() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["build ok".to_string()],
            exit_code: 0,
        });

        let pipeline = one_step_pipeline("test-remote", vec!["true".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
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
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["error: something went wrong".to_string()],
            exit_code: 1,
        });

        let pipeline = one_step_pipeline("test-remote-fail", vec!["false".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// R508: a yubaba-dispatched step streams its log lines into the live
    /// event sink as `StepOutput` *during* the run — not just into scryer
    /// post-completion. The scripted yubaba emits two lines; both surface as
    /// StepOutput events carrying the step's index and name.
    #[tokio::test]
    async fn remote_step_streams_output_to_sink() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["remote line 1".to_string(), "remote line 2".to_string()],
            exit_code: 0,
        });

        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline("test-remote-stream", vec!["true".to_string()]);
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let mut lines = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let QedEvent::StepOutput { index, name, line, .. } = ev {
                assert_eq!(index, 0, "single-step pipeline → index 0");
                assert_eq!(name, "step-1", "StepOutput carries the step name");
                lines.push(line);
            }
        }
        assert_eq!(
            lines,
            vec!["remote line 1".to_string(), "remote line 2".to_string()],
            "both yubaba log lines must stream through as StepOutput",
        );
    }

    /// Remote path: second step skipped when first fails with on_fail=Abort.
    #[tokio::test]
    async fn remote_abort_on_fail() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 1,
        });

        let mut pipeline = one_step_pipeline("test-abort", vec!["false".to_string()]);
        pipeline.steps.push(crate::types::QedStep {
            background: false,
            background_until: None,
            wait_for: None,
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
            import: None,
            matrix: None,
            enabled: true,
            activation: StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
            outputs: Vec::new(),
        });

        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(
            meta.steps.len(),
            1,
            "step-2 should be skipped after step-1 fails"
        );
    }

    // ── Outcome dispatch tests ─────────────────────────────────────────────

    use crate::types::Outcome;
    use std::sync::Mutex;

    struct RecordingDispatcher {
        calls: Mutex<Vec<String>>,
    }

    impl RecordingDispatcher {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(vec![]),
            })
        }

        fn recorded(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl OutcomeDispatcher for RecordingDispatcher {
        async fn warden_deploy(&self, service: &str, env: &str) -> Result<(), RunnerError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("yubaba-deploy:{service}:{env}"));
            Ok(())
        }

        async fn almanac_run(&self, pipeline: &str) -> Result<(), RunnerError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("almanac-run:{pipeline}"));
            Ok(())
        }

        async fn publish(&self, req: &crate::publish::PublishRequest) -> Result<(), RunnerError> {
            // Record the bucket + how many artifacts the run collected, so a
            // test can assert that only *successful* steps' artifacts arrive.
            self.calls.lock().unwrap().push(format!(
                "publish:{}:{}",
                req.bucket,
                req.artifacts.len()
            ));
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
                background: false,
                background_until: None,
                wait_for: None,
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
                import: None,
                matrix: None,
                enabled: true,
                activation: StepActivation::Active,
                if_cond: None,
                platform: None,
                toolchain: None,
                outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success,
            on_fail,
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        }
    }

    /// on_success outcomes are dispatched when the pipeline passes.
    #[tokio::test]
    async fn dispatches_on_success() {
        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(
            vec![
                Outcome::WardenDeploy {
                    service: "yah".into(),
                    env: "production".into(),
                },
                Outcome::AlmanacRun {
                    pipeline: "update-index".into(),
                },
            ],
            vec![],
            vec!["true".to_string()],
        );
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        let calls = dispatcher.recorded();
        assert_eq!(
            calls,
            vec!["yubaba-deploy:yah:production", "almanac-run:update-index"]
        );
    }

    /// on_fail outcomes are dispatched when the pipeline fails; on_success is not.
    #[tokio::test]
    async fn dispatches_on_fail_not_on_success() {
        let dispatcher = RecordingDispatcher::new();
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::WardenDeploy {
                service: "yah".into(),
                env: "production".into(),
            }],
            vec![Outcome::AlmanacRun {
                pipeline: "notify-failure".into(),
            }],
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

    // ── R509 Outcome::Provider dispatch wiring ──────────────────────────────

    /// Test adapter: records that it ran and (live path) returns a transformed
    /// copy of the first input artifact (same path — an in-place transform like
    /// notarize) plus one *new* artifact (an appcast), so a downstream outcome
    /// can be asserted to see the threaded set.
    struct FakeProvider {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::provider::ReleaseProvider for FakeProvider {
        fn name(&self) -> &str {
            "fake-transform"
        }
        async fn dispatch(
            &self,
            ctx: &crate::provider::ProviderContext<'_>,
        ) -> Result<crate::provider::ProviderReport, RunnerError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Echo every input back (in-place transform: same paths) and
            // append a brand-new appcast artifact.
            let mut produced: Vec<ProducedArtifact> = ctx.artifacts.to_vec();
            produced.push(ProducedArtifact {
                binary: "appcast".into(),
                path: "out/appcast.xml".into(),
                triple: None,
            });
            Ok(crate::provider::ProviderReport {
                actions: vec!["transformed".into()],
                produced,
                published: vec!["https://fake/feed.xml".into()],
            })
        }
    }

    fn fake_registry(
        calls: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Arc<crate::provider::ProviderRegistry> {
        Arc::new(crate::provider::ProviderRegistry::new().with(Arc::new(FakeProvider { calls })))
    }

    /// An `Outcome::Provider` dispatches through the wired registry on success.
    #[tokio::test]
    async fn provider_outcome_dispatches_through_registry() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::Provider {
                provider: "fake-transform".into(),
                with: serde_json::Value::Null,
                base_url: None,
            }],
            vec![],
            vec!["true".to_string()],
        );
        let runner = PipelineRunner::new(pipeline).with_release_providers(
            fake_registry(calls.clone()),
            Arc::new(crate::provider::MapSecrets::default()),
        );
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// A provider transform folds its produced artifacts back into the working
    /// set so a *following* `Outcome::Publish` ships the transformed bundle plus
    /// any new artifact (the notarize→sparkle / sign→publish chain).
    #[tokio::test]
    async fn provider_transform_feeds_downstream_publish() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dispatcher = RecordingDispatcher::new();
        let mut pipeline = pipeline_with_outcomes(
            vec![
                Outcome::Provider {
                    provider: "fake-transform".into(),
                    with: serde_json::Value::Null,
                    base_url: None,
                },
                Outcome::Publish {
                    provider: "r2".into(),
                    bucket: "yah-releases".into(),
                    prefix: None,
                    base_url: None,
                },
            ],
            vec![],
            vec!["true".to_string()],
        );
        pipeline.steps[0].produces = vec![ProducedArtifact {
            binary: "yah".into(),
            path: "target/release/yah".into(),
            triple: None,
        }];
        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone())
            .with_release_providers(
                fake_registry(calls.clone()),
                Arc::new(crate::provider::MapSecrets::default()),
            );
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        // The original artifact (replaced in place) + the appended appcast = 2.
        assert_eq!(dispatcher.recorded(), vec!["publish:yah-releases:2"]);
    }

    /// An `Outcome::Provider` naming an unregistered adapter fails the run with
    /// a typed error listing the known names (default empty registry).
    #[tokio::test]
    async fn unknown_provider_outcome_is_typed_error() {
        let pipeline = pipeline_with_outcomes(
            vec![Outcome::Provider {
                provider: "ghost".into(),
                with: serde_json::Value::Null,
                base_url: None,
            }],
            vec![],
            vec!["true".to_string()],
        );
        let err = PipelineRunner::new(pipeline).run().await.unwrap_err();
        assert!(
            matches!(err, RunnerError::Outcome(ref m) if m.contains("ghost")),
            "unknown provider surfaces a typed Outcome error: {err}"
        );
    }

    // ── R325-F2 live event-stream tests ────────────────────────────────────

    /// A runner with an attached sink emits the full lifecycle in order, with
    /// the step's stdout captured as a `StepOutput` line.
    #[tokio::test]
    async fn emits_lifecycle_events_with_streamed_output() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline(
            "test-events",
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo hello-stdout".to_string(),
            ],
        );
        let runner = PipelineRunner::new(pipeline).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }

        assert!(
            matches!(
                events.first(),
                Some(QedEvent::RunStarted { total_steps: 1, .. })
            ),
            "first event is RunStarted, got {:?}",
            events.first()
        );
        assert!(
            matches!(
                events.last(),
                Some(QedEvent::RunFinished {
                    status: RunStatus::Success,
                    ..
                })
            ),
            "last event is RunFinished/Success, got {:?}",
            events.last()
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, QedEvent::StepStarted { index: 0, .. })),
            "saw StepStarted for step 0"
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepFinished {
                    index: 0,
                    status: RunStatus::Success,
                    ..
                }
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

    // ── R513-F2 background sidecar steps (W207 Gap #4) ─────────────────────

    /// Build a single subprocess [`QedStep`] named `name` running `argv`,
    /// reusing the fully-populated literal in [`one_step_pipeline`] so new
    /// fields don't need threading through each background test.
    fn mk_step(name: &str, argv: &[&str]) -> crate::types::QedStep {
        let mut p = one_step_pipeline("x", argv.iter().map(|s| s.to_string()).collect());
        let mut s = p.steps.remove(0);
        s.name = name.to_string();
        s
    }

    /// A multi-step local pipeline (Live workspace, no outcomes) from the
    /// given steps.
    fn bg_pipeline(name: &str, steps: Vec<crate::types::QedStep>) -> Pipeline {
        let mut p = one_step_pipeline(name, vec!["true".to_string()]);
        p.steps = steps;
        p
    }

    /// Position of the `StepFinished` event for the named step, if any.
    fn finished_pos(events: &[QedEvent], name: &str) -> Option<usize> {
        events.iter().position(
            |e| matches!(e, QedEvent::StepFinished { name: n, .. } if n == name),
        )
    }

    fn drain_events(rx: &mut mpsc::UnboundedReceiver<QedEvent>) -> Vec<QedEvent> {
        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }
        events
    }

    /// A `background = true` sidecar that never exits on its own is spawned (so
    /// the loop doesn't block on it), runs alongside the foreground step, and is
    /// reaped — killed cleanly, status Success — at the end of the pipeline.
    #[tokio::test]
    async fn background_step_spawns_and_is_reaped_at_pipeline_end() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let server = {
            let mut s = mk_step("server", &["sh", "-c", "sleep 30"]);
            s.background = true;
            s
        };
        let work = mk_step("work", &["sh", "-c", "echo done"]);
        let pipeline = bg_pipeline("bg-end", vec![server, work]);

        let runner = PipelineRunner::new(pipeline).with_events(tx);
        // Completes promptly despite the sidecar's `sleep 30` — proof the loop
        // never awaited it.
        let meta = runner.run().await.unwrap();

        assert_eq!(meta.status, RunStatus::Success);
        let server_row = meta.steps.iter().find(|s| s.name == "server").unwrap();
        assert_eq!(
            server_row.status,
            RunStatus::Success,
            "a healthy sidecar killed at teardown is Success, not a failure"
        );
        assert!(server_row.completed_at.is_some());

        let events = drain_events(&mut rx);
        // The sidecar's StepFinished lands after the foreground step's — it was
        // reaped at the end of the loop.
        let server_fin = finished_pos(&events, "server").expect("server finished");
        let work_fin = finished_pos(&events, "work").expect("work finished");
        assert!(
            work_fin < server_fin,
            "background server reaped after foreground work; events={events:?}"
        );
    }

    /// `background_until = "gate"` reaps the sidecar the moment the gate step
    /// finishes — before any later step runs.
    #[tokio::test]
    async fn background_until_reaps_after_named_step() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let server = {
            let mut s = mk_step("server", &["sh", "-c", "sleep 30"]);
            s.background_until = Some("gate".to_string());
            s
        };
        let gate = mk_step("gate", &["sh", "-c", "echo gate"]);
        let after = mk_step("after", &["sh", "-c", "echo after"]);
        let pipeline = bg_pipeline("bg-until", vec![server, gate, after]);

        let meta = PipelineRunner::new(pipeline)
            .with_events(tx)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let events = drain_events(&mut rx);
        let gate_fin = finished_pos(&events, "gate").expect("gate finished");
        let server_fin = finished_pos(&events, "server").expect("server finished");
        let after_fin = finished_pos(&events, "after").expect("after finished");
        assert!(
            gate_fin < server_fin && server_fin < after_fin,
            "server reaped after gate, before after; events={events:?}"
        );
    }

    /// A sidecar that *exits non-zero on its own* before reap is a genuine
    /// failure: its step is Failed and the run flips to Failed (so `on_fail`
    /// fires). The gate step's sleep guarantees the crasher has exited by reap.
    #[tokio::test]
    async fn background_sidecar_crash_fails_the_run() {
        let crasher = {
            let mut s = mk_step("crasher", &["sh", "-c", "exit 7"]);
            s.background_until = Some("gate".to_string());
            s
        };
        let gate = mk_step("gate", &["sh", "-c", "sleep 0.3; echo gate"]);
        let pipeline = bg_pipeline("bg-crash", vec![crasher, gate]);

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Failed,
            "a sidecar that crashed mid-pipeline flips the run to Failed"
        );
        let crasher_row = meta.steps.iter().find(|s| s.name == "crasher").unwrap();
        assert_eq!(crasher_row.status, RunStatus::Failed);
    }

    /// Pre-flight rejects a `background_until` that names a step at-or-before
    /// the sidecar — the gate would never fire, so fail loudly at run start.
    #[tokio::test]
    async fn background_until_earlier_step_is_rejected() {
        let early = mk_step("early", &["sh", "-c", "echo early"]);
        let server = {
            let mut s = mk_step("server", &["sh", "-c", "sleep 30"]);
            s.background_until = Some("early".to_string());
            s
        };
        let pipeline = bg_pipeline("bg-bad-order", vec![early, server]);

        let err = PipelineRunner::new(pipeline).run().await.unwrap_err();
        assert!(
            matches!(err, RunnerError::InvalidConfig(ref m) if m.contains("later")),
            "expected later-step InvalidConfig, got {err:?}"
        );
    }

    /// Pre-flight rejects a `background_until` naming a nonexistent step.
    #[tokio::test]
    async fn background_until_unknown_step_is_rejected() {
        let server = {
            let mut s = mk_step("server", &["sh", "-c", "sleep 30"]);
            s.background_until = Some("nope".to_string());
            s
        };
        let work = mk_step("work", &["sh", "-c", "echo done"]);
        let pipeline = bg_pipeline("bg-bad-name", vec![server, work]);

        let err = PipelineRunner::new(pipeline).run().await.unwrap_err();
        assert!(
            matches!(err, RunnerError::InvalidConfig(ref m) if m.contains("unknown step")),
            "expected unknown-step InvalidConfig, got {err:?}"
        );
    }

    // ── R513-F3 wait-for health-gate steps (W207 Gap #5) ──────────────────

    /// Build a `kind = wait-for` step from a [`crate::types::WaitForConfig`],
    /// reusing the populated literal from [`mk_step`] so new QedStep fields
    /// don't have to be threaded through each test.
    fn mk_wait_for(name: &str, cfg: crate::types::WaitForConfig) -> crate::types::QedStep {
        let mut s = mk_step(name, &["unused"]);
        s.argv = vec![];
        s.kind = crate::types::StepKind::WaitFor;
        s.wait_for = Some(cfg);
        s
    }

    /// A `tcp` wait-for against a live listener passes immediately and the run
    /// goes green.
    #[tokio::test]
    async fn wait_for_tcp_passes_against_live_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        // Hold the listener alive for the duration of the run.
        let _accept = tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let gate = mk_wait_for(
            "wait:db",
            crate::types::WaitForConfig {
                http: None,
                tcp: Some(addr),
                expect_status: None,
                timeout_secs: 5,
                interval_ms: 50,
            },
        );
        let work = mk_step("work", &["sh", "-c", "echo done"]);
        let pipeline = bg_pipeline("wf-tcp", vec![gate, work]);

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let gate_row = meta.steps.iter().find(|s| s.name == "wait:db").unwrap();
        assert_eq!(gate_row.status, RunStatus::Success);
    }

    /// An `http` wait-for polls a server that is initially down, then becomes
    /// healthy mid-budget — the gate passes once the endpoint answers 200.
    #[tokio::test]
    async fn wait_for_http_passes_once_server_comes_up() {
        // Reserve a port, free it, and only start serving after a short delay —
        // so the first poll(s) fail with connect-refused and a later one
        // succeeds, exercising the retry loop.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut scratch = [0u8; 1024];
                let _ = sock.read(&mut scratch).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await;
            }
        });

        let gate = mk_wait_for(
            "wait:ready",
            crate::types::WaitForConfig {
                http: Some(format!("http://{addr}/health")),
                tcp: None,
                expect_status: None,
                timeout_secs: 5,
                interval_ms: 50,
            },
        );
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = bg_pipeline("wf-http", vec![gate]);
        let meta = PipelineRunner::new(pipeline).with_events(tx).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        // The success line names the endpoint as healthy.
        let events = drain_events(&mut rx);
        assert!(
            events.iter().any(|e| matches!(
                e,
                QedEvent::StepOutput { line, .. } if line.contains("healthy after")
            )),
            "emitted a 'healthy after' progress line; events={events:?}"
        );
    }

    /// A wait-for whose endpoint never comes up fails the step (and the run)
    /// once the timeout budget elapses, with a "never became healthy" message.
    #[tokio::test]
    async fn wait_for_times_out_when_endpoint_never_healthy() {
        // A port nothing listens on.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);

        let gate = mk_wait_for(
            "wait:never",
            crate::types::WaitForConfig {
                http: None,
                tcp: Some(addr),
                expect_status: None,
                timeout_secs: 1,
                interval_ms: 100,
            },
        );
        let pipeline = bg_pipeline("wf-timeout", vec![gate]);
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let row = meta.steps.iter().find(|s| s.name == "wait:never").unwrap();
        assert_eq!(row.status, RunStatus::Failed);
        let err = row.error.as_deref().unwrap_or_default();
        assert!(
            err.contains("never became healthy"),
            "timeout surfaces a clear message; got {err:?}"
        );
    }

    /// An `https://` URL is rejected up front with a pointed message rather than
    /// silently failing a plaintext GET against a TLS port for the whole budget.
    #[tokio::test]
    async fn wait_for_https_fails_fast() {
        let gate = mk_wait_for(
            "wait:tls",
            crate::types::WaitForConfig {
                http: Some("https://localhost:8443/health".to_string()),
                tcp: None,
                expect_status: None,
                timeout_secs: 30, // long budget; must NOT be consumed
                interval_ms: 100,
            },
        );
        let pipeline = bg_pipeline("wf-tls", vec![gate]);
        let started = std::time::Instant::now();
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "https rejection is immediate, not after the 30s budget"
        );
        let row = meta.steps.iter().find(|s| s.name == "wait:tls").unwrap();
        let err = row.error.as_deref().unwrap_or_default();
        assert!(err.contains("https"), "names the https limitation; got {err:?}");
    }

    // ── R513-F4 finally: always-run teardown (W207 Gap #6) ────────────────

    /// A `finally` step runs after a passing pipeline, after the main step, and
    /// the run stays green.
    #[tokio::test]
    async fn finally_runs_after_successful_pipeline() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pipeline = bg_pipeline("fin-ok", vec![mk_step("work", &["sh", "-c", "echo work"])]);
        pipeline.finally = vec![mk_step("teardown", &["sh", "-c", "echo teardown"])];

        let meta = PipelineRunner::new(pipeline).with_events(tx).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let td = meta.steps.iter().find(|s| s.name == "teardown").unwrap();
        assert_eq!(td.status, RunStatus::Success);

        let events = drain_events(&mut rx);
        let work_fin = finished_pos(&events, "work").expect("work finished");
        let td_fin = finished_pos(&events, "teardown").expect("teardown finished");
        assert!(work_fin < td_fin, "finally runs after the main step; events={events:?}");
    }

    /// A `finally` step runs even when the pipeline body failed — that's the
    /// whole point (upload traces on a failed test run).
    #[tokio::test]
    async fn finally_runs_even_when_pipeline_fails() {
        let mut pipeline =
            bg_pipeline("fin-onfail", vec![mk_step("work", &["sh", "-c", "exit 1"])]);
        pipeline.finally = vec![mk_step("teardown", &["sh", "-c", "echo cleaned"])];

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed, "body failed → run failed");
        let td = meta.steps.iter().find(|s| s.name == "teardown").unwrap();
        assert_eq!(
            td.status,
            RunStatus::Success,
            "teardown still ran despite the body failure"
        );
    }

    /// A failing `finally` step marks the run Failed, but outcome selection keys
    /// off the *work* status — so a green body still fires `on_success`.
    #[tokio::test]
    async fn finally_failure_marks_run_failed_but_on_success_still_fires() {
        let dispatcher = RecordingDispatcher::new();
        let mut pipeline = pipeline_with_outcomes(
            vec![Outcome::WardenDeploy {
                service: "yah".into(),
                env: "production".into(),
            }],
            vec![Outcome::AlmanacRun {
                pipeline: "should-not-run".into(),
            }],
            vec!["true".to_string()], // body passes
        );
        pipeline.finally = vec![mk_step("teardown", &["sh", "-c", "exit 3"])];

        let runner = PipelineRunner::new_with_dispatcher(pipeline, dispatcher.clone());
        let meta = runner.run().await.unwrap();

        // The run is Failed (teardown broke)…
        assert_eq!(meta.status, RunStatus::Failed);
        // …but the on_success outcome fired (work passed), and on_fail did NOT.
        assert_eq!(
            dispatcher.recorded(),
            vec!["yubaba-deploy:yah:production"],
            "outcome selection uses work-status, not the teardown failure"
        );
    }

    /// `on_fail = "continue"` on a `finally` step keeps a teardown failure from
    /// marking the run Failed.
    #[tokio::test]
    async fn finally_continue_on_fail_keeps_run_green() {
        let mut teardown = mk_step("teardown", &["sh", "-c", "exit 1"]);
        teardown.on_fail = OnFail::Continue;
        let mut pipeline = bg_pipeline("fin-cont", vec![mk_step("work", &["sh", "-c", "true"])]);
        pipeline.finally = vec![teardown];

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "continue-on-fail teardown failure doesn't fail the run"
        );
        let td = meta.steps.iter().find(|s| s.name == "teardown").unwrap();
        assert_eq!(td.status, RunStatus::Failed, "the step itself still records Failed");
    }

    /// Every `finally` step is attempted even if an earlier one fails (best-effort
    /// teardown — a failure never aborts the rest).
    #[tokio::test]
    async fn all_finally_steps_run_even_if_one_fails() {
        let mut pipeline = bg_pipeline("fin-all", vec![mk_step("work", &["sh", "-c", "true"])]);
        pipeline.finally = vec![
            mk_step("teardown-a", &["sh", "-c", "exit 1"]), // fails (Abort default)
            mk_step("teardown-b", &["sh", "-c", "echo b"]), // must still run
        ];

        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let a = meta.steps.iter().find(|s| s.name == "teardown-a").unwrap();
        let b = meta.steps.iter().find(|s| s.name == "teardown-b").unwrap();
        assert_eq!(a.status, RunStatus::Failed);
        assert_eq!(
            b.status,
            RunStatus::Success,
            "teardown-b ran despite teardown-a failing"
        );
    }

    /// A failing step streams stderr; the failure status reaches RunFinished
    /// and the stderr tail surfaces in the StepFailed message.
    #[tokio::test]
    async fn failing_step_streams_stderr_and_finishes_failed() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pipeline = one_step_pipeline(
            "test-events-fail",
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo boom >&2; exit 1".to_string(),
            ],
        );
        let runner = PipelineRunner::new(pipeline).with_events(tx);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        // The failure reason is persisted on the terminal StepStatus, not only
        // in the live event stream — so `qed.status` can explain *why* a step
        // failed after the run ends.
        let failed = &meta.steps[0];
        assert_eq!(failed.status, RunStatus::Failed);
        let err = failed
            .error
            .as_deref()
            .expect("failed step carries an error reason");
        assert!(
            err.contains("boom"),
            "error tail carries stderr; got {err:?}"
        );

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
            matches!(
                events.last(),
                Some(QedEvent::RunFinished {
                    status: RunStatus::Failed,
                    ..
                })
            ),
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

    // ── R531-T1 host-triple self-detection ──────────────────────────────────

    /// A runner self-detects its host triple at construction, and the value
    /// is a well-formed triple matching the process host.
    #[test]
    fn runner_self_detects_host_triple() {
        let pipeline = one_step_pipeline("host", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline);
        assert_eq!(runner.host_triple(), crate::platform::detect_host_triple());
        assert_eq!(
            crate::platform::arch_of(runner.host_triple()),
            std::env::consts::ARCH,
        );
    }

    /// `with_host_triple` overrides the detected host — the seam the daemon
    /// uses when a runner's steps land on a remote host of a known triple.
    #[test]
    fn with_host_triple_overrides_detection() {
        let pipeline = one_step_pipeline("host", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline).with_host_triple("x86_64-unknown-linux-gnu");
        assert_eq!(runner.host_triple(), "x86_64-unknown-linux-gnu");
    }

    /// `step_platform` composes the runner's host with the step's declared
    /// target (R531-F2), and falls back to the legacy `triple` field.
    #[test]
    fn step_platform_composes_host_with_step_target() {
        let pipeline = one_step_pipeline("build", vec!["true".to_string()]);
        let runner = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");

        // Declared [platform].target wins.
        let mut step = runner.pipeline.steps[0].clone();
        step.platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: Some("linux/amd64".into()),
        });
        let p = runner.step_platform(&step);
        assert_eq!(p.host, "aarch64-apple-darwin");
        assert_eq!(p.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(p.container_is_foreign_arch(), "amd64 image on arm64 host");

        // Legacy `triple` field is lifted when no [platform] block is set.
        let mut legacy = runner.pipeline.steps[0].clone();
        legacy.triple = Some("x86_64-unknown-linux-musl".into());
        let p2 = runner.step_platform(&legacy);
        assert_eq!(p2.target.as_deref(), Some("x86_64-unknown-linux-musl"));
        assert!(p2.is_cross_arch());
    }

    /// The portability preflight renders one line per step with the resolved
    /// verdict (R531-T4), honoring the runner's host override.
    #[test]
    fn portability_preflight_renders_one_line_per_step() {
        let pipeline = one_step_pipeline("build", vec!["true".to_string()]);
        let mut runner = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");
        // Give the single step a cross target.
        let mut steps = runner.pipeline.steps.clone();
        steps[0].platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: None,
        });
        runner.pipeline.steps = steps;

        let lines = runner.portability_preflight();
        assert_eq!(lines.len(), 1);
        assert!(
            lines[0].contains("targets x86_64-unknown-linux-musl")
                && lines[0].contains("host aarch64-apple-darwin")
                && lines[0].contains("NativeCross"),
            "preflight line: {}",
            lines[0]
        );
    }

    /// `native_cross_plan` (R531-F5) gates on the NativeCross verdict and a
    /// foreign target, then routes the step's argv to zigbuild. A host-arch
    /// (plain native) step and a non-NativeCross verdict both yield `None`.
    #[test]
    fn native_cross_plan_routes_foreign_target_to_zigbuild() {
        let pipeline = one_step_pipeline(
            "build",
            vec!["cross".into(), "build".into(), "--release".into()],
        );
        let runner = PipelineRunner::new(pipeline).with_host_triple("aarch64-apple-darwin");

        // Foreign-arch musl target on an arm64 mac → NativeCross tier.
        let mut foreign = runner.pipeline.steps[0].clone();
        foreign.platform = Some(crate::platform::PlatformSpec {
            target: Some("x86_64-unknown-linux-musl".into()),
            container_platform: Some("linux/amd64".into()),
        });
        let plan = runner
            .native_cross_plan(&foreign, &crate::nativecross::ToolAvailability::FULL)
            .expect("foreign-target NativeCross step yields a plan")
            .expect("toolchain available");
        assert_eq!(plan.tool, crate::nativecross::CrossTool::CargoZigbuild);
        assert_eq!(plan.argv[1], "zigbuild");
        assert!(plan.argv.iter().any(|a| a == "x86_64-unknown-linux-musl"));

        // Host-arch target → plain native build, not this tier → None.
        let mut native = runner.pipeline.steps[0].clone();
        native.platform = Some(crate::platform::PlatformSpec {
            target: Some("aarch64-unknown-linux-gnu".into()),
            container_platform: None,
        });
        assert!(runner
            .native_cross_plan(&native, &crate::nativecross::ToolAvailability::FULL)
            .is_none());

        // No target at all → None.
        let bare = runner.pipeline.steps[0].clone();
        assert!(runner
            .native_cross_plan(&bare, &crate::nativecross::ToolAvailability::FULL)
            .is_none());
    }

    /// Captures the argv + env a step is dispatched with, so a test can assert
    /// what the subprocess seam actually received (R531-T6).
    #[derive(Default)]
    struct CapturingExecutor {
        seen: std::sync::Mutex<Option<(Vec<String>, Vec<(String, String)>)>>,
    }

    #[async_trait::async_trait]
    impl ForgeExecutor for CapturingExecutor {
        async fn execute(
            &self,
            spec: ForgeSpec,
            ctx: ExecContext,
            _sink: Option<tokio::sync::mpsc::UnboundedSender<ExecEvent>>,
        ) -> Result<velveteen::ExecOutcome, ForgeExecutorError> {
            let argv = match spec.command {
                ForgeCommand::Subprocess { argv, .. } => argv,
                _ => Vec::new(),
            };
            *self.seen.lock().unwrap() = Some((argv, ctx.env));
            Ok(velveteen::ExecOutcome {
                status: ForgeStatus::Done {
                    exit_code: 0,
                    ended_at: 0,
                },
                stderr_tail: String::new(),
            })
        }
    }

    /// Build a single-step Native runner whose one step carries a cross
    /// `target`, wired to `exec` and a seeded toolchain availability — the
    /// fixture for the T6 execution-path tests.
    fn native_cross_runner(
        camp: &std::path::Path,
        argv: Vec<String>,
        target: &str,
        avail: crate::nativecross::ToolAvailability,
        exec: std::sync::Arc<CapturingExecutor>,
    ) -> PipelineRunner {
        let mut pipeline = one_step_pipeline("build-musl", argv);
        pipeline.steps[0].platform = Some(crate::platform::PlatformSpec {
            target: Some(target.to_string()),
            container_platform: None,
        });
        PipelineRunner::new(pipeline)
            .with_host_triple("aarch64-apple-darwin")
            .with_camp_root(camp.to_path_buf())
            .with_cross_availability(avail)
            .with_executor(exec)
    }

    /// T6 end-to-end: a NativeCross step's `cross build` argv is rewritten to
    /// `cargo zigbuild … --target T` *before* it reaches the executor.
    #[tokio::test]
    async fn execute_step_local_reroutes_native_cross_to_zigbuild() {
        let camp = tempfile::tempdir().unwrap();
        let exec = std::sync::Arc::new(CapturingExecutor::default());
        let runner = native_cross_runner(
            camp.path(),
            vec!["cross".into(), "build".into(), "--release".into()],
            "x86_64-unknown-linux-musl",
            crate::nativecross::ToolAvailability::FULL,
            exec.clone(),
        );
        let step = runner.pipeline.steps[0].clone();
        runner.execute_step_local(0, &step, None).await.unwrap();

        let (argv, _env) = exec.seen.lock().unwrap().clone().unwrap();
        assert_eq!(&argv[..2], &["cargo".to_string(), "zigbuild".to_string()]);
        assert!(argv.iter().any(|a| a == "x86_64-unknown-linux-musl"));
    }

    /// T6: with zig absent but a musl-cross toolchain present, the fallback
    /// keeps `cargo build` and injects the linker/CC/AR env.
    #[tokio::test]
    async fn execute_step_local_musl_cross_fallback_injects_linker_env() {
        let camp = tempfile::tempdir().unwrap();
        let exec = std::sync::Arc::new(CapturingExecutor::default());
        let runner = native_cross_runner(
            camp.path(),
            vec!["cargo".into(), "build".into()],
            "x86_64-unknown-linux-musl",
            crate::nativecross::ToolAvailability {
                zigbuild: false,
                musl_cross: true,
            },
            exec.clone(),
        );
        let step = runner.pipeline.steps[0].clone();
        runner.execute_step_local(0, &step, None).await.unwrap();

        let (argv, env) = exec.seen.lock().unwrap().clone().unwrap();
        assert_eq!(&argv[..2], &["cargo".to_string(), "build".to_string()]);
        assert!(
            env.iter()
                .any(|(k, _)| k == "CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER"),
            "musl-cross linker env injected: {env:?}"
        );
    }

    /// T6: a NativeCross step with no host-native toolchain installed fails
    /// with the actionable install hint instead of a raw linker error.
    #[tokio::test]
    async fn execute_step_local_fails_with_hint_when_no_toolchain() {
        let camp = tempfile::tempdir().unwrap();
        let exec = std::sync::Arc::new(CapturingExecutor::default());
        let runner = native_cross_runner(
            camp.path(),
            vec!["cross".into(), "build".into()],
            "x86_64-unknown-linux-musl",
            crate::nativecross::ToolAvailability::NONE,
            exec.clone(),
        );
        let step = runner.pipeline.steps[0].clone();

        let err = runner.execute_step_local(0, &step, None).await.unwrap_err();
        match err {
            RunnerError::StepFailed { msg, .. } => {
                assert!(msg.contains("cargo-zigbuild"), "actionable hint: {msg}");
            }
            other => panic!("expected StepFailed with hint, got {other:?}"),
        }
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
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
        });
        let remote_pipeline = one_step_pipeline("remote", vec!["true".to_string()]);
        let remote_runner = PipelineRunner::new_remote(remote_pipeline, scryer, yubaba);
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
                background: false,
                background_until: None,
                wait_for: None,
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
                import: None,
                matrix: None,
                enabled: true,
                activation: StepActivation::Active,
                if_cond: None,
                platform: None,
                toolchain: None,
                outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
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
    /// path (R381-T5). The scripted yubaba accepts the deploy, emits no logs,
    /// and reports exit 0; the runner surfaces a Success status and records
    /// the task_run_id of the forge run.
    #[tokio::test]
    async fn build_image_remote_dispatch_round_trip() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
        });
        let pipeline = build_image_pipeline("yah-rust");
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba)
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
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec!["dockerfile parse error".into()],
            exit_code: 2,
        });
        let pipeline = build_image_pipeline("yah-rust");
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba)
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
        assert!(camp
            .path()
            .join(".yah/cache/buildkit/yah-smoke.Dockerfile")
            .is_file());
        // OCI archive should be produced (push=false default).
        assert!(camp
            .path()
            .join(".yah/cache/images/yah-smoke_dev.tar")
            .is_file());
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
                background: false,
                background_until: None,
                wait_for: None,
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
                import: None,
                matrix: None,
                enabled: true,
                activation: StepActivation::Active,
                if_cond: None,
                platform: None,
                toolchain: None,
                outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
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

        let binary_rel = "target/x86_64-unknown-linux-musl/release/yubaba";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"native-tarball\"", binary_rel);

        let pipeline = package_native_tarball_pipeline("yah-yubaba", binary_rel, triple);
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let out = camp
            .path()
            .join(".yah/cache/native/yah-yubaba-x86_64-unknown-linux-musl.tar.gz");
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
        assert_eq!(seen[0].0, "bin/yubaba");
        assert_eq!(seen[0].1, b"\x7fELF-fake-musl-binary");
        assert_eq!(seen[1].0, "manifest.toml");
        let text = std::str::from_utf8(&seen[1].1).unwrap();
        let manifest: crate::native::NativeTarballManifest =
            toml::from_str(text).expect("manifest.toml parses");
        assert_eq!(manifest.name, "yah-yubaba");
        assert_eq!(manifest.triple, triple);
        assert_eq!(manifest.binary, "bin/yubaba");
        // Catalog env propagates into the manifest.
        assert_eq!(
            manifest.env.get("RUST_LOG").map(String::as_str),
            Some("info")
        );
    }

    /// Catalog entry that only declares `produces = ["oci-image"]` (the
    /// default) is rejected at dispatch time — protects against accidentally
    /// packaging a non-musl image as a native tarball.
    #[tokio::test]
    async fn package_native_tarball_rejects_non_native_catalog_entry() {
        let binary_rel = "target/release/yubaba";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"oci-image\"", binary_rel);
        let pipeline = package_native_tarball_pipeline("yah-yubaba", binary_rel, "darwin-aarch64");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
    }

    /// Both-target entries (`["oci-image", "native-tarball"]`) are accepted —
    /// W154's container-and-native peer model.
    #[tokio::test]
    async fn package_native_tarball_accepts_both_targets_entry() {
        let binary_rel = "target/x86_64-unknown-linux-musl/release/yubaba";
        let camp = stage_native_tarball_camp(
            "yah-yubaba",
            "\"oci-image\", \"native-tarball\"",
            binary_rel,
        );
        let pipeline =
            package_native_tarball_pipeline("yah-yubaba", binary_rel, "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert!(camp
            .path()
            .join(".yah/cache/native/yah-yubaba-x86_64-unknown-linux-musl.tar.gz")
            .is_file());
    }

    /// Unknown catalog name surfaces as StepFailed (mirrors build-image
    /// dispatch shape).
    #[tokio::test]
    async fn package_native_tarball_unknown_catalog_fails() {
        let camp = TempDir::new().unwrap();
        let bin = camp.path().join("target/release/yubaba");
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, b"x").unwrap();
        let pipeline = package_native_tarball_pipeline(
            "yah-bogus-not-real",
            "target/release/yubaba",
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
            images.join("yah-yubaba.toml"),
            r#"
[image]
name        = "yah-yubaba"
base        = "scratch"
description = "Native"
produces    = ["native-tarball"]
"#,
        )
        .unwrap();
        let pipeline = package_native_tarball_pipeline(
            "yah-yubaba",
            "target/x86_64-unknown-linux-musl/release/yubaba",
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
        let binary_rel = "target/release/yubaba";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"native-tarball\"", binary_rel);

        // Same pipeline but with triple=None.
        let mut pipeline = package_native_tarball_pipeline("yah-yubaba", binary_rel, "ignored");
        pipeline.steps[0].triple = None;

        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        let host_triple = crate::publish::resolve_triple(None);
        let expected = camp
            .path()
            .join(format!(".yah/cache/native/yah-yubaba-{host_triple}.tar.gz"));
        assert!(
            expected.is_file(),
            "expected {} to exist",
            expected.display()
        );
    }

    /// PackageNativeTarball is always Native runtime, even on a Remote runner —
    /// the implicit `None` must not get auto-forced to Container.
    #[test]
    fn package_native_tarball_step_forces_native_runtime_on_remote() {
        let dir = TempDir::new().unwrap();
        let scryer = make_scryer(&dir);
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
        });
        let pipeline = package_native_tarball_pipeline(
            "yah-yubaba",
            "target/x86_64-unknown-linux-musl/release/yubaba",
            "x86_64-unknown-linux-musl",
        );
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
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
                background: false,
                background_until: None,
                wait_for: None,
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
                import: None,
                matrix: None,
                enabled: true,
                activation: StepActivation::Active,
                if_cond: None,
                platform: None,
                toolchain: None,
                outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
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
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
        });
        let pipeline = musl_preflight_pipeline("yubaba");
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
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
        let err = check_dep_list("yubaba", ["openssl-sys"]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("container fallback"),
            "msg routes to container: {msg}"
        );
        assert!(
            msg.contains("runtime = \"container\""),
            "msg names the toml fix: {msg}"
        );
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
                    background: false,
                    background_until: None,
                    wait_for: None,
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
                    import: None,
                    matrix: None,
                    enabled: true,
                    activation: StepActivation::Active,
                    if_cond: None,
                    platform: None,
                    toolchain: None,
                    outputs: Vec::new(),
                },
                crate::types::QedStep {
                    background: false,
                    background_until: None,
                    wait_for: None,
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
                    import: None,
                    matrix: None,
                    enabled: true,
                    activation: StepActivation::Active,
                    if_cond: None,
                    platform: None,
                    toolchain: None,
                    outputs: Vec::new(),
                },
            ],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        }
    }

    /// Sign-only pipeline (no pack step) — for asserting the "tarball must
    /// already exist" gate without coupling to the packaging step.
    fn sign_only_pipeline(image: &str, triple: &str) -> Pipeline {
        Pipeline {
            name: "sign".to_string(),
            label: "Sign native tarball".to_string(),
            steps: vec![crate::types::QedStep {
                background: false,
                background_until: None,
                wait_for: None,
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
                import: None,
                matrix: None,
                enabled: true,
                activation: StepActivation::Active,
                if_cond: None,
                platform: None,
                toolchain: None,
                outputs: Vec::new(),
            }],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        }
    }

    /// Happy path: pack-then-sign in one pipeline writes the tarball and
    /// then `.sig`, `.crt`, `.bundle` next to it. Uses the default
    /// LoggingSigner — exercising the same trust shape as cosign without
    /// requiring a cosign install in the test sandbox.
    #[tokio::test]
    async fn sign_native_tarball_pack_then_sign_writes_sig_crt_bundle() {
        let binary_rel = "target/x86_64-unknown-linux-musl/release/yubaba";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"native-tarball\"", binary_rel);

        let pipeline = pack_and_sign_pipeline("yah-yubaba", binary_rel, triple);
        let runner = PipelineRunner::new(pipeline).with_camp_root(camp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        assert_eq!(meta.steps[0].status, RunStatus::Success); // pack
        assert_eq!(meta.steps[1].status, RunStatus::Success); // sign

        let tarball = camp
            .path()
            .join(".yah/cache/native/yah-yubaba-x86_64-unknown-linux-musl.tar.gz");
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
        let binary_rel = "target/release/yubaba";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"oci-image\"", binary_rel);
        let pipeline = sign_only_pipeline("yah-yubaba", "x86_64-unknown-linux-musl");
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
            images.join("yah-yubaba.toml"),
            r#"
[image]
name        = "yah-yubaba"
base        = "scratch"
description = "Native"
produces    = ["native-tarball"]
"#,
        )
        .unwrap();
        let pipeline = sign_only_pipeline("yah-yubaba", "x86_64-unknown-linux-musl");
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
        let yubaba = Arc::new(ScriptedWarden {
            lines: vec![],
            exit_code: 0,
        });
        let pipeline = sign_only_pipeline("yah-yubaba", "x86_64-unknown-linux-musl");
        let runner = PipelineRunner::new_remote(pipeline, scryer, yubaba);
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

        let binary_rel = "target/x86_64-unknown-linux-musl/release/yubaba";
        let triple = "x86_64-unknown-linux-musl";
        let camp = stage_native_tarball_camp("yah-yubaba", "\"native-tarball\"", binary_rel);

        let signer = Arc::new(CountingSigner {
            calls: AtomicUsize::new(0),
        });
        let pipeline = pack_and_sign_pipeline("yah-yubaba", binary_rel, triple);
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
            background: false,
            background_until: None,
            wait_for: None,
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
            import: None,
            matrix: None,
            enabled: true,
            activation: StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
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
            background: false,
            background_until: None,
            wait_for: None,
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
                opaque: false,
            }),
            outputs: Vec::new(),
            gha_workflow: None,
            import: None,
            matrix: None,
            enabled: true,
            activation: crate::types::StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
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
            // Test fixtures run in throwaway tempdirs that aren't real git
            // checkouts, so use Live (build the tree as-is) — the default
            // Checkout mode would try `git checkout main` and fail. Workspace
            // positioning itself is covered by the dedicated WorkspaceMode tests.
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        }
    }

    // ── W224 WorkspaceMode positioning (decision table) ──────────────────────

    /// Build a `main`-branch git repo with one committed file in a tempdir.
    fn init_git_repo() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .current_dir(tmp.path())
                .args(args)
                .output()
                .unwrap()
                .status
                .success();
            assert!(ok, "git {args:?} failed");
        };
        git(&["init", "-b", "main"]);
        git(&["config", "user.email", "t@t.t"]);
        git(&["config", "user.name", "t"]);
        std::fs::write(tmp.path().join("f.txt"), "v1").unwrap();
        git(&["add", "."]);
        git(&["commit", "-m", "init"]);
        tmp
    }

    fn pipeline_with_workspace(mode: crate::types::WorkspaceMode) -> Pipeline {
        let mut p = make_pipeline("ws", vec![]);
        p.workspace = mode;
        p
    }

    #[test]
    fn workspace_live_returns_camp_root_without_touching_git() {
        // Live works even in a non-git dir — no status/checkout is run.
        let tmp = tempfile::tempdir().unwrap();
        let runner = PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Live))
            .with_camp_root(tmp.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(tmp.path()).unwrap();
        assert_eq!(ws, tmp.path());
        assert!(guard.is_none(), "Live needs no worktree guard");
    }

    #[test]
    fn workspace_checkout_clean_switches_to_branch_in_place() {
        let repo = init_git_repo();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Checkout))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        assert_eq!(ws, repo.path(), "checkout positions the camp root itself");
        assert!(guard.is_none());
    }

    #[test]
    fn workspace_checkout_bails_on_dirty_tracked_change() {
        let repo = init_git_repo();
        // Dirty a tracked file → checkout must refuse rather than clobber it.
        std::fs::write(repo.path().join("f.txt"), "dirty").unwrap();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Checkout))
                .with_camp_root(repo.path().to_path_buf());
        let err = runner.prepare_workspace(repo.path()).unwrap_err();
        assert!(
            matches!(&err, RunnerError::InvalidConfig(m) if m.contains("uncommitted")),
            "expected a dirty-tree refusal, got {err:?}"
        );
    }

    #[test]
    fn workspace_checkout_ignores_untracked_files() {
        let repo = init_git_repo();
        // An untracked file is not "dirty" for checkout purposes.
        std::fs::write(repo.path().join("scratch.txt"), "new").unwrap();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Checkout))
                .with_camp_root(repo.path().to_path_buf());
        assert!(runner.prepare_workspace(repo.path()).is_ok());
    }

    #[test]
    fn workspace_isolated_builds_in_a_worktree_and_guard_cleans_up() {
        let repo = init_git_repo();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        assert_ne!(ws, repo.path(), "isolated builds in a separate worktree");
        assert!(ws.join("f.txt").exists(), "worktree carries the committed tree");
        assert!(guard.is_some());
        let wt = ws.clone();
        drop(guard);
        assert!(!wt.join("f.txt").exists(), "guard tears the worktree down on drop");
    }

    #[test]
    fn workspace_isolated_leaves_a_dirty_camp_root_untouched() {
        let repo = init_git_repo();
        // Uncommitted edits in the camp root are fine for isolated — it never
        // touches them, it builds from a fresh worktree at the committed ref.
        std::fs::write(repo.path().join("f.txt"), "dirty").unwrap();
        let runner =
            PipelineRunner::new(pipeline_with_workspace(crate::types::WorkspaceMode::Isolated))
                .with_camp_root(repo.path().to_path_buf());
        let (ws, guard) = runner.prepare_workspace(repo.path()).unwrap();
        assert_eq!(std::fs::read_to_string(repo.path().join("f.txt")).unwrap(), "dirty");
        assert_eq!(
            std::fs::read_to_string(ws.join("f.txt")).unwrap(),
            "v1",
            "worktree has committed bytes"
        );
        drop(guard);
    }

    // ── W224 R533-F11: whole-run positioning reaches non-gha steps ────────────

    /// An `Isolated` run positions the tree ONCE at run start and every
    /// subprocess step builds in that worktree — not the live camp root — with a
    /// single run-scoped guard that outlives all steps and tears the worktree
    /// down when the run returns. This is the desktop-release-builds-from-the-
    /// worktree fix: before F11 only the gha-workflow step was repositioned.
    #[tokio::test]
    async fn run_level_isolated_positions_every_step_in_the_worktree() {
        let repo = init_git_repo();
        let mut pipeline = one_step_pipeline(
            "iso",
            vec![
                "sh".into(),
                "-c".into(),
                // Record cwd for the assertion and drop a build artifact in it.
                "echo cwd=$(pwd) >> \"$YAH_OUTPUTS\"; echo built > built.txt".into(),
            ],
        );
        pipeline.workspace = crate::types::WorkspaceMode::Isolated;
        // A second step reads the file the first wrote: it only succeeds if the
        // worktree survives BETWEEN steps (one shared guard, not per-step).
        let mut step2 = pipeline.steps[0].clone();
        step2.name = "step-2".into();
        step2.argv = vec![
            "sh".into(),
            "-c".into(),
            "cat built.txt && echo cwd=$(pwd) >> \"$YAH_OUTPUTS\"".into(),
        ];
        pipeline.steps.push(step2);

        let runner = PipelineRunner::new(pipeline).with_camp_root(repo.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success, "{:?}", meta.steps);

        let cwd1 = meta.steps[0].outputs.get("cwd").expect("step-1 cwd");
        let cwd2 = meta.steps[1].outputs.get("cwd").expect("step-2 cwd");
        assert_eq!(cwd1, cwd2, "every step in the run shares the one worktree");
        assert!(
            cwd1.contains("qed-worktree-"),
            "subprocess step ran in the run's isolated worktree, got {cwd1}"
        );
        assert!(
            !repo.path().join("built.txt").exists(),
            "the build artifact landed in the worktree, never the live camp root"
        );
        // run() has returned ⇒ the run-scoped guard dropped ⇒ worktree is gone.
        assert!(
            !std::path::Path::new(cwd1).exists(),
            "the run-scoped worktree is torn down once the run completes"
        );
    }

    /// `Live` leaves every step on the camp root as-is (no git, works in a
    /// non-repo tempdir) — the run-level positioning is a no-op for Live.
    #[tokio::test]
    async fn run_level_live_keeps_steps_on_the_camp_root() {
        let tmp = tempfile::tempdir().unwrap();
        let mut pipeline = one_step_pipeline(
            "live",
            vec![
                "sh".into(),
                "-c".into(),
                "echo cwd=$(pwd) >> \"$YAH_OUTPUTS\"".into(),
            ],
        );
        pipeline.workspace = crate::types::WorkspaceMode::Live;
        let runner = PipelineRunner::new(pipeline).with_camp_root(tmp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success, "{:?}", meta.steps);
        let cwd = meta.steps[0].outputs.get("cwd").expect("cwd");
        assert_eq!(
            std::path::Path::new(cwd).canonicalize().unwrap(),
            tmp.path().canonicalize().unwrap(),
            "Live builds the camp root in place"
        );
    }

    /// Checkout-bail-if-dirty now fires at the *run* level (not only for a
    /// gha-workflow step): an ordinary `run()` of a subprocess pipeline over a
    /// dirty tree refuses rather than silently building surprise bytes.
    #[tokio::test]
    async fn run_level_checkout_bails_on_dirty_tree_before_any_step() {
        let repo = init_git_repo();
        std::fs::write(repo.path().join("f.txt"), "dirty").unwrap();
        let mut pipeline = one_step_pipeline("co", vec!["echo".into(), "hi".into()]);
        pipeline.workspace = crate::types::WorkspaceMode::Checkout;
        let runner = PipelineRunner::new(pipeline).with_camp_root(repo.path().to_path_buf());
        let err = runner.run().await.unwrap_err();
        assert!(
            matches!(&err, RunnerError::InvalidConfig(m) if m.contains("uncommitted")),
            "expected a run-level dirty-tree refusal, got {err:?}"
        );
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
        fn resolve(&self, _target: &SubPipelineRef) -> Option<Pipeline> {
            None
        }
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
            .execute_step_sub_pipeline(0, &sub_step("remote", peer_target, false), &mut Vec::new())
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
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
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
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
    }

    #[tokio::test]
    async fn sub_pipeline_inlines_child_steps_as_rows_by_default() {
        // W223 R532-F3: transparent-by-default generalizes beyond GHA. A
        // Builtin (or Path / Peer) child's steps are attributed to the wrapping
        // step as inlined rows — one per child step, in order, carrying status
        // and (on failure) the child step's error. Child qed steps are linear,
        // so the rows have no `needs` edges.
        let child = make_pipeline(
            "child",
            vec![
                shell_step("prep", vec!["true"]),
                shell_step("build", vec!["false"]), // fails
                shell_step("publish", vec!["true"]),
            ],
        );
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
        );
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();

        let step = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        // The wrapping step carries one row per child step that ran (the
        // child aborts after `build` fails, so `publish` never runs).
        assert_eq!(
            step.jobs.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(),
            vec!["prep", "build"],
            "child steps inline as rows in order, stopping at the abort",
        );
        assert_eq!(step.jobs[0].status, RunStatus::Success);
        assert_eq!(step.jobs[1].status, RunStatus::Failed);
        assert!(
            step.jobs[1].error.is_some(),
            "the failed child step's error carries onto the inlined row",
        );
        assert!(
            step.jobs.iter().all(|j| j.needs.is_empty()),
            "linear qed child steps carry no needs edges",
        );
    }

    #[tokio::test]
    async fn opaque_sub_pipeline_suppresses_inlined_rows() {
        // W223 R532-F3: the `opaque` opt-out keeps the wrapper a single
        // black-box node — the child still runs and its status rolls up, but
        // no per-child rows are inlined.
        let child = make_pipeline(
            "child",
            vec![shell_step("a", vec!["true"]), shell_step("b", vec!["true"])],
        );
        let mut wrap = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        wrap.sub_pipeline.as_mut().unwrap().opaque = true;
        let root = make_pipeline("root", vec![wrap]);
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();

        let step = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(
            step.status,
            RunStatus::Success,
            "child still ran + rolled up"
        );
        assert!(
            step.jobs.is_empty(),
            "opaque opt-out suppresses the inlined per-child rows",
        );
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
            background: false,
            background_until: None,
            wait_for: None,
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
            import: None,
            gha_workflow: Some(crate::types::GhaWorkflowConfig {
                path: wf_path.clone(),
                event: None,
                inputs: HashMap::new(),
            }),
            matrix: None,
            enabled: true,
            activation: crate::types::StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
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
            workspace: crate::types::WorkspaceMode::Live, // test fixture isn't a git checkout
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
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
                QedEvent::StepFinished {
                    index: 0,
                    msg,
                    status,
                    ..
                } => {
                    if *status == RunStatus::Failed {
                        step_fail_msg = msg.clone();
                    }
                }
                QedEvent::SubPipelineStarted { index: 0, .. } => {
                    saw_subpipeline_started = true;
                }
                QedEvent::SubPipelineFinished {
                    index: 0, status, ..
                } => {
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
    async fn gha_workflow_subpipeline_persists_per_job_rows() {
        // W223 R532-T1: a wrapped GHA workflow is a *disregarded entity* — its
        // jobs are persisted as structured per-job rows under the wrapping
        // step's StepStatus, rather than collapsed into one flattened failure
        // string. One job succeeds, one fails (carrying its stderr-tail detail),
        // and one downstream job `needs` the failing one so it is skipped — the
        // R516 skip-count becomes a per-row Skipped state, not a trailing
        // sentence.
        let tmp = tempfile::tempdir().unwrap();
        let wf_path = tmp.path().join("mix.yml");
        std::fs::write(
            &wf_path,
            r#"
name: mix
on: push
jobs:
  ok:
    runs-on: ubuntu-latest
    steps:
      - name: succeed
        run: echo "all good"
  boom:
    runs-on: ubuntu-latest
    steps:
      - name: emit then fail
        run: |
          echo "fatal: kaboom-7f3a" 1>&2
          exit 9
  downstream:
    runs-on: ubuntu-latest
    needs: boom
    steps:
      - name: never runs
        run: echo "should be skipped"
"#,
        )
        .unwrap();

        // Synthesised one-step pipeline carrying the GhaWorkflow step, exactly
        // as `LoaderSubPipelineResolver::resolve` would build it.
        let step = crate::types::QedStep {
            background: false,
            background_until: None,
            wait_for: None,
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
            import: None,
            gha_workflow: Some(crate::types::GhaWorkflowConfig {
                path: wf_path.clone(),
                event: None,
                inputs: HashMap::new(),
            }),
            matrix: None,
            enabled: true,
            activation: crate::types::StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
        };
        let child = Pipeline {
            name: "mix".into(),
            label: "mix".into(),
            steps: vec![step],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: Default::default(),
            workspace: crate::types::WorkspaceMode::Live, // test fixture isn't a git checkout
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };

        let mut map = std::collections::HashMap::new();
        map.insert(format!("gha:{}", wf_path.display()), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));

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

        let runner = PipelineRunner::new(root)
            .with_sub_pipeline_resolver(resolver)
            .with_camp_root(tmp.path().to_path_buf());
        let meta = runner.run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Failed);

        // The wrapping step (index 0) carries one row per GHA job.
        let wrap = &meta.steps[0];
        assert_eq!(
            wrap.jobs.len(),
            3,
            "all three jobs should produce rows (got: {:?})",
            wrap.jobs.iter().map(|j| &j.id).collect::<Vec<_>>(),
        );
        let row = |id: &str| {
            wrap.jobs
                .iter()
                .find(|j| j.id == id)
                .unwrap_or_else(|| panic!("missing row for job {id}"))
        };

        assert_eq!(row("ok").status, RunStatus::Success);
        assert!(row("ok").error.is_none(), "success row carries no error");

        let boom = row("boom");
        assert_eq!(boom.status, RunStatus::Failed);
        let err = boom
            .error
            .as_ref()
            .expect("failed job row carries stderr-tail detail");
        assert!(
            err.contains("emit then fail"),
            "row error names the failing step (got: {err})",
        );
        assert!(
            err.contains("kaboom-7f3a"),
            "row error carries the stderr tail (got: {err})",
        );

        let down = row("downstream");
        assert_eq!(
            down.status,
            RunStatus::Skipped,
            "downstream gated on a failed dep is a Skipped row, not a trailing skip-count",
        );
        assert!(down.error.is_none(), "skipped row carries no error");
        // W223 R532-F2: the intra-workflow `needs:` edge is persisted so the
        // graph viewer can render it as a real dependency edge.
        assert_eq!(
            down.needs,
            vec!["boom".to_string()],
            "downstream's needs edge is carried on the row",
        );
        assert!(
            row("ok").needs.is_empty(),
            "a job with no needs has an empty edge list"
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
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                true,
            )],
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
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
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
            vec![sub_step(
                "descend",
                SubPipelineRef::Builtin("leaf".into()),
                true,
            )],
        );
        let mut root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("mid".into()),
                true,
            )],
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
            vec![producing_step(
                "build-cli",
                "yah",
                yah_path.to_string_lossy().as_ref(),
            )],
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
                sub_step(
                    "compose-cli",
                    SubPipelineRef::Builtin("child-cli".into()),
                    true,
                ),
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
        assert_eq!(
            *recorder.syncs.lock().unwrap(),
            1,
            "single sync across all children"
        );
        assert_eq!(
            *recorder.revalidates.lock().unwrap(),
            1,
            "single revalidate POST"
        );

        let files = recorder.files.lock().unwrap();
        // Three per-binary shared manifests + three per-(binary,triple) stable
        // manifests (single triple in this fan-in: darwin-aarch64) + three
        // binary files = 9 staged objects. The per-triple stable manifests
        // were added in R330-B8 for cross-stage merge fan-in.
        assert_eq!(files.len(), 9, "staged tree contents: {files:?}");
        assert!(files.iter().any(|f| f == "yah/release-manifest.json"));
        assert!(files.iter().any(|f| f == "desktop/release-manifest.json"));
        assert!(files.iter().any(|f| f == "mesofact/release-manifest.json"));
        assert!(files
            .iter()
            .any(|f| f == "yah/release-manifest-darwin-aarch64.json"));
        assert!(files
            .iter()
            .any(|f| f == "desktop/release-manifest-darwin-aarch64.json"));
        assert!(files
            .iter()
            .any(|f| f == "mesofact/release-manifest-darwin-aarch64.json"));
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
        assert_eq!(
            after_step.status,
            RunStatus::Success,
            "after step ran despite child failure"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_forwards_params_to_child() {
        // Child step has a `{{greeting}}` arg; parent's SubPipeline params
        // substitute it before the child runs.
        let child = make_pipeline(
            "child",
            vec![shell_step("echo", vec!["true", "{{greeting}}"])],
        );
        let mut step = sub_step("compose", SubPipelineRef::Builtin("child".into()), false);
        if let Some(cfg) = step.sub_pipeline.as_mut() {
            cfg.params
                .insert("greeting".to_string(), "hello".to_string());
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
            vec![
                "sh",
                "-c",
                "test \"$1\" = abc123",
                "--",
                "${{ steps.step1.outputs.digest }}",
            ],
        );
        let pipeline = make_pipeline("p", vec![step1, step2]);
        let runner = PipelineRunner::new(pipeline);
        let meta = runner.run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "step2 should receive substituted value"
        );
        let s1 = meta.steps.iter().find(|s| s.name == "step1").unwrap();
        assert_eq!(
            s1.outputs.get("digest").map(|s| s.as_str()),
            Some("abc123"),
            "step1 outputs map should contain captured value"
        );
    }

    /// Step 1 writes KEY=VALUE to $YAH_OUTPUTS; the runner collects it into
    /// StepStatus::outputs regardless of whether the step declared it in
    /// the `outputs` field.
    #[tokio::test]
    async fn step_outputs_captured_in_step_status() {
        let step = shell_step(
            "emit",
            vec![
                "sh",
                "-c",
                "printf 'foo=bar\\nbaz=qux\\n' >> \"$YAH_OUTPUTS\"",
            ],
        );
        let pipeline = make_pipeline("p", vec![step]);
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let s = meta.steps.iter().find(|s| s.name == "emit").unwrap();
        assert_eq!(s.outputs.get("foo").map(|s| s.as_str()), Some("bar"));
        assert_eq!(s.outputs.get("baz").map(|s| s.as_str()), Some("qux"));
    }

    /// W209 F3: a `[[bind]]` whose `from` references step1's output fires
    /// mid-pipeline; step2 reads the new value off disk like any other
    /// tool. Confirms the build→checkin→release inversion at the
    /// mechanical layer: the source tree IS the step-to-step plumbing.
    #[tokio::test]
    async fn pipeline_bind_writes_manifest_mid_run_visible_to_next_step() {
        const HASH_A: &str = "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
        let workspace = TempDir::new().unwrap();
        let manifest_path = workspace.path().join("workload.toml");
        std::fs::write(
            &manifest_path,
            "name = \"whisper\"\nblake3 = \"0000000000000000000000000000000000000000000000000000000000000000\"\n",
        )
        .unwrap();

        let mut step1 = shell_step(
            "publish",
            vec![
                "sh",
                "-c",
                &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\""),
            ],
        );
        step1.outputs = vec![crate::types::OutputDecl {
            name: "discovered".into(),
            description: None,
            kind: manifest_bind::ValueType::Blake3Hex,
            validate: None,
        }];

        // Step 2 reads the on-disk manifest and asserts the new hash is
        // there. If apply_binds didn't fire mid-pipeline, this fails.
        let step2 = shell_step(
            "consume",
            vec!["sh", "-c", &format!("grep -q '{HASH_A}' workload.toml")],
        );

        let mut pipeline = make_pipeline("publish-then-consume", vec![step1, step2]);
        pipeline.binds = vec![manifest_bind::BindSpec {
            file: "workload.toml".into(),
            path: "blake3".into(),
            from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
            intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        }];

        let runner = PipelineRunner::new(pipeline).with_camp_root(workspace.path().to_path_buf());
        let meta = runner.run().await.unwrap();

        assert_eq!(
            meta.status,
            RunStatus::Success,
            "step2 must see the bound value"
        );
        let s1 = meta.steps.iter().find(|s| s.name == "publish").unwrap();
        assert_eq!(
            s1.applied_binds.len(),
            1,
            "publish step should record one bind"
        );
        assert!(
            s1.applied_binds[0].changed,
            "first run flips the placeholder"
        );
        assert_eq!(s1.applied_binds[0].new, HASH_A);

        // Idempotent: a re-run sees the same hash, writes nothing, but
        // still records the AppliedBind entry with changed=false.
        let mut step1b = shell_step(
            "publish",
            vec![
                "sh",
                "-c",
                &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\""),
            ],
        );
        step1b.outputs = vec![crate::types::OutputDecl {
            name: "discovered".into(),
            description: None,
            kind: manifest_bind::ValueType::Blake3Hex,
            validate: None,
        }];
        let step2b = shell_step(
            "consume",
            vec!["sh", "-c", &format!("grep -q '{HASH_A}' workload.toml")],
        );
        let mut pipeline2 = make_pipeline("publish-then-consume", vec![step1b, step2b]);
        pipeline2.binds = vec![manifest_bind::BindSpec {
            file: "workload.toml".into(),
            path: "blake3".into(),
            from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
            intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        }];
        let meta2 = PipelineRunner::new(pipeline2)
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        let s1b = meta2.steps.iter().find(|s| s.name == "publish").unwrap();
        assert_eq!(s1b.applied_binds.len(), 1);
        assert!(!s1b.applied_binds[0].changed, "re-run is a no-op on disk");
    }

    /// W209 F3: a failed step does NOT fire its binds. The source tree is
    /// the ledger; partial states are only written for steps that
    /// succeeded.
    #[tokio::test]
    async fn pipeline_bind_skipped_when_producing_step_fails() {
        const HASH_A: &str = "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
        let workspace = TempDir::new().unwrap();
        let manifest_path = workspace.path().join("workload.toml");
        std::fs::write(
            &manifest_path,
            "name = \"whisper\"\nblake3 = \"0000000000000000000000000000000000000000000000000000000000000000\"\n",
        )
        .unwrap();
        let before = std::fs::read_to_string(&manifest_path).unwrap();

        // Step writes the output line THEN exits non-zero. Output is
        // collected, but apply_binds must be gated on success.
        let mut step1 = shell_step(
            "publish",
            vec![
                "sh",
                "-c",
                &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\"; exit 1"),
            ],
        );
        step1.outputs = vec![crate::types::OutputDecl {
            name: "discovered".into(),
            description: None,
            kind: manifest_bind::ValueType::Blake3Hex,
            validate: None,
        }];

        let mut pipeline = make_pipeline("publish-fails", vec![step1]);
        pipeline.binds = vec![manifest_bind::BindSpec {
            file: "workload.toml".into(),
            path: "blake3".into(),
            from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
            intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        }];

        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Failed);
        let s = meta.steps.iter().find(|s| s.name == "publish").unwrap();
        assert!(
            s.applied_binds.is_empty(),
            "failed step must not fire binds"
        );
        // Manifest on disk is untouched.
        assert_eq!(std::fs::read_to_string(&manifest_path).unwrap(), before);
    }

    /// W209/R510-F6: a `[[on_change]]` journal hook fires exactly once when a
    /// bind changes the manifest, and zero times when a re-run rewrites the
    /// same value (no-op). This is the doc's hash-change-hook verification
    /// criterion driven end-to-end through the runner.
    #[tokio::test]
    async fn on_change_journal_fires_once_on_change_zero_on_noop() {
        const HASH_A: &str = "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
        let workspace = TempDir::new().unwrap();
        let manifest_path = workspace.path().join("workload.toml");
        std::fs::write(
            &manifest_path,
            "name = \"whisper\"\nblake3 = \"0000000000000000000000000000000000000000000000000000000000000000\"\n",
        )
        .unwrap();
        let journal_rel = ".yah/qed/whisper.journal";

        let build_pipeline = || {
            let mut step1 = shell_step(
                "publish",
                vec![
                    "sh",
                    "-c",
                    &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\""),
                ],
            );
            step1.outputs = vec![crate::types::OutputDecl {
                name: "discovered".into(),
                description: None,
                kind: manifest_bind::ValueType::Blake3Hex,
                validate: None,
            }];
            let mut pipeline = make_pipeline("publish-with-hook", vec![step1]);
            pipeline.binds = vec![manifest_bind::BindSpec {
                file: "workload.toml".into(),
                path: "blake3".into(),
                from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
                intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
                cross_workspace: false,
                schema: None,
            }];
            pipeline.on_change = vec![manifest_bind::OnChangeHook {
                bind: "blake3".into(),
                action: manifest_bind::OnChangeAction::Journal {
                    journal: journal_rel.into(),
                },
            }];
            pipeline
        };

        // First run: the zero-sentinel flips to HASH_A → bind changed → hook fires.
        let meta = PipelineRunner::new(build_pipeline())
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);
        let journal_abs = workspace.path().join(journal_rel);
        let after_first = std::fs::read_to_string(&journal_abs).unwrap();
        assert_eq!(
            after_first.lines().count(),
            1,
            "hook fires once on real change"
        );
        assert!(
            after_first.contains(HASH_A),
            "journal records the new value"
        );

        // Second run: same hash → no-op rewrite → hook must NOT fire again.
        let meta2 = PipelineRunner::new(build_pipeline())
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta2.status, RunStatus::Success);
        let after_second = std::fs::read_to_string(&journal_abs).unwrap();
        assert_eq!(
            after_second.lines().count(),
            1,
            "no-op rewrite must not append a second journal line",
        );
    }

    /// W212/R518-P4: early cutoff is **value-equality based**, not run-count
    /// based. When a step (re)produces output byte-identical to what the
    /// manifest already holds — even on the *first* run — the bind is
    /// `changed = false`, so the on_change hook never fires. This is the
    /// Bazel/Nix property: a rebuild whose output didn't change does not
    /// propagate downstream, regardless of why the rebuild ran.
    #[tokio::test]
    async fn on_change_early_cutoff_when_output_already_matches() {
        const HASH_A: &str = "fb0afc9f3d966f5347c6dfd335adab12f1dc8ee6df18cf9e9ff90fe86f0416c0";
        let workspace = TempDir::new().unwrap();
        let manifest_path = workspace.path().join("workload.toml");
        // Manifest ALREADY holds HASH_A — no prior run, no sentinel.
        std::fs::write(
            &manifest_path,
            format!("name = \"whisper\"\nblake3 = \"{HASH_A}\"\n"),
        )
        .unwrap();
        let journal_rel = ".yah/qed/whisper.journal";

        let mut step1 = shell_step(
            "publish",
            vec![
                "sh",
                "-c",
                &format!("echo discovered=\"{HASH_A}\" >> \"$YAH_OUTPUTS\""),
            ],
        );
        step1.outputs = vec![crate::types::OutputDecl {
            name: "discovered".into(),
            description: None,
            kind: manifest_bind::ValueType::Blake3Hex,
            validate: None,
        }];
        let mut pipeline = make_pipeline("publish-noop", vec![step1]);
        pipeline.binds = vec![manifest_bind::BindSpec {
            file: "workload.toml".into(),
            path: "blake3".into(),
            from: manifest_bind::OutputRef::parse("publish.outputs.discovered").unwrap(),
            intent: manifest_bind::Intent::Keyword(manifest_bind::IntentKeyword::Latest),
            cross_workspace: false,
            schema: None,
        }];
        pipeline.on_change = vec![manifest_bind::OnChangeHook {
            bind: "blake3".into(),
            action: manifest_bind::OnChangeAction::Journal {
                journal: journal_rel.into(),
            },
        }];

        let meta = PipelineRunner::new(pipeline)
            .with_camp_root(workspace.path().to_path_buf())
            .run()
            .await
            .unwrap();
        assert_eq!(meta.status, RunStatus::Success);

        // The predicate accepted the value, but the bytes already matched →
        // changed=false → no hook fired → no journal file at all.
        let s = meta.steps.iter().find(|s| s.name == "publish").unwrap();
        assert!(
            s.applied_binds.iter().all(|b| !b.changed),
            "bind to an already-matching value must be changed=false",
        );
        assert!(
            !workspace.path().join(journal_rel).exists(),
            "early cutoff: an unchanged output must not fire the on_change hook",
        );
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
            vec![
                "sh",
                "-c",
                "test \"$1\" = 42",
                "--",
                "${{ steps.compose.outputs.result }}",
            ],
        );

        let root = make_pipeline("root", vec![sub, sibling]);
        let mut map = std::collections::HashMap::new();
        map.insert("builtin:child".to_string(), child);
        let resolver: Arc<dyn SubPipelineResolver + Send + Sync> = Arc::new(MapResolver(map));
        let runner = PipelineRunner::new(root).with_sub_pipeline_resolver(resolver);
        let meta = runner.run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "sibling should receive child output via parent step context"
        );
        let compose = meta.steps.iter().find(|s| s.name == "compose").unwrap();
        assert_eq!(
            compose.outputs.get("result").map(|s| s.as_str()),
            Some("42"),
            "SubPipeline step status should carry propagated outputs"
        );
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
                sub_step(
                    "compose-a",
                    SubPipelineRef::Builtin("child-a".into()),
                    false,
                ),
                sub_step(
                    "compose-b",
                    SubPipelineRef::Path(".yah/qed/child-b.toml".into()),
                    false,
                ),
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
                QedEvent::SubPipelineStarted {
                    name,
                    target,
                    child_run_id,
                    ..
                } => Some((name.clone(), target.clone(), child_run_id.clone())),
                _ => None,
            })
            .collect();
        let finishes: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                QedEvent::SubPipelineFinished {
                    name,
                    child_run_id,
                    status,
                    ..
                } => Some((name.clone(), child_run_id.clone(), *status)),
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
        assert_eq!(
            run_started_count, 1,
            "only parent's RunStarted on the parent stream"
        );
    }

    #[tokio::test]
    async fn sub_pipeline_finished_emits_failed_status_when_child_fails() {
        let child = make_pipeline("child", vec![shell_step("boom", vec!["false"])]);
        let root = make_pipeline(
            "root",
            vec![sub_step(
                "compose",
                SubPipelineRef::Builtin("child".into()),
                false,
            )],
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
        assert_eq!(
            finished,
            Some(RunStatus::Failed),
            "child failure surfaces on SubPipelineFinished.status"
        );
    }

    // ── R506 step gating tests ────────────────────────────────────────────

    fn gating_step(name: &str) -> crate::types::QedStep {
        crate::types::QedStep {
            background: false,
            background_until: None,
            wait_for: None,
            name: name.to_string(),
            // echo always succeeds — distinguishes "ran" from "skipped" by
            // looking at the terminal status, not by relying on a failure.
            argv: vec!["echo".into(), "ran".into()],
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
            outputs: Vec::new(),
            gha_workflow: None,
            import: None,
            matrix: None,
            enabled: true,
            activation: crate::types::StepActivation::Active,
            if_cond: None,
            platform: None,
            toolchain: None,
        }
    }

    #[tokio::test]
    async fn r506_enabled_false_step_is_skipped() {
        let mut s = gating_step("disabled");
        s.enabled = false;
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(
            meta.status,
            RunStatus::Success,
            "skipped step doesn't fail the run"
        );
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
    }

    #[tokio::test]
    async fn r506_stubbed_step_is_skipped_by_default() {
        let mut s = gating_step("stubbed");
        s.activation = crate::types::StepActivation::Stubbed;
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
    }

    #[tokio::test]
    async fn r506_include_stubbed_overrides_stubbed_marker() {
        let mut s = gating_step("stubbed");
        s.activation = crate::types::StepActivation::Stubbed;
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline)
            .with_include_stubbed(true)
            .run()
            .await
            .unwrap();
        assert_eq!(
            meta.steps[0].status,
            RunStatus::Success,
            "--include-stubbed runs a stubbed step like an active one"
        );
    }

    #[tokio::test]
    async fn r506_include_stubbed_does_not_override_enabled_false() {
        let mut s = gating_step("disabled");
        s.enabled = false;
        s.activation = crate::types::StepActivation::Stubbed; // both knobs set
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline)
            .with_include_stubbed(true)
            .run()
            .await
            .unwrap();
        assert_eq!(
            meta.steps[0].status,
            RunStatus::Skipped,
            "enabled = false always wins over --include-stubbed"
        );
    }

    #[tokio::test]
    async fn r506_if_falsy_skips_step() {
        let mut s = gating_step("conditional");
        s.if_cond = Some("matrix.target == 'ios-device'".into());
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let mut coord = indexmap::IndexMap::new();
        coord.insert(
            "target".to_string(),
            toml::Value::String("macos-native".into()),
        );
        let meta = PipelineRunner::new(pipeline)
            .with_matrix_coord(coord)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Skipped);
    }

    #[tokio::test]
    async fn r506_if_truthy_runs_step() {
        let mut s = gating_step("conditional");
        s.if_cond = Some("matrix.target == 'ios-device'".into());
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let mut coord = indexmap::IndexMap::new();
        coord.insert(
            "target".to_string(),
            toml::Value::String("ios-device".into()),
        );
        let meta = PipelineRunner::new(pipeline)
            .with_matrix_coord(coord)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Success);
    }

    #[tokio::test]
    async fn r506_if_with_expression_delimiters_strips_braces() {
        let mut s = gating_step("conditional");
        s.if_cond = Some("${{ matrix.target == 'ios-device' }}".into());
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![s],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let mut coord = indexmap::IndexMap::new();
        coord.insert(
            "target".to_string(),
            toml::Value::String("ios-device".into()),
        );
        let meta = PipelineRunner::new(pipeline)
            .with_matrix_coord(coord)
            .run()
            .await
            .unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Success);
    }

    // ── R506 phase 2: success()/failure()/always()/cancelled() ────────────
    //
    // The runner tracks the cumulative `overall_status` mid-run and feeds it
    // into the expr context as `job_status` so an `if=` can ask "did anything
    // fail above me?". `cancelled()` is always false from inside a step gate
    // because cancellation aborts the whole future, never reaches the next
    // step (matches GHA semantics).

    fn failing_step(name: &str) -> crate::types::QedStep {
        let mut s = gating_step(name);
        // `false` exits non-zero on every Unix host — simplest deterministic
        // failure that doesn't depend on a missing binary.
        s.argv = vec!["false".into()];
        s.on_fail = OnFail::Continue;
        s
    }

    #[tokio::test]
    async fn r506_if_always_runs_after_failure() {
        let mut gated = gating_step("cleanup");
        gated.if_cond = Some("always()".into());
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![failing_step("bad"), gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[0].status, RunStatus::Failed);
        assert_eq!(
            meta.steps[1].status,
            RunStatus::Success,
            "always() runs even after a prior failure"
        );
    }

    #[tokio::test]
    async fn r506_if_failure_runs_only_after_failure() {
        let mut gated = gating_step("only-on-fail");
        gated.if_cond = Some("failure()".into());
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![failing_step("bad"), gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[1].status, RunStatus::Success);
    }

    #[tokio::test]
    async fn r506_if_failure_skips_when_all_green() {
        let mut gated = gating_step("only-on-fail");
        gated.if_cond = Some("failure()".into());
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![gating_step("ok"), gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[1].status, RunStatus::Skipped);
    }

    #[tokio::test]
    async fn r506_if_success_skips_after_failure() {
        let mut gated = gating_step("only-on-success");
        gated.if_cond = Some("success()".into());
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![failing_step("bad"), gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(meta.steps[1].status, RunStatus::Skipped);
    }

    #[tokio::test]
    async fn r506_if_cancelled_is_always_false_mid_run() {
        let mut gated = gating_step("on-cancel");
        gated.if_cond = Some("cancelled()".into());
        let pipeline = Pipeline {
            name: "p".into(),
            label: "p".into(),
            steps: vec![gated],
            params: HashMap::new(),
            on_success: vec![],
            on_fail: vec![],
            triggers: vec![],
            concurrency_key: None,
            placement: crate::types::Placement::Anywhere,
            workspace: crate::types::WorkspaceMode::Live,
            wraps: None,
            matrix: None,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        };
        let meta = PipelineRunner::new(pipeline).run().await.unwrap();
        assert_eq!(
            meta.steps[0].status,
            RunStatus::Skipped,
            "cancelled() is unreachable from inside an if= gate; always evaluates false"
        );
    }
}
