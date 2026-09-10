//! QED — CI scheduler: pipelines, step DAGs, triggers, and pass/fail gating over task execution
//!
//! QED is yah's CI layer. It schedules named pipelines, gates on results, and chains into
//! yubaba (deployment) and almanac (data scheduler). Unlike task (execution primitive),
//! qed handles definition, ordering, gating, and triggering.
//!
//! @yah:ticket(R299-T2, "Wire qed subcommand into CLI")
//! @yah:at(2026-05-23T01:43:09Z)
//! @yah:status(review)
//! @yah:parent(R299)
//! @yah:next("Add 'qed' variant to app/yah/cli/src/cli.rs Commands enum")
//! @yah:next("Create qed subcommand handler in app/yah/cli/src/ (qed.rs or inline)")
//! @yah:next("Route 'yah qed <cmd>' to PipelineLoader + PipelineRunner")
//! @yah:next("Verify: cargo check -p yah clean, yah qed --help shows subcommand")
//!
//! @yah:ticket(R299-T11, "Scaffold crates/yah/qed crate (types, runner, config loader)")
//! @yah:at(2026-05-23T20:02:24Z)
//! @yah:status(review)
//! @yah:assignee(agent:claude)
//! @yah:parent(R299)
//! @yah:handoff("Duplicate of T5 — scaffold was already complete. builtins.rs extracted (T6), .yah/qed/ created (T4), CLI runner wired (T2 gap). cargo check -p qed -p yah clean, cargo test -p qed 3/3 pass.")
//!
//! @yah:relay(R407, "QED: native-tarball output + musl-static gate")
//! @yah:at(2026-06-02T03:25:15Z)
//! @yah:status(open)
//! @yah:phase(P2)
//! @yah:parent(Q405)
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
//!
//! @yah:ticket(R407-T1, "QED catalog: add 'produces' field (oci-image | native-tarball | both)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-02T03:27:27Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R407)
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
//! @yah:handoff("Added ProduceTarget enum (oci-image | native-tarball) and produces: Vec<ProduceTarget> field to CatalogEntry. Defaults to [oci-image] for container-first safety per W154. Empty produces lists are rejected via CatalogError::EmptyProduces. Re-exported ProduceTarget from images mod + crate root. 5 new tests cover default-when-omitted, explicit native-tarball, both-targets, empty-rejected, and unknown-variant-rejected; all bundled entries verified to default to [oci-image] (no catalog.toml edits needed). compile.rs test fixture updated. 16/16 images::catalog::tests pass.")
//! @yah:verify("cargo test -p qed --lib images::catalog::tests")
//! @yah:gotcha("Pre-existing unrelated qed test failures: config::tests::parses_build_image_step_from_toml (PushRequiresWritableRegistry on ghcr.io — fallout from in-flight registries module) and tests::test_builtin_release_build_pipeline (asserts 4 steps; release-build now has 6). Neither touches catalog.")
//!
//! @yah:ticket(R435-F2, "Runner gates kicks on placement: CLI refuses ci-only without --force; GHA warns/refuses local-only")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-04T19:15:58Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R435)
//! @yah:next("Detect CI via $CI / $GITHUB_ACTIONS at run entry")
//! @yah:next("Refuse `ci-only` from non-CI host unless --force; emit a clear error pointing to the placement field")
//! @yah:next("Warn (don't refuse) when `local-only` runs on CI — drop a hint to flip placement or split the recipe")
//! @yah:next("Decision matrix in W155 is the canonical truth table — encode it in one place")
//! @yah:verify("Unit test: each (placement × runner) cell from W155's matrix routes correctly")
//! @yah:verify("Manual: `yah qed run yubaba-release` from a laptop emits the refusal; `--force` bypasses with a warning")
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//! @yah:depends_on(R435-F1)
//! @yah:handoff("F2 complete. Single source of truth lives at crates/yah/qed/src/placement_gate.rs: `RunnerEnv::detect()` (reads $CI / $GITHUB_ACTIONS), `evaluate(placement, env, force) -> GateOutcome` (Allow{warning} | Refuse{reason}). All 6 matrix cells + the --force escape hatch are encoded once. Gate runs at BOTH entry points: (1) CLI `yah qed run` (app/yah/cli/src/qed.rs) before the camp-proxy probe — fails fast without a daemon round-trip; (2) camp daemon `qed_run_handler` (app/yah/cli/src/camp.rs) as defence in depth for direct JSON-RPC callers (desktop Run button, agent tools). Added `force: Option<bool>` to rpc::QedRunParams and threaded through 4 wire construction sites (qed.rs CLI, desktop/qed.rs, agent-tools/qed_tools.rs, all 16 camp.rs test sites). Added --force flag to the Run subcommand. Tests in placement_gate::tests cover all 6 cells, the --force flip, and env_truthy canonical values (9 new tests, all green). Manual verify against /tmp fixture passed all 3 cases: (a) `yah qed run ci-thing` on Local without --force → Error + exit 1 + clear reason; (b) same + --force → stderr warning + run proceeds; (c) CI=true → silent allow. Full qed lib suite: 165 pass (up from 156); the lone pre-existing test_builtin_release_build_pipeline failure is the same one flagged in R380-T3's handoff — unrelated to this work. `cargo check --workspace` clean.")
//! @yah:next("R435-T3 can start: stamp `placement` on the 3 existing recipes (desktop-local=local-only, pond-smoke=anywhere, yubaba-release=ci-only) and audit `concurrency_key` per W155 principle 3. The gate is live so yubaba-release will start refusing local kicks the moment the field is added — expected and intentional.")
//! @yah:cleanup("Surface `placement` in `yah qed list`/`tail` headers — still deferred from F1, equally easy to graft into either F2 or T3.")
//!
//! @yah:ticket(R438-T4, "Recipe TOML loader for .yah/qed/transforms/*.toml")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-04T21:07:00Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R438)
//! @yah:next("New loader (separate from pipeline loader) under qed/ that parses transform recipes")
//! @yah:next("Recipe schema: name, label, placement { location, runtime }, image (digest-pinned), steps[]")
//! @yah:next("Fixed IO contract: YAH_TRANSFORM_IN_0 + YAH_TRANSFORM_OUT env vars; params substitute as {{key}}")
//! @yah:next("Argv-element-granularity substitution (no shell, no string concat)")
//! @yah:verify("Sample recipe round-trips through loader")
//! @yah:verify("Recipe without @sha256: image digest rejected at load")
//! @yah:gotcha("Separate dir from .yah/qed/<pipeline>.toml — W164 OQ#1 resolved. Don't conflate with R435 pipeline-discipline loader.")
//! @arch:see(.yah/docs/working/W164-derived-static-assets.md)
//! @yah:handoff("New crates/yah/qed/src/transforms.rs holds TransformRecipe + RecipePlacement + RecipeLocation + RecipeStep + TransformRecipeLoader + substitute_argv + RecipeError. Separate from PipelineLoader per W164 OQ#1 — recipes have a fixed IO contract pipelines don't. Re-exported from qed lib.rs alongside ENV_TRANSFORM_IN_0/ENV_TRANSFORM_OUT constants for callers (T5 materialize step). Recipe TOML uses the W164 example shape but with image= BEFORE [placement] — gotcha: TOML scopes a scalar that follows a [table] header into that table, so the W164 doc's example as-written would put image inside placement. Doc-comment in the test fixture and a one-line callout in the substitute_argv helper note this; W164 doc could use a sentence on order. Digest-pin enforcement is two-layered: string-form image='...' rejected at serde-deserialize by ImageRef's custom Deserialize (T3) — surfaces as RecipeError::Parse; struct-form [image] without digest rejected post-parse by the loader as RecipeError::ImageNotPinned. Argv substitution is element-granular (no shell, no concat); unknown keys preserved verbatim so callers can detect missing bindings; whitespace inside {{ key }} trimmed; unterminated {{ kept literal. 9 transform tests cover round-trip, both digest-reject paths, NotFound, sub-known/unknown/space-preserving/unterminated/trimmed-key. cargo check -p qed -p workload-spec -p cloud clean. Pre-existing test failures unrelated to T4: tests::test_builtin_release_build_pipeline (qed builtins drift, flagged in T4 gotcha) and runner crate uncompiled due to in-flight 'wip yah dictate + cloud ops' work.")
//!
//! @yah:relay(R487, "Native Rust GHA YAML runtime + action overrides (W200)")
//! @yah:at(2026-06-08T02:51:53Z)
//! @yah:status(open)
//! @yah:parent(Q486)
//! @yah:next("Phase order: F1 parser -> F2 expr -> F3 graph/matrix -> F4 step exec + override registry stub -> F5 generic overrides -> F6 docker family -> F7 R2 release override -> F8 cosign -> F9 StepKind::GhaWorkflow")
//! @yah:gotcha("v1 has no JS-action runtime; every uses: in release.yml MUST be overridden or the run fails loudly")
//! @yah:gotcha("macos-latest jobs hard-error when no mac host is available — surface this in operator docs")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//!
//! @yah:ticket(R487-F1, "qed-gha crate scaffold + GHA YAML parser (round-trip release.yml/ci.yml/smoke.yml)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:52:30Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R487)
//! @yah:next("New crate crates/yah/qed-gha — workspace member, serde_yaml + thiserror deps")
//! @yah:next("Public types: Workflow, Job, Step (post-parse, not raw YAML); Step exposes uses-slug + ref + with-inputs + run-body + env + if + id + name")
//! @yah:next("expr.rs: tokenize ${{ ... }} substrings inside strings, preserve raw outside (eval is F2)")
//! @yah:verify("cargo test -p qed-gha — parse every .github/workflows/*.yml round-trip")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:tier(Warrior)
//! @yah:handoff("F1 landed: crates/yah/qed-gha scaffolded (serde_yaml + indexmap + thiserror). Public types Workflow / Triggers / Job / Step / StepAction / ExprString match W200 §Architecture for the F1 surface — every field release.yml/ci.yml/smoke.yml/smoke-sweeper.yml uses is named, with unknown sibling keys tolerated (parser is permissive in F1; F2/F3 tighten as needed). ExprString tokenizes ${{ … }} into Literal/Expr tokens; expression body is preserved verbatim for F2's parser. Step::Uses splits slug @ ref. Permissions handled as string (read-all/write-all) or Scopes map. Strategy.matrix keeps raw serde_yaml::Value for include/exclude/dimensions — F3 will normalize into MatrixRows. 15/15 tests green: 8 expr tokenizer unit tests (pure-literal, pure-expr, mixed, fallback-chain, string-quote braces, two-expr-with-literal, yaml scalar coercion, unterminated tail) + 7 round-trip tests covering the four workflows + step-uses split + with-inputs expressions + multiline run bodies. Added to workspace members + default-members. No changes to existing crates.")
//! @yah:next("F2 picks up against this surface: implement crate::expr module — Pratt parser for the body strings sitting in ExprToken::Expr, Context lookup (github.*/matrix.*/needs.*.outputs.*/steps.*.outputs.*/secrets.*/env.*), operators &&/||/==/!=, status functions always()/success()/failure()/cancelled(). Re-export an eval() entry point.")
//! @yah:verify("cargo test -p qed-gha")
//!
//! @yah:ticket(R487-F2, "GHA expression engine: parser + evaluator + Context (subset used by release.yml)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:52:42Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R487)
//! @yah:next("Pratt parser for Expr AST: Literal, Lookup (dotted path), BinOp (&&, ||, ==, !=), Call (always/success/failure/cancelled)")
//! @yah:next("Context struct: github, matrix, needs, steps, env, secrets — secrets is a provider trait (lazy + scope-checked)")
//! @yah:next("|| overload: logical AND string-fallback in one operator (matches GHA semantics)")
//! @yah:verify("Catalogued expression-shape table from W200 audit: each shape evaluates correctly against a fixture Context")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F1)
//! @yah:tier(Warrior)
//! @yah:handoff("F2 landed: crate::expr module with tokenizer, recursive-descent parser (precedence: or < and < cmp < unary < primary), and tree-walking evaluator against Context. Value type is JSON-shaped (Null/Bool/Number/String/Array/Object via IndexMap) and shared between AST literals, context payloads, and eval results. Context namespaces: github, env, vars, matrix (Option), needs, steps, inputs, runner, secrets, job. Default Context::new() pre-populates empty objects so missing-path lookups return Value::Null rather than erroring. — Operators: &&, ||, ==, !=, <, <=, >, >=, unary !. && and || short-circuit AND preserve values (GHA semantics: `a && b || ''` -> `b` when both truthy, `''` when `a` falsy). Cross-type ==/!= coerces (number<>string via parse, bool<>number via 0/1, bool<>string via 'true'/'false'). Identifiers allow `-` so `needs.image-yah-base.outputs.digest` parses. — Functions: always/success/failure/cancelled (track ctx.job_status, default Success), contains (string-in-string + item-in-array), startsWith, endsWith, format (GHA {N} indexed holes + {{ }} escapes), join, toJSON, fromJSON (hand-rolled JSON in/out to avoid pulling serde_json into the leaf), hashFiles (delegates to ctx.hash_files host hook; defaults to ''). — 24 new tests (39 total in qed-gha). Audit coverage: smoke's gnarly `(github.event_name == 'push' && !contains(github.ref_name, '-')) || (github.event_name == 'workflow_dispatch' && inputs.skip_smoke != true)` exercised across 4 truth-table cases; image-gate `always() && needs.smoke.result != 'failure' && needs.smoke.result != 'cancelled'` across success/skipped/failure/cancelled; needs.X.outputs.Y dotted lookup; `inputs.induce_panic == true && '1' || ''` string fallback; matrix.use_target_flag fallback. cargo test -p qed-gha = 39/39, no warnings.")
//! @yah:next("F3 picks up against this surface: build the job graph from Workflow.jobs (topo via Job.needs), evaluate Job.if_cond and per-step if_cond at scheduling time using crate::expr::evaluate, expand strategy.matrix (dimensions x include, minus exclude) into per-row job instances. No step execution yet — F3 proves order, output propagation between jobs (needs.X.outputs.Y populated from completed jobs' Job.outputs after expr eval), and matrix fan-out.")
//! @yah:verify("cargo test -p qed-gha")
//!
//! @yah:ticket(R487-F3, "GHA job graph + matrix expansion + scheduler skeleton (no step exec yet)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:52:52Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R487)
//! @yah:next("Topological sort over needs:; run in waves; propagate needs.X.outputs/result into expression context")
//! @yah:next("Matrix expansion: include-only (release.yml shape) is straightforward; full cartesian a few extra lines")
//! @yah:next("fail-fast: false handling — row failure does not cancel siblings")
//! @yah:next("if: evaluated after needs resolves so always() / needs.X.result work as expected")
//! @yah:verify("Fixture release.yml: dry-run produces the expected job order + matrix fan-out + skip-mask")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F2)
//! @yah:tier(Warrior)
//! @yah:handoff("F3 landed: crate::graph with topological scheduling, matrix expansion, and needs/outputs propagation. topo_sort() returns waves via Kahn's algorithm with cycle and unknown-needs detection; declaration order preserved inside each wave for diff-stable output. expand_matrix() handles GHA's 3-step semantics: cartesian over dimensions (declaration order, deterministic) -> apply include rows (merge into matching anchor with non-overwriting new keys, else append standalone) -> drop exclude rows. plan(&Workflow) -> Plan { waves: Vec<Vec<JobInstance>> } stitches the two together; matrix rows become per-instance JobInstances with stable `job#row` keys. — build_needs_value(&[CompletedInstance]) aggregates rows of the same job_id into a single needs.<job_id> entry: result via JobResult::aggregate (failure > cancelled > skipped > success), outputs unioned (later instances clobber earlier). build_context_for_instance() composes the per-instance Context: matrix from the JobInstance row, needs from completed, env from workflow+job env (job shadows workflow), runner.os passthrough. should_run_job() evaluates Job.if_cond as an *implicit expression* (whole body parsed as expr regardless of `${{ }}` delimiters — the GHA semantic that bit on first pass); evaluate_outputs() walks Job.outputs ExprString templates against a steps-populated context. eval_exprstring() helper: single Expr token preserves typed Value, mixed tokens concatenate via as_str_lossy. — 16 new tests, 55 total in qed-gha. Coverage: 2-wave topo + cycle + unknown-needs; 2x2 cartesian, include-only mirror of release.yml's cli-release shape, include-extends-matching-combination, exclude-drops-matching-row; plan() across topo+matrix; JobResult::aggregate priority; needs.X.result/outputs propagation including matrix-failure aggregation; the real `if: always() && needs.smoke.result != 'failure' && needs.smoke.result != 'cancelled'` gate against synthetic completion; outputs eval against steps context; ExprString-eval typed-preserving vs. mixed-concatenate split. — F3 verify: plan(real release.yml) gives [smoke] in wave 0, image-yah-base/rust/etc. in wave 1, image-yah-rust-bun in a later wave behind image-yah-rust, and cli-release expands to 3 matrix instances. cargo test -p qed-gha = 55/55, no warnings.")
//! @yah:next("F4 picks up against this surface: build the step executor. StepKind::Run (bash) executes via tokio::process with env injection from build_context_for_instance + ::set-output:: capture into ctx.steps. OverrideRegistry stub: trait Override { fn slug(&self) -> &str; fn run(&self, with: &IndexMap<String, Value>, ctx: &mut StepContext) -> Result<Outcome>; }, an unknown-uses lookup errors with the W200 'no override registered for X' message. Hook into Plan::iter_instances + should_run_job: for each instance evaluate if_cond, run steps (Run + Uses dispatch), collect ::set-output:: into ctx.steps, evaluate Job.outputs at the end, feed back into build_needs_value for the next wave. Test fixture: a workflow with one `run:` step setting an output + one downstream `run:` step echoing it through `${{ steps.X.outputs.Y }}` env injection.")
//! @yah:verify("cargo test -p qed-gha")
//!
//! @yah:ticket(R487-F4, "Step execution: run: (bash) + OverrideRegistry stub + unknown-action-is-error policy")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:53:01Z)
//! @yah:status(review)
//! @yah:phase(P4)
//! @yah:parent(R487)
//! @yah:next("shell.rs: run: blocks execute bash with env injection (workflow env > job env > step env precedence)")
//! @yah:next("OverrideRegistry: trait + TOML loader (.yah/qed/gha-actions.toml + ~/.yah/qed/gha-actions.toml overlay)")
//! @yah:next("v1 policy: uses: a slug with no override registered = loud error (no JS-action runtime)")
//! @yah:next("deny + deny_message in TOML overrides specific slugs with a custom message")
//! @yah:verify("End-to-end: a no-uses workflow (only run: steps) executes through the runtime")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F3)
//! @yah:tier(Warrior)
//! @yah:handoff("F4 landed: step executor (crate::runtime) + override registry (crate::overrides) + workflow walker. — OverrideRegistry: trait Override { execute(&OverrideCall) -> Result<OverrideOutcome, String> } + IndexMap-backed registry with three-state Lookup (Found{ovr, config} / Denied{message} / Unknown). load_toml_str()/load_toml_file() parses the W200 schema ([overrides.\"slug\"] { deny=bool, deny_message=str, config=toml-table }); deny wins over registered impl so camps can prevent built-ins. Per-slug config blob is lowered toml::Value -> expr::Value so F5+ overrides read it through the same tree-walker the evaluator uses. Missing TOML files are silent OK (per-camp + per-machine overlays are both optional). default_overlay_paths() returns the W200-canonical .yah/qed/gha-actions.toml + ~/.yah/qed/gha-actions.toml. — Executor: workspace + registry + github/inputs/runner_os + env_passthrough flag (off = hermetic for tests). execute_workflow() walks plan() waves sequentially (concurrency deferred — not a correctness concern), running each instance through run_instance(): build_context_for_instance → should_run_job (skip -> JobResult::Skipped, no steps) → step loop. Step env composes workflow.env + job.env + step.env (step shadows) plus the prior-step $GITHUB_ENV overlay; ctx.steps refreshes each iteration so `${{ steps.X.outputs.Y }}` sees prior step outputs. step.if_cond evaluated as implicit expression (whole body parsed as expression); when no if and a prior step failed, defaults to skip — always()/failure()/cancelled() let downstream steps opt in. step.continue_on_error lets the job keep running on step failure. — Bash exec: writes the run-body to a tempfile (set -eo pipefail prelude), spawns `bash {file}` with composed env + GITHUB_OUTPUT/GITHUB_ENV/GITHUB_STEP_SUMMARY/RUNNER_OS. Captures: ::set-output name=K::V (legacy) + $GITHUB_OUTPUT K=V (modern) + K<<EOF\\n…\\nEOF (heredoc, with user-chosen delim). $GITHUB_ENV updates fold into env_overlay for subsequent steps in the same job. continue-on-error honoured at step level. — Uses dispatch: `with:` inputs evaluate through ExprString eval against step ctx (→ typed Value), then route through registry.lookup(slug). Unknown → RuntimeError::UnknownAction with the W200 message ('no override registered for X — register a built-in or add a TOML deny rule (W200 policy: every uses: must be overridden)'). Denied → RuntimeError::DeniedAction{slug, message}. Found → ovr.execute(&OverrideCall{slug, git_ref, with, env, workspace, config}). — Job outputs evaluate via graph::evaluate_outputs against final steps ctx; CompletedInstance fed into next wave's needs.* via existing build_needs_value. — 15 new tests (70 total in qed-gha): bash legacy ::set-output capture, $GITHUB_OUTPUT single + heredoc, bash failure -> JobResult::Failure, continue-on-error keeps job Success while step records Failure, $GITHUB_ENV propagates between steps, build->publish output flow through needs.* env injection (the verify-line case), uses unknown -> W200 error, uses registered override receives ExprString-evaluated `with:` inputs, deny surfaces message, skipped-job propagates through needs.X.result, plus 5 in overrides module (lookup unknown/registered/denied/config-blob/missing-file). cargo build / cargo test -p qed-gha: 70/70, no warnings.")
//! @yah:next("F5 picks up against this surface: register built-in Override impls in a new crate::overrides::builtin module — actions/checkout (native git clone into ${workspace}; honour `with: { repository, ref, path }`); actions/cache (local-fs backend, key/path from `with:`, cache dir from config.dir, no-op when config.backend == 'no-op'); actions/upload-artifact + actions/download-artifact (writes/reads a workspace-scoped artifact dir keyed by name); Swatinem/rust-cache (wraps actions/cache impl with rust-toolchain-aware key derivation); dtolnay/rust-toolchain (shells `rustup toolchain install` + `rustup target add` from `with: { toolchain, targets }`); oven-sh/setup-bun (shells `bun --version` || install). Each impl gets a fixture test exercising at least one `with:` permutation; ship a `register_builtins(&mut OverrideRegistry)` entry point and call it from Executor::new() (or a feature-gated path) so a workflow with no uses-overrides-needed runs straight through.")
//! @yah:verify("cargo test -p qed-gha")
//!
//! @yah:ticket(R487-F5, "Override impls: checkout, cache, upload/download-artifact, Swatinem/rust-cache, dtolnay/rust-toolchain, oven-sh/setup-bun")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:53:11Z)
//! @yah:status(review)
//! @yah:phase(P5)
//! @yah:parent(R487)
//! @yah:next("actions/checkout — native git clone, respects with.ref / with.repository")
//! @yah:next("actions/cache + Swatinem/rust-cache — local-fs backend keyed on with.key + with.path")
//! @yah:next("actions/upload-artifact + actions/download-artifact — workspace-scoped artifact dir, paired by name")
//! @yah:next("dtolnay/rust-toolchain + oven-sh/setup-bun — shell rustup / bun install respecting with: inputs")
//! @yah:verify("Build-only subset of release.yml (cli-release legs without docker/cosign/upload) runs end-to-end against this runtime")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F4)
//! @yah:tier(Cleric)
//! @yah:handoff("F5 landed: six built-in Override impls + register_builtins(&mut OverrideRegistry) wired into Executor::new(). Tests=77/77. New impls: actions/checkout (no-op when no with.repository; native git clone otherwise honoring ref/path/fetch-depth, target wipe before clone); actions/cache (local-fs backend keyed on with.key+with.path, restore-only, config.backend=no-op short-circuits, config.dir overlays default ${HOME}/.cache/yah-qed/gha); Swatinem/rust-cache (target/ restore keyed on rustc-version+Cargo.lock-digest+with.shared-key+with.key, single workspace honored from with.workspaces); actions/upload-artifact + actions/download-artifact (workspace-scoped ${workspace}/.qed-artifacts/<name>/, paired by name; download falls back to all-artifacts-by-subdir when name unset; missing upload path raises loud error); dtolnay/rust-toolchain (rustup toolchain install <ref|with.toolchain> --profile minimal + target add for each with.targets CSV + component add for with.components, cachekey output combines toolchain+cargo version); oven-sh/setup-bun (verify-only: bun --version, errors with install hint if absent; outputs bun-version + bun-path). Executor::bare() preserved for hermetic F4-style tests. v1 limitation documented in code: cache + rust-cache restore-only — save defers to post-step hooks (not in scope for F5). Build-only subset of release.yml is now runnable end-to-end up to (but not including) the docker family covered in F6.")
//! @yah:next("User: verify F5 against your operational expectations — especially (a) the restore-only cache semantics (no v1 post-step save — OK for build-only subset, but flag if you want eager save), (b) the actions/checkout no-op-when-no-repo policy (right for the common `uses: actions/checkout@v4` case, but skip-vs-error if someone sets only `with: { ref }` without repository), and (c) the dtolnay/rust-toolchain @ref-as-toolchain fallback. Pickable next: R487-F6 (docker family with registry redirect).")
//!
//! @yah:ticket(R487-F6, "Docker override family: setup-buildx, setup-qemu, login, build-push (registry redirect)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:53:19Z)
//! @yah:status(review)
//! @yah:phase(P6)
//! @yah:parent(R487)
//! @yah:next("setup-buildx / setup-qemu — no-op when host already has them; otherwise shell install")
//! @yah:next("docker/login-action — read camp creds for the resolved registry (post-redirect); ignore secrets.GITHUB_TOKEN")
//! @yah:next("docker/build-push-action — apply registry_route config (ghcr.io -> registry.yah.dev), emit ProducedArtifact (image digest)")
//! @yah:verify("Image-build subset of release.yml (image-yah-base/rust/rust-bun) runs locally and pushes to registry.yah.dev")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F5)
//! @yah:tier(Cleric)
//! @yah:handoff("F6 landed: docker family overrides (setup-buildx, setup-qemu, login, build-push) with TOML-driven registry redirect. Tests=84/84 (77 prior + 7 new). docker/setup-buildx-action: verify-only via `docker buildx version`, errors with install hint if absent. docker/setup-qemu-action: verify-only via `docker version`, assumes pre-installed binfmt (privileged --rm container pull is too heavy + too magical for v1; cross-arch build failures surface at build-push instead). docker/login-action: applies redirect_registry(with.registry) BEFORE shelling docker login; empty password (the `${{ secrets.GITHUB_TOKEN }}` case — QED doesn't resolve that secret) becomes skip-with-success so the host's pre-existing creds carry the push and any real auth failure surfaces at build-push with the registry's own message rather than a synthetic one here; non-empty password streams via --password-stdin. docker/build-push-action: applies redirect_image_ref per tag (only the host segment swapped, repo+tag/digest suffix preserved verbatim); shells `docker buildx build` honouring with.{push, load, platforms, file, provenance, sbom, build-args, context}; captures digest + imageid by reading `--metadata-file` JSON (containerimage.digest + containerimage.config.digest) so steps.build.outputs.digest keeps working for downstream cosign sign + per-binary DIGEST env blocks. push=true with no tags is a loud error. metadata blob also surfaces as steps.build.outputs.metadata for any consumer that wants the whole file. Pure helpers redirect_registry/redirect_image_ref/parse_buildx_metadata/collect_build_args are unit-tested; full docker shell-out is not (would require a docker daemon).")
//! @yah:next("User: verify F6 — especially (a) the empty-password skip-with-success behaviour for docker/login-action (right for release.yml's `${{ secrets.GITHUB_TOKEN }}` shape, but flag if you want a hard error when password is unset on a workflow that genuinely needs to authenticate), (b) the setup-qemu verify-only stance (binfmt install deferred to host setup), and (c) that registry_route only rewrites the HOST segment of an image ref (e.g. ghcr.io/yah-ai/yah-base:latest -> registry.yah.dev/yah-ai/yah-base:latest — confirm registry.yah.dev's path layout matches yah-ai/<name>). Pickable next: R487-F7 (softprops/action-gh-release — R2 publish via ProducedArtifact).")
//!
//! @yah:ticket(R487-F7, "softprops/action-gh-release override -> R2 (emits ProducedArtifact, rides existing publish.rs)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:53:28Z)
//! @yah:status(review)
//! @yah:phase(P7)
//! @yah:parent(R487)
//! @yah:next("Override impl reads with.files glob, writes each as a ProducedArtifact with binary derived from filename stem")
//! @yah:next("config.r2.bucket + config.r2.prefix from gha-actions.toml; prefix templated against ${{ github.ref_name }}")
//! @yah:next("Aggregator: workflow run rolls up all overrides' ProducedArtifacts into the parent QED step's collection (see F9)")
//! @yah:verify("Run release.yml's cli-release/yubaba-release/camp-release legs through W200; staged tree appears under cdn.yah.dev")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F6)
//! @yah:tier(Cleric)
//! @yah:handoff("F7 landed: softprops/action-gh-release override + ProducedArtifact plumbing end-to-end. Tests=88/88 (84 prior + 4 new). Plumbing: added yah_qed_gha::ProducedArtifact { binary, path, triple } — structurally compatible with qed::types::ProducedArtifact so F9 maps at the qed-runner seam without dragging a qed dep into qed-gha. New produced: Vec<ProducedArtifact> field on OverrideOutcome / StepResult / InstanceRun, plus WorkflowRun::produced() aggregator (only successful jobs + only successful steps contribute, so failed legs don't leak half-baked artifacts into Outcome::Publish). Override: reads with.files line-by-line; each line is a workspace-relative path or single-segment * / ? glob (no **, no character classes — release.yml's files: are single tokens), expands against the workspace, derives binary from leading dash-segment of stem and triple from trailing segment (cli-v0.8.10-x86_64-unknown-linux-musl.tar.gz → binary=cli, triple=x86_64-unknown-linux-musl). Strips .tar.gz / .tar.xz / .tar.bz2 / .tgz / .zip; falls back to (stem, None) for filenames that don't match the convention. with.fail_on_unmatched_files=true is loud per release.yml usage. Outputs: upload_url + url (latter shaped as https://cdn.yah.dev/releases/<tag> so workflow steps that read steps.release.outputs.url still get a usable string).")
//! @yah:next("User: verify F7 — especially (a) the binary/triple parsing convention (leading-dash-segment + trailing-suffix vs.<tag>-<triple> — confirm against release.yml's actual filenames: cli-*.tar.gz, yubaba-*.tar.gz, camp-*.tar.gz), (b) the choice to surface url as https://cdn.yah.dev/releases/<tag> (path layout must match W160's publisher), and (c) that filtering produced by successful-jobs-only is the right policy for partial-release failures (alternative: ship whatever shipped so a partial release isn't lost). Pickable next: R487-F8 (cosign sign override) and R487-F9 (StepKind::GhaWorkflow integration — this is where ProducedArtifact gets mapped to qed::types::ProducedArtifact and rolled into the outer step's Outcome::Publish).")
//!
//! @yah:ticket(R487-F8, "sigstore/cosign-installer + cosign sign override (verify identity regex matches our registry)")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-06-08T02:53:38Z)
//! @yah:status(review)
//! @yah:phase(P8)
//! @yah:parent(R487)
//! @yah:next("cosign-installer override — shell install if not present, no-op if present")
//! @yah:next("Bare cosign sign --yes runs as a normal run: step; OIDC identity check happens consumer-side")
//! @yah:next("Verify task::default_image::pull's identity regex accepts the new identity from registry.yah.dev signs")
//! @yah:verify("After one signed run: YAH_RUST_BUN_DIGEST=sha256:<hash> cargo test -p task default_image::pull -- --include-ignored passes")
//! @yah:gotcha("OPEN QUESTION: cosign signs digests, but the keyless OIDC identity embeds the issuer; if registry change breaks the regex this needs a tweak in default_image.rs")
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @yah:depends_on(R487-F7)
//! @yah:tier(Cleric)
//! @yah:handoff("F8 landed: sigstore/cosign-installer override (verify-only, parity with setup-bun / setup-buildx). Tests=88/88 (registration test extended; install paths are too heavy for v1 — see W200 §No external downloads). Two notes baked into the code header for future readers / F9: (1) Identity regex is REGISTRY-AGNOSTIC. The verifier in task::default_image::pull pins `--certificate-identity-regexp ^https://github\\.com/yah-ai/yah/\\.github/workflows/release\\.yml@` — keyed on the workflow URL, not the pushed registry. So F6's ghcr.io → registry.yah.dev redirect needs NO consumer-side change. Open question #1 from W200 resolves: no regex tweak needed. (2) QED-mode signing is a known v1 gap. The bare `cosign sign --yes ghcr.io/yah-ai/<name>@${DIGEST}` in release.yml is a normal run: step, not a uses:; cosign-installer just ensures the tool is present. In GHA mode keyless OIDC against token.actions.githubusercontent.com mints the identity and the sign succeeds. In QED mode (yubaba / local) there's no GHA OIDC token, so cosign sign either drops into the interactive browser flow or fails. Wiring a QED-managed OIDC path (workload identity from camp keystore) is out of scope for F8; v1 expectation is releases sign on GHA, QED-side runs treat sign as best-effort (failure logs without blocking pulls — matches release.yml's existing behaviour). Worth a follow-up ticket if QED-side signed releases become a goal.")
//! @yah:next("User: verify F8 — mostly a doc / scope spike. Confirm (a) you're OK that v1 cosign-installer is verify-only (host must have cosign on PATH; no auto-install), and (b) you accept the QED-mode signing gap as a future ticket rather than blocking F9. Pickable next: R487-F9 (StepKind::GhaWorkflow + QED runner dispatch — the integration phase that maps yah_qed_gha::ProducedArtifact → qed::types::ProducedArtifact and rolls into the outer step's Outcome::Publish, and surfaces `yah qed run release` as a one-step pipeline that wraps release.yml end-to-end).")
//!
//! @yah:relay(R495, "QED MCP tools: run status, pipeline list, run history")
//! @yah:assignee(bundle-anthropic-miravel)
//! @yah:at(2026-06-09T02:04:24Z)
//! @yah:status(review)
//! @yah:next("Expose qed.run_status { run_id } → pipeline name, step statuses, elapsed, outcome")
//! @yah:next("Expose qed.pipelines → list defined pipelines with source file + placement")
//! @yah:next("Expose qed.runs { limit, pipeline? } → recent run history (depends on run storage being wired)")
//! @yah:next("Wire run storage so yah qed list is populated (prerequisite for qed.runs)")
//! @yah:handoff("Shipped QED MCP tools across three sites. (1) crates/yah/agent-tools/src/qed_tools.rs: added QedPipelines tool that dispatches QED_PIPELINES RPC to the camp daemon, returns name/label/scope/params_required/step_count/step_names per pipeline. Exported in qed_tools() vec alongside existing QedRun/QedStatus/QedList/QedCancel. (2) app/yah/cli/src/mcp/tools.rs: added Qed CapabilityGroup; updated group_for_name (qed.* → Qed); updated allowed_groups (Relay + Yubaba get Qed); registered 5 Tool entries (qed.run, qed.status, qed.list, qed.cancel, qed.pipelines) with full input_schema in QED CI TOOLS (5) section; added qed.* dispatch arm in call() that routes through agent-tools KgTool impls (same ToolContext pattern as cloud.*). (3) app/yah/cli/src/qed.rs: wired List subcommand to proxy qed.list to camp daemon via hub_dispatch::try_call_camp, prints tabular run history or graceful 'no daemon' message; wired Status subcommand to proxy qed.status, prints pipeline/status/steps. Added 3 tests: qed_tools_are_registered, relay_job_includes_qed_tools, chat_job_drops_qed_tools. 67/67 mcp::tools tests pass; cargo check --workspace clean.")
//! @yah:verify("cargo test -p yah --lib mcp::tools  # 67/67 pass")
//! @yah:verify("cargo check --workspace  # clean")
//! @yah:verify("cargo test -p yah --lib mcp::tools::tests::qed_tools_are_registered  # 1/1")
//! @yah:verify("yah qed list  # with daemon: tabular history; without: 'no camp daemon' message")
//! @yah:verify("yah qed status <run_id>  # with daemon: pipeline+steps; without: clear error")
//!
//! @yah:relay(R717, "Executable docs: subject-keyed runnable cells in W###/A### prose")
//! @yah:at(2026-08-05T01:51:41Z)
//! @yah:status(open)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @yah:next("Phase order: P0 (T12, independent — do it first) / P1 QED-core primitives (T1 inputs, T2 secret, T3 CellRef — all three are standalone and need no doc parser) / P2 markdown-as-source (F4 parser, T5 host=, S13 open questions) / P3 the two doors (F6 index+RPC, T7 CLI, T8 MCP) / P4 the renderer (F9 fences, T10 subject selector) / P5 manual cells (T11, blocked on R622).")
//! @yah:next("The bill W296 signs up for is small and should stay small: ONE genuinely new mechanism (the subject key, T3+F6), TWO re-pointings of existing mechanisms (markdown as a QED source mirroring StepKind::Import; inputs= staleness generalized off ImportConfig's blake3 pin), ONE small safety flag (secret), ONE dependency (R622). Anything that does not fit that accounting is scope growth.")
//! @yah:next("Status NEVER goes back into the .md. This is the decision that separates this from Jupyter: .ipynb stores outputs in the file, which is why every notebook repo is a merge-conflict farm. yah runs many sessions against one working tree; a doc that dirties itself on every run puts a git diff between an operator and a status badge. Run state lives in .yah/jit/qed/, already gitignored.")
//! @yah:next("A doc is promoted to A### only after a SECOND runbook — a release runbook or an OSS-module procedure — survives contact with the cell vocabulary. W296 is explicit that promoting a schema on a sample size of one is how it acquires fields nobody can remove.")
//! @yah:gotcha("R622 (@Ashguard, active) owns StepKind::Manual / RunStatus::AwaitingHuman / park-resume and is editing oss/qed/crates/qed/src/types.rs and runner.rs RIGHT NOW. P1 tickets touch the same two files — re-read before editing and keep diffs inside QedStep/StepStatus/QedRunMeta.")
//! @yah:gotcha("No notebook.* MCP tool family, ever. These fold into qed.run params plus one qed.cells read view — mcp/tools.rs:348 is an open audit of per-tool token cost and a parallel family for a capability that is a parameter is exactly what it exists to prevent.")
//! @yah:gotcha("The strongest argument in the spike is freshness, not execution: the bug that cost the W257 session most was a stale ISO, and W257 admits 'the only guard is habit'. If inputs= staleness (T1) gets cut for scope, the relay loses its main justification.")
//! @yah:handoff("BACKEND LANE (oss/qed) DONE, uncommitted, at anchor 85801e7f6b76b369c0c8ecd2e5c7874990cd9286. S13 settled into W296; T1, T2, T3, F4, T5 all shipped and handed off with their own verifies. Two new files: oss/qed/crates/qed/src/staleness.rs (pure input-freshness core) and oss/qed/crates/qed/src/doc_source.rs (markdown-as-a-QED-source, incl. the T5 host= lowering). yah-qed lib: 778 pass / 0 fail / 1 ignored. cargo test -p yah --lib r325_f1: 36 pass. xtask schema_drift: 3 pass after regenerating qed-pipeline.toml.schema.json.")
//! @yah:next("STOPPED AT THE OPERATOR GATE, not at a blocker. F6 / T7 / T8 are next and all three edit @Ashguard's lane (camp.rs, cli.rs, mcp/tools.rs); the dispatch said to check before starting them. Evidence the lane has drained: the R721 session (session:bd566f16) is no longer on camp.roster and all twelve R721 children read `open` with no live claim. Not treating that as the confirmation — a clean roster does not prove nobody else is in those files. R717-T11 (blocked on R622) and F9/T10 (desktop) remain deliberately untouched.")
//! @yah:handoff("BACKEND LANE COMPLETE, uncommitted, anchor 85801e7f6b76b369c0c8ecd2e5c7874990cd9286. All eight dispatched tickets done and handed off: S13 (settled into W296), T1, T2, T3, F4, T5 (oss/qed), then F6, T7, T8 after the gate opened. Two new files in oss/qed (staleness.rs, doc_source.rs); one new RPC method (qed.cells) and two new qed.run params (doc, cell). Skipped as dispatched: T11 (blocked on R622), F9/T10 (desktop, sequenced separately).")
//! @yah:next("PATHSPEC, whole relay: oss/qed/crates/qed/src/{types,runner,staleness,doc_source,lib,config,transform,matrix,import}.rs crates/yah/rpc/src/lib.rs crates/yah/agent-tools/src/qed_tools.rs app/yah/cli/src/{camp.rs,qed.rs,mcp/tools.rs} app/yah/desktop/src/qed.rs .yah/schema/qed-pipeline.toml.schema.json .yah/docs/working/W296-executable-docs-notebook-cells.md")
//! @yah:next("FOR THE DESKTOP TICKETS (F9/T10) NOW UNBLOCKED: qed.cells returns {doc, param_fingerprint, cells[{cell_id, run_id, status, started_at, completed_at, error, outputs, input_hashes}]}; a cell absent from the result has NEVER run for that subject and renders unrun. Staleness is NOT on the wire — recompute it the way app/yah/cli/src/qed.rs::doc_cell_is_stale does (re-hash input_hashes' keys, compare via yah_qed::input_freshness). T10's subject selector wants ParamDef::options_from per R717-S13 Q3; that field is NOT built yet and T10 owns it. Cell inventory (ids, assert-vs-show, host) comes from parsing the .md with yah_qed::parse_doc, not from the RPC — the daemon owns run state, the tree owns what the doc says.")
//!
//! @yah:ticket(R717-S13, "Settle W296's four open questions: cell cwd, badge expiry, enumerable subjects, per-subject concurrency")
//! @yah:status(review)
//! @yah:assignee(agent:bundle-anthropic-glimmerstone)
//! @yah:at(2026-08-08T21:20:01Z)
//! @yah:kind(spike)
//! @yah:phase(P2)
//! @yah:parent(R717)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @yah:next("Q1 cwd — do cells share a working directory across a run? A QED run positions a workspace once; a runbook's cells mostly want the camp root; host= cells run somewhere else entirely. W296 leans 'yes, pipeline cwd semantics unchanged' but says the mental model must be STATED rather than discovered. Blocks nothing, but R717-T5 needs the answer written down.")
//! @yah:next("Q2 expiry — what invalidates a green badge by time? A systemctl is-active docker that passed six weeks ago is not evidence about today. W296 leans on rendering AGE prominently plus staleness, and explicitly does NOT want a third expiry axis (per-cell ttl=). Confirm or overturn; do not add ttl= by default.")
//! @yah:next("Q3 enumerable subjects — W257's subject is a machine name that already exists as a file in .yah/infra/machines/. A selector that enumerates those beats free-text params but couples the notebook to one domain. W296's lean: a param KIND that names an enumerable source. Affects R717-T10's selector.")
//! @yah:next("Q4 concurrency — two operators bringing up two boxes from W257 at once is the EXPECTED case, and the param fingerprint already handles the state. Open: should concurrency_key default to per-subject rather than per-pipeline for doc runs? Note R622 decided a parked step RELEASES its concurrency_key, which bears on this.")
//! @yah:next("Deliverable is an edit to W296 turning these four into decisions, not a separate doc. Tier: Wizard — four coupled design calls, one of which (Q3) shapes a public param vocabulary.")
//! @yah:assumes("W296 leans a particular way on Q1, Q2 and Q4; those leans are unvalidated and the spike may overturn any of them.")
//! @yah:handoff("SETTLED. W296's Open questions section is replaced by a Settled questions (R717-S13, 2026-08-07) section — four decisions, in the doc, not a separate one. Q1/Q2 confirm their leans, Q3 refines its lean, Q4 overturns its own premise.")
//! @yah:handoff("Q1 cwd DECIDED: yes, pipeline cwd semantics unchanged; no per-cell workspace, no cell-to-cell cwd inheritance. Rider (a): a doc-sourced pipeline defaults to workspace = live, overridable only from the notebook= fence — WorkspaceMode's default Checkout bails on a dirty tree, which makes a runbook unrunnable on this shared tree, and is wrong semantics anyway (a runbook asserts about the operator's actual tree and the live world, not bytes at another ref). Rider (b): a host= cell has TWO cwds and QED owns one. Local cwd is the positioned root (where ssh is invoked); remote cwd is the SSH login default, because the machine TOML records a connection, not a remote workspace. A host= cell needing a remote directory writes an explicit cd in its body. Do NOT add remote_cwd= — it would be a second positioning mechanism against a tree QED neither owns nor versions. R717-T5 carries this rule as a module doc comment.")
//! @yah:handoff("Q2 expiry DECIDED: confirmed, NO ttl=. Exactly two axes and neither is a RunStatus — staleness (computed at read time: blake3(inputs now) != recorded, or the cell body text changed since the run) and age (rendered, never a verdict). Why ttl is refused: staleness and age are facts about the tree and the clock; a ttl is a prediction about the world's volatility written at authoring time by someone who cannot know it, and a red carrying no evidence is exactly what trains readers to stop reading reds. The pressure goes to age as primary badge text (passed - 6w ago), a one-click re-run, and — where a doc genuinely needs decay — an assert cell that checks recency itself, which keeps the rule executable and visible in the prose.")
//! @yah:handoff("Q3 enumerable subjects DECIDED: enumerate by PATH GLOB, not by domain kind. ParamDef gains options_from: Option<String>, a camp-relative glob whose matches' file stems become the param's options at read time (node = { required = true, options_from = .yah/infra/machines/*.toml }). This is the lean with the domain coupling removed: the coupling only bites if the vocabulary names the domain (kind = machine would put fleet concepts in QED's param schema forever); a path glob names a DIRECTORY CONVENTION, which a notebook with host= cells already depends on and any other domain reuses unchanged. Three details are the actual decision: it composes with the existing options path (resolution fills options, so resolve_params and ParamError::NotInOptions validate unchanged and the desktop gets the dropdown it already renders); resolution is at READ time not load time, so a newly-added machine appears without editing the doc; and a glob matching nothing is an AUTHORING ERROR, not a silent degrade to free text, since a typo'd glob must not be indistinguishable from a correct one. Implementing ticket is R717-T10; R717-T7 may consume it to validate --subject.")
//! @yah:handoff("Q4 concurrency DECIDED, and the question's PREMISE WAS STALE. It asks per-subject rather than per-pipeline; it no longer defaults to per-pipeline. R719-F1 inverted DEFAULT_CONCURRENCY_KEY to @camp, camp-GLOBAL (types.rs:517) — strictly worse for this case than the default the question was written against, since two operators bringing up two different boxes would now serialize against each other AND against every other unkeyed recipe in the camp. Decision: a doc-sourced pipeline defaults concurrency_key to @doc:<doc_rel_path>#<param_fingerprint>. Two subjects run concurrently; two runs of the same subject serialize. An explicit key in the notebook= fence still wins. This is not a carve-out from R719-F1, it is that rule applied: the camp-global default exists because an unkeyed BUILD recipe's contended resource is the camp tree; a doc cell's contended resource is the remote SUBJECT, and the param fingerprint names it exactly — so the doc source is supplying the correct key, not forgetting one.")
//! @yah:gotcha("Q4's known hole, deliberately NOT closed: a parked manual step releases its concurrency key and reacquires on resume (types.rs:2154, R622). So while an operator stands at the box answering a BIOS cell the per-subject key is free, and a second run of the SAME subject can start and interleave. That is a two-operator collision on one physical machine; a lock in QED cannot prevent it and pretending otherwise is worse than admitting it. Do NOT add a park-holds-the-key exception — R622 released it on purpose so a wizard parked overnight cannot hold a camp key hostage. Mitigation that does exist: both runs record the same param_fingerprint, so the collision is visible in cell history afterwards.")
//! @yah:gotcha("CORRECTION found while settling, fixed in W296 in place: hostkey_fingerprint is NOT a top-level key in a machine TOML — it lives under [registration] (.yah/infra/machines/us-west-003.toml, stamped at TOFU time by R707-T1). So W296's cell-inventory bind row now reads path = registration.hostkey_fingerprint, and its Verification check 3 grep was anchored ^hostkey_fingerprint and matched nothing; unanchored now. Anyone writing the step-7 bind for a doc cell needs the dotted path.")
//! @yah:handoff("Tree anchor at handoff: 85801e7f6b76b369c0c8ecd2e5c7874990cd9286 — the shared tree as I left it. Diff against it (`git diff 85801e7f6b76b369c0c8ecd2e5c7874990cd9286..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:next("Deliverable is the W296 edit, done. Downstream consumers: R717-T5 writes the Q1 host= cwd rule as a module doc comment; R717-F4 defaults the synthesized Pipeline to workspace=live + concurrency_key=@doc:<doc>#<fp> per Q1/Q4; R717-T10 implements ParamDef::options_from per Q3; nobody adds ttl= per Q2.")
//! @yah:verify("The four Open questions no longer exist in .yah/docs/working/W296-executable-docs-notebook-cells.md; the section is titled Settled questions (R717-S13, 2026-08-07) and each of Q1-Q4 states a decision plus the reason it beat its alternative.")
//! @yah:handoff("Reconciliation audit: spike fully settled, no residual. W296 'Settled questions (R717-S13, 2026-08-07)' section confirmed landed verbatim in 871fde1c by content — doc-only ticket, no test suite applicable.")
//!
//! @yah:ticket(R577-B5, "desktop-release lost its two Linux matrix rows; the R577-F2 assertion still expects three")
//! @yah:at(2026-08-12T03:16:57Z)
//! @yah:status(open)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R577)
//! @yah:severity(low)
//! @yah:gotcha("Filed 2026-08-11 from R719-F7, from the outside - I did not touch either file. yah-qed tests::desktop_release_matrix_routes_each_row_to_its_own_platform fails: it asserts three matrix rows over the REAL checked-in recipe, and .yah/qed/desktop-release.toml now declares one. The row removal is UNCOMMITTED and carries a long in-file rationale (both Linux rows published nothing in two waves, publish-desktop.sh exits 0 off Darwin, and the matrix parents AND-of-rows status reported a good 0.8.22 release as failed). So the recipe change looks deliberate and the test is the stale half.")
//! @yah:next("Decide, then make the two agree. Either the one-row matrix is the intended shape (drop the two Linux entries from the assertion at oss/qed/crates/qed/src/lib.rs, keeping the Windows-stays-absent rationale), or the rows come back WITH the publish leg that consumes them (a Linux branch in publish-desktop.sh writing appimage/deb into the manifest). It is a product call about what desktop-release ships, which is why R719-F7 did not just edit the assertion green.")
//! @yah:gotcha("THE SYMPTOM CHANGED, 2026-09-08 — the row-count assertion is no longer what fails, so do not go looking for it. `tests::desktop_release_matrix_routes_each_row_to_its_own_platform` now panics at oss/qed/crates/qed/src/lib.rs:602 with `desktop-release pipeline loads: NotFound(\"desktop-release\")`. Cause: `.yah/qed/desktop-release.toml` was DELETED in commit 9e454f03 and the recipe now lives at `.yah/qed/yah-desktop-release.toml`; the test still calls `.load(\"desktop-release\")`. Confirmed pre-existing and unrelated to the caller who found it (R560-B12, which touched only the qed runner's produced-artifact path): `git cat-file -e HEAD:.yah/qed/desktop-release.toml` fails. THIS DOES NOT RETIRE THE PRODUCT CALL in the next-step below — it stacks on top of it. Renaming the load target green would make the test load a ONE-row recipe and then assert three rows, i.e. it would land you back at exactly the mismatch this ticket was filed for. Fix the name and the row question together, or neither.")

pub mod artifact_local;
pub mod artifact_retrieval;
pub mod build_context;
pub mod config;
pub mod dag;
pub mod doc_source;
pub mod eject;
pub mod events;
pub mod export;
pub mod globals;
pub mod image_overlay;
pub mod images;
pub mod import;
pub mod matrix;
pub mod native;
pub mod nativecross;
pub mod participants;
pub mod peers;
pub mod placement_gate;
pub mod platform;
pub mod ports;
pub mod preflight;
pub mod provider;
pub mod publish;
pub mod registries;
pub mod runner;
pub mod secrets_bridge;
pub mod staleness;
pub mod toolchain;
pub mod transform;
pub mod types;

/// The wait-for probe/backoff primitive itself lives in the standalone
/// `pleasehold` crate (split out so a caller that only wants "wait for this
/// thing to materialize" doesn't have to pull in qed's scheduler stack).
pub use pleasehold as waitfor;

pub use config::{ConfigError, GhaWorkflowEntry, LoaderSubPipelineResolver, PipelineLoader};
pub use dag::{DagError, DEFAULT_MAX_PARALLEL};
pub use events::{OutputStream, QedEvent};
pub use images::{CatalogEntry, CatalogError, CatalogManifest, ProduceTarget};
pub use eject::{
    eject, freshness as eject_freshness, generated_header, validate_ejected, EjectFreshness,
    GeneratedHeader, ValidateError as EjectValidateError,
};
pub use export::{export_pipeline, Degradation, ExportReport};
pub use globals::{CampGlobals, ReleaseGlobals, TagHygiene};
pub use import::{content_hash, expand_import, ImportExpansion, ImportFreshness};
pub use native::{
    native_tarball_output_path, pack_native_tarball, resolve_signer, tarball_stem, CosignSigner,
    LoggingSigner, NativeTarballManifest, SignedBlob, SigningIdentity, SigstoreSigner,
    ENV_COSIGN_IDENTITY_TOKEN, ENV_COSIGN_KEY,
};
pub use nativecross::{
    is_native_cross_target, plan_native_cross, rewrite_build_argv, select_cross_tool, CrossTool,
    CrossToolUnavailable, NativeCrossPlan, ToolAvailability,
};
pub use peers::{PeerConfig, PeerConfigError, PeerEntry};
pub use placement_gate::{evaluate as evaluate_placement_gate, GateOutcome, RunnerEnv};
pub use platform::{
    arch_of, detect_host_triple, gha_runner_arch, host_native_crossable, preflight_line,
    resolve as resolve_platform, resolve_placement, Platform, PlatformSpec, Resolution,
};
pub use ports::{
    workflow_ports, PortError, PortInput, PortOutput, PortSecret, WorkflowPorts,
};
pub use preflight::{
    audit_workspace, check_dep_list, check_musl_compatibility, render_markdown, AuditRow,
    MuslPreflightError, WorkspaceAudit, KNOWN_GLIBC_ONLY_CRATES,
};
pub use provider::{
    EventLogConfig, EventLogProvider, MapSecrets, NotarizeProvider, ProviderContext,
    ProviderRegistry, ProviderReport, ReleaseProvider, SecretSource, EVENT_LOG_PROVIDER,
};
pub use publish::{
    index_key, merge_index, resolve_release_version, stage_release, ChannelManifest, IndexTriple,
    IndexUpdate, IndexVersion, LoggingReleasePublisher, PublishRequest,
    PublishingOutcomeDispatcher, ReleaseIndex, ReleasePublisher, StageReport,
};
/// Re-exported so daemon/UI glue can match on workflow step shapes without
/// taking a direct `qed-gha` dep edge — the catalog converter in
/// `camp.rs::qed_pipelines_handler` walks these to flatten jobs/steps.
pub use yah_qed_gha;
pub use registries::{extract_registry_host, RegistryConfig, RegistryConfigError, RegistryEntry};
pub use runner::{
    pipeline_has_node_bound_participant, pipeline_is_fully_offloaded, pipeline_needs_offload,
    sub_pipeline_admission_gap,
    AdmissionControl, AdmissionGap, AdmissionLane,
    ChildEventFactory, ChildRunInfo,
    LoggingOutcomeDispatcher,
    ManualAnswer, ManualGate, ManualParkHandle, ManualParkRequest, OutcomeDispatcher,
    PipelineRunner, RunWhere, RunnerError,
};
/// Re-exported so daemon glue (camp.rs boot-reconcile, R603-T4) can parse a
/// persisted bare-uuid `task_run_id` back into the workload identity that
/// [`PipelineRunner::resume_terminal_publish_for_remote_step`] takes, without a
/// direct `observation` dep edge.
pub use observation::ForgeId;
pub use velveteen::TaskRuntime;
pub use velveteen_exec::{
    RecipeError, RecipeLocation, RecipePlacement, RecipeStep, TransformRecipe,
    TransformRecipeLoader,
};
pub use doc_source::{
    parse_doc, DocCell, DocSource, DocSourceError, ManualCell, NotebookConfig,
};
pub use staleness::{hash_declared_inputs, input_freshness, InputFreshness, ABSENT_INPUT};
pub use toolchain::{
    detect_host_versions, effective_pins, resolve_pin, version_satisfies, PinResolution,
    PreflightEntry, Tool, ToolchainPreflight, ToolchainSpec,
};
pub use transform::{
    transform_workflow, transform_workflow_src, FlagKind, FlagSeverity, TransformReport,
    TransformedStep,
};
pub use types::{
    new_run_id, param_fingerprint, sub_pipeline_ref_token, validate_sub_pipeline_graph, CellRef,
    GhaWorkflowConfig,
    ImportConfig, JobRow, ManifestStitchConfig, ManualConfig, Outcome, OutputDecl, Pipeline,
    Placement,
    PipelineClass,
    ProducedArtifact, QedRunId, QedRunLaunch, QedRunMeta, QedStep, RunStatus, StepActivation,
    StepKind,
    StepStatus, StepValidationError, SubPipelineCollect, SubPipelineConfig, SubPipelineError,
    SubPipelineRef, SubPipelineResolver, Trigger, WaitForConfig, WorkspaceMode,
    DEFAULT_CONCURRENCY_KEY, MAX_SUB_PIPELINE_DEPTH, PARALLEL_CONCURRENCY_KEY,
};

/// Returns the argv that an external scheduler (e.g. almanac) should submit as a TaskSpec
/// to dispatch a named pipeline.
///
/// Almanac treats qed as a subprocess and never depends on the qed crate directly.
/// This function is the stable contract: callers construct
/// `TaskSpec { argv: qed::almanac_dispatch_argv("check"), .. }`.
///
/// Params are appended as `--<key>=<value>` flags, matching `yah qed run` CLI behaviour.
pub fn almanac_dispatch_argv(
    pipeline: &str,
    params: &std::collections::HashMap<String, String>,
) -> Vec<String> {
    let mut argv = vec![
        "yah".to_string(),
        "qed".to_string(),
        "run".to_string(),
        pipeline.to_string(),
    ];
    for (k, v) in params {
        argv.push(format!("--{}={}", k, v));
    }
    argv
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Locate the yah monorepo's `.yah/qed` pipeline dir by walking up from the
    /// crate manifest. In-tree this crate is nested at `oss/qed/crates/qed`, so
    /// the old fixed 3-`parent()`-hop math (written for `crates/yah/qed`) now
    /// overshoots; ascend until the marker is found. When consumed as the
    /// standalone github.com/yah-ai/qed export mirror there is no yah `.yah/qed`,
    /// so these workspace-coupled tests skip rather than fail.
    ///
    /// R857: the marker is "the directory holds at least one pipeline", NOT a
    /// specific filename. It used to probe for `release.toml`, and renaming that
    /// file to `yah-release.toml` made this return `None` — so every caller took
    /// the skip arm and passed while measuring nothing. A skip-on-miss helper
    /// keyed to one filename turns any rename into a silent green, which is the
    /// exact failure these composite tests exist to catch.
    fn find_qed_dir() -> Option<std::path::PathBuf> {
        let mut dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let candidate = dir.join(".yah").join("qed");
            let has_pipeline = std::fs::read_dir(&candidate).is_ok_and(|mut entries| {
                entries.any(|e| {
                    e.is_ok_and(|e| e.path().extension().is_some_and(|ext| ext == "toml"))
                })
            });
            if has_pipeline {
                return Some(candidate);
            }
            if !dir.pop() {
                return None;
            }
        }
    }

    // R467-cleanup: the four per-builtin tests (check / smoke / release-build /
    // desktop-release) were deleted alongside the `builtins.rs` module. Each
    // pipeline is now an ordinary `.yah/qed/P00*-<name>.toml` file; loader
    // round-trip coverage lives in `config::tests`, and the composite-graph
    // test below exercises the same `load_and_validate_graph` surface against
    // the workspace `.yah/qed/`.

    /// R488-F6: `.yah/qed/yah-release.toml` parses, the SubPipeline graph
    /// (GhaWorkflow child + by-name desktop-release child) validates without
    /// cycles or depth violations, and a single terminal Outcome::Publish is
    /// declared at the parent so one revalidate POST fires after both
    /// children finish. (R499-T2: pipeline name was `full-release` until
    /// the yubaba-release wrapper collapsed into this file and the canonical
    /// name shifted to `release`.)
    #[test]
    fn test_release_composite_pipeline() {
        // Resolve workspace `.yah/qed` by walking up from the crate manifest so
        // the test runs regardless of cwd (and of nesting depth under oss/).
        let Some(qed_dir) = find_qed_dir() else {
            eprintln!("skip: yah .yah/qed pipelines not present (standalone export)");
            return;
        };
        let loader = PipelineLoader::new(qed_dir);
        let pipeline = loader
            .load_and_validate_graph("yah-release")
            .expect("release pipeline loads + graph validates");
        assert_eq!(pipeline.name, "yah-release");
        assert_eq!(pipeline.steps.len(), 2, "two SubPipeline children");
        for step in &pipeline.steps {
            assert_eq!(step.kind, crate::types::StepKind::SubPipeline);
            let cfg = step.sub_pipeline.as_ref().expect("sub_pipeline block");
            assert!(cfg.propagate.produces, "child produces roll up to parent");
        }
        let pubs: Vec<_> = pipeline
            .on_success
            .iter()
            .filter(|o| matches!(o, crate::Outcome::Publish { .. }))
            .collect();
        assert_eq!(pubs.len(), 1, "exactly one terminal Outcome::Publish");
    }

    /// `peer-binaries` (R494-T3; renamed from `peer-release` in R707) — yah
    /// orchestrating a cross-build wave over its
    /// external/ peers, then itself, under one terminal publish. Loads
    /// against the real workspace `.yah/qed/peers.toml` registry so a
    /// missing or misspelled peer key surfaces here at parse time. Active
    /// children today (publish order): peer(yubaba) + peer(qed) +
    /// peer(mesofact), then path(release.toml) for yah itself. cheers is
    /// registered but its `release-build` pipeline doesn't exist yet, so that
    /// SubPipeline step stays commented out. (R499-T1: yah step retargeted
    /// from builtin(release-build) → path after the old release-build card was
    /// retired.)
    #[test]
    fn test_peer_binaries_composite_pipeline() {
        let Some(qed_dir) = find_qed_dir() else {
            eprintln!("skip: yah .yah/qed pipelines not present (standalone export)");
            return;
        };
        let loader = PipelineLoader::new(qed_dir);
        let pipeline = loader
            .load_and_validate_graph("oss-binaries")
            .expect("oss-binaries pipeline loads + graph validates");
        assert_eq!(pipeline.name, "oss-binaries");
        assert_eq!(
            pipeline.steps.len(),
            4,
            "active SubPipeline children: yubaba + qed + mesofact peers, then yah path",
        );

        // Collect the peers and the yah path target rather than asserting a
        // fixed pair, so adding/removing a peer is a one-line list edit here.
        let mut peers: Vec<String> = Vec::new();
        let mut yah_path: Option<String> = None;
        for step in &pipeline.steps {
            assert_eq!(step.kind, crate::types::StepKind::SubPipeline);
            let cfg = step.sub_pipeline.as_ref().expect("sub_pipeline block");
            assert!(cfg.propagate.produces, "child produces roll up to parent");
            match &cfg.target {
                crate::SubPipelineRef::Peer { camp, pipeline } => {
                    assert_eq!(pipeline, "release-build");
                    peers.push(camp.clone());
                }
                crate::SubPipelineRef::Path(p) => {
                    yah_path = Some(p.to_str().unwrap().to_string());
                }
                other => panic!("unexpected SubPipelineRef in peer-binaries: {other:?}"),
            }
        }
        assert_eq!(
            peers,
            vec!["yubaba", "qed", "mesofact"],
            "peer release-build children in publish order",
        );
        assert_eq!(
            yah_path.as_deref(),
            Some(".yah/qed/yah-release.toml"),
            "yah self-release path step present",
        );

        let pubs: Vec<_> = pipeline
            .on_success
            .iter()
            .filter(|o| matches!(o, crate::Outcome::Publish { .. }))
            .collect();
        assert_eq!(pubs.len(), 1, "exactly one terminal Outcome::Publish");
    }

    /// R577-F2 — `desktop-release`'s pipeline-level matrix must dispatch each
    /// row to a machine of that row's *platform*, and the whole point of the
    /// darwin row is that it lands on the fleet's only Mac (`us-west-015`,
    /// tagged `os:darwin` by R631) rather than on a Pi5 that cannot emit
    /// Mach-O.
    ///
    /// This asserts the routing over the real checked-in recipe rather than
    /// over a fixture, because the thing that can regress is the recipe: drop
    /// `native = true` from a step, or add a row for a platform the fleet has
    /// no node for, and the placement silently changes. Both host directions
    /// are pinned — an arm64 Mac coordinator (this camp) and an arm64 Linux
    /// one — since arch alone cannot tell those two apart and that blindness
    /// is exactly what this ticket fixed in `resolve_placement`.
    #[test]
    fn desktop_release_matrix_routes_each_row_to_its_own_platform() {
        let Some(qed_dir) = find_qed_dir() else {
            eprintln!("skip: yah .yah/qed pipelines not present (standalone export)");
            return;
        };
        let pipeline = PipelineLoader::new(qed_dir)
            .load("desktop-release")
            .expect("desktop-release pipeline loads");

        const ARM_MAC: &str = "aarch64-apple-darwin";
        const ARM_LINUX: &str = "aarch64-unknown-linux-gnu";

        let jobs = crate::matrix::plan(&pipeline);
        let mut rows: Vec<String> = Vec::new();
        for job in &jobs {
            // Every step of a row is pinned to that row's target (the matrix is
            // at pipeline level precisely so no step escapes to the
            // coordinator), so the row's target is well-defined.
            let targets: std::collections::BTreeSet<Option<String>> = job
                .pipeline
                .steps
                .iter()
                .map(|s| s.platform.as_ref().and_then(|p| p.target.clone()))
                .collect();
            assert_eq!(
                targets.len(),
                1,
                "row {} has steps on mixed targets: {targets:?}",
                job.label()
            );
            let target = targets
                .into_iter()
                .next()
                .flatten()
                .unwrap_or_else(|| panic!("row {} declares no platform.target", job.label()));
            for step in &job.pipeline.steps {
                let spec = step.platform.as_ref().expect("every step declares platform");
                assert!(
                    spec.native,
                    "step `{}` of row {} dropped native=true — it would cross-compile \
                     or emulate instead of landing on real silicon",
                    step.name,
                    job.label(),
                );
                assert!(
                    spec.container_platform.is_none(),
                    "step `{}` of row {} declares a container_platform; desktop bundling \
                     needs the host userland, and a container would exempt it from the \
                     OS half of placement",
                    step.name,
                    job.label(),
                );
            }

            let on_mac = crate::platform::resolve_placement(ARM_MAC, Some(&target), None, true);
            let on_linux = crate::platform::resolve_placement(ARM_LINUX, Some(&target), None, true);
            match crate::platform::os_tag_of(&target) {
                "darwin" => {
                    assert_eq!(
                        on_mac,
                        crate::platform::Resolution::NativeCross,
                        "a darwin row on a Mac coordinator builds right here",
                    );
                    assert_eq!(
                        on_linux,
                        crate::platform::Resolution::Offload {
                            target: target.clone()
                        },
                        "a darwin row on a Linux coordinator MUST offload — this is the \
                         us-west-015 leg (R577)",
                    );
                    assert!(
                        crate::platform::build_worker_mesh_tags(
                            crate::platform::arch_of(&target),
                            "darwin",
                        )
                        .contains(&"os:darwin".to_string()),
                        "the darwin offload must request os:darwin so it cannot tag-match \
                         the Linux Pi5s (R631)",
                    );
                }
                "linux" => {
                    assert_eq!(
                        on_mac,
                        crate::platform::Resolution::Offload {
                            target: target.clone()
                        },
                        "a Linux row on this camp's arm64 Mac MUST offload — same arch is \
                         not the same platform, and macOS cannot produce an AppImage/deb",
                    );
                }
                other => panic!("row {} targets unexpected OS `{other}`", job.label()),
            }
            rows.push(target);
        }

        rows.sort();
        assert_eq!(
            rows,
            vec!["aarch64-apple-darwin"],
            "the W235 fan-out rows. Both Linux rows were removed in 497a8a6b \
             (2026-08-12) for the reason recorded in desktop-release.toml's own \
             matrix comment: nothing downstream consumes them (publish-desktop.sh \
             exits 0 off Darwin), so a green Linux row and a failed one produce \
             the same empty artifact set — while the AND-of-rows parent status \
             reported a good 0.8.22 release as failed. Windows likewise stays \
             absent until the fleet has a node. Restoring a row means restoring \
             the publish leg that consumes it, and updating this list with it.",
        );
    }

    #[test]
    fn almanac_dispatch_argv_no_params() {
        let argv = almanac_dispatch_argv("check", &HashMap::new());
        assert_eq!(argv, vec!["yah", "qed", "run", "check"]);
    }

    #[test]
    fn almanac_dispatch_argv_with_params() {
        let mut params = HashMap::new();
        params.insert("provider".to_string(), "groq".to_string());
        let argv = almanac_dispatch_argv("smoke", &params);
        assert!(argv.starts_with(&[
            "yah".to_string(),
            "qed".to_string(),
            "run".to_string(),
            "smoke".to_string()
        ]));
        assert!(argv.contains(&"--provider=groq".to_string()));
    }
}
