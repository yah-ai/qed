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

pub mod artifact_local;
pub mod artifact_retrieval;
pub mod config;
pub mod eject;
pub mod events;
pub mod export;
pub mod image_overlay;
pub mod images;
pub mod import;
pub mod matrix;
pub mod native;
pub mod nativecross;
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
pub mod toolchain;
pub mod transform;
pub mod types;
pub mod waitfor;

pub use config::{ConfigError, GhaWorkflowEntry, LoaderSubPipelineResolver, PipelineLoader};
pub use events::{OutputStream, QedEvent};
pub use images::{CatalogEntry, CatalogError, CatalogManifest, ProduceTarget};
pub use eject::{
    eject, freshness as eject_freshness, validate_ejected, EjectFreshness, GeneratedHeader,
    ValidateError as EjectValidateError,
};
pub use export::{export_pipeline, Degradation, ExportReport};
pub use import::{content_hash, expand_import, ImportExpansion, ImportFreshness};
pub use native::{
    native_tarball_output_path, pack_native_tarball, tarball_stem, CosignSigner, LoggingSigner,
    NativeTarballManifest, SignedBlob, SigstoreSigner,
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
    resolve_release_version, stage_release, ChannelManifest, LoggingReleasePublisher,
    PublishRequest, PublishingOutcomeDispatcher, ReleasePublisher, StageReport,
};
/// Re-exported so daemon/UI glue can match on workflow step shapes without
/// taking a direct `qed-gha` dep edge — the catalog converter in
/// `camp.rs::qed_pipelines_handler` walks these to flatten jobs/steps.
pub use yah_qed_gha;
pub use registries::{extract_registry_host, RegistryConfig, RegistryConfigError, RegistryEntry};
pub use runner::{
    pipeline_needs_offload, LoggingOutcomeDispatcher, OutcomeDispatcher, PipelineRunner, RunWhere,
    RunnerError,
};
/// Re-exported so daemon glue (camp.rs boot-reconcile, R603-T4) can parse a
/// persisted bare-uuid `task_run_id` back into the workload identity that
/// [`PipelineRunner::resume_terminal_publish_for_remote_step`] takes, without a
/// direct `observation` dep edge.
pub use observation::ForgeId;
pub use velveteen::TaskRuntime;
pub use velveteen::{
    RecipeError, RecipeLocation, RecipePlacement, RecipeStep, TransformRecipe,
    TransformRecipeLoader,
};
pub use toolchain::{
    detect_host_versions, effective_pins, resolve_pin, version_satisfies, PinResolution,
    PreflightEntry, Tool, ToolchainPreflight, ToolchainSpec,
};
pub use transform::{
    transform_workflow, transform_workflow_src, FlagKind, FlagSeverity, TransformReport,
    TransformedStep,
};
pub use types::{
    new_run_id, sub_pipeline_ref_token, validate_sub_pipeline_graph, GhaWorkflowConfig,
    ImportConfig, JobRow, ManifestStitchConfig, Outcome, OutputDecl, Pipeline, Placement,
    ProducedArtifact, QedRunId, QedRunMeta, QedStep, RunStatus, StepActivation, StepKind,
    StepStatus, StepValidationError, SubPipelineCollect, SubPipelineConfig, SubPipelineError,
    SubPipelineRef, SubPipelineResolver, Trigger, WaitForConfig, MAX_SUB_PIPELINE_DEPTH,
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
    fn find_qed_dir() -> Option<std::path::PathBuf> {
        let mut dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        loop {
            let candidate = dir.join(".yah").join("qed");
            if candidate.join("P013-release.toml").is_file() {
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

    /// R488-F6: `.yah/qed/P013-release.toml` parses, the SubPipeline graph
    /// (GhaWorkflow child + by-name desktop-release child) validates without
    /// cycles or depth violations, and a single terminal Outcome::Publish is
    /// declared at the parent so one revalidate POST fires after both
    /// children finish. (R499-T2: pipeline name was `full-release` until
    /// P007-yubaba-release.toml collapsed into this file and the canonical
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
            .load_and_validate_graph("release")
            .expect("release pipeline loads + graph validates");
        assert_eq!(pipeline.name, "release");
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

    /// `peer-release` (R494-T3) — yah orchestrating a republish wave over its
    /// external/ peers, then itself, under one terminal publish. Loads
    /// against the real workspace `.yah/qed/peers.toml` registry so a
    /// missing or misspelled peer key surfaces here at parse time. Active
    /// children today (publish order): peer(yubaba) + peer(qed) +
    /// peer(mesofact), then path(P013-release.toml) for yah itself. cheers is
    /// registered but its `release-build` pipeline doesn't exist yet, so that
    /// SubPipeline step stays commented out. (R499-T1: yah step retargeted
    /// from builtin(release-build) → path after P003-release-build.toml was
    /// retired.)
    #[test]
    fn test_peer_release_composite_pipeline() {
        let Some(qed_dir) = find_qed_dir() else {
            eprintln!("skip: yah .yah/qed pipelines not present (standalone export)");
            return;
        };
        let loader = PipelineLoader::new(qed_dir);
        let pipeline = loader
            .load_and_validate_graph("peer-release")
            .expect("peer-release pipeline loads + graph validates");
        assert_eq!(pipeline.name, "peer-release");
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
                other => panic!("unexpected SubPipelineRef in peer-release: {other:?}"),
            }
        }
        assert_eq!(
            peers,
            vec!["yubaba", "qed", "mesofact"],
            "peer release-build children in publish order",
        );
        assert_eq!(
            yah_path.as_deref(),
            Some(".yah/qed/P013-release.toml"),
            "yah self-release path step present",
        );

        let pubs: Vec<_> = pipeline
            .on_success
            .iter()
            .filter(|o| matches!(o, crate::Outcome::Publish { .. }))
            .collect();
        assert_eq!(pubs.len(), 1, "exactly one terminal Outcome::Publish");
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
