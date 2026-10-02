//! QED — CI scheduler: pipelines, step DAGs, triggers, and pass/fail gating over task execution
//!
//! QED is yah's CI layer. It schedules named pipelines, gates on results, and chains into
//! yubaba (deployment) and almanac (data scheduler). Unlike task (execution primitive),
//! qed handles definition, ordering, gating, and triggering.
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
//! @arch:see(.yah/docs/working/W154-yubaba-dual-runtime.md)
//! @arch:see(.yah/docs/working/W170-qed-recipe-discipline.md)
//! @arch:see(.yah/docs/working/W164-derived-static-assets.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W200-qed-gha-action-overrides.md)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
//! @arch:see(.yah/docs/working/W296-executable-docs-notebook-cells.md)
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
pub mod buildcap;
pub mod config;
pub mod dag;
pub mod doc_source;
pub mod eject;
pub mod events;
pub mod export;
pub mod fleet_portability;
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
pub use fleet_portability::{
    build_tool_head, camp_tree_reference, fleet_clause, fleet_portable,
    gaps as fleet_portability_gaps, is_dispatchable_kind, PortabilityGap,
};
pub use globals::{CampGlobals, ReleaseGlobals, TagHygiene};
pub use import::{content_hash, expand_import, ImportExpansion, ImportFreshness};
pub use native::{
    native_tarball_output_path, pack_native_tarball, resolve_signer, tarball_stem, CosignSigner,
    LoggingSigner, NativeTarballManifest, SignedBlob, SigningIdentity, SigstoreSigner,
    ENV_COSIGN_IDENTITY_TOKEN, ENV_COSIGN_KEY,
};
pub use buildcap::{
    plan as plan_build_capacity, probe_and_plan as probe_build_capacity, BuildCapacity, Capacity,
    HostProbe, InstallCommand, Requirement, Shell,
};
pub use nativecross::{
    is_native_cross_target, plan_native_cross, rewrite_build_argv, select_cross_tool, CrossTool,
    CrossToolUnavailable, NativeCrossPlan, ToolAvailability,
};
pub use peers::{PeerConfig, PeerConfigError, PeerEntry};
pub use placement_gate::{evaluate as evaluate_placement_gate, GateOutcome, RunnerEnv};
pub use platform::{
    arch_of, capability_demotion, derive_placement, detect_host_triple, gha_runner_arch,
    host_native_crossable, preflight_line, resolve as resolve_platform, resolve_placement,
    Platform, PlatformSpec, Resolution,
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
    index_key, index_legacy_bare_digest_paths, merge_index, normalize_index,
    resolve_release_version, stage_release, ChannelManifest, IndexTriple, IndexUpdate, IndexVersion,
    LoggingReleasePublisher, PublishRequest, PublishingOutcomeDispatcher, ReleaseIndex,
    ReleasePublisher, StageReport, LEGACY_BARE_DIGEST_KEYS,
};
/// Re-exported so daemon/UI glue can match on workflow step shapes without
/// taking a direct `qed-gha` dep edge — the catalog converter in
/// `camp.rs::qed_pipelines_handler` walks these to flatten jobs/steps.
pub use yah_qed_gha;
pub use registries::{extract_registry_host, RegistryConfig, RegistryConfigError, RegistryEntry};
pub use runner::{
    pipeline_capability_demotions, pipeline_has_node_bound_participant,
    pipeline_is_fully_offloaded, pipeline_needs_offload, sub_pipeline_admission_gap,
    AdmissionControl, AdmissionGap, AdmissionLane, NoopSubPipelineResolver,
    // R892-B1, drive-by: `camp.rs`'s `child_abort_hook` names this type and the
    // export was missing, so the whole `yah` lib failed to resolve. The type
    // itself is `runner::ChildAbortHook` and was already public there.
    ChildAbortHook, ChildEventFactory, ChildRunInfo,
    LoggingOutcomeDispatcher,
    ManualAnswer, ManualGate, ManualParkHandle, ManualParkRequest, OutcomeDispatcher,
    PipelineRunner, RunWhere, RunnerError, StepPortability,
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
    Environment,
    GhaWorkflowConfig,
    ImportConfig, JobRow, ManifestStitchConfig, ManualAudience, ManualConfig, Outcome, OutputDecl,
    Pipeline,
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

    // The composite-graph test exercises the `load_and_validate_graph`
    // surface against the workspace `.yah/qed/`.

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
        // R707: the filename stem IS the pipeline name, so this string has to
        // track the file. It named `desktop-release` after the camp's copy had
        // become `yah-desktop-release.toml`, which made the test a permanent
        // `NotFound` rather than the recipe check it is meant to be (found and
        // fixed under R906-F2; red at bae81d65).
        let pipeline = PipelineLoader::new(qed_dir)
            .load("yah-desktop-release")
            .expect("yah-desktop-release pipeline loads");

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

            // R555-B10: the *derivation*, deliberately — this asserts what the
            // recipe declares routes to, and must not depend on which cross
            // toolchains the machine running the test has installed.
            let on_mac = crate::platform::derive_placement(ARM_MAC, Some(&target), None, true);
            let on_linux = crate::platform::derive_placement(ARM_LINUX, Some(&target), None, true);
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
