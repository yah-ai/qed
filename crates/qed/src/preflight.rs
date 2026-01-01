//! Cross-compile preflight (R407-T3, W143, W154).
//!
//! Walks a cargo workspace package's transitive dep tree and flags crates
//! that link glibc-only system libraries — these can't build statically
//! against musl and would fail at link time during the cross build. The
//! gate runs *before* the build step so the pipeline can route to the
//! container fallback (`runtime = "container"`) with a clear actionable
//! error rather than dying mid-cross-build with a confusing linker error.
//!
//! This is the W154 musl-static gate: native-runtime deployment under
//! Constable assumes a static musl binary, and any glibc-only dep makes
//! that impossible. The gate is the producer-side guarantee that the
//! workload classification in W154 (warden → native, codec-heavy yah CLI
//! → container) actually reflects what's buildable.
//!
//! ## Surface
//!
//! - [`KNOWN_GLIBC_ONLY_CRATES`] — hand-maintained list of crate names that
//!   are known not to musl-static cleanly. Grows as real cross-build
//!   failures surface; entries carry a comment with the remediation.
//! - [`check_dep_list`] — pure: takes a sequence of crate names and returns
//!   the offending subset against the gate list. Used in tests and as the
//!   core of the metadata-driven check.
//! - [`check_musl_compatibility`] — shells `cargo metadata` for a workspace
//!   package and runs [`check_dep_list`] over its transitive deps.
//!
//! ## Integration
//!
//! A `kind = "musl-static-preflight"` pipeline step runs this gate against
//! `step.package` (a workspace member name) before the cross build that
//! depends on it. The runner integration lives in
//! [`crate::runner::PipelineRunner::execute_step_musl_static_preflight`].

use cargo_metadata::MetadataCommand;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Crates that link glibc-only system libraries / APIs and therefore can't
/// be built statically against musl. Hand-maintained — grows as real
/// failures surface during musl cross builds. Sorted alphabetically for
/// easy diff review when entries land.
pub const KNOWN_GLIBC_ONLY_CRATES: &[&str] = &[
    // CUDA driver bindings — Linux nvidia driver isn't musl-friendly.
    "cudarc",
    // System libdbus.
    "dbus",
    // hyper-tls pulls openssl-sys; switch to hyper-rustls.
    "hyper-tls",
    // glibc-specific NSS plugin host APIs.
    "libnss-mdns",
    // Links system udev (glibc-specific build).
    "libudev-sys",
    // NSS plugin glue.
    "nss-files",
    // OpenSSL via system libssl is dynamic-only on musl distros; use
    // rustls or set `vendored` feature on the `openssl` crate.
    "openssl-sys",
    // CUDA toolkit bindings.
    "rust-cuda",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MuslPreflightError {
    #[error("`cargo metadata` failed for `{package}`: {reason}")]
    CargoMetadata { package: String, reason: String },
    #[error("package `{package}` not found in the workspace metadata at {manifest_path}")]
    PackageNotFound { package: String, manifest_path: PathBuf },
    #[error(
        "package `{package}` cannot build musl-static — depends on glibc-only crate(s): {offenders:?}. \
         Route this build to the container fallback (set `runtime = \"container\"` on the upstream build step, \
         or use the `cross` toolchain) — or replace the offending dep with a musl-friendly alternative."
    )]
    NotMuslSafe { package: String, offenders: Vec<String> },
}

/// Pure dep-list gate. Used in tests directly and by
/// [`check_musl_compatibility`] after harvesting names from `cargo metadata`.
/// Returns `Ok(())` when no input matches the gate list.
pub fn check_dep_list<I, S>(package: &str, dep_names: I) -> Result<(), MuslPreflightError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut offenders: Vec<String> = dep_names
        .into_iter()
        .filter(|d| KNOWN_GLIBC_ONLY_CRATES.contains(&d.as_ref()))
        .map(|d| d.as_ref().to_string())
        .collect();
    offenders.sort();
    offenders.dedup();
    if offenders.is_empty() {
        Ok(())
    } else {
        Err(MuslPreflightError::NotMuslSafe {
            package: package.to_string(),
            offenders,
        })
    }
}

/// Shell `cargo metadata` for `workspace_root/Cargo.toml`, locate the
/// workspace member named `package`, and run [`check_dep_list`] against the
/// names of every crate in its resolved transitive closure.
///
/// The check is intentionally coarse: any appearance of a gated crate
/// anywhere in the resolved set fails the preflight, even if the crate is
/// only pulled by an optional feature that the target build won't enable.
/// False positives are recoverable (the operator can route to container
/// fallback or refine the gate list); silently shipping a non-musl-static
/// binary into the native runtime path is not.
pub fn check_musl_compatibility(
    workspace_root: &Path,
    package: &str,
) -> Result<(), MuslPreflightError> {
    let manifest_path = workspace_root.join("Cargo.toml");
    let metadata = MetadataCommand::new()
        .manifest_path(&manifest_path)
        .exec()
        .map_err(|e| MuslPreflightError::CargoMetadata {
            package: package.to_string(),
            reason: e.to_string(),
        })?;

    // Anchor on the named workspace member so the gate is workload-aware:
    // walking ALL workspace deps would over-trigger on crates that the
    // target binary doesn't pull.
    let root_id = metadata
        .workspace_members
        .iter()
        .find(|id| metadata[id].name == package)
        .cloned()
        .ok_or_else(|| MuslPreflightError::PackageNotFound {
            package: package.to_string(),
            manifest_path: manifest_path.clone(),
        })?;

    // Walk the resolved graph from root, collecting every reachable
    // crate name. `resolve.nodes` is a flat list keyed by PackageId; we
    // do a small BFS to stay on the named package's closure.
    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or_else(|| MuslPreflightError::CargoMetadata {
            package: package.to_string(),
            reason: "cargo metadata returned no resolved dep graph".into(),
        })?;
    let node_by_id: std::collections::HashMap<_, _> =
        resolve.nodes.iter().map(|n| (n.id.clone(), n)).collect();

    let mut seen = std::collections::HashSet::new();
    let mut frontier = vec![root_id.clone()];
    let mut reachable_names = Vec::new();
    while let Some(id) = frontier.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(node) = node_by_id.get(&id) {
            reachable_names.push(metadata[&id].name.clone());
            for dep in &node.dependencies {
                frontier.push(dep.clone());
            }
        }
    }

    check_dep_list(package, reachable_names.iter().map(String::as_str))
}

/// One row of a workspace musl-static audit (R407-T4): per workspace member,
/// did the gate pass and (when it didn't) what crates blocked it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRow {
    pub package: String,
    pub offenders: Vec<String>,
}

impl AuditRow {
    pub fn is_clean(&self) -> bool {
        self.offenders.is_empty()
    }
}

/// Full workspace audit: one [`AuditRow`] per resolvable workspace member,
/// ordered alphabetically for stable diffs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceAudit {
    pub rows: Vec<AuditRow>,
}

impl WorkspaceAudit {
    pub fn clean(&self) -> impl Iterator<Item = &AuditRow> {
        self.rows.iter().filter(|r| r.is_clean())
    }
    pub fn blocked(&self) -> impl Iterator<Item = &AuditRow> {
        self.rows.iter().filter(|r| !r.is_clean())
    }
}

/// Run [`check_musl_compatibility`] across every workspace member and return
/// the per-package result. Used by `yah qed audit-musl` to give the
/// W154 inventory answer ("which yah crates can musl-static today").
///
/// Errors from individual member checks become rows with `offenders.is_empty()
/// == false` if they were `NotMuslSafe`, or are propagated up if they were
/// metadata-shape errors (PackageNotFound is impossible by construction —
/// we iterate `metadata.workspace_members`).
pub fn audit_workspace(workspace_root: &Path) -> Result<WorkspaceAudit, MuslPreflightError> {
    let manifest_path = workspace_root.join("Cargo.toml");
    let metadata = MetadataCommand::new()
        .manifest_path(&manifest_path)
        .exec()
        .map_err(|e| MuslPreflightError::CargoMetadata {
            package: "<workspace>".into(),
            reason: e.to_string(),
        })?;

    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or_else(|| MuslPreflightError::CargoMetadata {
            package: "<workspace>".into(),
            reason: "cargo metadata returned no resolved dep graph".into(),
        })?;
    let node_by_id: std::collections::HashMap<_, _> =
        resolve.nodes.iter().map(|n| (n.id.clone(), n)).collect();

    let mut rows: Vec<AuditRow> = Vec::with_capacity(metadata.workspace_members.len());
    for member_id in &metadata.workspace_members {
        let package = metadata[member_id].name.clone();
        let mut seen = std::collections::HashSet::new();
        let mut frontier = vec![member_id.clone()];
        let mut reachable_names = Vec::new();
        while let Some(id) = frontier.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            if let Some(node) = node_by_id.get(&id) {
                reachable_names.push(metadata[&id].name.clone());
                for dep in &node.dependencies {
                    frontier.push(dep.clone());
                }
            }
        }
        let offenders = match check_dep_list(&package, reachable_names.iter().map(String::as_str)) {
            Ok(()) => Vec::new(),
            Err(MuslPreflightError::NotMuslSafe { offenders, .. }) => offenders,
            Err(other) => return Err(other),
        };
        rows.push(AuditRow { package, offenders });
    }
    rows.sort_by(|a, b| a.package.cmp(&b.package));
    Ok(WorkspaceAudit { rows })
}

/// Render a [`WorkspaceAudit`] as a markdown report — a summary line plus a
/// table grouped clean-first-then-blocked. Stable byte-for-byte across runs
/// of the same metadata (no timestamps, no env-dependent strings).
pub fn render_markdown(audit: &WorkspaceAudit) -> String {
    let total = audit.rows.len();
    let clean = audit.clean().count();
    let blocked = total - clean;
    let mut out = String::new();
    out.push_str("# yah workspace musl-static audit\n\n");
    out.push_str(&format!(
        "**{clean}/{total} workspace members are musl-static clean** ({blocked} blocked).\n\n"
    ));
    out.push_str(
        "Gated crate list: [`KNOWN_GLIBC_ONLY_CRATES`](../../crates/yah/qed/src/preflight.rs). \
         Regenerate with `yah qed audit-musl`.\n\n",
    );
    out.push_str("| Package | Musl-static | Glibc-only deps |\n");
    out.push_str("|---------|-------------|-----------------|\n");
    for row in audit.clean() {
        out.push_str(&format!("| `{}` | ✓ | — |\n", row.package));
    }
    for row in audit.blocked() {
        let offenders = row
            .offenders
            .iter()
            .map(|o| format!("`{o}`"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("| `{}` | ✗ | {} |\n", row.package, offenders));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_dep_list_passes_clean_tree() {
        let deps = ["tokio", "serde", "thiserror", "uuid"];
        check_dep_list("warden", deps).unwrap();
    }

    #[test]
    fn check_dep_list_flags_openssl_sys() {
        let deps = ["tokio", "openssl-sys", "serde"];
        let err = check_dep_list("warden", deps).unwrap_err();
        match err {
            MuslPreflightError::NotMuslSafe { package, offenders } => {
                assert_eq!(package, "warden");
                assert_eq!(offenders, vec!["openssl-sys".to_string()]);
            }
            other => panic!("expected NotMuslSafe, got {other:?}"),
        }
    }

    #[test]
    fn check_dep_list_collects_and_dedups_multiple_offenders() {
        // Each appears twice; output should dedupe + sort.
        let deps = [
            "tokio",
            "openssl-sys",
            "hyper-tls",
            "openssl-sys",
            "dbus",
            "hyper-tls",
        ];
        let err = check_dep_list("yah", deps).unwrap_err();
        match err {
            MuslPreflightError::NotMuslSafe { offenders, .. } => {
                assert_eq!(offenders, vec!["dbus", "hyper-tls", "openssl-sys"]);
            }
            other => panic!("expected NotMuslSafe, got {other:?}"),
        }
    }

    #[test]
    fn known_glibc_list_is_sorted_for_easy_diff_review() {
        let mut sorted = KNOWN_GLIBC_ONLY_CRATES.to_vec();
        sorted.sort();
        assert_eq!(
            sorted, KNOWN_GLIBC_ONLY_CRATES,
            "KNOWN_GLIBC_ONLY_CRATES must stay sorted",
        );
    }

    /// Smoke test against this very workspace: the qed crate is musl-clean
    /// by design (no openssl-sys, no dbus, no cuda). If this test ever
    /// fails, either the qed dep tree gained a glibc-only dep (regression
    /// to fix) or [`KNOWN_GLIBC_ONLY_CRATES`] picked up a false positive
    /// (refine the list).
    #[test]
    fn qed_workspace_passes_musl_gate() {
        let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .find(|p| p.join("Cargo.lock").is_file())
            .expect("workspace root has Cargo.lock")
            .to_path_buf();
        // The qed crate itself is the safest target: pure data + tokio +
        // tar/flate2. If this errors with PackageNotFound the workspace
        // member name has drifted.
        check_musl_compatibility(&workspace_root, "qed").expect("qed crate is musl-safe");
    }

    // ── R407-T4 workspace audit ────────────────────────────────────────────

    #[test]
    fn audit_workspace_returns_one_row_per_member_and_qed_is_clean() {
        let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .find(|p| p.join("Cargo.lock").is_file())
            .expect("workspace root has Cargo.lock")
            .to_path_buf();
        let audit = audit_workspace(&workspace_root).expect("workspace audit succeeds");
        assert!(!audit.rows.is_empty(), "workspace has members");
        // qed must be in the audit and must be clean by design.
        let qed = audit
            .rows
            .iter()
            .find(|r| r.package == "qed")
            .expect("qed appears in audit");
        assert!(qed.is_clean(), "qed should be musl-static clean: {qed:?}");
        // Rows are sorted alphabetically.
        let mut sorted = audit.rows.clone();
        sorted.sort_by(|a, b| a.package.cmp(&b.package));
        assert_eq!(audit.rows, sorted, "audit rows are alphabetically sorted");
    }

    #[test]
    fn render_markdown_groups_clean_first_then_blocked_with_summary() {
        let audit = WorkspaceAudit {
            rows: vec![
                AuditRow { package: "warden".into(), offenders: vec![] },
                AuditRow { package: "yah".into(), offenders: vec!["openssl-sys".into()] },
                AuditRow { package: "qed".into(), offenders: vec![] },
                AuditRow {
                    package: "desktop".into(),
                    offenders: vec!["dbus".into(), "hyper-tls".into()],
                },
            ],
        };
        let md = render_markdown(&audit);
        assert!(md.contains("**2/4 workspace members are musl-static clean** (2 blocked)."));
        // Header table present.
        assert!(md.contains("| Package | Musl-static | Glibc-only deps |"));
        // Clean rows render with the check icon.
        assert!(md.contains("| `warden` | ✓ | — |"));
        assert!(md.contains("| `qed` | ✓ | — |"));
        // Blocked rows list offenders inline, backtick-wrapped.
        assert!(md.contains("| `yah` | ✗ | `openssl-sys` |"));
        assert!(md.contains("| `desktop` | ✗ | `dbus`, `hyper-tls` |"));
        // Clean group precedes blocked group.
        let clean_at = md.find("| `warden` |").unwrap();
        let blocked_at = md.find("| `yah` |").unwrap();
        assert!(
            clean_at < blocked_at,
            "clean rows render before blocked rows for scanability",
        );
    }

    #[test]
    fn render_markdown_is_byte_stable_for_the_same_audit() {
        let audit = WorkspaceAudit {
            rows: vec![
                AuditRow { package: "a".into(), offenders: vec![] },
                AuditRow { package: "b".into(), offenders: vec!["openssl-sys".into()] },
            ],
        };
        assert_eq!(render_markdown(&audit), render_markdown(&audit));
    }

    #[test]
    fn workspace_audit_clean_and_blocked_iterators_partition_rows() {
        let audit = WorkspaceAudit {
            rows: vec![
                AuditRow { package: "a".into(), offenders: vec![] },
                AuditRow { package: "b".into(), offenders: vec!["dbus".into()] },
                AuditRow { package: "c".into(), offenders: vec![] },
            ],
        };
        assert_eq!(audit.clean().count(), 2);
        assert_eq!(audit.blocked().count(), 1);
        assert_eq!(audit.blocked().next().unwrap().package, "b");
    }

    #[test]
    fn package_not_found_surfaces_clearly() {
        let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .find(|p| p.join("Cargo.lock").is_file())
            .expect("workspace root has Cargo.lock")
            .to_path_buf();
        let err =
            check_musl_compatibility(&workspace_root, "definitely-not-a-real-package").unwrap_err();
        assert!(matches!(err, MuslPreflightError::PackageNotFound { .. }));
    }
}
