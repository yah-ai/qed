//! Native matrix expansion (R505) — pipeline + step scope, mirrors GHA's
//! `strategy.matrix` semantics so the GHA bridge stays a passthrough.
//!
//! Schema:
//!
//! ```toml
//! [pipeline.matrix]
//! target = ["winit", "macos-native", "ios-sim", "ios-device", "vst"]
//! arch   = ["x86_64", "aarch64"]
//! exclude = [
//!   { target = "ios-sim",    arch = "x86_64" },
//!   { target = "ios-device", arch = "x86_64" },
//! ]
//! include = [
//!   { target = "macos-native", arch = "universal" },
//! ]
//! ```
//!
//! `include` / `exclude` are reserved keys; every other top-level key in the
//! `[matrix]` table is a dimension whose value must be a TOML array.
//!
//! Expansion algorithm (verbatim from
//! [`crate::yah_qed_gha::graph::expand_matrix`]):
//!
//! 1. Take the cartesian product of dimensions in declaration order.
//! 2. For each `include` row: if it carries at least one original-dimension
//!    key, merge into matching existing rows (without overwriting); otherwise
//!    append as a standalone row.
//! 3. Drop rows matching any `exclude` row (all listed keys equal).
//!
//! Substitution: `${{ matrix.<key> }}` placeholders (matching GHA's
//! `${{ }}` expression delimiter) in step `argv`, `env`, and `cwd` are
//! replaced with the row's value coerced to string via [`toml_value_to_str`].

use indexmap::IndexMap;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

use crate::types::{Pipeline, QedStep};

/// One concrete row produced by [`expand_matrix`] — an ordered map from
/// dimension name to its value for this row. Order matches the declaration
/// order in the source TOML (dimensions first, then include-only keys).
pub type MatrixCoord = IndexMap<String, toml::Value>;

/// A `[matrix]` block, lowered into dimensions + include/exclude rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MatrixSpec {
    /// Named axes; cartesian product is over these in declaration order.
    pub dimensions: IndexMap<String, Vec<toml::Value>>,
    /// `include` rows: extend matching combinations or append as standalone.
    pub include: Vec<MatrixCoord>,
    /// `exclude` rows: drop any combination matching all listed keys.
    pub exclude: Vec<MatrixCoord>,
}

impl MatrixSpec {
    /// `true` when the spec has no dimensions and no include rows. Such
    /// a spec produces zero rows and is treated as "no matrix declared"
    /// by the planner.
    pub fn is_empty(&self) -> bool {
        self.dimensions.is_empty() && self.include.is_empty()
    }
}

impl<'de> Deserialize<'de> for MatrixSpec {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = toml::Value::deserialize(d)?;
        let table = match value {
            toml::Value::Table(t) => t,
            _ => return Err(D::Error::custom("[matrix] must be a TOML table")),
        };
        let mut dimensions: IndexMap<String, Vec<toml::Value>> = IndexMap::new();
        let mut include: Vec<MatrixCoord> = Vec::new();
        let mut exclude: Vec<MatrixCoord> = Vec::new();
        for (key, val) in table {
            match key.as_str() {
                "include" | "exclude" => {
                    let rows = match val {
                        toml::Value::Array(arr) => arr,
                        _ => {
                            return Err(D::Error::custom(format!(
                                "matrix.{key} must be an array of tables"
                            )));
                        }
                    };
                    let mut out = Vec::with_capacity(rows.len());
                    for (i, row) in rows.into_iter().enumerate() {
                        let row_table = match row {
                            toml::Value::Table(t) => t,
                            _ => {
                                return Err(D::Error::custom(format!(
                                    "matrix.{key}[{i}] must be a table"
                                )));
                            }
                        };
                        let mut coord = IndexMap::new();
                        for (k, v) in row_table {
                            coord.insert(k, v);
                        }
                        out.push(coord);
                    }
                    if key == "include" {
                        include = out;
                    } else {
                        exclude = out;
                    }
                }
                _ => {
                    let arr = match val {
                        toml::Value::Array(a) => a,
                        _ => {
                            return Err(D::Error::custom(format!(
                                "matrix dimension `{key}` must be an array"
                            )));
                        }
                    };
                    dimensions.insert(key, arr);
                }
            }
        }
        Ok(MatrixSpec {
            dimensions,
            include,
            exclude,
        })
    }
}

impl Serialize for MatrixSpec {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Re-emit as a flat TOML table: dimensions first, then include/exclude
        // when non-empty. Using toml::Value as the intermediate preserves
        // declaration order through the serializer.
        let mut table = toml::value::Table::new();
        for (k, v) in &self.dimensions {
            table.insert(k.clone(), toml::Value::Array(v.clone()));
        }
        if !self.include.is_empty() {
            table.insert(
                "include".to_string(),
                toml::Value::Array(self.include.iter().map(coord_to_value).collect()),
            );
        }
        if !self.exclude.is_empty() {
            table.insert(
                "exclude".to_string(),
                toml::Value::Array(self.exclude.iter().map(coord_to_value).collect()),
            );
        }
        toml::Value::Table(table).serialize(s)
    }
}

fn coord_to_value(coord: &MatrixCoord) -> toml::Value {
    let mut t = toml::value::Table::new();
    for (k, v) in coord {
        t.insert(k.clone(), v.clone());
    }
    toml::Value::Table(t)
}

/// Expand a [`MatrixSpec`] into its concrete rows. Returns an empty vector
/// when the spec has no dimensions and no include rows.
pub fn expand_matrix(spec: &MatrixSpec) -> Vec<MatrixCoord> {
    let dim_keys: Vec<String> = spec.dimensions.keys().cloned().collect();
    let dim_vals: Vec<Vec<toml::Value>> = spec.dimensions.values().cloned().collect();

    // Step 1 — cartesian product.
    let mut rows: Vec<MatrixCoord> = if dim_keys.is_empty() {
        Vec::new()
    } else {
        cartesian(&dim_keys, &dim_vals)
    };

    // Step 2 — apply include rows.
    let original_keys: std::collections::HashSet<&str> =
        dim_keys.iter().map(|s| s.as_str()).collect();
    for inc in &spec.include {
        if dim_keys.is_empty() {
            rows.push(inc.clone());
            continue;
        }
        let (orig_part, new_part): (MatrixCoord, MatrixCoord) = inc
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .partition(|(k, _)| original_keys.contains(k.as_str()));

        if orig_part.is_empty() {
            rows.push(new_part);
            continue;
        }

        let mut matched_any = false;
        for row in rows.iter_mut() {
            if orig_part.iter().all(|(k, v)| row.get(k) == Some(v)) {
                matched_any = true;
                for (k, v) in &new_part {
                    if !row.contains_key(k) {
                        row.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        if !matched_any {
            let mut row = IndexMap::new();
            row.extend(orig_part);
            row.extend(new_part);
            rows.push(row);
        }
    }

    // Step 3 — drop excluded rows.
    if !spec.exclude.is_empty() {
        rows.retain(|row| {
            !spec
                .exclude
                .iter()
                .any(|ex| ex.iter().all(|(k, ev)| row.get(k) == Some(ev)))
        });
    }

    rows
}

fn cartesian(keys: &[String], vals: &[Vec<toml::Value>]) -> Vec<MatrixCoord> {
    let mut out: Vec<MatrixCoord> = vec![IndexMap::new()];
    for (k, vs) in keys.iter().zip(vals.iter()) {
        let mut next = Vec::with_capacity(out.len() * vs.len().max(1));
        for row in &out {
            if vs.is_empty() {
                next.push(row.clone());
                continue;
            }
            for v in vs {
                let mut nr = row.clone();
                nr.insert(k.clone(), v.clone());
                next.push(nr);
            }
        }
        out = next;
    }
    out
}

/// One concrete schedulable unit produced by [`plan`]. Pipeline-level matrix
/// jobs carry `coord = Some(row)`; non-matrix pipelines carry `coord = None`.
#[derive(Debug, Clone)]
pub struct PlannedJob {
    pub coord: Option<MatrixCoord>,
    pub pipeline: Pipeline,
}

impl PlannedJob {
    /// Compact display label: `target=macos-native arch=universal` (one
    /// `key=value` pair per coordinate axis, in declaration order). Returns
    /// the pipeline name when no coord is present.
    pub fn label(&self) -> String {
        match &self.coord {
            None => self.pipeline.name.clone(),
            Some(coord) => coord
                .iter()
                .map(|(k, v)| format!("{k}={}", toml_value_to_str(v)))
                .collect::<Vec<_>>()
                .join(" "),
        }
    }
}

/// Expand a pipeline into the concrete jobs the runner will walk. When
/// `pipeline.matrix` is present, returns one job per expanded row with
/// `${{ matrix.<key> }}` substituted into step `argv` / `env` / `cwd`.
/// When `pipeline.matrix` is absent, returns a single job mirroring the
/// pipeline as-is. Step-level matrices are expanded *within* each job —
/// every step that carries its own `[matrix]` block fans out into N step
/// instances substituted against that row's coord.
pub fn plan(pipeline: &Pipeline) -> Vec<PlannedJob> {
    let pipeline_rows: Vec<Option<MatrixCoord>> = match &pipeline.matrix {
        Some(spec) if !spec.is_empty() => {
            let rows = expand_matrix(spec);
            if rows.is_empty() {
                vec![None]
            } else {
                rows.into_iter().map(Some).collect()
            }
        }
        _ => vec![None],
    };

    pipeline_rows
        .into_iter()
        .map(|coord| {
            let mut clone = pipeline.clone();
            // Strip the top-level matrix so the cloned pipeline reads as a
            // fully-resolved leaf and downstream code can't accidentally
            // re-expand it.
            clone.matrix = None;

            // Expand step-level matrices first (each one fans out into N
            // steps), then substitute the pipeline-level coord across every
            // resulting step. Step-level substitution happens during fan-out
            // so each step instance sees its own coord.
            let mut expanded_steps: Vec<QedStep> = Vec::with_capacity(clone.steps.len());
            for step in clone.steps.drain(..) {
                expanded_steps.extend(expand_step(step));
            }
            clone.steps = expanded_steps;

            if let Some(row) = &coord {
                apply_matrix_to_pipeline(&mut clone, row);
            }
            PlannedJob {
                coord,
                pipeline: clone,
            }
        })
        .collect()
}

fn expand_step(step: QedStep) -> Vec<QedStep> {
    let spec = match step.matrix.clone() {
        Some(s) if !s.is_empty() => s,
        _ => return vec![strip_step_matrix(step)],
    };
    let rows = expand_matrix(&spec);
    if rows.is_empty() {
        return vec![strip_step_matrix(step)];
    }
    rows.into_iter()
        .map(|row| {
            let mut copy = strip_step_matrix(step.clone());
            // Differentiate the step name by appending the coord — `build`
            // becomes `build [arch=aarch64 os=linux]` so the dashboard shows
            // distinct rows.
            let suffix = row
                .iter()
                .map(|(k, v)| format!("{k}={}", toml_value_to_str(v)))
                .collect::<Vec<_>>()
                .join(" ");
            if !suffix.is_empty() {
                copy.name = format!("{} [{}]", copy.name, suffix);
            }
            apply_matrix_to_step(&mut copy, &row);
            copy
        })
        .collect()
}

fn strip_step_matrix(mut step: QedStep) -> QedStep {
    step.matrix = None;
    step
}

fn apply_matrix_to_pipeline(pipeline: &mut Pipeline, coord: &MatrixCoord) {
    for step in &mut pipeline.steps {
        apply_matrix_to_step(step, coord);
    }
}

fn apply_matrix_to_step(step: &mut QedStep, coord: &MatrixCoord) {
    let lookup: HashMap<&str, String> = coord
        .iter()
        .map(|(k, v)| (k.as_str(), toml_value_to_str(v)))
        .collect();
    for arg in &mut step.argv {
        *arg = substitute_matrix(arg, &lookup);
    }
    for value in step.env.values_mut() {
        *value = substitute_matrix(value, &lookup);
    }
    if let Some(cwd) = &mut step.cwd {
        *cwd = substitute_matrix(cwd, &lookup);
    }
    // R533-F9: a build target lifted from `--target ${{ matrix.<key> }}` lives
    // in `platform.target` (and `container_platform`); concretize it per row so
    // each fanned-out instance carries its own resolved triple for F3 resolve().
    if let Some(platform) = &mut step.platform {
        if let Some(target) = &mut platform.target {
            *target = substitute_matrix(target, &lookup);
        }
        if let Some(cp) = &mut platform.container_platform {
            *cp = substitute_matrix(cp, &lookup);
        }
    }
}

/// Lossy `toml::Value` → string coercion for matrix coord values. Strings
/// passthrough unquoted; booleans render as `true`/`false`; numbers via
/// their TOML Display impl. Arrays/tables stringify via their Debug-ish
/// TOML serialization — those are unusual in matrix entries and exist only
/// to keep substitution total.
pub fn toml_value_to_str(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Datetime(d) => d.to_string(),
        toml::Value::Array(_) | toml::Value::Table(_) => v.to_string(),
    }
}

/// Replace `${{ matrix.<key> }}` occurrences with the coord's value.
/// Whitespace inside the braces is tolerated; an unknown key is left
/// untouched (the runner will surface it as an unsubstituted literal,
/// matching how `apply_params` leaves unknown `{{key}}`).
pub fn substitute_matrix(input: &str, coord: &HashMap<&str, String>) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 3 < bytes.len() && &bytes[i..i + 3] == b"${{" {
            // Find the closing `}}`.
            if let Some(end) = find_close(bytes, i + 3) {
                let body = &input[i + 3..end];
                let trimmed = body.trim();
                if let Some(key) = trimmed.strip_prefix("matrix.") {
                    let key = key.trim();
                    if let Some(val) = coord.get(key) {
                        out.push_str(val);
                        i = end + 2;
                        continue;
                    }
                }
                // Unknown expression — leave the original verbatim so the
                // runner can either substitute it later (e.g. via
                // `${{ steps.X.outputs.Y }}` for sibling outputs) or surface
                // it as an unresolved literal.
                out.push_str(&input[i..end + 2]);
                i = end + 2;
                continue;
            }
        }
        out.push(input[i..].chars().next().unwrap());
        i += input[i..].chars().next().unwrap().len_utf8();
    }
    out
}

fn find_close(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < bytes.len() {
        if &bytes[i..i + 2] == b"}}" {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> toml::Value {
        toml::Value::String(v.into())
    }

    fn coord(pairs: &[(&str, &str)]) -> MatrixCoord {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), s(v)))
            .collect()
    }

    #[test]
    fn cartesian_two_dims() {
        let spec: MatrixSpec = toml::from_str(
            r#"
os = ["linux", "macos"]
rust = ["stable", "beta"]
"#,
        )
        .unwrap();
        let rows = expand_matrix(&spec);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].get("os"), Some(&s("linux")));
        assert_eq!(rows[0].get("rust"), Some(&s("stable")));
        assert_eq!(rows[1].get("rust"), Some(&s("beta")));
        assert_eq!(rows[2].get("os"), Some(&s("macos")));
    }

    #[test]
    fn include_extends_matching_combination() {
        let spec: MatrixSpec = toml::from_str(
            r#"
os = ["linux", "macos"]
rust = ["stable"]
include = [{ os = "linux", rust = "stable", extra = "special" }]
"#,
        )
        .unwrap();
        let rows = expand_matrix(&spec);
        assert_eq!(rows.len(), 2);
        let linux = rows
            .iter()
            .find(|r| r.get("os") == Some(&s("linux")))
            .unwrap();
        assert_eq!(linux.get("extra"), Some(&s("special")));
        let macos = rows
            .iter()
            .find(|r| r.get("os") == Some(&s("macos")))
            .unwrap();
        assert!(macos.get("extra").is_none());
    }

    #[test]
    fn include_with_no_anchor_appends_standalone_row() {
        let spec: MatrixSpec = toml::from_str(
            r#"
os = ["linux"]
include = [{ os = "windows", arch = "x86_64" }]
"#,
        )
        .unwrap();
        let rows = expand_matrix(&spec);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].get("os"), Some(&s("windows")));
        assert_eq!(rows[1].get("arch"), Some(&s("x86_64")));
    }

    #[test]
    fn exclude_drops_matching_rows() {
        let spec: MatrixSpec = toml::from_str(
            r#"
os = ["linux", "macos"]
arch = ["x86_64", "aarch64"]
exclude = [{ os = "macos", arch = "x86_64" }]
"#,
        )
        .unwrap();
        let rows = expand_matrix(&spec);
        assert_eq!(rows.len(), 3);
        let any_excluded = rows
            .iter()
            .any(|r| r.get("os") == Some(&s("macos")) && r.get("arch") == Some(&s("x86_64")));
        assert!(!any_excluded);
    }

    #[test]
    fn noisetable_release_apple_expands_to_seven_rows() {
        // The release.apple matrix from W208's frame: 5 targets × 2 archs,
        // minus four excludes (mac-native both archs + ios on x86_64),
        // plus one include (macos-native universal) = 7 rows.
        let spec: MatrixSpec = toml::from_str(
            r#"
target = ["winit", "macos-native", "ios-sim", "ios-device", "vst"]
arch   = ["x86_64", "aarch64"]
exclude = [
  { target = "ios-sim",      arch = "x86_64" },
  { target = "ios-device",   arch = "x86_64" },
  { target = "macos-native", arch = "x86_64" },
  { target = "macos-native", arch = "aarch64" },
]
include = [
  { target = "macos-native", arch = "universal" },
]
"#,
        )
        .unwrap();
        let rows = expand_matrix(&spec);
        assert_eq!(rows.len(), 7, "rows: {rows:?}");
        // Spot-check the universal mac row sits at the end (include appended
        // after cartesian + excludes).
        let last = rows.last().unwrap();
        assert_eq!(last.get("target"), Some(&s("macos-native")));
        assert_eq!(last.get("arch"), Some(&s("universal")));
        // And that none of the ios-x86_64 rows survived.
        for row in &rows {
            let is_ios = matches!(
                row.get("target"),
                Some(toml::Value::String(t)) if t.starts_with("ios-"),
            );
            if is_ios {
                assert_eq!(row.get("arch"), Some(&s("aarch64")));
            }
        }
    }

    #[test]
    fn substitute_matrix_replaces_known_keys() {
        let c = coord(&[("arch", "aarch64"), ("target", "macos-native")]);
        let lookup: HashMap<&str, String> = c
            .iter()
            .map(|(k, v)| (k.as_str(), toml_value_to_str(v)))
            .collect();
        let out = substitute_matrix(
            "cargo build --target ${{ matrix.arch }}-apple-${{ matrix.target }}",
            &lookup,
        );
        assert_eq!(out, "cargo build --target aarch64-apple-macos-native");
    }

    #[test]
    fn substitute_matrix_leaves_unknown_expressions_alone() {
        let c = coord(&[("arch", "aarch64")]);
        let lookup: HashMap<&str, String> = c
            .iter()
            .map(|(k, v)| (k.as_str(), toml_value_to_str(v)))
            .collect();
        // Non-matrix expressions (sibling step outputs) and unknown matrix
        // keys both pass through verbatim.
        let out = substitute_matrix(
            "tag=${{ matrix.arch }}-${{ steps.x.outputs.sha }}-${{ matrix.unknown }}",
            &lookup,
        );
        assert_eq!(
            out,
            "tag=aarch64-${{ steps.x.outputs.sha }}-${{ matrix.unknown }}"
        );
    }

    #[test]
    fn round_trips_through_toml() {
        let original: MatrixSpec = toml::from_str(
            r#"
os = ["linux", "macos"]
arch = ["x86_64", "aarch64"]
include = [{ os = "linux", arch = "aarch64", extra = "qemu" }]
exclude = [{ os = "macos", arch = "x86_64" }]
"#,
        )
        .unwrap();
        let serialized = toml::to_string(&original).unwrap();
        let reparsed: MatrixSpec = toml::from_str(&serialized).unwrap();
        assert_eq!(reparsed, original);
    }

    #[test]
    fn matrix_spec_with_only_include_works() {
        let spec: MatrixSpec = toml::from_str(
            r#"
include = [
  { os = "linux", target = "x86_64-unknown-linux-gnu" },
  { os = "linux", target = "aarch64-unknown-linux-musl" },
]
"#,
        )
        .unwrap();
        let rows = expand_matrix(&spec);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("target"), Some(&s("x86_64-unknown-linux-gnu")));
    }

    #[test]
    fn plan_with_no_matrix_returns_single_job() {
        let pipeline = test_pipeline(None, vec![test_step("build", &["echo", "hi"], None)]);
        let jobs = plan(&pipeline);
        assert_eq!(jobs.len(), 1);
        assert!(jobs[0].coord.is_none());
        assert_eq!(jobs[0].pipeline.steps.len(), 1);
        assert_eq!(jobs[0].pipeline.steps[0].argv, vec!["echo", "hi"]);
    }

    #[test]
    fn plan_with_pipeline_matrix_substitutes_argv() {
        let matrix: MatrixSpec = toml::from_str(
            r#"
arch = ["x86_64", "aarch64"]
"#,
        )
        .unwrap();
        let step = test_step(
            "build",
            &[
                "cargo",
                "build",
                "--target",
                "${{ matrix.arch }}-apple-darwin",
            ],
            None,
        );
        let pipeline = test_pipeline(Some(matrix), vec![step]);
        let jobs = plan(&pipeline);
        assert_eq!(jobs.len(), 2);
        assert_eq!(
            jobs[0].pipeline.steps[0].argv,
            vec!["cargo", "build", "--target", "x86_64-apple-darwin"],
        );
        assert_eq!(
            jobs[1].pipeline.steps[0].argv,
            vec!["cargo", "build", "--target", "aarch64-apple-darwin"],
        );
        // The cloned pipeline no longer carries its matrix block — downstream
        // code sees a fully-resolved leaf.
        assert!(jobs[0].pipeline.matrix.is_none());
    }

    #[test]
    fn plan_expands_step_level_matrix_inside_single_job() {
        let step_matrix: MatrixSpec = toml::from_str(
            r#"
target = ["x86_64", "aarch64"]
"#,
        )
        .unwrap();
        let mut step = test_step(
            "check",
            &["cargo", "check", "--target", "${{ matrix.target }}"],
            None,
        );
        step.matrix = Some(step_matrix);
        let pipeline = test_pipeline(None, vec![step]);
        let jobs = plan(&pipeline);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].pipeline.steps.len(), 2);
        assert!(jobs[0].pipeline.steps[0].name.contains("target=x86_64"));
        assert_eq!(jobs[0].pipeline.steps[0].argv.last().unwrap(), "x86_64");
        assert_eq!(jobs[0].pipeline.steps[1].argv.last().unwrap(), "aarch64");
    }

    #[test]
    fn plan_combines_pipeline_and_step_matrix() {
        // pipeline.matrix × step.matrix → N × M concrete step instances per job.
        let pm: MatrixSpec = toml::from_str(r#"os = ["linux"]"#).unwrap();
        let sm: MatrixSpec = toml::from_str(r#"arch = ["x86", "arm"]"#).unwrap();
        let mut step = test_step(
            "build",
            &["build", "${{ matrix.os }}/${{ matrix.arch }}"],
            None,
        );
        step.matrix = Some(sm);
        let pipeline = test_pipeline(Some(pm), vec![step]);
        let jobs = plan(&pipeline);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].pipeline.steps.len(), 2);
        // Pipeline-level coord substitutes after step expansion, so both step
        // instances see the same pipeline-level `os = linux`.
        assert_eq!(jobs[0].pipeline.steps[0].argv[1], "linux/x86");
        assert_eq!(jobs[0].pipeline.steps[1].argv[1], "linux/arm");
    }

    #[test]
    fn planned_job_label_uses_coord_pairs() {
        let pipeline = test_pipeline(
            Some(toml::from_str(r#"arch = ["x86_64"]"#).unwrap()),
            vec![test_step("build", &[], None)],
        );
        let jobs = plan(&pipeline);
        assert_eq!(jobs[0].label(), "arch=x86_64");
    }

    // ── helpers ──

    fn test_pipeline(matrix: Option<MatrixSpec>, steps: Vec<QedStep>) -> Pipeline {
        Pipeline {
            name: "test".into(),
            label: "Test".into(),
            steps,
            params: Default::default(),
            on_success: Default::default(),
            on_fail: Default::default(),
            triggers: Default::default(),
            concurrency_key: None,
            placement: Default::default(),
            workspace: crate::types::WorkspaceMode::default(),
            wraps: None,
            matrix,
            toolchain: None,
            binds: Vec::new(),
            on_change: Vec::new(),
            finally: Vec::new(),
        }
    }

    fn test_step(name: &str, argv: &[&str], env_pairs: Option<&[(&str, &str)]>) -> QedStep {
        QedStep {
            background: false,
            background_until: None,
            wait_for: None,
            manifest_stitch: None,
            name: name.into(),
            argv: argv.iter().map(|s| (*s).into()).collect(),
            cwd: None,
            env: env_pairs
                .map(|p| p.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect())
                .unwrap_or_default(),
            timeout: None,
            on_fail: Default::default(),
            produces: Default::default(),
            runtime: None,
            kind: Default::default(),
            image: None,
            tag: None,
            push: false,
            binary_path: None,
            triple: None,
            package: None,
            context: None,
            load: false,
            sub_pipeline: None,
            outputs: Default::default(),
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

    #[test]
    fn step_matrix_substitutes_lifted_platform_target() {
        // R533-F9: a build step whose target was lifted to
        // `platform.target = "${{ matrix.target }}"` concretizes per matrix row.
        let mut step = test_step("build", &["cargo", "zigbuild", "--target", "${{ matrix.target }}"], None);
        step.platform = Some(crate::platform::PlatformSpec {
            target: Some("${{ matrix.target }}".into()),
            container_platform: None,
            native: false,
        });
        let mut spec = MatrixSpec::default();
        spec.dimensions.insert(
            "target".into(),
            vec![
                toml::Value::String("x86_64-unknown-linux-musl".into()),
                toml::Value::String("aarch64-unknown-linux-musl".into()),
            ],
        );
        step.matrix = Some(spec);

        let fanned = expand_step(step);
        assert_eq!(fanned.len(), 2);
        let targets: Vec<&str> = fanned
            .iter()
            .map(|s| s.platform.as_ref().unwrap().target.as_deref().unwrap())
            .collect();
        assert_eq!(targets, vec!["x86_64-unknown-linux-musl", "aarch64-unknown-linux-musl"]);
        // And the argv `--target` token resolved to the same concrete triple.
        assert!(fanned[0].argv.contains(&"x86_64-unknown-linux-musl".to_string()));
    }
}
