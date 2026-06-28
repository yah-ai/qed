//! Vendored GitHub Actions workflow JSON Schema (R533-T3).
//!
//! W224 ("import, don't emulate") makes a `workflow.yml` an **import source** —
//! so the import/transform front-end needs a validator for it. Rather than fetch
//! the schema live (non-hermetic, silently drifting), we vendor SchemaStore's
//! [`github-workflow.json`] as a **pinned, content-addressed asset**: the bytes
//! live in `vendor/github-workflow.schema.json`, [`SCHEMA_BLAKE3`] is their
//! content-address, and two guards keep the pin honest —
//!
//! 1. [`vendored_bytes_match_pin`](tests) asserts the embedded bytes hash to the
//!    pin (catches a stray edit of the vendored file), and
//! 2. the `P014-gha-schema-drift` QED pipeline recomputes the *live* upstream
//!    hash and fails on divergence, so a SchemaStore change is a **deliberate
//!    bump** (re-run `scripts/vendor-gha-schema.sh`, paste the new hash here),
//!    never a surprise.
//!
//! This module is the vendored-asset surface only: it embeds the schema and
//! exposes its identity. Wiring it into the transformer as the build-time input
//! validator is R533-F4's job ([`schema_json`] is the input it validates
//! against).
//!
//! [`github-workflow.json`]: https://json.schemastore.org/github-workflow.json

/// Upstream URL the schema was vendored from. The drift-check pipeline fetches
/// exactly this.
pub const SCHEMA_SOURCE_URL: &str = "https://json.schemastore.org/github-workflow.json";

/// blake3 hex content-address of the vendored bytes. Matches `b3sum --no-names`
/// of `vendor/github-workflow.schema.json` and
/// [`content_hash`](crate::schema::schema_blake3). Bump deliberately on a
/// drift-check failure.
pub const SCHEMA_BLAKE3: &str = "a2485ae78ab0c16ef1d8271e0969e133762845a7c3ee370f1644aebd941dea9d";

/// Date (UTC) the pin was last taken — provenance for a deliberate bump.
pub const SCHEMA_VENDORED_AT: &str = "2026-06-18";

/// License of the vendored asset. SchemaStore is Apache-2.0 — clean to vendor.
pub const SCHEMA_LICENSE: &str = "Apache-2.0";

/// The vendored schema bytes, embedded at build time.
const SCHEMA_BYTES: &[u8] = include_bytes!("../vendor/github-workflow.schema.json");

/// The vendored GitHub Actions workflow JSON Schema, as a UTF-8 string. This is
/// the validator input R533-F4 checks an imported `workflow.yml` against.
///
/// Panics only if the vendored file is not UTF-8 — which a vendor-time check
/// and [`vendored_bytes_match_pin`](tests) both rule out; in practice infallible.
pub fn schema_json() -> &'static str {
    std::str::from_utf8(SCHEMA_BYTES).expect("vendored github-workflow schema is UTF-8")
}

/// Raw vendored schema bytes (the content-addressed asset).
pub fn schema_bytes() -> &'static [u8] {
    SCHEMA_BYTES
}

/// blake3 hex of the embedded bytes, recomputed. Equals [`SCHEMA_BLAKE3`] unless
/// the vendored file was edited without re-pinning (which the test catches).
/// Same algorithm as the import primitive's `content_hash`, so an imported
/// workflow and its schema share one content-addressing scheme.
pub fn schema_blake3() -> String {
    blake3::hash(SCHEMA_BYTES).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vendored_bytes_match_pin() {
        assert_eq!(
            schema_blake3(),
            SCHEMA_BLAKE3,
            "vendored github-workflow.schema.json was edited without re-pinning \
             SCHEMA_BLAKE3 — run scripts/vendor-gha-schema.sh and bump the const",
        );
        assert_eq!(SCHEMA_BLAKE3.len(), 64, "blake3 hex is 64 chars");
    }

    #[test]
    fn vendored_schema_is_valid_json() {
        // serde_json proves the vendored bytes are a parseable JSON document —
        // the minimum bar for a validator input.
        let v: serde_json::Value =
            serde_json::from_str(schema_json()).expect("vendored schema parses as JSON");
        assert_eq!(
            v.get("$schema").and_then(|s| s.as_str()),
            Some("http://json-schema.org/draft-07/schema#"),
            "github-workflow.json is a draft-07 schema",
        );
        // Sanity: it is the workflow schema (has the top-level `jobs` shape).
        assert!(
            v.get("properties").and_then(|p| p.get("jobs")).is_some(),
            "schema describes workflow `jobs`",
        );
    }

    #[test]
    fn pin_provenance_is_populated() {
        assert!(SCHEMA_SOURCE_URL.starts_with("https://"));
        assert_eq!(SCHEMA_LICENSE, "Apache-2.0");
        assert!(!SCHEMA_VENDORED_AT.is_empty());
    }
}
