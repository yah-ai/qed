# Vendored assets

## `github-workflow.schema.json`

The JSON Schema for GitHub Actions workflow files, vendored from SchemaStore
as a **pinned, content-addressed asset** (R533-T3, [W224](../../../../.yah/docs/working/W224-qed-gha-import-boundary.md)).

W224 settles the GHA boundary as *import, not emulate*: a `workflow.yml` is an
**import source**, so QED needs a validator for it. We pin the schema rather than
fetch it live for three reasons — reproducibility/hermeticity, no silent drift
(SchemaStore tracks GHA's evolving format), and a clean license to vendor.

| | |
|---|---|
| **Source** | <https://json.schemastore.org/github-workflow.json> |
| **blake3** | `a2485ae78ab0c16ef1d8271e0969e133762845a7c3ee370f1644aebd941dea9d` |
| **Vendored** | 2026-06-18 |
| **License** | Apache-2.0 ([SchemaStore](https://github.com/SchemaStore/schemastore/blob/master/LICENSE)) |

The blake3 hex above is the content-address. It is asserted against the embedded
bytes by a unit test (`schema::tests::vendored_bytes_match_pin`) and against the
*live* upstream by the drift-check pipeline (`P014-gha-schema-drift`), which fails
when SchemaStore diverges so the bump is **deliberate, never a surprise**.

### Bumping

Upstream changed and the drift check is red? Re-vendor deliberately:

```sh
scripts/vendor-gha-schema.sh          # re-downloads + prints the new blake3
# then paste the new hash into:
#   - src/schema.rs  (SCHEMA_BLAKE3, SCHEMA_VENDORED_AT)
#   - vendor/NOTICE.md  (this table)
```
