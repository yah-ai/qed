//! Injectable image-build handler for the docker push family (R594).
//!
//! W224 retired *running* `docker/login-action` / `docker/build-push-action` in
//! the runtime — the tier classifier ([`crate::tier`]) declines them with a
//! "replace with a native `build-image` step" stanza. That keeps the standalone
//! qed-gha crate honest: on its own it never shells `docker`.
//!
//! But the qed runner's whole job is to *interpret and execute* a real
//! `.github/workflows/*.yml`, image jobs included. So the runner injects an
//! [`ImageBuilder`] via [`crate::Executor::with_image_builder`]. When one is
//! present, the runtime routes the two docker push-family slugs to it (applying
//! the camp's registry route + auth, then building locally via `docker buildx`
//! or remotely on a build-worker) instead of erroring. When absent, the tier-3
//! error is preserved unchanged — so nothing about the bare crate's contract
//! shifts.
//!
//! The handler returns a [`ToolkitOutcome`] (same shape a toolkit action
//! produces) so the runtime finalizes the step identically — `steps.<id>.
//! outputs.digest` / `.imageid` / `.metadata` flow through for downstream
//! `cosign sign` and the per-binary `DIGEST` env blocks in `release.yml`.
//!
//! @yah:ticket(R605-F1, "QED-side OIDC signing identity: camp-keystore workload-identity → cosign keyless (retire GitHub OIDC dependency)")
//! @yah:status(review)
//! @yah:at(2026-08-16T18:35:04Z)
//! @yah:assignee(agent:bundle-anthropic-miravel)
//! @yah:parent(R605)
//! @yah:verify("A QED-run release signs with a QED-minted OIDC identity and `cosign verify` / `cosign verify-blob --bundle` pass against the pinned issuer, with zero calls to token.actions.githubusercontent.com")
//! @yah:gotcha("Until this lands, *signed* releases still cut on GitHub — do not claim QED release parity without this.")
//! @yah:gotcha("The premise this ticket was filed on does not hold: you cannot make cosign keyless work off GitHub by minting your own OIDC token. Public-good Fulcio only issues certs for OIDC issuers on its own configured allowlist (Dex/Google/GitHub/GitLab/SPIFFE/Kubernetes per Sigstore's 'OIDC in Fulcio' docs) — a camp-minted issuer gets nothing from it. The only two real routes are (a) key-based cosign, or (b) a self-hosted Fulcio+Rekor+CT with its own TUF trust root. W235 §'Off-GHA forfeits GitHub OIDC keyless cosign' already picked (a); this ticket implements (a) and leaves a --identity-token hook for (b).")
//! @yah:handoff("SIGN SIDE landed in oss/qed/crates/qed/src/native.rs: new SigningIdentity enum {Keyless{identity_token: Option<String>}, Key{key_ref}} + ENV_COSIGN_KEY (QED_COSIGN_KEY) / ENV_COSIGN_IDENTITY_TOKEN (QED_COSIGN_IDENTITY_TOKEN) + SigningIdentity::from_env{,_with} (key wins over token; blank reads as unset; injectable lookup so tests do not race process env). CosignSigner grew an `identity` field + keyless()/with_key()/with_bin() ctors and a sign_blob_argv() split out so the per-identity flag shape is unit-testable without a cosign install. Key mode emits `--key <ref>` and NO --output-certificate, so SignedBlob.certificate_path became Option<PathBuf> (its only consumer was the tracing::info! at runner.rs:5044).")
//! @yah:handoff("New free fn native::resolve_signer() -> Arc<dyn SigstoreSigner>: real CosignSigner when an identity is configured in the env, LoggingSigner otherwise. This closes the long-standing runner.rs:84 gotcha (release CI MUST wire CosignSigner explicitly or it ships stub .sig/.crt) - CI now only has to export QED_COSIGN_KEY. Wired at all four live PipelineRunner construction sites: app/yah/cli/src/qed.rs and camp.rs x3 (matrix-row child, qed.run, queued-run). The daemon path matters most since the CLI proxies to it.")
//! @yah:handoff("VERIFY SIDE, shared spec: new ReleaseTrust enum + ReleaseTrust::parse in oss/yubaba/crates/cloud/src/release_manifest.rs, next to DEFAULT_YUBABA_COSIGN_IDENTITY. Convention: a `key:<ref>` prefix on any existing cosign-identity config value selects key-based verification; anything else stays a keyless cert-identity regexp. Deliberately a string convention, not a schema change - flipping a fleet to key-based trust is a value edit, and every existing config value keeps its exact prior meaning. A bare KMS URI without the prefix stays keyless on purpose (sniffing a scheme out of a free-form regexp would silently swap trust models). COSIGN_OIDC_ISSUER now has ONE canonical home here; cloud_init.rs and yubaba_fetch.rs re-export it instead of each holding a private copy.")
//! @yah:handoff("VERIFY SIDE, three consumers all threaded through ReleaseTrust: (1) cloud_init.rs build_cosign_verify_block - key mode emits `--key` and drops the .cert curl line entirely; (2) app/yah/cli/src/yubaba_fetch.rs - Verifier::verify signature is now (artifact, sig, Option<&Path> cert, &ReleaseTrust), CosignVerifier builds argv from trust.verify_flags(), and fetch_verify_cache skips the .cert download when the trust arm does not consume one; (3) app/yah/web/marketing/public/install.sh - same `key:` convention via a case arm, default path byte-identical to before, and YAH_INSTALL_COSIGN_IDENTITY now overrides the pin.")
//! @yah:handoff("release.yml: the six OCI image `cosign sign` steps are now identity-agnostic (if QED_COSIGN_KEY is set use --key, else keyless exactly as today). Inert on GHA - QED_COSIGN_KEY is unset there - so this is a no-op for the current substrate and removes a blocker for F2 running those jobs off GitHub.")
//! @yah:handoff("TESTS all green: yah-qed lib 675 pass (9 new in native:: covering keyless argv unchanged from the GHA shape, identity-token pass-through, key argv emits no --output-certificate, and the four from_env precedence/blank cases); yah-cloud lib 653 pass (4 new ReleaseTrust cases + a new cloud_init render test asserting key mode emits --key, no keyless flags, no .cert fetch, and stays colon-space-free per the R330-F28 cloud-init pothole); yah --test camp_yubaba_fetch 9 pass (new key_trust_verifies_without_fetching_a_certificate, whose fixture serves NO cert blob so a stray fetch fails loudly); yah-qed-gha 129 pass (release.yml still round-trips); cargo check -p yah --tests clean; sh -n on install.sh clean; release.yml parses as YAML.")
//! @yah:handoff("Tree anchor at handoff: ab7a2dd12b268a9593873f24630e6eabcef95b6a — the shared tree as I left it. Diff against it (`git diff ab7a2dd12b268a9593873f24630e6eabcef95b6a..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:next("Provision the actual key material - nothing here creates any. Generate the release key pair, vault it, and publish the PUBLIC key at a stable URL (cdn.yah.dev/keys/... is the example ref the tests use). The seam is inert until that exists. The per-run grant that hands the private key to a remote QED run is R555-F5.")
//! @yah:next("Native-tarball .sig publication for key mode is NOT done and was deliberately scoped out: release.yml's three `cosign sign-blob` steps (~lines 1327/1729/1964) still hardcode --output-certificate, and their .cert output threads onward into the R2 publish and the manifest fragment's cert_url. Making those key-aware means touching the artifact-publish layout, not just a flag - it belongs with the publish work, and the verify side already tolerates a missing cert (ReleaseTrust::needs_certificate is false for key trust, and the manifest's cert_url simply goes unread).")
//! @yah:next("OCI image signing under QED needs F2 first: the six release.yml sign steps are ready for a key, but the image jobs themselves cannot run off GitHub until F2 lands the docker/buildx substrate.")
//! @yah:next("If the self-hosted-Fulcio route is ever wanted instead of key-based, SigningIdentity::Keyless{identity_token} is the hook already in place (cosign --identity-token is grounded in the sign-blob docs). It needs a private Fulcio + Rekor + CT log and a custom TUF trust root distributed to every verifier - a much larger job than key-based, and W235 already argued against it.")
//! @yah:verify("cargo test -p yah-qed --lib (675 pass) - VERIFIED")
//! @yah:verify("cd oss/yubaba && cargo test -p yah-cloud --lib (653 pass) - VERIFIED")
//! @yah:verify("cargo test -p yah --test camp_yubaba_fetch (9 pass) - VERIFIED")
//! @yah:verify("cd oss/qed && cargo test -p yah-qed-gha (129 pass, release.yml round-trips) - VERIFIED")
//! @yah:verify("STILL UNVERIFIABLE, and the ticket's original criterion: a tagged release actually cut on QED producing cosign-verifiable signatures. Needs the key from next-step 1 plus F2 - no cosign binary is even installed on this host.")
//! @yah:assumes("cosign sign-blob --key / --identity-token / --bundle / --yes and cosign verify-blob --key were grounded against Sigstore's published flag docs, not run - there is no cosign on this host, so no invocation in this change has been executed for real. --output-signature / --output-certificate are grounded differently and more strongly: release.yml has been using them in production.")
//! @yah:handoff("Native-tarball .sig publication for key mode landed in .github/workflows/release.yml (previously scoped out). All three tarball publish jobs (publish-cli, publish-yubaba, publish-mesofact) now branch on QED_COSIGN_KEY exactly like the OCI image jobs: key mode signs with `cosign sign-blob --key` and emits no .cert; keyless mode is byte-identical to before. The R2 publish step skips the `.cert` upload when absent, and the manifest-fragment step emits `cert_url: \"\"` in key mode instead of a dangling URL. Verified: cert_url is never read downstream when a trust arm doesn't need it (yubaba_fetch.rs already gates on ReleaseTrust::needs_certificate; the marketing /releases page and rollout::Asset both carry cert_url through unread). Inert on GHA today (QED_COSIGN_KEY unset there), same as the OCI image steps.")
//! @yah:handoff("Verified: `python3 -c \"import yaml; yaml.safe_load(...)\"` clean, `actionlint .github/workflows/release.yml` exit 0 (no findings), `cargo test -p yah-qed-gha` 133 pass (release.yml round-trips through the native GHA runtime parser).")
//! @yah:handoff("Tree anchor at handoff: 2fb9f19e551fe63171d6fe8238da756f810cf0d6 — the shared tree as I left it. Diff against it (`git diff 2fb9f19e551fe63171d6fe8238da756f810cf0d6..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:next("Provision the actual cosign key pair, vault the private key, publish the public key at a stable URL (cdn.yah.dev/keys/... per the tests). This is the one real blocker left and is an operator/infra action — generating + custody of a production release-signing key and publishing to live infra — not something to default on. No cosign binary is installed on this host either, so even a dry run can't happen here.")
//! @yah:next("Once the key exists: export QED_COSIGN_KEY on a QED release run and confirm `cosign verify` / `cosign verify-blob --bundle` pass with zero calls to token.actions.githubusercontent.com — the ticket's original verification criterion, still unverified.")
//! @yah:next("OCI image signing under QED needs R605-F2 (docker/buildx substrate) first — the six release.yml image-sign steps are already key-aware and just waiting on a runner that can execute those jobs off GitHub.")
//! @yah:handoff("Key material provisioned end-to-end (operator authorized doing this live): cosign key pair generated locally, private key + password vaulted (`yah keys` slots `release-cosign-key` / `release-cosign-key-pw`, mirroring the existing `tauri-signing-key`/`-pw` convention), public key published to production at https://cdn.yah.dev/keys/yah-release.pub (R2 bucket yah-dev, key `keys/yah-release.pub`, via `yah cloud bucket put`). Verified live: `curl https://cdn.yah.dev/keys/yah-release.pub` returns the exact vaulted bytes, and a blob signed with the vaulted private key verifies clean against that CDN-served public key with real cosign (`cosign verify-blob --key <cdn-fetched-pub> --signature ... blob` -> Verified OK). This is the ticket's own original verification criterion, previously 'STILL UNVERIFIABLE' -- now actually proven, short of cutting a real tagged release.")
//! @yah:handoff("DISCOVERED + FIXED (unrelated to key/keyless threading, but blocks this ticket's own verification criterion and the pre-existing GHA path too): `sigstore/cosign-installer@v3`'s own action.yml defaults `cosign-release` to v3.0.6 (confirmed by reading the action.yml at that ref), and cosign 3.x deprecates/ignores `--output-signature`/`--output-certificate` under its new mandatory bundle-format -- verified against real v3.0.6 and v3.1.3 binaries: v3.1.3 hard-errors ('must specify --bundle'), v3.0.6 silently ignores the legacy flags, falls back to ephemeral keys, and blocks on an interactive Sigstore OAuth device-flow login (killed before completing -- no external auth happened). This means release.yml's tarball sign-blob steps -- keyless AND my new key-mode -- would break on the next real GHA-cut release, independent of QED. Fix: pinned `cosign-release: 'v2.6.5'` (current, actively-maintained 2.x line, released same day as 3.1.3) on all 10 `cosign-installer@v3` steps in release.yml. Verified against a real v2.6.5 binary: the exact argv shape release.yml and native.rs's CosignSigner both emit (--key/--output-signature/--output-certificate/--bundle) works cleanly.")
//! @yah:handoff("DISCOVERED + FIXED (real, live side effect I hit by accident): cosign sign-blob defaults `--tlog-upload=true` even in --key mode, so testing the (correct, already-merged) argv shape against a real cosign binary submitted a real, permanent, public Sigstore Rekor entry -- for the actual production release key, over a throwaway test string ('R605-F1 signing smoke test <ts>'), tlog index 2491165170. Not a secret leak (no private key or password exposed) but a genuine unplanned public disclosure I want on record. Root cause: SigningIdentity::Key's argv never disabled tlog upload, so every real QED key-signed release would silently phone home to a third-party public log too -- directly contradicting the point of key-based signing per W235 (work anywhere, no public-Sigstore-infra dependency). Fixed in oss/qed/crates/qed/src/native.rs `sign_blob_argv()`: Key arm now adds `--tlog-upload=false` (Keyless arm untouched -- public cert-transparency IS the point there). Mirrored the same flag into release.yml's 7 OCI `cosign sign --key` and 3 tarball `cosign sign-blob --key` invocations. Re-verified end-to-end after the fix: sign with --tlog-upload=false against the real vaulted key produces no tlog entry (confirmed by absence of the 'tlog entry created' log line cosign prints), and still verifies clean offline against the published public key.")
//! @yah:handoff("Local plaintext key material was cleaned up after vaulting/publishing (temp dirs under /var/folders and /tmp rm'd); the canonical copies live only in the encrypted vault + the published public key.")
//! @yah:verify("curl https://cdn.yah.dev/keys/yah-release.pub -- live, matches vaulted bytes exactly. VERIFIED.")
//! @yah:verify("cosign v2.6.5 sign-blob --key <vaulted release-cosign-key> --tlog-upload=false, then cosign verify-blob --key <cdn-fetched pub> -- 'Verified OK', no tlog entry created. VERIFIED live against real cosign, not just unit-tested argv.")
//! @yah:verify("cargo test -p yah-qed --lib native:: -- 17 pass, including new key_argv_disables_tlog_upload_but_keyless_does_not. VERIFIED.")
//! @yah:verify("cargo test -p yah-qed --lib (full) -- 876 pass. cargo test -p yah-qed-gha -- 133 pass (release.yml still round-trips with the cosign-release pin added). VERIFIED.")
//! @yah:verify("python3 yaml.safe_load + actionlint .github/workflows/release.yml -- clean, exit 0. VERIFIED.")
//! @yah:verify("yah keys get release-cosign-key / release-cosign-key-pw round-trip byte-identical to the generated cosign.key / password (only difference: yah keys get does not append cosign's own trailing newline, immaterial to PEM parsing). VERIFIED.")
//! @yah:gotcha("A public Sigstore Rekor entry (tlog index 2491165170) now exists for the production release public key, created by my own pre-fix testing, over the string 'R605-F1 signing smoke test <timestamp>' -- not a real release, not a secret leak, but a permanent public record. Flagging so nobody is confused seeing an odd entry in Rekor's log for this key later.")
//! @yah:gotcha("cosign-installer@v3's default cosign-release floats to whatever v3.0.6 (or later) currently is -- if this camp ever bumps that pin, re-verify --output-signature/--output-certificate/--key/--bundle/--tlog-upload against the new binary before trusting it in production; cosign 3.x's flag/bundle-format semantics are not backward compatible with what this whole pipeline (sig_url/cert_url schema, install.sh, cloud_init.rs) assumes.")
//! @yah:gotcha("R555-F5 (the per-run grant that hands the vaulted private key to a remote QED runner) is still not built -- QED_COSIGN_KEY is a bare env var read directly from process env (native runtime defaults env_passthrough=true), so today only a LOCAL `yah qed run release` with QED_COSIGN_KEY/COSIGN_PASSWORD manually exported (e.g. from `yah keys get release-cosign-key`) can actually sign with the vaulted key. A daemon-proxied run needs the daemon process itself to carry those env vars, or R555-F5's grant mechanism.")

use std::path::Path;

use indexmap::IndexMap;

use crate::expr::Value;
use crate::toolkit::ToolkitOutcome;

/// Per-call inputs handed to an [`ImageBuilder`] — mirrors
/// [`crate::toolkit::ToolkitCall`] (the handler is the injected analogue of a
/// toolkit action for the docker push family).
pub struct ImageBuildCall<'a> {
    /// The `uses:` slug minus `@ref` — `docker/login-action` or
    /// `docker/build-push-action`.
    pub slug: &'a str,
    /// Already-evaluated `with:` inputs (`${{ … }}` expanded, secrets resolved).
    pub with: &'a IndexMap<String, Value>,
    /// Composed step env (workflow + job + step).
    pub env: &'a IndexMap<String, String>,
    /// Working directory the workflow runs steps in (the checkout root).
    pub workspace: &'a Path,
}

/// Implementor contract for the injected image builder. Implemented by the qed
/// runner (`QedImageBuilder`), which owns the camp registry route/auth overlay
/// and the local-buildx / remote-fleet build execution.
pub trait ImageBuilder: Send + Sync {
    /// Handle one docker push-family step. `Err` is an unrecoverable
    /// spawn/IO/config failure (surfaces as [`crate::RuntimeError`]); a graceful
    /// `docker` non-zero exit rides back as
    /// [`ToolkitOutcome`]`{ conclusion: Failure, … }` so `continue-on-error` /
    /// `if: failure()` keep working.
    fn handle(&self, call: &ImageBuildCall<'_>) -> Result<ToolkitOutcome, String>;
}

/// The docker push-family slugs the runtime routes to an injected
/// [`ImageBuilder`]. These are exactly the slugs the tier classifier maps to
/// [`crate::tier::NativeReplacement::RegistryPublish`].
pub fn is_image_push_action(slug: &str) -> bool {
    matches!(slug, "docker/login-action" | "docker/build-push-action")
}
