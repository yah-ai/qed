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
//! @yah:at(2026-08-03T07:07:45Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
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
