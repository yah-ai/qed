//! Deriving a recipe's admission grant — the publish side of R555-F4 / W235 §(c).
//!
//! [`workload_spec::admission`] owns the grant *format* and the *verification*.
//! This module owns the one thing neither of those can: what a
//! [`TransformRecipe`] is allowed to run, expressed in that format.
//!
//! # Why the grant is derived rather than stored
//!
//! A signed recipe carries only two scalars — a signature and the key that
//! produced it (see [`RecipeAdmission`]). The grant document itself is
//! **recomputed** from the recipe at signing time and again at dispatch time, by
//! this function, and the signature is over those bytes.
//!
//! The alternative — pasting the encoded grant into the recipe TOML — would
//! duplicate the image, the argv and the placement into a second place that can
//! drift from the first. Deriving it means an edit to the recipe changes the
//! grant bytes, which invalidates the signature, which is exactly the behaviour
//! wanted: **editing a signed recipe un-signs it.**
//!
//! # The drift this module can still have, and what pins it
//!
//! [`grant_for_recipe_step`] has to predict what
//! [`build_workload_spec`](crate::remote) will produce, without calling it —
//! the grant must be computable at authoring time, when no `ForgeId` exists yet.
//! That is a duplicated mapping, and a duplicated mapping drifts.
//!
//! `admission_grant_covers_the_spec_the_driver_actually_builds` is what keeps it
//! honest: it lowers a real recipe both ways and asserts the derived grant
//! covers the driver-built spec. If someone adds a field to
//! `build_workload_spec` that the grant should cover, that test goes red rather
//! than a signed recipe silently failing admission on a live node an hour into a
//! build.

use serde::Deserialize;
use velveteen::TaskLocation;
use workload_spec::admission::{AdmissionGrant, GrantRuntime};
use workload_spec::TierTag;

use crate::executor::AdmissionEnvelope;
use crate::transforms::{RecipeSecret, RecipeStep, TransformRecipe};

/// The `[admission]` block of a signed recipe TOML.
///
/// Two scalars, no grant body — see the module docs for why. Written by
/// `cargo xtask recipe-sign`; absent on an unsigned recipe, which is legal and
/// runs anywhere the node's policy is not `required`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RecipeAdmission {
    /// Hex Ed25519 detached signature over the derived grant document.
    pub signature: String,
    /// Hex Ed25519 public key that produced `signature`. Present so a node can
    /// check attribution before touching the crypto, and so a human can tell
    /// which key to look for without re-deriving anything.
    pub key: String,
}

/// Why a grant could not be derived for a recipe step.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GrantDerivationError {
    /// A `local` recipe never reaches kamaji, so there is no gate to satisfy
    /// and no tier to name. Signing one would produce a document that can never
    /// be checked — a false assurance, which is worse than none.
    #[error(
        "recipe {recipe:?} is placed locally; an admission grant only means \
         something for a recipe dispatched to a node (W235 §(c))"
    )]
    NotRemote { recipe: String },
}

/// Derive the admission grant for one step of a remotely-placed recipe.
///
/// The grant deliberately carries no hash of the recipe FILE — see
/// [`workload_spec::admission`]'s module docs for why that field is circular
/// (signing rewrites the file) and over-binding (a comment edit would un-sign
/// the recipe). What identifies the recipe here is the grant's own body.
pub fn grant_for_recipe_step(
    recipe: &TransformRecipe,
    step: &RecipeStep,
) -> Result<AdmissionGrant, GrantDerivationError> {
    // Mirrors `build_workload_spec`'s own match. A node-pinned placement carries
    // no tier of its own and forge is conventionally infra.
    let tier = match &recipe.placement.location {
        TaskLocation::RemoteAny { tier, .. } => tier.clone(),
        TaskLocation::Remote { .. } => TierTag("infra".into()),
        TaskLocation::Local => {
            return Err(GrantDerivationError::NotRemote {
                recipe: recipe.name.clone(),
            })
        }
    };

    let native = matches!(recipe.placement.runtime, velveteen::TaskRuntime::Native);

    // A native forge is fork+exec'd, so `mark_native_exec` points `workdir` and
    // `YAH_PRODUCED_DIR` at the per-run host produced dir. Both are per-run, so
    // the workdir goes in as a template with the forge id as its hole and the
    // env var goes in by name only — which is all the grant ever constrains
    // about env.
    let (workdir, env_names) = if native {
        (
            Some(format!(
                "{}/{{{{forge_id}}}}",
                workload_spec::forge_produced::HOST_ROOT
            )),
            vec![crate::remote::PRODUCED_DIR_ENV.to_string()],
        )
    } else {
        (None, Vec::new())
    };

    Ok(AdmissionGrant {
        recipe: recipe.name.clone(),
        image: workload_spec::admission::image_ref_string(&recipe.image),
        tier: tier.0,
        runtime: if native {
            GrantRuntime::Native
        } else {
            GrantRuntime::Container
        },
        // `build_workload_spec` sets host networking on every remote forge
        // (R590-B7: builds git-clone their own sources and kamaji's default
        // netns has no egress), so every recipe grant admits it.
        host_network: true,
        // Only `build_image_workload_spec` sets the nested-sandbox marker, and a
        // transform recipe never lowers to `ForgeCommand::BuildImage`. A recipe
        // grant therefore never admits the widening — which is the strict
        // direction, and the one R636-B2's coupling wants.
        nested_sandbox: false,
        workdir,
        // `WorkloadSpec::for_forge` leaves `entrypoint` unset and nothing on the
        // recipe path fills it; argv carries the whole invocation.
        entrypoint: Vec::new(),
        // The UNSUBSTITUTED argv — holes intact. That is the point: the grant
        // describes the shape the author wrote, and the dispatcher's
        // substitution has to fit it.
        argv: step.argv.clone(),
        env_names,
        // R555-F5: the vault credentials this recipe declared, and nothing
        // else. Empty on a recipe with no `[[secrets]]` block, which is every
        // recipe in the tree today — and empty is the refusing direction, so a
        // recipe that has not asked for a credential cannot read one.
        secrets: recipe.secrets.iter().map(RecipeSecret::to_grant).collect(),
    })
}

/// Derive the grant and pair it with the recipe's stored signature, ready to
/// hand to [`ExecContext::with_admission`](crate::ExecContext::with_admission).
///
/// `None` when the recipe is unsigned — the dispatcher then sends no admission
/// annotations at all, which a node running `permissive` accepts and one running
/// `required` refuses. That is the intended rollout gradient, not an oversight.
pub fn envelope_for_recipe_step(
    recipe: &TransformRecipe,
    step: &RecipeStep,
) -> Result<Option<AdmissionEnvelope>, GrantDerivationError> {
    let Some(signed) = &recipe.admission else {
        return Ok(None);
    };
    let grant = grant_for_recipe_step(recipe, step)?;
    Ok(Some(AdmissionEnvelope {
        grant: grant.encode(),
        signature: signed.signature.clone(),
        public_key: signed.key.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecContext;
    use velveteen::{ForgeCommand, ForgeId, ForgeSpec, Initiator, MeshAccess, TaskPlacement,
        TaskRuntime};
    use workload_spec::ImageRef;

    fn recipe(location: TaskLocation, runtime: TaskRuntime) -> TransformRecipe {
        let toml = format!(
            r#"
name  = "rusty-v8-musl"
label = "Build rusty_v8 from source"
image = "ghcr.io/yah-ai/b:v1@sha256:{}"
[placement]
location = {}
runtime  = "{}"
[[steps]]
name = "build"
argv = ["build-v8.sh '{{{{target}}}}' '{{{{YAH_TRANSFORM_OUT}}}}'"]
timeout = 9000
"#,
            "a".repeat(64),
            match &location {
                TaskLocation::Local => "\"local\"".to_string(),
                TaskLocation::Remote { node, .. } => {
                    format!("{{ kind = \"remote\", node = \"{}\" }}", node.0)
                }
                TaskLocation::RemoteAny { tier, mesh_tags } => format!(
                    "{{ kind = \"remote_any\", tier = \"{}\", mesh_tags = {:?} }}",
                    tier.0, mesh_tags
                ),
            },
            match runtime {
                TaskRuntime::Container => "container",
                TaskRuntime::Native => "native",
                TaskRuntime::MicroVm => "microvm",
            }
        );
        toml::from_str(&toml).expect("recipe fixture parses")
    }

    fn remote_any() -> TransformRecipe {
        recipe(
            TaskLocation::RemoteAny {
                tier: TierTag("infra".into()),
                mesh_tags: vec!["tier:x86".into()],
            },
            TaskRuntime::Container,
        )
    }

    #[test]
    fn a_local_recipe_cannot_be_granted() {
        let r = recipe(TaskLocation::Local, TaskRuntime::Container);
        assert!(matches!(
            grant_for_recipe_step(&r, &r.steps[0]).unwrap_err(),
            GrantDerivationError::NotRemote { .. }
        ));
    }

    #[test]
    fn the_grant_carries_the_unsubstituted_argv() {
        let r = remote_any();
        let g = grant_for_recipe_step(&r, &r.steps[0]).unwrap();
        assert_eq!(g.argv, vec![r.steps[0].argv[0].clone()]);
        assert!(g.argv[0].contains("{{YAH_TRANSFORM_OUT}}"));
        assert_eq!(g.tier, "infra");
        assert!(g.host_network);
        assert!(!g.nested_sandbox);
    }

    #[test]
    fn a_recipe_edit_changes_the_signed_bytes() {
        // "Editing a signed recipe un-signs it" — the property the derive-rather
        // -than-store choice exists for.
        let a = remote_any();
        let mut b = a.clone();
        b.steps[0].argv[0] = "evil.sh".into();
        assert_ne!(
            grant_for_recipe_step(&a, &a.steps[0]).unwrap().encode(),
            grant_for_recipe_step(&b, &b.steps[0]).unwrap().encode(),
        );
    }

    #[test]
    fn an_unsigned_recipe_yields_no_envelope() {
        let r = remote_any();
        assert_eq!(envelope_for_recipe_step(&r, &r.steps[0]).unwrap(), None);
    }

    #[test]
    fn a_signed_recipe_yields_the_derived_grant_with_its_stored_signature() {
        let mut r = remote_any();
        r.admission = Some(RecipeAdmission {
            signature: "aa".into(),
            key: "bb".into(),
        });
        let env = envelope_for_recipe_step(&r, &r.steps[0]).unwrap().unwrap();
        assert_eq!(env.signature, "aa");
        assert_eq!(env.public_key, "bb");
        assert_eq!(
            env.grant,
            grant_for_recipe_step(&r, &r.steps[0]).unwrap().encode()
        );
    }

    /// The anti-drift pin. `grant_for_recipe_step` predicts what
    /// `build_workload_spec` produces without calling it (the grant must be
    /// computable before a `ForgeId` exists); this drives both and asserts they
    /// still agree, for the container and the native shape.
    ///
    /// R555-F5 widened it to carry a declared secret through, because the
    /// grant's allow-list and the mount the dispatcher attaches are derived by
    /// two different functions (`RecipeSecret::to_grant` / `::to_mount`) and a
    /// drift between them means a signed recipe that refuses itself on the node,
    /// an hour into a build.
    #[test]
    fn admission_grant_covers_the_spec_the_driver_actually_builds() {
        for runtime in [TaskRuntime::Container, TaskRuntime::Native] {
            let mut r = recipe(
                TaskLocation::RemoteAny {
                    tier: TierTag("infra".into()),
                    mesh_tags: vec!["tier:x86".into()],
                },
                runtime,
            );
            r.secrets = vec![RecipeSecret {
                cluster: "r2-write".into(),
                path: std::path::PathBuf::from("/run/yah/r2.json"),
                mode: 0o400,
            }];
            let step = &r.steps[0];
            let grant = grant_for_recipe_step(&r, step).unwrap();

            // What the reconciler dispatches: argv with the holes filled in.
            let substituted = vec![
                "build-v8.sh 'x86_64-unknown-linux-musl' '/yah/produced/deadbeef.out'".to_string(),
            ];
            let spec = ForgeSpec {
                command: ForgeCommand::Subprocess {
                    argv: substituted,
                    image: Some(r.image.clone()),
                },
                where_: TaskPlacement::new(r.placement.location.clone(), r.placement.runtime),
                timeout: None,
                label: None,
                initiator: Initiator::Gnome {
                    camp: "test".into(),
                    shift: "test".into(),
                },
                mesh_access: MeshAccess::default(),
            };
            let forge_id = ForgeId::new();
            let mut ws = crate::remote::build_workload_spec(&forge_id, &spec).unwrap();
            let ctx = ExecContext::default()
                .with_secrets(r.secrets.iter().map(RecipeSecret::to_mount).collect());
            crate::remote::apply_exec_context(&mut ws, &ctx).unwrap();
            assert_eq!(ws.secrets.len(), 1, "{runtime:?}: the mount must reach the spec");

            grant
                .covers(&ws)
                .unwrap_or_else(|e| panic!("{runtime:?}: derived grant does not cover the \
                     spec build_workload_spec produced: {e}"));
        }
    }

    /// A recipe that declares nothing gets a grant that admits nothing — the
    /// direction that matters, since every recipe in the tree is in this state.
    #[test]
    fn a_recipe_with_no_secrets_block_is_granted_none() {
        let r = remote_any();
        assert!(grant_for_recipe_step(&r, &r.steps[0]).unwrap().secrets.is_empty());
    }

    #[test]
    fn adding_a_secret_to_a_signed_recipe_un_signs_it() {
        // Same property `a_recipe_edit_changes_the_signed_bytes` pins for argv:
        // the credential list is inside the signature, so it cannot be widened
        // after the fact.
        let a = remote_any();
        let mut b = a.clone();
        b.secrets = vec![RecipeSecret {
            cluster: "cosign-signing-key".into(),
            path: std::path::PathBuf::from("/run/yah/cosign.key"),
            mode: 0o400,
        }];
        assert_ne!(
            grant_for_recipe_step(&a, &a.steps[0]).unwrap().encode(),
            grant_for_recipe_step(&b, &b.steps[0]).unwrap().encode(),
        );
    }

    /// Guards the one field the grant deliberately leaves to a structural rule.
    #[test]
    fn image_is_pinned_by_digest_in_the_grant() {
        let r = remote_any();
        let g = grant_for_recipe_step(&r, &r.steps[0]).unwrap();
        assert!(g.image.contains(&r.image.digest));
        assert_eq!(
            g.image,
            workload_spec::admission::image_ref_string(&ImageRef {
                registry: r.image.registry.clone(),
                repository: r.image.repository.clone(),
                tag: r.image.tag.clone(),
                digest: r.image.digest.clone(),
            })
        );
    }
}
