//! `velveteen-exec::routing` — one [`ForgeExecutor`] that picks a driver from
//! the spec's own placement (R555-F3).
//!
//! Consumers that hold a single `Arc<dyn ForgeExecutor>` and run *whatever a
//! recipe declares* — the cloud reconciler's materialize step is the one this
//! was written for — cannot pick a driver up front: one asset's recipe says
//! `location = "local"` and the next says `remote_any { mesh_tags = [...] }`,
//! and both flow through the same injected executor. Before this existed the
//! choice was made at injection time, which meant whichever driver was chosen
//! refused half the recipes (R555-T2 made both drivers refuse the placement
//! they don't own, precisely so the mistake could not run silently).
//!
//! The remote side is built **lazily**. Standing up a
//! [`RemoteForgeDriver`](crate::remote::RemoteForgeDriver) means loading the
//! camp's machine inventory and opening a scryer store, and most applies never
//! touch a remote recipe — so a camp with no cloud declared must not start
//! paying for one, nor fail an all-local apply because it has no machines.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use velveteen::{ForgeSpec, TaskLocation};

use crate::executor::{ExecContext, ExecEvent, ExecOutcome, ForgeExecutor, ForgeExecutorError};

/// Builds the remote driver on first use. Returns a human-readable reason on
/// failure — it is surfaced verbatim to the operator, so it should name what
/// could not be loaded (machine inventory, scryer store), not just "error".
pub type RemoteExecutorFactory =
    Arc<dyn Fn() -> Result<Arc<dyn ForgeExecutor>, String> + Send + Sync>;

/// Routes each [`ForgeSpec`] to the driver its `placement.location` names.
pub struct PlacementRouter {
    local: Arc<dyn ForgeExecutor>,
    remote_factory: Option<RemoteExecutorFactory>,
    /// Built at most once, on the first remotely-placed spec. `Mutex` rather
    /// than `OnceCell` because the factory is fallible and we want a retry on
    /// the next asset rather than a permanently poisoned cell.
    remote: Mutex<Option<Arc<dyn ForgeExecutor>>>,
}

impl PlacementRouter {
    /// Route local specs to `local` and refuse remote ones. Use this when the
    /// caller genuinely has no cloud to dispatch to — the refusal names the
    /// missing wiring instead of silently building on the wrong host.
    pub fn local_only(local: Arc<dyn ForgeExecutor>) -> Self {
        Self {
            local,
            remote_factory: None,
            remote: Mutex::new(None),
        }
    }

    /// Route local specs to `local`, and remotely-placed ones to a driver built
    /// on demand by `factory`.
    pub fn new(local: Arc<dyn ForgeExecutor>, factory: RemoteExecutorFactory) -> Self {
        Self {
            local,
            remote_factory: Some(factory),
            remote: Mutex::new(None),
        }
    }

    async fn remote_executor(&self) -> Result<Arc<dyn ForgeExecutor>, ForgeExecutorError> {
        let factory = self.remote_factory.as_ref().ok_or_else(|| {
            ForgeExecutorError::Remote(
                "this executor has no remote driver wired, so a recipe declaring a remote \
                 placement cannot be dispatched. Inject a router built with a mesh yubaba \
                 client (see `yah cloud apply`), or set the recipe's placement.location = \
                 \"local\""
                    .to_string(),
            )
        })?;
        let mut slot = self.remote.lock().await;
        if let Some(existing) = slot.as_ref() {
            return Ok(existing.clone());
        }
        let built = factory().map_err(ForgeExecutorError::Remote)?;
        *slot = Some(built.clone());
        Ok(built)
    }
}

#[async_trait]
impl ForgeExecutor for PlacementRouter {
    async fn execute(
        &self,
        spec: ForgeSpec,
        ctx: ExecContext,
        sink: Option<UnboundedSender<ExecEvent>>,
    ) -> Result<ExecOutcome, ForgeExecutorError> {
        if matches!(spec.where_.location, TaskLocation::Local) {
            return self.local.execute(spec, ctx, sink).await;
        }
        let remote = self.remote_executor().await?;
        remote.execute(spec, ctx, sink).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use velveteen::{ForgeCommand, ForgeStatus, Initiator, MeshAccess, TaskPlacement, TaskRuntime};
    use workload_spec::{MeshIdent, TierTag};

    /// Records how many specs it saw and always succeeds.
    struct Counting {
        label: &'static str,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ForgeExecutor for Counting {
        async fn execute(
            &self,
            _spec: ForgeSpec,
            _ctx: ExecContext,
            _sink: Option<UnboundedSender<ExecEvent>>,
        ) -> Result<ExecOutcome, ForgeExecutorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ExecOutcome {
                status: ForgeStatus::Done {
                    exit_code: 0,
                    ended_at: 0,
                },
                stderr_tail: self.label.to_string(),
            })
        }
    }

    fn spec_at(location: TaskLocation) -> ForgeSpec {
        ForgeSpec {
            command: ForgeCommand::Subprocess {
                argv: vec!["true".to_string()],
                image: None,
            },
            where_: TaskPlacement::new(location, TaskRuntime::Native),
            timeout: None,
            label: None,
            initiator: Initiator::Human {
                camp: "test".into(),
            },
            mesh_access: MeshAccess::default(),
            cache_key: None,
        }
    }

    fn remote_any() -> TaskLocation {
        TaskLocation::RemoteAny {
            tier: TierTag("infra".into()),
            mesh_tags: Vec::new(),
        }
    }

    fn remote_node() -> TaskLocation {
        TaskLocation::Remote {
            node: MeshIdent("us-west-002".into()),
        }
    }

    #[tokio::test]
    async fn local_specs_never_reach_the_remote_side() {
        let local_calls = Arc::new(AtomicUsize::new(0));
        let built = Arc::new(AtomicUsize::new(0));
        let built_for_factory = built.clone();
        let router = PlacementRouter::new(
            Arc::new(Counting {
                label: "local",
                calls: local_calls.clone(),
            }),
            Arc::new(move || {
                built_for_factory.fetch_add(1, Ordering::SeqCst);
                Err("must not be built".to_string())
            }),
        );

        let out = router
            .execute(spec_at(TaskLocation::Local), ExecContext::default(), None)
            .await
            .expect("local spec routes to the local driver");
        assert_eq!(out.stderr_tail, "local");
        assert_eq!(local_calls.load(Ordering::SeqCst), 1);
        // The whole point of the lazy factory: an all-local apply must not pay
        // for (or fail on) a cloud it never dispatches to.
        assert_eq!(built.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn remote_specs_build_the_remote_driver_once_and_reuse_it() {
        let remote_calls = Arc::new(AtomicUsize::new(0));
        let built = Arc::new(AtomicUsize::new(0));
        let remote_for_factory = remote_calls.clone();
        let built_for_factory = built.clone();
        let router = PlacementRouter::new(
            Arc::new(Counting {
                label: "local",
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::new(move || {
                built_for_factory.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(Counting {
                    label: "remote",
                    calls: remote_for_factory.clone(),
                }) as Arc<dyn ForgeExecutor>)
            }),
        );

        for _ in 0..2 {
            let out = router
                .execute(
                    spec_at(remote_any()),
                    ExecContext::default(),
                    None,
                )
                .await
                .expect("remote spec routes to the remote driver");
            assert_eq!(out.stderr_tail, "remote");
        }
        assert_eq!(remote_calls.load(Ordering::SeqCst), 2);
        assert_eq!(built.load(Ordering::SeqCst), 1, "driver built once, reused");
    }

    #[tokio::test]
    async fn local_only_router_refuses_a_remote_spec_by_naming_the_missing_wiring() {
        let router = PlacementRouter::local_only(Arc::new(Counting {
            label: "local",
            calls: Arc::new(AtomicUsize::new(0)),
        }));
        let err = router
            .execute(
                spec_at(remote_node()),
                ExecContext::default(),
                None,
            )
            .await
            .expect_err("no remote driver wired");
        let msg = err.to_string();
        assert!(msg.contains("no remote driver wired"), "{msg}");
    }

    #[tokio::test]
    async fn a_failed_factory_is_retried_on_the_next_spec() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_factory = attempts.clone();
        let router = PlacementRouter::new(
            Arc::new(Counting {
                label: "local",
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::new(move || {
                let n = attempts_for_factory.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err("machine inventory not readable".to_string())
                } else {
                    Ok(Arc::new(Counting {
                        label: "remote",
                        calls: Arc::new(AtomicUsize::new(0)),
                    }) as Arc<dyn ForgeExecutor>)
                }
            }),
        );

        let err = router
            .execute(
                spec_at(remote_node()),
                ExecContext::default(),
                None,
            )
            .await
            .expect_err("first build fails");
        assert!(err.to_string().contains("machine inventory"), "{err}");

        let out = router
            .execute(
                spec_at(remote_node()),
                ExecContext::default(),
                None,
            )
            .await
            .expect("second attempt rebuilds");
        assert_eq!(out.stderr_tail, "remote");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
