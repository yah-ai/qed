//! Participant sets (R823-F2) — **N hosts concurrently inside ONE run, each
//! knowing the others' addresses, producing one verdict.**
//!
//! # What was missing, precisely
//!
//! QED could already put a step on another machine: a `native = true` step
//! whose target is foreign to the host resolves to
//! [`Offload`](crate::platform::Resolution::Offload) and is dispatched to an
//! arch-matched build-worker (R590-F4), with its build context carried across
//! the host boundary (R590-F2 / R636-B1). And
//! [`Placement`](crate::types::Placement) says *whether* a run may happen here
//! (`local-only | ci-only | anywhere`).
//!
//! Neither of those is a participant set. Offload is one host doing work *for*
//! the coordinator; `[pipeline.matrix]` fans a step into rows that are
//! independent jobs and never learn of each other. A multi-node etude — two
//! machines exchanging packets over a real LAN, asserting on what the LAN did
//! to them — needs all of the following at once, and had no expression:
//!
//! 1. **Rendezvous.** A participant needs the *others'* addresses before its
//!    own first step runs. So the addressing is allocated by the plan and
//!    injected before dispatch. It is never discovered mid-run.
//! 2. **Verdict.** One participant failing fails the run, and a participant
//!    that *never started* must be distinguishable from one that started and
//!    failed — for a fleet those are different diagnoses ("the node is
//!    unplugged" vs "the code is wrong") and collapsing them is how hardware
//!    CI earns its reputation for flakiness.
//! 3. **Teardown.** A participant left running after a peer dies holds a
//!    machine. That is the leak that *reads* as flakiness.
//!
//! # The shape
//!
//! ```toml
//! [pipeline.participants]
//! port_base = 34500
//!
//! [pipeline.participants.role.runner]
//! coordinator = true            # exactly one role is the coordinator
//!
//! [pipeline.participants.role.responder]
//! node    = "us-west-011"       # dispatch target: a NAMED node, never a tag match
//! address = "100.64.0.11"       # what peers dial
//! ports   = ["clock"]           # QED assigns the numbers
//!
//! [[pipeline.steps]]
//! name        = "responder"
//! participant = "responder"
//! background  = true
//! background_until = "runner"
//! argv = ["clock_case_responder"]
//!
//! [[pipeline.steps]]
//! name        = "runner"
//! participant = "runner"
//! needs       = ["responder"]
//! argv = ["clock_case_runner"]
//! ```
//!
//! One role is one participant on one host. There is deliberately no `count`
//! fan-out: every instance of a role would share that role's single node and
//! address, so N-of-a-role would be N processes on one box wearing a
//! distributed costume. N hosts is spelled as N roles, which is also the only
//! spelling under which each host's address is a stated fact rather than an
//! inferred one.
//!
//! # Why the node is named and not tag-matched
//!
//! [`TaskLocation::RemoteAny`](velveteen::TaskLocation::RemoteAny) picks a node
//! at *dispatch* time, by admission. That is fine for a build — nobody needs to
//! know where it landed until it is done — and fatal for a rendezvous, whose
//! whole content is knowing the address *before* the first step runs. So a
//! remote participant pins [`TaskLocation::Remote`](velveteen::TaskLocation)
//! to the named node, and requirement (1) above is met by construction rather
//! than by a discovery protocol that could fail late.
//!
//! # Why `address` is declared rather than resolved
//!
//! QED has no machine inventory; yah's lives in `.yah/infra/machines/*.toml`,
//! outside this crate on purpose (the same reason
//! [`BuildContextPublisher`](crate::build_context::BuildContextPublisher) is a
//! trait declared here and implemented where the credentials are). Declaring it
//! also keeps two genuinely different facts apart: the node QED *dispatches to*
//! and the address peers *dial* are not the same thing the moment a node has
//! more than one interface. A resolver seam can fill an omitted `address` later
//! without changing any of this vocabulary.
//!
//! # Ports are assigned, not probed
//!
//! Numbers are handed out sequentially from `port_base` in declaration order,
//! and the participant binds what it was told. QED does not check that a port
//! is free: from the coordinator there is no way to learn anything true about a
//! remote node's port table, so a probe would buy a false sense of having
//! checked. A participant that cannot bind fails loudly, at itself, naming the
//! port — which is the diagnosis you want.
//!
//! @yah:ticket(R823-T3, "Prove a participant set on two real hosts — first live run of R823-F2's remote sidecar dispatch + teardown")
//! @yah:status(review)
//! @yah:at(2026-09-03T22:34:14Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R823)
//! @arch:see(.yah/docs/working/W235-remote-qed.md)
//! @yah:depends_on(R823-F2)
//! @yah:next("Tier: Warrior — the code is written and unit-tested; this is operating real hardware and reading what it actually does, which is judgment plus patience rather than design.")
//! @yah:next("WHAT IS ALREADY TRUE, so nobody rebuilds it. R823-F2 shipped the whole participant-set layer: [pipeline.participants] parsing + allocation (oss/qed/crates/qed/src/participants.rs), rendezvous env injected at all three exec sites, node-bound placement, remote sidecar dispatch (PipelineRunner::spawn_remote_participant_step) and explicit teardown via RemoteForgeDriver::kill in reap_background. 932 unit tests green. What has NEVER happened is a participant set running on two real machines.")
//! @yah:next("THE FIRST RUN. Smallest honest case: a two-role pipeline whose coordinator is local and whose one peer is pinned to a real node (us-west-002 or us-west-011 — check .yah/infra/machines/*.toml for the current mesh address before writing it into `address`, do not copy 100.64.0.11 out of the doc examples). Peer argv = something that binds its assigned port and answers; coordinator argv = something that dials it and exits 0. Read $QED_PARTICIPANTS with jq rather than inventing a second addressing convention.")
//! @yah:next("THE THREE THINGS TO ACTUALLY CHECK, in order of what is most likely wrong. (1) REACHABILITY: the peer binds the QED-assigned port inside a forge container that has host networking (HOST_NETWORK_ANNOTATION, R590-B7) — verify the coordinator can actually reach address:port, because that inference is the single least-tested link in the chain and it was read out of velveteen-exec rather than observed. (2) TEARDOWN: after the run, confirm NOTHING is left running on the node — `yah cloud` / a yubaba workload list, not just a green card. A leaked participant is the failure this feature exists to prevent and it is invisible from the coordinator. (3) NEVER-STARTED: deliberately point a participant at a powered-off or nonexistent node and confirm the run reports 'never started ... fleet fault' on failure_reason rather than a test failure.")
//! @yah:next("EXPECT FINDINGS AND WRITE THEM DOWN. Add what the first run teaches to the '§Participant sets (R823-F2)' section of .yah/docs/working/W235-remote-qed.md — that section currently ends with an explicit 'Not proven against hardware' paragraph, and retiring it is part of this ticket.")
//! @yah:gotcha("THE SECOND CONSUMER IS IN ANOTHER CAMP AND IS WAITING ON THIS. noisetable's .yah/qed/clock-cases.fleet.toml is the motivating case (W160 Call 2, noisetable R675-T4): its clock_case_runner currently SSHes responders up itself via SshPeers, and the plan is for the participant set to replace that while the case list, assertions and verdict rule stay put. Do NOT migrate it as part of this ticket — prove the mechanism on a throwaway pipeline first. A migration that fails tells you nothing about which half was wrong.")
//! @yah:handoff("PROVEN ON TWO REAL HOSTS. A participant set ran with the coordinator local on the camp Mac and one `responder` pinned to us-west-003; the coordinator dialled 100.64.0.9:34500 — the node's mesh address plus a QED-assigned port, neither written down anywhere on either side — and got its token back. Both halves read $QED_PARTICIPANTS and look each other up by role name. Host networking (HOST_NETWORK_ANNOTATION, R590-B7) does exactly what W235 inferred it would; that paragraph is now observation rather than inference. Case kept as .yah/qed/participant-smoke.toml, negative twin as .yah/qed/participant-never-started.toml. us-west-003 chosen over the ticket's suggested 002/011 because R833-F8's node-pin-smoke already proved remote dispatch there and it is the only node with the image pre-pulled — a first run should fail on the participant layer, not on a node bring-up.")
//! @yah:handoff("FINDING 1, AND IT BLOCKED THE FIRST RUN ENTIRELY. Fleet wiring did not know participant sets existed. `pipeline_needs_offload` — the CLI's and the daemon's \"do I need a mesh dispatcher\" question — was computed purely from each step's platform resolution, and a participant step declares no `platform`, so on the camp Mac every one resolved NativeCross, the answer was false, and `--where=auto` on a pipeline whose entire content is a rendezvous died on \"no remote dispatcher is wired\". Placement there is a property of the BINDING, not of the target triple. Fixed with a new `pipeline_has_node_bound_participant` (oss/qed/crates/qed/src/runner.rs), folded into `pipeline_needs_offload` so every call site gets it at once — CLI in-process, CLI proxy gate, daemon needs_fleet, matrix fanout — rather than patching the one site I happened to be standing on. Read off the raw declaration, not off an allocated plan, so a mis-declared set still fails with `participants::plan_for`'s own diagnosis instead of as a silently driverless run. The CLI's `--where=auto` notice now says which of the two reasons fired; saying \"native cross-arch step\" for a participant set sends the reader hunting a `platform` block that does not exist.")
//! @yah:handoff("FINDING 2 — TEARDOWN LEAKS ON THE LIVE FLEET, AND R823-F2 COULD NOT SEE IT. Filed as R823-B4 (depends_on R854), because the remedy is a fleet roll, not code. Five runs, every one green, every one leaving its responder RUNNING on us-west-003 holding port 34500. Proved at the RPC: POST /workloads/forge.<id>/destroy — the exact dotted mesh ident RemoteForgeDriver::kill sends — answers {\"status\":\"destroyed\"} on the fleet's 0.8.28 while ctr shows the task RUNNING and ss shows the port bound. The client-side call is correct; the node is old. R854's reap_task (kills, WAITS on the shim exit, deletes, re-probes) first appears in-tree at aeb0f08c/v0.8.31 and is on no node. WHAT I FIXED HERE is the invisibility, which was ours: reap_background dropped the teardown result on the floor (`let _ = ...kill(&id).await`) on the reasoning that a best-effort cleanup must not bury a real failure — right about the status, wrong about the silence. It now reports the kill error, or, when the RPC reported success and the sidecar has not stopped within REMOTE_TEARDOWN_SETTLE (20s, sized above kamaji's own 15s reap budget), a line naming the workload AND the node. Status is unchanged either way: a held machine is an infrastructure fault and the coordinator still owns the verdict. Emitted as the participant's own stderr, so it rides every rail a step's output already rides with no schema change.")
//! @yah:handoff("WHY THE LEAK IS WORSE THAN A HELD MACHINE, which is the thing to carry forward: run 2 passed on its FIRST dial with no retry because run 1's leaked responder was still listening at the same address. Every run of a given set is handed the same address and the same assigned port, so a leftover is indistinguishable from a healthy peer and a leak makes the NEXT run green. A run that dispatches a fresh responder always needs one `Connection refused` retry while the container comes up; that retry's absence is the tell.")
//! @yah:handoff("FINDING 3 — THE VERDICT RULE WAS COMPUTED ON EVERY RUN AND RENDERED ON NONE. The never-started case dispatches to a node that does not exist; yubaba refuses it with an excellent message (no candidates matching required.nodes=[us-west-404], plus every declared machine) and participants::verdict duly produced \"1 of 2 participants never started — this is a fleet fault, not a test failure\". `yah qed run` printed two ✗ rows and \"ended with status Failed\" and threw the summary away: render_run (app/yah/cli/src/qed.rs) never read QedRunMeta::failure_reason. The entire point of the verdict rule is a distinction the operator can SEE, so in practice it had not shipped. One `if let` fixes it, and it is not participant-specific — every producer of failure_reason was equally invisible on that path.")
//! @yah:handoff("FIXED IN PASSING, NOT MINE: waitfor::tests::tcp_probe_fails_against_a_dead_port is flaky. It binds an ephemeral port, drops it, and assumes nothing takes the number before the probe — on this camp's shared machine something did, once, mid-verification, and it passed on the very next run. Now retries with a freshly-allocated port up to five times, so losing the race once is not a red bar and losing it five times in a row says to go look at probe_tcp_once. W235's §Participant sets rewritten: the \"Not proven against hardware\" paragraph is retired and replaced with the four findings, and the reachability paragraph is downgraded from inference to observation with a date.")
//! @yah:verify("CHECK 1 REACHABILITY — PASS, observed not inferred. `yah qed run participant-smoke --in-process` → `runner: got b'R823-T3-OK\\n' from 100.64.0.9:34500`, the coordinator on the camp Mac (192.168.10.31) reaching a container on us-west-003 over the mesh address from .yah/infra/machines/us-west-003.toml [registration].mesh_ipv4. Reproduced on five separate runs.")
//! @yah:verify("CHECK 2 TEARDOWN — FAILS ON THE FLEET, now loud. Checked on the node with `sudo ctr -n yah tasks ls`, `sudo ss -ltnp | grep :34500` and GET /workloads, never off the green card. Final run emits: \"qed: teardown: participant workload forge.96f070be-... did not stop within 20s of a teardown that reported success — it may still be RUNNING on us-west-003, holding its assigned port.\" Leak filed as R823-B4. Every responder this ticket started was hand-killed afterwards; us-west-003 has no RUNNING task and port 34500 is free as of sign-off.")
//! @yah:verify("CHECK 3 NEVER-STARTED — PASS after the render fix. `yah qed run participant-never-started --in-process` exits 1 and prints: \"reason: 1 of 2 participants never started — this is a fleet fault, not a test failure: `responder` never started: step `responder` was never accepted by node `us-west-404`\". Before the fix the same run printed only two ✗ rows and \"ended with status Failed\".")
//! @yah:verify("BUILD BAR. `cd oss/qed && cargo test -p yah-qed --lib` → 935 passed / 0 failed / 1 ignored (up from 934, one new test `a_node_bound_participant_needs_fleet_wiring_on_any_host` pinning that a node-bound role needs fleet wiring on EITHER host — unlike a cross-arch build, which stops needing the fleet on the matching arch — and that an all-local set still needs nothing). `cargo test -p yah --lib qed::` → 7 passed / 0 failed. `cargo xtask install --profile debug` clean, PATH resolves there, and every hardware run above was made with the freshly installed binary (verified by `strings`, not by mtime). One install attempt failed mid-session on a peer's transient non-compiling kamaji-proto edit; it was theirs, it cleared on its own, and nothing of theirs was touched.")
//! @yah:cleanup("The teardown note has hardware verification but no unit test. Covering it needs a RemoteSidecar, which holds a concrete Arc&lt;RemoteForgeDriver&gt; and therefore a scryer plus a WardenClient — buildable with velveteen-exec's test_support and a paused tokio clock, but a bigger fixture than the change. Worth adding when someone next has that fixture open.")
//! @yah:assumes("NOT PROVEN, stated rather than implied: only ONE remote participant, and no set with two remote peers that talk to each other rather than to the coordinator. Left alone deliberately — with teardown a no-op on the shipped fleet (R823-B4), an N-peer set leaks N machines per run. The noisetable clock-cases migration (W160 Call 2, noisetable R675-T4) was deliberately not attempted, per this ticket's own gotcha.")

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// First port number handed out when the set declares no `port_base`.
///
/// Chosen inside the IANA dynamic/private range (49152–65535 is the strict
/// reading; 32768+ is what Linux actually uses for ephemeral ports) but *below*
/// the Linux default ephemeral floor of 32768 — an assigned port must not
/// collide with one the kernel might hand to an unrelated outbound socket on
/// the same host mid-run.
pub const DEFAULT_PORT_BASE: u16 = 34500;

/// The address a participant with no `node` (i.e. one that runs on the host
/// that kicked the run) is dialled at.
pub const LOCAL_ADDRESS: &str = "127.0.0.1";

/// Env var carrying the whole allocated set as JSON — see
/// [`ParticipantPlan::rendezvous_env`].
pub const ENV_PARTICIPANTS: &str = "QED_PARTICIPANTS";

/// Env var naming which participant *this* step is, so one binary can be both
/// halves of a case.
pub const ENV_PARTICIPANT_SELF: &str = "QED_PARTICIPANT_SELF";

/// One declared role in `[pipeline.participants.role.<name>]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ParticipantSpec {
    /// The named fleet node this role's steps are dispatched to. Omitted ⇒ the
    /// host that kicked the run. Never a tag match — see the module header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// The address peers dial to reach this participant. Required whenever
    /// [`node`](Self::node) is set; defaults to [`LOCAL_ADDRESS`] otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// Named ports QED assigns numbers for. The names are the participant's own
    /// vocabulary (`"clock"`, `"control"`); QED only guarantees each gets a
    /// distinct number and that every participant is told all of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<String>,
    /// Exactly one role carries this. The coordinator's exit status is the
    /// run's verdict; every other participant is a peer it talks to.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub coordinator: bool,
}

/// A pipeline's `[pipeline.participants]` block.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ParticipantSet {
    /// First port number assigned; subsequent ports count up from here across
    /// the whole set. Defaults to [`DEFAULT_PORT_BASE`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_base: Option<u16>,
    /// Declared roles, in file order — which is also port-assignment order, so
    /// the numbers a reader computes by hand match the ones QED hands out.
    #[serde(default, rename = "role")]
    #[cfg_attr(
        feature = "json-schema",
        schemars(schema_with = "crate::types::permissive_schema")
    )]
    pub roles: IndexMap<String, ParticipantSpec>,
}

/// One allocated participant: a role bound to a host, an address and concrete
/// port numbers. This is what [`ParticipantPlan::rendezvous_env`] serializes,
/// and what every participant in the run is told about every other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    /// The role name — also this participant's id in
    /// [`ENV_PARTICIPANT_SELF`].
    pub name: String,
    /// `None` ⇒ the host that kicked the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    pub address: String,
    /// Port name → assigned number. `BTreeMap` so the JSON is byte-stable for a
    /// given set, which is what lets a test assert on it.
    pub ports: BTreeMap<String, u16>,
    pub coordinator: bool,
}

/// The allocated set: every role resolved to a host, an address and ports,
/// computed **before** any step is dispatched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantPlan {
    participants: Vec<Participant>,
}

/// Everything that can be wrong with a declared set, caught at plan time so no
/// step of a mis-declared run ever starts.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParticipantError {
    #[error("[pipeline.participants] declares no roles — drop the block or declare one")]
    Empty,
    #[error(
        "[pipeline.participants] declares no coordinator — exactly one role needs \
         `coordinator = true`, and its exit status is the run's verdict"
    )]
    NoCoordinator,
    #[error(
        "[pipeline.participants] declares {count} coordinators ({names}) — exactly \
         one role may be the coordinator, because a run has one verdict"
    )]
    ManyCoordinators { count: usize, names: String },
    #[error(
        "participant `{role}`: role names must be non-empty and use only \
         [A-Za-z0-9_-] (they travel as ids in {env})",
        env = ENV_PARTICIPANT_SELF
    )]
    BadRoleName { role: String },
    #[error(
        "participant `{role}`: declares `node = \"{node}\"` but no `address` — QED \
         has no machine inventory, and a rendezvous cannot be built from a name \
         its peers cannot dial. Add `address = \"<ip-or-host>\"`."
    )]
    RemoteWithoutAddress { role: String, node: String },
    #[error("participant `{role}`: port name `{port}` is declared twice")]
    DuplicatePort { role: String, port: String },
    #[error(
        "participant `{role}`: port names must be non-empty and use only \
         [A-Za-z0-9_-]"
    )]
    BadPortName { role: String, port: String },
    #[error(
        "[pipeline.participants] needs {needed} ports from port_base {base}, which \
         runs past 65535 — lower `port_base` or declare fewer ports"
    )]
    PortRangeExhausted { needed: usize, base: u16 },
    #[error(
        "step `{step}`: `participant = \"{participant}\"` names no declared role — \
         [pipeline.participants] declares {declared}"
    )]
    UnknownRole {
        step: String,
        participant: String,
        declared: String,
    },
    #[error(
        "step `{step}`: `participant = \"{participant}\"` but the pipeline declares no \
         [pipeline.participants] block — the role has no host, no address and no \
         place in the verdict"
    )]
    StepWithoutSet { step: String, participant: String },
    #[error(
        "participant `{role}`: no step declares `participant = \"{role}\"` — a role \
         with no steps can only ever report as never-started, so this is a typo, \
         not a configuration"
    )]
    RoleWithoutSteps { role: String },
}

fn is_token(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl ParticipantSet {
    /// `true` when the block declares nothing — treated as "no participant set"
    /// rather than as an error, mirroring
    /// [`MatrixSpec::is_empty`](crate::matrix::MatrixSpec::is_empty).
    pub fn is_empty(&self) -> bool {
        self.roles.is_empty()
    }

    /// Allocate the set: bind every role to a host and an address, assign every
    /// declared port a number, and check the invariants a run depends on.
    ///
    /// Pure and deterministic — the same set always yields the same plan, so a
    /// port number in a failure message is reproducible.
    pub fn plan(&self) -> Result<ParticipantPlan, ParticipantError> {
        if self.roles.is_empty() {
            return Err(ParticipantError::Empty);
        }

        let coordinators: Vec<&String> = self
            .roles
            .iter()
            .filter(|(_, spec)| spec.coordinator)
            .map(|(name, _)| name)
            .collect();
        match coordinators.len() {
            0 => return Err(ParticipantError::NoCoordinator),
            1 => {}
            n => {
                return Err(ParticipantError::ManyCoordinators {
                    count: n,
                    names: coordinators
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                })
            }
        }

        let base = self.port_base.unwrap_or(DEFAULT_PORT_BASE);
        let needed: usize = self.roles.values().map(|s| s.ports.len()).sum();
        // `base` itself is a valid port, so N ports occupy base..=base+N-1.
        if needed > 0 && (base as usize) + needed - 1 > u16::MAX as usize {
            return Err(ParticipantError::PortRangeExhausted { needed, base });
        }

        // Counted as a `usize` offset from `base` rather than as a running
        // `u16`, because assigning the very last legal port (65535) and then
        // incrementing overflows — which the guard above cannot prevent, since
        // that assignment is legal and it is only the *next* one that would not
        // be. `port_range_exhaustion_is_caught_at_plan_time` pins the boundary.
        let mut assigned: usize = 0;
        let mut participants = Vec::with_capacity(self.roles.len());
        for (name, spec) in &self.roles {
            if !is_token(name) {
                return Err(ParticipantError::BadRoleName { role: name.clone() });
            }
            let address = match (&spec.node, &spec.address) {
                (_, Some(addr)) => addr.clone(),
                (None, None) => LOCAL_ADDRESS.to_string(),
                (Some(node), None) => {
                    return Err(ParticipantError::RemoteWithoutAddress {
                        role: name.clone(),
                        node: node.clone(),
                    })
                }
            };
            let mut ports = BTreeMap::new();
            for port in &spec.ports {
                if !is_token(port) {
                    return Err(ParticipantError::BadPortName {
                        role: name.clone(),
                        port: port.clone(),
                    });
                }
                if ports.contains_key(port) {
                    return Err(ParticipantError::DuplicatePort {
                        role: name.clone(),
                        port: port.clone(),
                    });
                }
                ports.insert(port.clone(), (base as usize + assigned) as u16);
                assigned += 1;
            }
            participants.push(Participant {
                name: name.clone(),
                node: spec.node.clone(),
                address,
                ports,
                coordinator: spec.coordinator,
            });
        }

        Ok(ParticipantPlan { participants })
    }
}

impl ParticipantPlan {
    /// Every allocated participant, in declaration order.
    pub fn participants(&self) -> &[Participant] {
        &self.participants
    }

    /// Look one up by role name.
    pub fn get(&self, name: &str) -> Option<&Participant> {
        self.participants.iter().find(|p| p.name == name)
    }

    /// The one participant whose exit status is the run's verdict.
    ///
    /// Infallible: [`ParticipantSet::plan`] refuses a set without exactly one.
    pub fn coordinator(&self) -> &Participant {
        self.participants
            .iter()
            .find(|p| p.coordinator)
            .expect("plan() rejects a set without exactly one coordinator")
    }

    /// The rendezvous env for a step belonging to `self_name` (or for a step
    /// with no participant, when `None`).
    ///
    /// Two variables, not a combinatorial explosion of
    /// `QED_PARTICIPANT_<ROLE>_PORT_<NAME>`:
    ///
    /// - [`ENV_PARTICIPANTS`] — the whole allocated set as a JSON array, in
    ///   declaration order. A Rust participant deserializes it into
    ///   `Vec<`[`Participant`]`>`; a shell one reaches for `jq`.
    /// - [`ENV_PARTICIPANT_SELF`] — this step's own role name, so one binary
    ///   can be both halves of a case and decide which by reading it.
    ///
    /// Every participant is told about **every** participant, itself included.
    /// A case is a script over a peer set, and "the peers other than me" is a
    /// filter the participant can apply; "the peers including me" is
    /// information it cannot reconstruct.
    pub fn rendezvous_env(&self, self_name: Option<&str>) -> Vec<(String, String)> {
        let json = serde_json::to_string(&self.participants)
            .expect("Participant is a plain serde struct with no non-string map keys");
        let mut env = vec![(ENV_PARTICIPANTS.to_string(), json)];
        if let Some(name) = self_name {
            env.push((ENV_PARTICIPANT_SELF.to_string(), name.to_string()));
        }
        env
    }

    /// The order participants are torn down in: **peers first, coordinator
    /// last.**
    ///
    /// Not arbitrary. Tearing the coordinator down first makes every peer's
    /// counterpart vanish at once, and each one then logs a connection failure
    /// — so the journal ends with N transport errors and the real cause is
    /// buried above them. Reaping the peers first leaves the coordinator's
    /// account of what happened as the last thing written.
    pub fn teardown_order(&self) -> impl Iterator<Item = &Participant> {
        self.participants
            .iter()
            .filter(|p| !p.coordinator)
            .chain(self.participants.iter().filter(|p| p.coordinator))
    }
}

/// Allocate a pipeline's participant set and check it against the steps that
/// claim to be in it — the single seam both the loader (fail the *load*) and
/// the runner (fail the run before any step dispatches) go through, so a set
/// cannot be accepted by one and rejected by the other.
///
/// `Ok(None)` for the overwhelming majority of pipelines, which declare no set:
/// a step may then not name a participant, and nothing else changes.
pub fn plan_for(pipeline: &crate::types::Pipeline) -> Result<Option<ParticipantPlan>, ParticipantError> {
    let all_steps = || pipeline.steps.iter().chain(pipeline.finally.iter());
    let Some(set) = pipeline.participants.as_ref().filter(|s| !s.is_empty()) else {
        if let Some(step) = all_steps().find(|s| s.participant.is_some()) {
            return Err(ParticipantError::StepWithoutSet {
                step: step.name.clone(),
                participant: step.participant.clone().unwrap_or_default(),
            });
        }
        return Ok(None);
    };

    let plan = set.plan()?;

    for step in all_steps() {
        let Some(role) = step.participant.as_deref() else {
            continue;
        };
        if plan.get(role).is_none() {
            return Err(ParticipantError::UnknownRole {
                step: step.name.clone(),
                participant: role.to_string(),
                declared: plan
                    .participants()
                    .iter()
                    .map(|p| p.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
    }

    // A declared role nothing claims is dead config that would surface at
    // verdict time as a never-started participant — i.e. as a fleet fault,
    // which is the one diagnosis it definitely is not. Catch it at load.
    for participant in plan.participants() {
        if !all_steps().any(|s| s.participant.as_deref() == Some(participant.name.as_str())) {
            return Err(ParticipantError::RoleWithoutSteps {
                role: participant.name.clone(),
            });
        }
    }

    Ok(Some(plan))
}

/// How one participant ended. The split between the first two variants is the
/// point of the type — see [`verdict`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParticipantOutcome {
    /// Its steps never executed at all: dispatch was refused, the node was
    /// unreachable, or the run aborted before this participant was reached.
    NeverStarted { reason: String },
    /// It ran and ended badly.
    Failed { detail: String },
    /// It ran to completion, or was a healthy peer torn down on schedule.
    Completed,
}

/// One participant's contribution to the run verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantReport {
    pub name: String,
    pub coordinator: bool,
    pub outcome: ParticipantOutcome,
}

/// The run-level answer a participant set produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail { summary: String },
}

impl Verdict {
    pub fn passed(&self) -> bool {
        matches!(self, Verdict::Pass)
    }
}

/// Fold per-participant outcomes into the run's single verdict.
///
/// The ordering of the checks is the diagnosis, not a formality:
///
/// 1. **Any participant that never started fails the run first**, and says so
///    in those words. This is the fleet's problem — a node that is off, a
///    dispatch that was refused — and it must never be reported as "the test
///    failed", because an operator who reads it that way goes looking at the
///    code. This is requirement (2) in the module header, and it is the whole
///    reason [`ParticipantOutcome`] has two failure variants.
/// 2. Then genuine failures, coordinator first (it is the one holding the
///    assertions; a peer's failure is usually a consequence).
/// 3. A set that produced no coordinator report at all fails: a run whose
///    verdict-bearing participant left no account did not pass, it went
///    missing.
pub fn verdict(reports: &[ParticipantReport]) -> Verdict {
    let never: Vec<String> = reports
        .iter()
        .filter_map(|r| match &r.outcome {
            ParticipantOutcome::NeverStarted { reason } => {
                Some(format!("`{}` never started: {reason}", r.name))
            }
            _ => None,
        })
        .collect();
    if !never.is_empty() {
        return Verdict::Fail {
            summary: format!(
                "{} of {} participants never started — this is a fleet fault, not a \
                 test failure: {}",
                never.len(),
                reports.len(),
                never.join("; "),
            ),
        };
    }

    let mut failed: Vec<&ParticipantReport> = reports
        .iter()
        .filter(|r| matches!(r.outcome, ParticipantOutcome::Failed { .. }))
        .collect();
    failed.sort_by_key(|r| !r.coordinator);
    if !failed.is_empty() {
        let detail = failed
            .iter()
            .map(|r| match &r.outcome {
                ParticipantOutcome::Failed { detail } => format!("`{}`: {detail}", r.name),
                _ => unreachable!("filtered to Failed"),
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Verdict::Fail {
            summary: format!("{} participant(s) failed — {detail}", failed.len()),
        };
    }

    if !reports.iter().any(|r| r.coordinator) {
        return Verdict::Fail {
            summary: "no coordinator report — the participant holding the run's \
                      verdict left no account of itself"
                .to_string(),
        };
    }

    Verdict::Pass
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(toml_src: &str) -> ParticipantSet {
        toml::from_str(toml_src).expect("participant set parses")
    }

    #[test]
    fn parses_the_documented_shape() {
        let s = set(r#"
            port_base = 34500

            [role.runner]
            coordinator = true

            [role.responder]
            node    = "us-west-011"
            address = "100.64.0.11"
            ports   = ["clock"]
        "#);
        assert_eq!(s.port_base, Some(34500));
        assert_eq!(s.roles.len(), 2);
        // Declaration order is preserved — port assignment depends on it.
        assert_eq!(
            s.roles.keys().collect::<Vec<_>>(),
            vec!["runner", "responder"]
        );
        assert!(s.roles["runner"].coordinator);
        assert_eq!(s.roles["responder"].node.as_deref(), Some("us-west-011"));
    }

    #[test]
    fn plan_binds_hosts_addresses_and_ports() {
        let plan = set(r#"
            [role.runner]
            coordinator = true
            ports = ["control"]

            [role.responder]
            node    = "us-west-011"
            address = "100.64.0.11"
            ports   = ["clock", "aux"]
        "#)
        .plan()
        .unwrap();

        let runner = plan.get("runner").unwrap();
        assert_eq!(runner.node, None);
        // A role with no node is dialled at loopback, not left blank.
        assert_eq!(runner.address, LOCAL_ADDRESS);
        assert_eq!(runner.ports["control"], DEFAULT_PORT_BASE);

        let responder = plan.get("responder").unwrap();
        assert_eq!(responder.node.as_deref(), Some("us-west-011"));
        assert_eq!(responder.address, "100.64.0.11");
        // Sequential from the base, across the whole set in declaration order —
        // `control` took the base, so these follow it.
        assert_eq!(responder.ports["clock"], DEFAULT_PORT_BASE + 1);
        assert_eq!(responder.ports["aux"], DEFAULT_PORT_BASE + 2);
    }

    #[test]
    fn every_assigned_port_is_distinct_across_the_whole_set() {
        let plan = set(r#"
            [role.a]
            coordinator = true
            ports = ["x", "y"]
            [role.b]
            node = "n1"
            address = "10.0.0.1"
            ports = ["x", "y"]
            [role.c]
            node = "n2"
            address = "10.0.0.2"
            ports = ["x"]
        "#)
        .plan()
        .unwrap();
        let mut all: Vec<u16> = plan
            .participants()
            .iter()
            .flat_map(|p| p.ports.values().copied())
            .collect();
        assert_eq!(all.len(), 5);
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 5, "same port handed to two participants");
    }

    #[test]
    fn plan_is_deterministic() {
        let src = r#"
            [role.a]
            coordinator = true
            ports = ["x"]
            [role.b]
            node = "n1"
            address = "10.0.0.1"
            ports = ["y"]
        "#;
        assert_eq!(set(src).plan().unwrap(), set(src).plan().unwrap());
    }

    #[test]
    fn a_named_node_without_an_address_is_refused() {
        let err = set(r#"
            [role.runner]
            coordinator = true
            [role.responder]
            node = "us-west-011"
        "#)
        .plan()
        .unwrap_err();
        assert_eq!(
            err,
            ParticipantError::RemoteWithoutAddress {
                role: "responder".into(),
                node: "us-west-011".into(),
            }
        );
    }

    #[test]
    fn exactly_one_coordinator() {
        let none = set(r#"
            [role.a]
            [role.b]
        "#)
        .plan()
        .unwrap_err();
        assert_eq!(none, ParticipantError::NoCoordinator);

        let two = set(r#"
            [role.a]
            coordinator = true
            [role.b]
            coordinator = true
        "#)
        .plan()
        .unwrap_err();
        assert!(matches!(
            two,
            ParticipantError::ManyCoordinators { count: 2, .. }
        ));
    }

    #[test]
    fn empty_set_is_an_error_not_a_silent_no_op() {
        assert_eq!(
            ParticipantSet::default().plan().unwrap_err(),
            ParticipantError::Empty
        );
        // ...but `is_empty` lets the caller treat an absent block as "no set".
        assert!(ParticipantSet::default().is_empty());
    }

    #[test]
    fn bad_names_are_refused() {
        let mut s = ParticipantSet::default();
        s.roles.insert(
            "has space".into(),
            ParticipantSpec {
                coordinator: true,
                ..Default::default()
            },
        );
        assert!(matches!(
            s.plan().unwrap_err(),
            ParticipantError::BadRoleName { .. }
        ));

        let bad_port = set(r#"
            [role.a]
            coordinator = true
            ports = ["has space"]
        "#)
        .plan()
        .unwrap_err();
        assert!(matches!(bad_port, ParticipantError::BadPortName { .. }));
    }

    #[test]
    fn duplicate_port_name_within_a_role_is_refused() {
        let err = set(r#"
            [role.a]
            coordinator = true
            ports = ["clock", "clock"]
        "#)
        .plan()
        .unwrap_err();
        assert_eq!(
            err,
            ParticipantError::DuplicatePort {
                role: "a".into(),
                port: "clock".into(),
            }
        );
    }

    #[test]
    fn port_range_exhaustion_is_caught_at_plan_time() {
        let err = set(r#"
            port_base = 65535
            [role.a]
            coordinator = true
            ports = ["x", "y"]
        "#)
        .plan()
        .unwrap_err();
        assert_eq!(
            err,
            ParticipantError::PortRangeExhausted {
                needed: 2,
                base: 65535
            }
        );

        // The boundary itself is legal: one port at 65535 fits exactly.
        assert!(set(r#"
            port_base = 65535
            [role.a]
            coordinator = true
            ports = ["x"]
        "#)
        .plan()
        .is_ok());
    }

    #[test]
    fn unknown_keys_are_refused_rather_than_silently_ignored() {
        // A typo'd `adress` that parsed would produce a participant nobody can
        // reach, diagnosed three layers away as a connection refusal.
        assert!(toml::from_str::<ParticipantSet>(
            r#"
            [role.a]
            coordinator = true
            adress = "10.0.0.1"
        "#
        )
        .is_err());
    }

    // ── rendezvous ─────────────────────────────────────────────────────────

    #[test]
    fn rendezvous_env_carries_the_whole_set_and_who_you_are() {
        let plan = set(r#"
            [role.runner]
            coordinator = true
            [role.responder]
            node = "us-west-011"
            address = "100.64.0.11"
            ports = ["clock"]
        "#)
        .plan()
        .unwrap();

        let env: BTreeMap<String, String> =
            plan.rendezvous_env(Some("responder")).into_iter().collect();
        assert_eq!(env[ENV_PARTICIPANT_SELF], "responder");

        let decoded: Vec<Participant> = serde_json::from_str(&env[ENV_PARTICIPANTS]).unwrap();
        // Every participant is told about EVERY participant, itself included.
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].name, "runner");
        assert_eq!(decoded[1].address, "100.64.0.11");
        assert_eq!(decoded[1].ports["clock"], DEFAULT_PORT_BASE);
    }

    #[test]
    fn a_step_with_no_participant_still_sees_the_set_but_has_no_self() {
        let plan = set(r#"
            [role.a]
            coordinator = true
        "#)
        .plan()
        .unwrap();
        let env: BTreeMap<String, String> = plan.rendezvous_env(None).into_iter().collect();
        assert!(env.contains_key(ENV_PARTICIPANTS));
        assert!(!env.contains_key(ENV_PARTICIPANT_SELF));
    }

    // ── teardown ───────────────────────────────────────────────────────────

    #[test]
    fn teardown_reaps_peers_first_and_the_coordinator_last() {
        let plan = set(r#"
            [role.peer_a]
            node = "n1"
            address = "10.0.0.1"
            [role.runner]
            coordinator = true
            [role.peer_b]
            node = "n2"
            address = "10.0.0.2"
        "#)
        .plan()
        .unwrap();
        let order: Vec<&str> = plan
            .teardown_order()
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(order, vec!["peer_a", "peer_b", "runner"]);
    }

    // ── verdict ────────────────────────────────────────────────────────────

    fn report(name: &str, coordinator: bool, outcome: ParticipantOutcome) -> ParticipantReport {
        ParticipantReport {
            name: name.into(),
            coordinator,
            outcome,
        }
    }

    #[test]
    fn all_completed_passes() {
        let v = verdict(&[
            report("runner", true, ParticipantOutcome::Completed),
            report("responder", false, ParticipantOutcome::Completed),
        ]);
        assert_eq!(v, Verdict::Pass);
    }

    #[test]
    fn one_peer_failing_fails_the_run() {
        let v = verdict(&[
            report("runner", true, ParticipantOutcome::Completed),
            report(
                "responder",
                false,
                ParticipantOutcome::Failed {
                    detail: "bind: address in use".into(),
                },
            ),
        ]);
        let Verdict::Fail { summary } = v else {
            panic!("expected a failure")
        };
        assert!(summary.contains("responder"), "{summary}");
        assert!(summary.contains("address in use"), "{summary}");
    }

    #[test]
    fn never_started_outranks_failed_and_says_it_is_a_fleet_fault() {
        // The distinction this whole enum exists for: a run where the node was
        // never reached must not read as "the assertions failed".
        let v = verdict(&[
            report(
                "runner",
                true,
                ParticipantOutcome::Failed {
                    detail: "connection refused".into(),
                },
            ),
            report(
                "responder",
                false,
                ParticipantOutcome::NeverStarted {
                    reason: "node us-west-011 unreachable".into(),
                },
            ),
        ]);
        let Verdict::Fail { summary } = v else {
            panic!("expected a failure")
        };
        assert!(summary.contains("never started"), "{summary}");
        assert!(summary.contains("fleet fault"), "{summary}");
        assert!(summary.contains("us-west-011"), "{summary}");
        // The coordinator's downstream "connection refused" is a symptom of the
        // peer never starting; it must not be what the operator reads first.
        assert!(!summary.contains("connection refused"), "{summary}");
    }

    #[test]
    fn coordinator_failure_is_named_before_a_peers() {
        let v = verdict(&[
            report(
                "responder",
                false,
                ParticipantOutcome::Failed {
                    detail: "peer detail".into(),
                },
            ),
            report(
                "runner",
                true,
                ParticipantOutcome::Failed {
                    detail: "assertion detail".into(),
                },
            ),
        ]);
        let Verdict::Fail { summary } = v else {
            panic!("expected a failure")
        };
        let coord_at = summary.find("assertion detail").unwrap();
        let peer_at = summary.find("peer detail").unwrap();
        assert!(coord_at < peer_at, "{summary}");
    }

    #[test]
    fn a_set_with_no_coordinator_report_does_not_pass() {
        let v = verdict(&[report(
            "responder",
            false,
            ParticipantOutcome::Completed,
        )]);
        assert!(matches!(v, Verdict::Fail { .. }));
    }

    #[test]
    fn an_empty_report_set_does_not_pass() {
        // Vacuous truth is the wrong answer here: a run that produced no
        // participant reports at all did not verify anything.
        assert!(matches!(verdict(&[]), Verdict::Fail { .. }));
    }
}
