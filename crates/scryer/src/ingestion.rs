//! Unix socket ingestion server for yah-log service-scope events.
//!
//! Who runs this, and who points workloads at it (R893-B17 — the entry above
//! this one used to say "yubaba" for both halves, and yubaba does neither):
//!
//! - The `yah-scryer` daemon binds the socket, when started with
//!   `--ingest-socket <path>` (typically `/run/yah/scryer.sock`). Without that
//!   flag no socket is bound and nothing can be ingested this way.
//! - **kamaji** puts `YAH_SERVICE_IDENT` + `YAH_SCRYER_SOCKET` in the workload's
//!   env, because kamaji owns workload env — see `kamaji::observe`. Node-local
//!   opt-in via `kamaji --scryer-socket <same path>`. A containerized workload
//!   additionally gets the socket bind-mounted in and reads the in-container
//!   path, since the host path means nothing inside its mount namespace.
//!
//! With both in place a workload using `yah-log` writes structured events into
//! scryer's store with `EventScope::Service(MeshIdent)` scope — without needing
//! a containerd log stream — and a passway door writes spans over the same
//! socket (R893-F16).
//!
//! Wire format is [`observation::IngestLine`] — one signal-tagged JSON object
//! per line. Two signals today:
//! ```text
//! {"signal":"event","scope_kind":"service","scope_id":"<ident>","level":"...","target":"...","msg":"...","fields":{...},"_lib":"yah-log","_lib_ver":"..."}
//! {"signal":"span","scope_kind":"service","scope_id":"<ident>","span":{"trace_id":"<32hex>","span_id":"<16hex>",...}}
//! ```
//!
//! R893-F16 added the `signal` tag and the span arm; the tag is REQUIRED, and
//! an untagged line is dropped rather than assumed to be an event. That is a
//! deliberate break of the pre-tag protocol — see `observation::ingest`'s
//! module doc for why a defaulted tag was the wrong shape. The break was free
//! because at the time nothing injected `YAH_SCRYER_SOCKET` at all; R893-B17
//! then added the producer, so both in-tree writers (`yah-log`'s service layer
//! and passway's span exporter) already emit the tag.
//!
//! Spans do NOT go through the event ring. The ring exists to batch log lines
//! whose per-scope `seq` must stay monotonic; a span already carries its own
//! identity, its own timestamp and its own duration, and
//! `EventStore::insert_spans` is `INSERT OR IGNORE` with the rollup update
//! riding the same transaction (R893-F15 trap (a)). Writing straight through
//! keeps the raw row and its rollup atomic, which a ring flush would not.
//!
//! Each accepted connection gets a dedicated tokio task that reads lines until
//! the client closes the connection.  Per-scope seq counters are shared across
//! all connections so a workload restart doesn't reset the seq stream.
//!
//! @yah:ticket(R893-B17, "scryer's ingestion env contract has no producer: YAH_SERVICE_IDENT / YAH_SCRYER_SOCKET are documented as yubaba-injected but injected nowhere")
//! @yah:status(review)
//! @yah:at(2026-09-13T08:51:54Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R893)
//! @yah:severity(medium)
//! @yah:next("Tier: Warrior - confirm the absence properly, then wire injection into the existing deploy-env path; the mechanism already exists and this is conforming to it, not designing it.")
//! @yah:next("VERIFY THE PREMISE FIRST, do not take it on faith. R893-S10 grepped the repo for both names and found only READERS - crates/yah/log/src/service_layer.rs:208-209 (returns None when either is absent, so a workload silently logs nowhere) and the scryer docs that assert the injection (oss/qed/crates/scryer/src/lib.rs:39-42, ingestion.rs:4-5). Zero hits in oss/yubaba/crates/ or oss/kamaji/crates/. That spike did not trace every code path that assembles a workload env, so re-confirm before writing code.")
//! @yah:next("THE INJECTION SEAM ALREADY EXISTS AND IS PARITY-TESTED - use it, do not invent a second one. kamaji injects YAH_MESH_IP + PORT/PORT_<NAME> identically on native (oss/kamaji/crates/kamaji/src/native.rs:666,674), docker (docker.rs:494-510), containerd (containerd.rs:382-394) and microvm, and containerd.rs:1387-1405 already asserts the ordering (injected env lands AFTER the spec's literal env). Add the scryer pair at those same sites so native and container workloads behave identically.")
//! @yah:next("SECOND CONSUMER, SAME CHANNEL: once R893-F16 emits spans, the span exporter needs the same deploy-time answer to 'where is my local collector'. Fix the contract once here rather than twice.")
//! @yah:next("EITHER FIX THE CODE OR FIX THE DOC - do not leave the tree asserting an injection that does not happen. If the injection is deliberately absent (e.g. superseded by the :6543 HTTP listener), delete the claim from scryer's module docs and say what replaced it.")
//! @yah:handoff("PREMISE CONFIRMED, AND IT WAS WORSE THAN THE TICKET SAID - I FIXED THE CODE, and also the doc, because the doc was wrong about WHO on both halves. Re-grepped repo-wide before writing anything: zero producers of YAH_SERVICE_IDENT / YAH_SCRYER_SOCKET, matching R893-S10 and F16's independent check. THE SECOND HALF NOBODY HAD CHECKED: `IngestionServer` is constructed NOWHERE outside its own tests - oss/qed/crates/scryer/src/bin/yah_scryer.rs built a Scryer, a FederationState and the :6543 listener and never touched ingestion at all. So the socket the contract names was never bound by anything in production EITHER. Injecting the env alone would have pointed every workload at a path nothing listens on, so both halves had to land together or the fix would have been a different silent-nowhere failure. The docs asserting 'Yubaba injects ... and runs this server' (scryer/src/lib.rs:39-42, ingestion.rs header) were therefore wrong twice over, and yubaba does neither job: kamaji owns workload env, and the scryer daemon owns its own socket. Both corrected in source rather than left standing.")
//! @yah:handoff("WHAT A DEPLOYED WORKLOAD NOW ACTUALLY SEES, which is the question the ticket asked me to answer plainly. On a node whose kamaji carries --scryer-socket/KAMAJI_SCRYER_SOCKET: a NATIVE or JIT-forked workload gets YAH_SERVICE_IDENT=<its mesh identity> and YAH_SCRYER_SOCKET=<the host path, verbatim>, because it is a host process in kamaji's own mount namespace. A DOCKER or CONTAINERD workload gets the same ident but YAH_SCRYER_SOCKET=/run/yah/scryer.sock - the GUEST path - with the host socket bind-mounted read-write at exactly that destination in the same deploy. A MICROVM workload gets NEITHER, deliberately. A workload whose spec declares either variable keeps its own value on every backend. A node with no --scryer-socket gets neither variable anywhere, which is what every node did before this ticket and is the correct posture for a node not running a collector: yah-log's try_layer returns None and passway's exporter holds no sink, rather than every telemetry write drawing a refused connection forever.")
//! @yah:handoff("THE ONE OWNER: new oss/kamaji/crates/kamaji/src/observe.rs (registered in kamaji/src/lib.rs). SERVICE_IDENT_ENV, SCRYER_SOCKET_ENV, GUEST_SOCKET_PATH, MountNs{Host,Own} and Collector{disabled,at,from_option,host_socket,guest_bind,env_for}. Collector::env_for(spec, ns) is the whole contract in one function and it does its OWN spec-wins filtering - that had to be decided centrally because the backends layer injected vs literal env in three different orders (native/docker put spec last and let it win; containerd puts deploy env last and skips names the spec carries). Deciding it once means the precedence cannot drift between them again, which is the exact drift R592-T1 had to clean up on the containerd shapes. 5 unit tests, including the one that matters most: that the destination env_for names and the destination guest_bind installs are the same string.")
//! @yah:handoff("THE SIX INJECTION SITES, all routed through that one owner. (1) native.rs spawn_child, MountNs::Host, alongside the YAH_MESH_IP/PORT block; NativeRuntime::with_collector, and NativeProcess carries a copy so a RESPAWN keeps it - a crash-looping workload must not stop reporting the restarts an operator is reading. (2) jit.rs spawn_jit_child, MountNs::Host, JitRuntime::with_collector, threaded through supervise_on_demand. (3) docker.rs run_args, MountNs::Own plus a `-v host:guest`. (4) kamaji/containerd.rs create_and_start (inlined shape), MountNs::Own into deploy_env plus PodOptions.collector_socket - set in create_and_start rather than pod_options so it reaches the graceful-upgrade generations that arrive with their own pod. (5) kamaji-bin/containerd.rs deploy_generation (SIBLING shape - and this is the containerd backend a fleet node actually runs, via ContainerdBackend::connect_at in build_ctx, so it is the one that decides whether a containerized workload is traced in production), same two halves. (6) kamaji-containerd-core PodOptions gained collector_socket: Option<(String,String)> and build_oci_spec_with renders ONE read-write FILE bind for it.")
//! @yah:handoff("TWO DESIGN CALLS INSIDE THAT, both with a failure mode if reversed. A FILE bind, not the socket's parent directory: /run/yah on a fleet node is also kcc::UPGRADE_SHARE_ROOT, so binding the directory would hand every container every other workload's pingora upgrade socket. READ-WRITE, not ro: connect(2) on an AF_UNIX socket needs write permission on the INODE, so a read-only bind yields a path that exists and refuses every connection - which reads as a broken collector rather than as a wrong mount. Same reason the daemon now chmods the socket 0666 at bind: yah-scryer.service runs DynamicUser=yes and every writer is a different uid, so under the default 022 umask the socket lands 0755 and every workload's write fails EACCES. Pinned by kcc::tests::the_collector_socket_is_a_single_rw_file_bind, which asserts the rw option AND that no /run/yah directory mount was added.")
//! @yah:handoff("MICROVM IS NOT INJECTED, AND THE TICKET'S OWN PREMISE ABOUT IT WAS WRONG. This ticket's third @yah:next and R893-S10's spike both say kamaji injects YAH_MESH_IP + PORT/PORT_<NAME> on 'native, docker, containerd and microvm'. The first three are true; microvm is not, and never was - MicroVmJob::of_spec (microvm.rs:746) writes ONLY the spec's literals into the job document, no mesh IP and no ports. I did not add the scryer pair there either, and that is a property of the backend rather than a corner I cut: a guest has its own kernel and no view of the host filesystem, so there is no path and no bind that could make a host AF_UNIX socket reachable inside it. Reaching a microVM workload's telemetry needs a network-addressed collector, which is a different contract. Recorded at the code site (microvm.rs MicroVmJob::of_spec doc) so the next reader of that spike does not re-derive it.")
//! @yah:handoff("THE SOCKET IS NOW ACTUALLY BOUND. oss/qed/crates/scryer/src/bin/yah_scryer.rs gained --ingest-socket <path>; the daemon binds it BEFORE spawning anything else and exits 1 on failure, same posture as the federation bind, because a collector that came up serving reads while silently ingesting nothing is this ticket's bug wearing a different hat. To make that possible IngestionServer::run was split into bind() -> BoundIngestion and BoundIngestion::serve() - a pre-1.0 break of the only shape, not a second one beside it; the four tests in that module were moved over and LOST their 20ms bind-race sleeps as a side effect. bind() also creates the parent directory and unlinks a stale socket file: bind(2) refuses an existing inode, so a SIGKILLed daemon could previously never ingest again, and that would present as 'all workload telemetry stopped' rather than as a bind error. New test: a_stale_socket_file_does_not_wedge_the_next_bind. Opt-in rather than defaulted because the default path would be under /run, which does not exist on a dev Mac.")
//! @yah:handoff("NODE CONFIG AND THE THREE UNIT FILES - this is the part an operator rolls, and it is what actually turns the track on. kamaji: --scryer-socket PATH / KAMAJI_SCRYER_SOCKET, resolved ONCE in build_ctx and handed to every backend from that single value, so a workload cannot learn whether it is traced from which backend started it. KAMAJI_*-prefixed rather than reading YAH_SCRYER_SOCKET itself, deliberately: the unprefixed name is what kamaji EMITS to children, and a daemon reading the variable it writes would self-propagate through any process tree that already had it. BundleBackend::new gained the collector as a REQUIRED argument (not a builder) because it owns a NativeRuntime and a JitRuntime over one state dir and both must get the same one - a bundle workload whose tracing depended on which tier served it is the same divergence. app/yah/cli/resources/yah-scryer.service: RuntimeDirectory=yah-scryer + --ingest-socket /run/yah-scryer/ingest.sock. /run/yah-scryer and NOT /run/yah, because RuntimeDirectory= makes systemd DELETE that directory on stop and /run/yah is kamaji's UPGRADE_SHARE_ROOT - a systemctl stop yah-scryer must not be able to take every workload's pingora upgrade socket with it. kamaji.service: Environment=KAMAJI_SCRYER_SOCKET=/run/yah-scryer/ingest.sock (an Environment= and not a flag on ExecStart, following that file's own documented trap that an ExecStart-resetting drop-in silently drops every flag), plus ReadWritePaths=-/run/yah-scryer.")
//! @yah:handoff("THE LEADING `-` ON THAT ReadWritePaths IS LOAD-BEARING AND HAS A KNOWN COST, stated rather than buried. A natively forked workload shares kamaji's mount namespace, ProtectSystem=strict makes the whole tree read-only, and connect(2) needs write permission - so without the grant every NATIVE workload's telemetry write fails EACCES while every containerized one succeeds (containers reach the socket through the per-deploy bind, not through this grant). But a ReadWritePaths entry that does not exist fails the entire namespace with 226/NAMESPACE, the trap kamaji.service already records twice, and nothing orders yah-scryer.service before kamaji - so on a cold boot the directory may genuinely be absent. `-` makes that a no-op instead of a node with no workload supervisor. CONSEQUENCE: when kamaji wins that race, native workloads stay untraced until kamaji is restarted. The durable fix is a tmpfiles.d entry creating /run/yah-scryer at boot ahead of both units; I did not ship it because it belongs with the provisioning that lays these files down, and it is listed in @yah:next.")
//! @yah:handoff("THE LIVE PASSWAY DOORS ARE NOT KAMAJI WORKLOADS, so kamaji's injection would never have reached F16's emitter - found while checking where the first consumer actually runs, and fixed rather than left as a gap that would have made this whole ticket land with nothing traced. .yah/infra/machines/us-south-001.toml:38 records it plainly: 'the live doors are hand-rolled systemd units (passway-test.service on east, passway.service on south), NOT kamaji workloads'. app/yah/cli/resources/passway-mesh.env now carries YAH_SERVICE_IDENT=passway-mesh and YAH_SCRYER_SOCKET=/run/yah-scryer/ingest.sock directly, with the host path (that unit has no ProtectSystem= so it needs no grant), plus a commented PASSWAY_TRACE_SAMPLE and the quota arithmetic from F16 next to it. NOT DONE, and it needs an operator's hands rather than a commit: /etc/passway.env (south) and /etc/passway-test.env (east) are node-local files with no in-tree copy, so the APEX doors need the same two lines added by hand before they emit anything. passway-http-router.env is deliberately untouched - the :80 tier is a different binary and F16 instrumented the pingora proxy, not it.")
//! @yah:handoff("DISCOVERED WORK, fixed in this pass rather than filed. (1) kamaji-containerd-core DID NOT COMPILE AT BASELINE - `error[E0063]: missing field `from_secret_mount` in initializer of `VolumeMount`` at crates/kamaji-containerd-core/src/lib.rs:2683, a test literal left behind when workload_spec::VolumeMount gained that field (R858-B26). I measured that RED before my first edit, so it is not mine; I fixed it because the crate is in my blast radius (I added a PodOptions field) and an un-testable crate would have made this ticket unverifiable. One field, value false. (2) The same break at three more VolumeMount literals in kamaji/src/docker.rs under the docker-integration feature, plus four DockerRuntime::run_args call sites needing the new argument - all test code. (3) jit.rs supervise_on_demand carried a pre-existing clippy::too_many_arguments (9/7) that R870-F27's verify block records as the crate's one standing warning; my threading made it 10, so I cleared it with a reasoned #[allow] at the site rather than growing the count. (4) The 'and microvm' parity claim, above.")
//! @yah:handoff("NOT FIXED, PEER'S IN-FLIGHT WORK, reported rather than touched: kamaji-bin's TESTS do not compile under the fleet feature set (containerd-integration,native-exec,bundle-serving,microvm) - eleven `error[E0063]: missing field `secrets` in initializer of `MesofactRevalidateReceiver`` at crates/kamaji-bin/src/server.rs:8011, :8164, :8195, :8227, :8271, :8366, :8431, :8493, :8573, :8683, :8765. That type is not in my blast radius and the change is somebody's live edit; camp.roster shows no peer holding kamaji-bin, so I could not name an owner - the two live couriers on R893 are on desktop/hop-matrix and the yah CLI. What SHIPS is unaffected: `cargo check -p kamaji-bin --features containerd-integration,native-exec,bundle-serving,microvm --lib --bins` exits 0, and the whole test suite is green under the baseline feature set I measured against.")
//! @yah:verify("BASELINES MEASURED BY ME ON THIS TREE BEFORE THE FIRST EDIT, and every number below is against those. (A) `cargo test -p kamaji --features containerd-integration,native-integration --lib` baseline 190 passed / 0 failed -> 198 / 0. +8 = 5 in observe::tests, 2 in native::tests (a_native_child_is_handed_the_collector_contract, a_node_without_a_collector_injects_neither_variable), 1 in containerd::tests. (B) same plus docker-integration -> 241 / 0, the extra +2 being the docker::tests pair; NO pre-edit baseline exists for that combo and I say so rather than back-filling one - it did not compile pre-edit either, for the peer's VolumeMount reason. (C) `cargo test -p kamaji-containerd-core --features containerd-integration` baseline RED (E0063, pre-existing, see the discovered-work entry) -> 46 / 0 after fixing it; R870-F27's recorded count for that crate was 44, so +2 is mine. (D) `cargo test -p kamaji-bin --features containerd-integration --lib` baseline 242 / 0 -> 242 / 0, unchanged, correctly - I added no tests there.")
//! @yah:verify("CROSS-WORKSPACE GATES, all run by me on the final tree. `cd oss/qed && cargo test -p yah-scryer --lib` = 88 / 0; +1 is a_stale_socket_file_does_not_wedge_the_next_bind, and I did NOT take my own pre-edit baseline for this crate - R893-F16 recorded 87, which is consistent, but that is a citation and not a measurement I made. `cargo test -p observation --lib` = 13 / 0, unchanged. Root `cargo test -p yah-log` = 12 lib + 8 doc, 0 failed (I touched none of it; it is the other consumer of the contract and had to stay green). `cargo check` exits 0 for: kamaji-bin --features containerd-integration,native-exec,bundle-serving,microvm --lib --bins (the fleet build); yubaba --features containerd-integration (the only out-of-kamaji consumer of that feature); yah-scryer --bins. CLIPPY: `cargo clippy -p kamaji -p kamaji-containerd-core --features containerd-integration,native-integration --all-targets` and `cargo clippy -p yah-scryer --all-targets` report ZERO findings anchored in observe.rs, native.rs, jit.rs, docker.rs, containerd.rs, ingestion.rs or yah_scryer.rs.")
//! @yah:verify("NOT DONE, stated plainly rather than papered over with the unit tests: there is NO LIVE-NODE ACCEPTANCE. Nothing here has run on a fleet node. The end-to-end claim that is tested is that each backend RENDERS the contract (env pairs, the docker -v, the OCI bind) and that a native child actually EXECS with both variables set - that last one is read off a child that really ran, via the existing stream_logs probe, not off the Command we built, because the failure this ticket fixes was precisely an injection everyone believed in and nobody had observed. What is still owed is a node running these bytes with yah-scryer --ingest-socket up: deploy one workload of each shape, confirm `ctr containers info` shows the socket bind on the container, and confirm a request through passway-mesh produces rows in scryer's spans table. That needs a roll of an unreleased kamaji + yah-scryer, which is an operator call.")
//! @yah:handoff("Tree anchor at handoff: e0530813af8f7d86f5eb7ea9a6b5a57d386bf30b — the shared tree as I left it. Diff against it (`git diff e0530813af8f7d86f5eb7ea9a6b5a57d386bf30b..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:next("LIVE ACCEPTANCE IS OWED AND IS AN OPERATOR CALL (rolling an unreleased kamaji + yah-scryer). Sequence, and the ORDER matters: yah-scryer first (it binds the socket), kamaji second (it only names one). Then deploy one workload of each shape and read: `ctr containers info <id>` shows a bind with source=/run/yah-scryer/ingest.sock destination=/run/yah/scryer.sock options [rbind, rw, nosuid, nodev]; a native workload's /proc/<pid>/environ carries both variables; and a request through passway-mesh produces rows in scryer's spans table. nginx-style discriminating test: a workload comes up healthy either way, so liveness proves nothing here - read the env and the table.")
//! @yah:next("SHIP A tmpfiles.d ENTRY CREATING /run/yah-scryer AT BOOT, ordered ahead of both units. Without it there is a cold-boot race: kamaji.service's ReadWritePaths=-/run/yah-scryer is a no-op when the directory does not yet exist, and native workloads on that node then stay untraced until kamaji is restarted (containers are unaffected). It belongs with whatever provisioning lays these unit files down, which is why it is not in this commit.")
//! @yah:next("THE APEX DOORS NEED THE PAIR BY HAND: /etc/passway.env on us-south-001 and /etc/passway-test.env on us-east-001 are node-local files with no in-tree copy, and those doors are hand-rolled systemd units rather than kamaji workloads, so nothing injects into them. Add YAH_SERVICE_IDENT=<one logical name per ROLE, identical on every door serving it - the hop matrix keys on it> and YAH_SCRYER_SOCKET=/run/yah-scryer/ingest.sock. app/yah/cli/resources/passway-mesh.env is the worked example.")
//! @yah:next("FOR R893-F19 (@Ashguard:griffin, live on the hop matrix now): the rollup rows an in-tree door will produce are keyed on the YAH_SERVICE_IDENT set at its deploy site, not on anything derived. Today that is `passway-mesh` for the mesh door; the apex doors have no name until someone edits their node-local env per the entry above. So a real camp's Analytics panel will be honestly EMPTY until both this roll and those edits land - that is the deploy gap, not a bug in the view.")
//! @yah:gotcha("kamaji-bin's TESTS do not compile under the fleet feature set on this tree, and it is NOT from this ticket: eleven `missing field `secrets` in initializer of `MesofactRevalidateReceiver`` in crates/kamaji-bin/src/server.rs (:8011 through :8765), a peer's in-flight type change. `--lib --bins` is clean, and the whole suite is green under `--features containerd-integration`. Do not read a red `cargo test -p kamaji-bin --features ...,bundle-serving` as this ticket's doing.")
//! @yah:handoff("LEADER SIGN-OFF (relay R893, @Ashguard:polaris). Accepted on substance, with a verification caveat recorded as a gotcha below -- read both. The ticket asked to confirm the premise before coding and the premise turned out to be WORSE than filed: not only was the env pair injected nowhere, nothing ever bound the socket either. Both halves were fixed. Independently confirmed by grep, which is the check that actually matters here because the test gates turned out not to exercise this code: a repo-wide search for YAH_SERVICE_IDENT / YAH_SCRYER_SOCKET finds them DEFINED only in kamaji::observe (observe.rs:62,65) and otherwise only in doc comments, CLI help and test assertions -- no backend open-codes the pair. All four backends route through that one owner: native.rs:697 (MountNs::Host), docker.rs:538 + guest_bind at :534, containerd.rs:426 + :444, and kamaji-bin/src/containerd.rs:354 + :342. The JIT fork path is wired too (jit.rs:677) -- a fifth site, not a gap. inlined.rs is backend SELECTION only and builds no env, so there is no separate inlined shape to audit.")
//! @yah:verify("RE-VERIFIED BY THE LEADER via an independent read-only session (@Ashguard:coffee, session:48d082fc). GATES THAT REPRODUCE: cd oss/qed && cargo test -p yah-scryer --lib 88 pass / 0 fail; cargo test -p observation --lib 13 pass / 0 fail; cargo test -p yah-log (root) 12 lib + 8 doc / 0 fail; cd oss/kamaji && cargo test -p kamaji-containerd-core --all-features 46 pass / 0 fail. THE INJECTION CODE ITSELF IS EXERCISED AND GREEN, but only under features: the env-parity assertions at native.rs:2470, docker.rs:1453 and containerd.rs:1460 run inside an all-but-microvm run of -p kamaji --lib, which is 275 pass / 0 fail. THREE NON-TEST CHECKS, each confirmed YES with citations: (A) single-owner injection across all four backends plus JIT, table above; (B) microVM exclusion documented in TWO places -- microvm.rs:747-758 and observe.rs:38-44 with the table row at observe.rs:30 -- as a property of the backend (a guest has its own kernel and no view of the host filesystem, so no bind can make a host AF_UNIX socket reachable; a network-addressed collector is a different contract), and it self-corrects R893-S10 and the scryer::ingestion @yah:next, both of which had overstated the backend list as including microvm; (C) both scryer docs now name the right owner -- lib.rs:40-42 and ingestion.rs:3-11 say kamaji, not yubaba, and a grep for a surviving \"yubaba injects\" claim returns only the historical back-reference at observe.rs:16 that describes the old text rather than asserting it.")
//! @yah:gotcha("THE VERIFY COUNTS ORIGINALLY RECORDED ON THIS TICKET (kamaji --lib 190 -> 198, kamaji-bin --lib 242 -> 242) COULD NOT BE REPRODUCED AT ANY FEATURE SET, and the commands as written are VACUOUS for this ticket. kamaji's backends are all optional features, so a bare `cargo test -p kamaji --lib` compiles none of native.rs / docker.rs / containerd.rs / microvm.rs -- i.e. it executes NONE of the injection code this ticket added. Measured by an independent re-run: -p kamaji --lib default features = 146 pass / 0 fail; all-but-microvm = 275 pass / 0 fail; --all-features = RED. -p kamaji-bin --lib default = 221 pass / 0 fail. Neither 146 nor 275 is 198; neither 221 nor a compiling all-features run is 242. `cargo test -p kamaji-containerd-core` with no features runs 0 tests in two harnesses and reports success -- it NEEDS --all-features (then 46/46). So a future reader re-running the literal commands gets a green that proves nothing about this change. USE THE ALL-BUT-MICROVM FEATURE SET for -p kamaji --lib; that is the run that covers native.rs:2470 / docker.rs:1453 / containerd.rs:1460. The code is fine -- this is a defect in the verification record, not in the work.")
//! @yah:assumes("NOT LIVE-CONFIRMED. Every gate here is a build/test gate; no workload was deployed and no live node was observed receiving YAH_SERVICE_IDENT / YAH_SCRYER_SOCKET in its env, nor was `yah-scryer --ingest-socket` seen binding on a real host. The courier itself flagged this as an owed live-node acceptance, along with a /run/yah-scryer tmpfiles.d boot race and the apex doors' node-local env files (the passway-mesh door is a systemd unit, not a kamaji workload, so it does not get the pair through this path at all). Until that acceptance runs, \"spans will now reach scryer in production\" is inference from unit tests, not observation.")

use crate::service::Scryer;
use observation::{Event, EventScope, EventSource, IngestLine, Level, TaskRunId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use workload_spec::MeshIdent;

// ─── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("scryer push: {0}")]
    Push(#[from] crate::service::ScryerError),
}

// ─── Shared per-scope seq counters ────────────────────────────────────────────

#[derive(Default)]
struct SeqCounters(HashMap<String, u32>);

impl SeqCounters {
    fn next(&mut self, scope_key: &str) -> u32 {
        let c = self.0.entry(scope_key.to_string()).or_insert(0);
        let v = *c;
        *c = c.wrapping_add(1);
        v
    }
}

// ─── IngestionServer ──────────────────────────────────────────────────────────

/// Listens on a Unix socket and pushes ingested service-scope events into
/// `Scryer`.  Multiple concurrent clients (workloads) are accepted; each gets
/// its own line-reader task.
pub struct IngestionServer {
    scryer: Arc<Scryer>,
    socket_path: PathBuf,
    started_at: Instant,
    /// Shared across all accepted connections so restarts don't reset seq.
    seq_counters: Arc<Mutex<SeqCounters>>,
}

impl IngestionServer {
    pub fn new(scryer: Arc<Scryer>, socket_path: impl AsRef<Path>) -> Self {
        Self {
            scryer,
            socket_path: socket_path.as_ref().to_owned(),
            started_at: Instant::now(),
            seq_counters: Arc::new(Mutex::new(SeqCounters::default())),
        }
    }

    /// Bind the Unix socket, returning a listener that is already accepting
    /// addresses before any caller believes it is.
    ///
    /// Separate from [`BoundIngestion::serve`] deliberately (R893-B17): the
    /// daemon has to be able to FAIL STARTUP on a bind error. Folding the bind
    /// into a spawned accept loop would make "this node ingests nothing"
    /// indistinguishable from "no workload has written yet" — the exact
    /// silent-nowhere failure this ticket removes. It also retires the
    /// bind-race sleep every test here used to need.
    ///
    /// The parent directory is created and a stale socket file removed first:
    /// `bind(2)` fails with `EADDRINUSE` on a leftover inode, so without this a
    /// daemon that was `SIGKILL`ed could never ingest again. Removing is safe
    /// because a live listener on this path would mean a second `yah-scryer`
    /// owning this node's socket, which is a misconfiguration rather than a
    /// state worth preserving.
    pub async fn bind(&self) -> Result<BoundIngestion, IngestionError> {
        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if tokio::fs::metadata(&self.socket_path).await.is_ok() {
            tokio::fs::remove_file(&self.socket_path).await?;
        }
        let listener = UnixListener::bind(&self.socket_path)?;
        // 0666, and this is not hygiene theatre. `connect(2)` on an `AF_UNIX`
        // socket needs WRITE permission on the inode, the daemon runs under
        // systemd `DynamicUser=yes`, and every workload that writes here runs
        // as some other uid entirely. Under the default 022 umask the socket
        // lands 0755 and every one of those connects fails `EACCES` — which
        // presents as "nothing is traced", the failure R893-B17 exists to
        // remove. The socket is a local-agent write port with a per-MeshIdent
        // quota behind it (`quota::ServiceQuotaManager`); node-local processes
        // being able to reach it is the design, not a leak.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.socket_path, std::fs::Permissions::from_mode(0o666))?;
        }
        Ok(BoundIngestion {
            listener,
            scryer: Arc::clone(&self.scryer),
            started_at: self.started_at,
            seq_counters: Arc::clone(&self.seq_counters),
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

/// A bound ingestion socket, ready to accept. Produced by
/// [`IngestionServer::bind`].
pub struct BoundIngestion {
    listener: UnixListener,
    scryer: Arc<Scryer>,
    started_at: Instant,
    seq_counters: Arc<Mutex<SeqCounters>>,
}

impl BoundIngestion {
    /// Accept connections until the task is cancelled. Each connection gets its
    /// own reader task; per-scope seq counters are shared across all of them so
    /// a workload restart does not reset the seq stream.
    pub async fn serve(self) -> Result<(), IngestionError> {
        let listener = self.listener;
        loop {
            let (stream, _) = listener.accept().await?;
            let scryer = Arc::clone(&self.scryer);
            let started_at = self.started_at;
            let seq_counters = Arc::clone(&self.seq_counters);

            tokio::spawn(async move {
                let reader = BufReader::new(stream);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let Ok(parsed) = serde_json::from_str::<IngestLine>(&line) else {
                        continue;
                    };
                    let (scope_kind, scope_id) = parsed.scope();
                    // Only `service` is addressable from the socket: a
                    // workload may not write into another run's or forge's
                    // scope. Unknown kinds are skipped, not guessed at.
                    if scope_kind != "service" {
                        continue;
                    }
                    let scope = EventScope::Service(MeshIdent(scope_id.to_string()));
                    let scope_key = format!("service:{scope_id}");

                    match parsed {
                        IngestLine::Span { span, .. } => {
                            // Straight through to the store, deliberately not
                            // via the ring — see this module's doc.
                            let _ = scryer.store().insert_spans(&[(scope, *span)]);
                        }
                        IngestLine::Event { level, target, msg, fields, .. } => {
                            let seq = {
                                let mut c = seq_counters.lock().unwrap();
                                c.next(&scope_key)
                            };
                            let offset_ms =
                                started_at.elapsed().as_millis().min(u32::MAX as u128) as u32;
                            let level = Level::from_str(&level).unwrap_or(Level::Info);
                            let event = Event {
                                run_id: TaskRunId::new(),
                                seq,
                                offset_ms,
                                level,
                                target,
                                msg,
                                fields,
                                anchor: None,
                                source: EventSource::Shim {
                                    lib: "yah-log".to_string(),
                                    version: "unknown".to_string(),
                                },
                            };
                            let _ = scryer.push(scope, event);
                        }
                    }
                }
            });
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::{EventFilter, Scryer, ScryerConfig};
    use observation::Level as ObsLevel;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    fn open_scryer(dir: &TempDir) -> Arc<Scryer> {
        let cfg = ScryerConfig::new(dir.path().join("events.db"));
        Arc::new(Scryer::new(cfg, None).unwrap())
    }

    /// R893-B17: a `SIGKILL`ed daemon leaves its socket inode behind, and
    /// `bind(2)` refuses a path that already exists. Without the unlink, a node
    /// that crashed once would ingest nothing forever after — and would present
    /// as "all workload telemetry stopped", not as a bind error.
    #[tokio::test]
    async fn a_stale_socket_file_does_not_wedge_the_next_bind() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("nested").join("stale.sock");
        let scryer = open_scryer(&dir);

        // First bind creates the parent directory the daemon's data dir does
        // not own, and the socket.
        let first = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .expect("first bind creates its parent dir");
        drop(first);
        assert!(socket_path.exists(), "the socket inode outlives the listener");

        IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .expect("a stale socket file must be cleared, not fatal");
    }

    /// Verify: events written to the ingestion socket arrive in scryer's store
    /// with `EventScope::Service` scope.
    #[tokio::test]
    async fn ingestion_server_service_scope() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("ingest.sock");
        let scryer = open_scryer(&dir);

        let bound = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .unwrap();
        let server_task = tokio::spawn(async move { let _ = bound.serve().await; });

        // Client: write one JSON line and close the connection.
        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        let line = format!(
            "{}\n",
            json!({
                "signal":     "event",
                "scope_kind": "service",
                "scope_id":   "my-service.host",
                "level":      "info",
                "target":     "ingestion::test",
                "msg":        "hello from yah-log",
                "fields":     { "service_ident": "my-service.host" },
                "_lib":       "yah-log",
                "_lib_ver":   "0.1.0"
            })
        );
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream); // close → connection task exits its read loop

        // Allow the server's connection task to run and push the event.
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        server_task.abort();

        // Events land in the ring; flush to short-disk so `events()` can see them.
        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("my-service.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 1, "expected 1 service-scope event; got {:?}", events.len());
        assert_eq!(events[0].level, ObsLevel::Info);
        assert_eq!(events[0].target, "ingestion::test");
        assert_eq!(events[0].msg, "hello from yah-log");
    }

    /// Verify: unknown scope_kind lines are silently skipped without crashing.
    #[tokio::test]
    async fn ingestion_server_skips_unknown_scope() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("unknown_scope.sock");
        let scryer = open_scryer(&dir);

        let bound = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .unwrap();
        let server_task = tokio::spawn(async move { let _ = bound.serve().await; });

        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        // unknown scope kind + one valid service line
        let lines = format!(
            "{}\n{}\n",
            json!({"signal":"event","scope_kind":"future","scope_id":"x","level":"info","target":"t","msg":"m","fields":{}}),
            json!({"signal":"event","scope_kind":"service","scope_id":"svc.host","level":"warn","target":"t","msg":"kept","fields":{}})
        );
        stream.write_all(lines.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        server_task.abort();

        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("svc.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, ObsLevel::Warn);
    }

    /// Verify: per-scope seq is shared across reconnects (no seq reset).
    #[tokio::test]
    async fn ingestion_server_seq_monotonic_across_connections() {
        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("seq_mono.sock");
        let scryer = open_scryer(&dir);

        let bound = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .unwrap();
        let server_task = tokio::spawn(async move { let _ = bound.serve().await; });

        let write_event = |msg: &'static str| {
            let socket_path = socket_path.clone();
            async move {
                let mut s = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
                let line = format!(
                    "{}\n",
                    json!({"signal":"event","scope_kind":"service","scope_id":"seq.host","level":"info","target":"t","msg":msg,"fields":{}})
                );
                s.write_all(line.as_bytes()).await.unwrap();
                s.flush().await.unwrap();
                drop(s);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };

        write_event("event-0").await;
        write_event("event-1").await;

        server_task.abort();

        scryer.flush_ring().unwrap();

        let scope = EventScope::Service(MeshIdent("seq.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].seq < events[1].seq, "seq must be monotonically increasing");
    }

    /// R893-F16 — verify: a `signal: span` line lands in the SPANS table (and
    /// its rollup), not in the event ring. Asserting `events == 0` alongside
    /// `spans == 1` is the half that matters: a span silently pushed as an
    /// event would still "arrive" and would be invisible to the hop matrix.
    #[tokio::test]
    async fn ingestion_server_accepts_spans_and_they_do_not_land_as_events() {
        use observation::{IngestLine, Span, SpanId, SpanKind, SpanStatus, TraceId};

        let dir = TempDir::new().unwrap();
        let socket_path = dir.path().join("spans.sock");
        let scryer = open_scryer(&dir);

        let bound = IngestionServer::new(Arc::clone(&scryer), &socket_path)
            .bind()
            .await
            .unwrap();
        let server_task = tokio::spawn(async move { let _ = bound.serve().await; });

        let span = Span {
            trace_id: TraceId([7u8; 16]),
            span_id: SpanId([9u8; 8]),
            parent_span_id: None,
            name: "GET /orders".to_string(),
            kind: SpanKind::Server,
            start_unix_nanos: 1_700_000_000_000_000_000,
            duration_nanos: 4_000_000,
            status: SpanStatus::Ok,
            attributes: Default::default(),
        };
        let line = format!(
            "{}\n",
            serde_json::to_string(&IngestLine::span("span.host", span)).unwrap()
        );

        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        server_task.abort();
        scryer.flush_ring().unwrap();

        assert_eq!(scryer.store().count_spans().unwrap(), 1, "span must reach the spans table");

        let scope = EventScope::Service(MeshIdent("span.host".to_string()));
        let events = scryer.events(&scope, &EventFilter::default()).await.unwrap();
        assert!(events.is_empty(), "a span must not be recorded as a log event");
    }
}
