//! Core observation types shared between task-runs and scryer.
//!
//! Types here are intentionally free of I/O — store layers own persistence.
//!
//! @yah:ticket(R893-S10, "Intra-service latency (Istio-style): what signal type, given scryer Events carry no span or duration")
//! @yah:status(review)
//! @yah:at(2026-09-12T21:15:06Z)
//! @yah:kind(spike)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:phase(P4)
//! @yah:parent(R893)
//! @arch:see(.yah/docs/working/W346-services-tab-three-views-and-the-tab-boundary.md)
//! @yah:next("Operator ask, 2026-09-11: record intra-service latencies — service to db, db to R2 stream, and the other internal hops — the way a service mesh does. This is NOT the front-door panel (R893-F8, which is settled as an external monitor) and must not gate it. W346 §5.4 has the two-column comparison of why they are different products.")
//! @yah:next("START FROM THE GAP, verified: scryer's Event (this file, :242) is a log line keyed to a TaskRunId — run_id / seq / offset_ms / level / target / msg / freeform `fields` / anchor / source. No span id, no parent span, no duration. RESERVED_FIELD_PATHS (:271) is described as OTel-semconv-lite but the span model itself is absent. So this is a new signal type ALONGSIDE events, not a schema extension — say so explicitly in the output rather than discovering it in implementation.")
//! @yah:next("Questions the spike has to answer, in order: (1) what is the span/hop model and does it adopt OTel outright rather than minting a third yah-local shape; (2) how does context propagate across yubaba → kamaji → workload → db → R2 stream, given native workloads and containers take different paths; (3) who collects and stores it, and whether that is the same store R893-F8 uses or a separate one; (4) what does the Analytics panel show, per W346 §4 — aggregates over a window, never a now-value. Output is a recommendation plus the tickets it implies, not code.")
//! @yah:handoff("ANSWER 1 - THE SPAN MODEL: adopt OpenTelemetry's DATA MODEL and WIRE FORMAT (W3C Trace Context traceparent/tracestate; OTLP span shape trace_id/span_id/parent_span_id/name/kind/start/duration/status/attributes), and do NOT link the OTel SDK. This is not a new position and should not be argued as one - it is the SECOND application of a position this codebase already adopted and reasoned out at oss/yubaba/crates/yubaba/src/node.rs:18-58 ('We adopt OpenTelemetry's semantic-convention attribute names. We do not adopt the OpenTelemetry SDK'), which crates/yah/hub/src/in_process.rs:148 already instructs new payload families to follow. Minting a third yah-local shape loses on three grounds: (a) the PROPAGATION format is not ours to choose - traceparent is what every library, proxy and browser already emits, so a yah-local header makes every future third-party component structurally un-instrumentable; (b) the binary-size argument that justified skipping the SDK for node metrics applies identically here (yubaba/kamaji ship as curl-fetched musl-static binaries with size actively tracked, node.rs:41-45); (c) node.rs:55-58's asymmetry holds - a real OTLP exporter later is a rename-free loop over keys that are already valid OTLP attribute keys.")
//! @yah:handoff("ANSWER 1, CONCRETE SHAPE: a NEW Span type ALONGSIDE Event in oss/qed/crates/observation/src/types.rs - not fields added to Event. Gap re-confirmed by reading: Event (:253-265) carries run_id/seq/offset_ms/level/target/msg/fields/anchor/source, with no span identity and no duration; RESERVED_FIELD_PATHS (:285-296) reserves $.db.statement and $.http.status_code but has no duration slot. Span carries trace_id(16B) / span_id(8B) / parent_span_id(Option) / name / kind(Server|Client|Producer|Consumer|Internal) / start_unix_nanos / duration_nanos / status / flat-dotted attributes using semconv names (http.request.method, http.response.status_code, db.system.name, server.address, service.name) with yah-specific keys under yah.*. REUSE the existing EventScope (types.rs - Service(MeshIdent) / TaskRun / Forge) rather than minting a second scoping concept, so a span and a log line agree on what a service is.")
//! @yah:handoff("ANSWER 2 - PROPAGATION: THE OPERATOR'S THREE EXAMPLES ARE NOT ONE CHAIN. Read the paths and they split into a request-scoped trace, a control edge that must stay out of it, and an async pipeline that CANNOT be a span at all. (a) THE PUBLIC EDGE CANNOT START A TRACE. passway-demux on :443 peeks the TLS ClientHello and splices raw TCP WITHOUT terminating TLS (oss/passway/crates/sni-demux/src/lib.rs:1-16); it holds no key and sees no plaintext, and R777's tenant-isolation verdict depends on that staying true. It can never read or write a traceparent. The :80 tier (oss/passway/crates/http-router/src/lib.rs:1-21) parses HTTP but only 308-redirects or splices. The FIRST process that can mint a trace is the per-tenant passway.")
//! @yah:handoff("ANSWER 2 (b) - PASSWAY IS THE ENVOY-SIDECAR ANALOGUE AND ALREADY HAS BOTH HOOKS. It is a pingora ProxyHttp impl with request_filter -> upstream_peer -> upstream_request_filter -> response_filter (oss/passway/crates/passway/src/proxy.rs:1-32; response_filter was added by R870-F15, see :56). Mint-or-adopt traceparent in request_filter, write it onto the forwarded request in upstream_request_filter, close the SERVER span in response_filter. That is the entire ingress-side instrumentation and it needs no new process, no sidecar and no mesh. GROUNDED TRAP: traceparent is NOT in HOP_BY_HOP (oss/passway/crates/passway/src/hardening.rs:44-53) so it survives the forward by default - BUT RFC7230 6.1 nomination IS honoured, so any client sending 'Connection: traceparent' strips it unless traceparent is added to NEVER_NOMINATE_STRIP (hardening.rs:70). Without that, an outside caller can silently turn tracing off.")
//! @yah:handoff("ANSWER 2 (c) - YUBABA->KAMAJI IS A CONTROL EDGE, KEEP IT OUT OF THE REQUEST TRACE. kamaji-proto is a custom framed protocol with its own RequestId correlation token (oss/kamaji/crates/kamaji-proto/src/messages.rs:42-45) carrying YubabaToKamaji/KamajiToYubaba (:385, :495) - Deploy/Drain/etc. No user request ever flows over it. Instrumenting it measures DEPLOY latency, which is a real signal and a different one; putting it in the request trace would invent causality that does not exist. (d) NATIVE VS CONTAINER IS ALREADY SOLVED AND THE SEAM IS DEPLOY-TIME ENV. kamaji injects an identical contract on every backend: YAH_MESH_IP + PORT/PORT_<NAME> on native (oss/kamaji/crates/kamaji/src/native.rs:666,674), docker (docker.rs:494-510), containerd (containerd.rs:382-394) and microvm; the parity is test-asserted (containerd.rs:1387-1405 asserts mesh-IP env is appended after the spec's literal env). So WHERE to send spans rides an existing, parity-tested channel and needs no new mechanism. But env is DEPLOY-TIME: it can carry the collector address and can never carry per-request context. Per-request context rides the request, i.e. (b).")
//! @yah:handoff("ANSWER 2 (e) - SERVICE->DB IS AN IN-PROCESS CLIENT SPAN, NOT AN INTERCEPTABLE HOP. The db is turso 0.7.2 (root Cargo.toml:871), an embedded client - there is no proxy between workload and db to instrument, so a db.* span can only be produced inside the workload's own code. That means a yah-side helper crate, and crates/yah/log already owns exactly this shape for logs (crates/yah/log/src/service_layer.rs). RESERVED_FIELD_PATHS already reserves $.db.statement (types.rs:292), so half the vocabulary exists.")
//! @yah:handoff("ANSWER 2 (f) - DB->R2 IS NOT A SPAN AND MUST NOT BE MODELLED AS ONE. This is the single most important finding of the spike. The operator's 'db to R2 stream' example is an ASYNCHRONOUS WAL tail in a SEPARATE PROCESS: tenant-streamer, deliberately not a task inside yubaba (W253 tenet 1 control/data separation, plus the litestream precedent - oss/yubaba/crates/tenant-streamer/src/lib.rs:10-24). No request causes a given WAL frame to ship, so there is no parent span to attach to and no causal chain; forcing it into a trace would be a fabrication. It is a PIPELINE-LAG GAUGE, and the number ALREADY EXISTS: RpoStatus::watermark_age, pushed each tick to yubaba's leader-resident non-raft RpoWatermarkRegistry and read by lease_detector::judge_readiness (oss/yubaba/crates/tenant-streamer/src/rpo_report.rs:1-5). Turning it into an Analytics series is a PERSISTENCE problem, not an instrumentation one - the value is produced then discarded after each gate read. CONSEQUENCE: TWO signals ship out of this spike, not one - request-scoped spans (a,b,d,e) and a periodic lag gauge (f). Do not model them the same way, and do not sequence the cheap one behind the expensive one.")
//! @yah:handoff("ANSWER 3 - THE COLLECTOR IS SCRYER, AND THE STORE IS NOT R893-F8's. F8 is settled as a Cloudflare Health Checks API pull (its third @yah:next), so it has NO yah-side store at all - it reads a third party's. There is therefore no store to share and the question answers itself: separate, necessarily. The right collector is scryer, reusing its SUBSTRATE while staying a distinct signal. Read from oss/qed/crates/scryer/src/lib.rs:5-48 it already has: per-machine SQLite at /var/lib/yah/scryer/events.db; EventScope::Service(MeshIdent) ingestion; a Unix-socket ingestion::IngestionServer (F8) which is exactly the local-agent shape a span emitter wants; quota::ServiceQuotaManager with a 1000 ev/s per-MeshIdent default (spans are higher-volume than logs, so this ceiling matters more here than it does for logs); federation + federation_http for cross-machine; and long_tier::LongTierStore writing per-day Parquet shards to R2 keyed by (machine_id, day), which is the correct physical layout for windowed aggregates. yubaba already advertises the node's scryer at http://<mesh-ip>:6543 via /services (oss/yubaba/crates/yubaba/src/main.rs:211-216, 1137-1141). STORAGE SHAPE: a NEW spans table beside events (oss/qed/crates/scryer/src/store.rs:41 CREATE TABLE events), plus a rollup table keyed by (service, peer, window) - computing p50/p95 over raw spans on every panel load does not survive fleet volume.")
//! @yah:handoff("ANSWER 3, THE REAL BLOCKER - AND A STALE CLAIM CORRECTED. Everything downstream of scryer currently reads as 'blocked on R556-F7, the remote scryer transport, which does not exist'. THAT IS NO LONGER TRUE. R556-F7 is ARCHIVED and its four children R556-T8..T11 shipped (crates/yah/hub/src/lib.rs:133); measured 2026-09-10 and recorded at crates/yah/hub/src/in_process.rs:101, us-east-001 has yah-scryer.service installed, enabled, active and LISTENING on 0.0.0.0:6543, and the published node tarball on cdn.yah.dev ships yah-scryer. What actually blocks Mode-1 is R556-F6's gate (b) - THREE OPERATOR CONFIG ACTIONS on us-east-001, written up at in_process.rs:100-102: (1) the yah-analytics R2 bucket HAS NEVER EXISTED ('yah cloud bucket ls' -> NoSuchBucket; note 'bucket head' cannot see this, it prints identical text for an absent key and an absent bucket); (2) no /etc/systemd/system/yah-scryer.service.d/ drop-in exists, so the long tier is off; (3) yubaba's ExecStart carries no --scryer-endpoint, so /services still answers [] - cosmetic for Mode-2 and LOAD-BEARING for Mode-1. R556-F6 is in handoff and owns those three; depends_on it rather than re-filing them.")
//! @yah:handoff("ANSWER 4 - WHAT THE ANALYTICS PANEL SHOWS, per W346 section 4 (:204-215): aggregates over a window only, and the discriminator is 'you would fix it by nothing - it is a measurement of the past'. Section 4.1 (:225-229) is the live precedent for getting this wrong (MeshHealthCard renders four now-values in Analytics and is being moved to Infra), which is exactly the misfiling this answer must not repeat. THE PANEL SHOWS, per window, reusing AnalyticsView.tsx's existing LOOKBACK_OPTIONS (:69) and ChipGroup (:261): (1) A HOP MATRIX - rows = (caller service, callee), cells = call rate, error rate, p50/p95/p99 latency over the window. That is the Istio 'service graph with golden signals' and it is the product the operator asked for. (2) LATENCY DISTRIBUTION PER HOP OVER TIME, bucketed, so a regression reads as a shape change rather than as one number moving. (3) THE DB->R2 LAG GAUGE AS A SERIES - p50/max watermark_age per tenant across the window.")
//! @yah:handoff("ANSWER 4, WHAT MUST NOT GO IN ANALYTICS - written down so it is not re-litigated by the implementer. (a) THE CURRENT VALUE of any of the above. 'Streamer lag right now' and 'this hop is erroring right now' are declared-vs-live answers about a service and belong in SERVICES; the split is identical to W346 section 4.2 (:242-248) splitting the front door into a dot in Services and a series in Analytics. Same measurement, two tabs, and the rule already says which half goes where. (b) A LIVE TRACE WATERFALL / single-trace view. That is a debugging tool over one recent request - a now-value with extra steps - and if it is ever built it is not an Analytics card. Recommendation: do not build it in this relay at all. (c) Anything sourced from MeshHealthCard's RPCs, which section 4.1 is in the middle of removing.")
//! @yah:handoff("SEQUENCING AND NON-GATING. Order: R556-F6 gate (b) config actions -> observation Span type + scryer spans table -> passway instrumentation -> hop-matrix panel. THE LAG-GAUGE SERIES IS INDEPENDENT OF ALL OF IT - watermark_age already exists and only needs persisting - so it is the cheapest item on this list and must not be sequenced behind the span model. NOTHING in this spike is a dependency of R893-F8, per W346 section 5.4:337-339. UNKNOWNS, named rather than filled: (1) whether turso 0.7.2 exposes a statement-level hook a db.* span can attach to without wrapping every call site - would settle by reading the turso crate's connection API, which I did not open; (2) whether the desktop should read spans through hub Mode-1 federation or a new RPC namespace - would settle by reading crates/yah/hub/src/in_process.rs's analytics_* implementations end to end, which I read only in annotation form; (3) sampling policy (head vs tail, and rate) - genuinely an operator call, and quota::ServiceQuotaManager's 1000 ev/s per-MeshIdent default is the number to argue against.")
//! @yah:gotcha("STALE IN-TREE CLAIM FOUND WHILE SPIKING, not fixed here because this ticket is docs-only (filed as its own bug instead). packages/yah/ui/src/components/analytics/AnalyticsView.tsx:366 (MODE1_EVENT_SURFACE_EMPTY) tells the operator 'Event ingestion isn't wired for the live mesh backend yet (R556-F7) ... per-service events will appear once a remote scryer transport lands'. The transport LANDED - R556-F7 is archived and R556-T8..T11 shipped (crates/yah/hub/src/lib.rs:133), and us-east-001 has been running yah-scryer on :6543 since at least 2026-09-10 (crates/yah/hub/src/in_process.rs:101). The copy names the wrong cause and points the reader at an archived ticket; the true cause is R556-F6's gate (b) config actions. The same stale citation is repeated in the board annotations at AnalyticsView.tsx:34 and :48.")
//! @yah:gotcha("DISCOVERED GAP - it blocks the span path for the same reason it blocks the log path. scryer's own docs assert a deploy-time env contract whose producer I could not find: 'Yubaba injects YAH_SERVICE_IDENT + YAH_SCRYER_SOCKET into workload env' (oss/qed/crates/scryer/src/lib.rs:39-42 and ingestion.rs:4-5). A grep of oss/yubaba/crates/ and oss/kamaji/crates/ for either name returns ZERO hits. Only the READER exists - crates/yah/log/src/service_layer.rs:208-209 reads both and returns None when either is absent, so a workload silently logs nowhere. Stated as a read result rather than a certainty: I grepped the whole repo for both names and every non-doc hit is on the reader side; I did not trace every code path that assembles a workload env.")
//! @yah:handoff("SPIKE COMPLETE. RECOMMENDATION IN ONE LINE: adopt the OTel data model and W3C Trace Context wire format WITHOUT the OTel SDK (the second application of node.rs:18-58's already-adopted position), as a NEW Span type alongside Event; instrument at passway's existing pingora hooks, which are the only place in the ingress chain that can see a header; collect into scryer as a distinct signal on its existing substrate, which is NOT R893-F8's store because F8 has no yah-side store at all; and split the operator's ask into TWO signals, because db-to-R2 is an async WAL tail with no causal chain and can never be a span.")
//! @yah:handoff("SIX TICKETS FILED, all parented to R893, each tier-stamped in its first @yah:next. R893-F15 (Warrior) Span type in observation + spans table in scryer. R893-F16 (Wizard) passway span emission from the pingora hooks + traceparent added to NEVER_NOMINATE_STRIP; depends_on F15. R893-B17 (Warrior) scryer's YAH_SERVICE_IDENT / YAH_SCRYER_SOCKET env contract is documented but has no producer. R893-F18 (Warrior) persist watermark_age as a series + the db-to-R2 lag panel; deliberately depends on NOTHING, it is the cheapest item and must not be sequenced behind the span work. R893-F19 (Warrior) the Analytics hop matrix; depends_on F15, F16, R556-F6. R893-B20 (Thief) the Analytics empty-state copy citing an archived ticket.")
//! @yah:handoff("NO CODE WAS WRITTEN AND types.rs WAS NOT TOUCHED except through board verbs, per the dispatch. This is a docs-only outcome; there is no commit to cite.")
//! @yah:handoff("SCOPE NOTE ON WHAT THE SPIKE FOUND THAT THE BRIEF DID NOT ANTICIPATE: the brief's premise that this is blocked on a missing remote scryer transport is out of date. R556-F7 is archived, R556-T8..T11 shipped, and yah-scryer runs on us-east-001 today. What blocks Mode-1 is R556-F6's gate (b), three operator config actions. That correction is recorded in full in the ANSWER 3 handoff entry and filed as R893-B20 for the user-facing copy that still asserts the old cause.")
//! @yah:verify("Read as a reviewer: every file:line cited in the ANSWER entries was opened during this spike. The two claims stated as inference rather than as fact are marked as such in the gotchas - the missing YAH_SERVICE_IDENT producer (grep result, not an exhaustive trace of env assembly) and the three turso/hub/sampling unknowns listed in the SEQUENCING entry.")
//! @yah:verify("The four dispatch questions are answered in order and each is labelled ANSWER 1 through ANSWER 4 in the handoff list; ANSWER 4 explicitly enumerates what must NOT go in Analytics, which is the W346 section 4 rule the spike was required to respect.")
//!
//! @yah:ticket(R893-F15, "Span signal type in observation + a spans table in scryer (OTel data model, no OTel SDK)")
//! @yah:status(review)
//! @yah:at(2026-09-13T07:48:25Z)
//! @yah:assignee(agent:bundle-anthropic-ashguard)
//! @yah:parent(R893)
//! @yah:next("Tier: Warrior - the design decision is made (R893-S10); this is a well-specified type plus a SQLite table plus serde round-trip tests, mechanical once the shape is fixed.")
//! @yah:next("ADD a Span type ALONGSIDE Event, do NOT extend Event. Event (types.rs:253-265) is a log line keyed to a TaskRunId with no span identity and no duration; RESERVED_FIELD_PATHS (:285-296) has no duration slot. Fields: trace_id (16 bytes), span_id (8 bytes), parent_span_id (Option), name, kind (Server|Client|Producer|Consumer|Internal), start_unix_nanos, duration_nanos, status, and a flat dotted attribute map.")
//! @yah:next("ADOPT OTel's data model and semconv NAMES, link NO OTel crate. This follows the position already reasoned out at oss/yubaba/crates/yubaba/src/node.rs:18-58 and reinforced at crates/yah/hub/src/in_process.rs:148 - flat dotted keys that are already valid OTLP attribute keys (http.request.method, http.response.status_code, db.system.name, server.address, service.name), anything yah-specific under yah.*. Do not mint a third yah-local shape and do not pull in opentelemetry/opentelemetry_sdk/opentelemetry-otlp.")
//! @yah:next("REUSE EventScope for scoping (Service(MeshIdent) / TaskRun / Forge) rather than inventing a second scoping concept, so a span and a log line agree on what a service is.")
//! @yah:next("STORE: a new spans table in oss/qed/crates/scryer/src/store.rs beside the existing CREATE TABLE events (:41), plus a rollup table keyed by (service, peer, window). Computing p50/p95 over raw spans on every panel load will not survive fleet volume - decide the rollup cadence here, not in the view.")
//! @arch:see(.yah/docs/working/W346-services-tab-three-views-and-the-tab-boundary.md)
//! @yah:handoff("LANDED - THE SPAN TYPE, and this is the field list F16 and F19 are written against. oss/qed/crates/observation/src/types.rs, new section after RESERVED_FIELD_PATHS. `Span` (:642) = trace_id: TraceId / span_id: SpanId / parent_span_id: Option<SpanId> (None for a root, NOT the all-zero sentinel) / name: String / kind: SpanKind / start_unix_nanos: u64 / duration_nanos: u64 / status: SpanStatus / attributes: BTreeMap<String, AttrValue>. `TraceId(pub [u8;16])` (:360) and `SpanId(pub [u8;8])` (:367) serialize as LOWERCASE HEX STRINGS, not byte arrays - so the serde form is byte-identical to what traceparent and OTLP/JSON carry and F16 needs no conversion layer; both carry INVALID, is_valid(), to_hex(), from_hex(), Display, FromStr. `SpanKind` (:457) = Internal|Server|Client|Producer|Consumer, snake_case on the wire. `SpanStatus` (:495) = Unset (default) | Ok | Error { message: Option<String> }, internally tagged on `code`, plus from_parts(code, message) for the column pair a store writes. `AttrValue` (:537) = Bool|Int(i64)|Double(f64)|Str, serde untagged, so 503 stays an integer and a `>= 500` predicate means something - do NOT pass status codes as strings. Span carries NO scope of its own: stores and transports pair it with EventScope exactly as they do Event, which is how \"a span and a log line agree on what a service is\" is actually enforced rather than just asserted. Event was NOT touched.")
//! @yah:handoff("ATTRIBUTE VOCABULARY - use the consts, do not re-spell the strings. types.rs:600-637 exports ATTR_SERVICE_NAME, ATTR_SERVER_ADDRESS, ATTR_SERVER_PORT, ATTR_CLIENT_ADDRESS, ATTR_HTTP_REQUEST_METHOD, ATTR_HTTP_RESPONSE_STATUS_CODE, ATTR_HTTP_ROUTE, ATTR_URL_PATH, ATTR_DB_SYSTEM_NAME, ATTR_DB_QUERY_TEXT, ATTR_ERROR_TYPE (all verbatim OTel semconv), plus RESERVED_SPAN_ATTRIBUTES (:620) as the span-side analogue of RESERVED_FIELD_PATHS. TWO yah-namespaced keys were minted, which is what the conventions prescribe for non-standard attributes: ATTR_YAH_PEER_SERVICE = \"yah.peer.service\" (:614) and ATTR_YAH_TENANT = \"yah.tenant\". THE PEER KEY IS LOAD-BEARING FOR F19 AND F16 MUST EMIT IT: `server.address` is a host or IP, and a hop matrix keyed on an address collapses two tenants behind one mesh IP into one row. `Span::peer_ident()` (:684) is the single resolver - yah.peer.service, else server.address, else None - and both the rollup key and the hop matrix go through it, so there is one definition of \"the other end of this hop\" rather than three. NO OTel CRATE WAS LINKED; observation/Cargo.toml still reads serde / serde_json / uuid / workload-spec only. The reasoning is quoted forward from oss/yubaba/crates/yubaba/src/node.rs:18-58 into a section comment at types.rs:330-352 so the next editor finds it at the code site.")
//! @yah:handoff("ROLLUP CADENCE - DECIDED HERE, AS THE TICKET REQUIRED. **60-second tumbling windows, computed at WRITE TIME inside the same transaction as the raw span insert.** observation::ROLLUP_WINDOW_NANOS (types.rs:702) + rollup_window_start(). Three reasons, in order of weight. (1) WHY PRE-AGGREGATE AT ALL: at quota::ServiceQuotaManager's 1000 ev/s per-MeshIdent ceiling a one-hour panel load scans up to 3.6M raw spans per service; with 60s windows it reads 60 rows per (service, peer, kind). (2) WHY 60s: it is the floor that still serves every entry in AnalyticsView's LOOKBACK_OPTIONS by MERGING buckets, and it decouples the panel from raw-span retention - aggregates keep answering for windows whose raw spans have already been pruned to the Parquet long tier. (3) WHY AT WRITE TIME AND NOT A BACKGROUND SWEEPER: a sweeper needs a schedule, a liveness story and a catch-up path for whatever it missed while the node was down; riding the existing flush transaction has none of those and cannot drift from the raw table, because either both land or neither does. THE COROLLARY F19 MUST NOT UNDO: quantiles are NOT stored, an OTel EXPLICIT-BUCKET HISTOGRAM is. Quantiles do not merge - averaging 60 windows' p95 is not the p95 of their union, it is a fabricated number - whereas element-wise bucket addition is exact. observation::LatencyHistogram (:745) with LATENCY_BUCKET_BOUNDS_NANOS (:717) = OTel semconv's advised http.server.request.duration buckets converted to nanos, 14 bounds / 15 buckets, fixed-size array so a boundary change fails deserialization LOUDLY instead of silently misattributing counts to shifted bounds. It carries record / merge / quantile / p50 / p95 / p99 / mean_nanos / count. Quantiles are bucket-resolution estimates with Prometheus histogram_quantile semantics - 100 spans that were all exactly 1ms report p50 = 2.55ms, the midpoint of the 5ms bucket they share. That is documented at the method and asserted in the tests; do not \"fix\" it.")
//! @yah:handoff("STORE - oss/qed/crates/scryer/src/store.rs, two new tables appended to the existing SCHEMA const so they materialise on the next EventStore::open of an existing events.db (CREATE TABLE IF NOT EXISTS; no migration step, nothing dropped, events untouched). `spans` (:85) keyed (scope_kind, scope_id, trace_id, span_id) with indexes spans_by_start and an UN-scoped spans_by_trace, because assembling one trace means finding its spans across every service. `span_rollups` (:115) keyed (scope_kind, scope_id, peer, kind, window_start_unix_nanos), columns error_count + histogram_json. API: insert_spans (:420, returns the count of NEW spans), query_spans (:511) over a new SpanFilter (:177 - scope / trace_id / kind / start_range / limit, mirroring ScopeFilter), query_hop_rollups (:563 - the hop matrix; scope: None returns every service on the machine), prune_spans_older_than (:635) and prune_span_rollups_older_than (:647) as TWO DELIBERATELY SEPARATE CLOCKS, count_spans. Three traps recorded at the code. (a) IDEMPOTENCY: insert_spans is INSERT OR IGNORE like insert_events, and an unconditional rollup update would turn a harmless ring re-flush into double-counted latency - only spans whose insert actually changed a row contribute to the rollup. Asserted by store::tests::span_reflush_is_idempotent_in_both_tables. (b) `kind` IS IN THE ROLLUP KEY, a deliberate refinement of the ticket's \"(service, peer, window)\": one hop yields a Client span on the caller and a Server span on the callee, and summing them double-counts every call. F19 must render them as separate cells or pick a side. (c) query_hop_rollups returns the ALIGNED REQUESTED window bounds, not the bounds that had data, so HopRollup::rate_per_sec shows a genuine drop when a hop falls silent instead of an unchanged rate. HopRollup (types.rs:869) lives in observation, not scryer, so the desktop/hub side can name the type without depending on the store crate; it carries count / error_rate / rate_per_sec over the merged histogram.")
//! @yah:handoff("DISCOVERED WORK, fixed in this pass rather than filed. (1) oss/qed/crates/scryer/src/store.rs - the (scope_kind, scope_id) -> EventScope match was DUPLICATED between row_to_scope and list_scopes, and query_hop_rollups would have made it a third copy. Extracted to scope_from_parts (:672); row_to_scope and list_scopes now both call it, and list_scopes lost its divergent inline copy (it had the same unknown-kind fallback, so this is behaviour-preserving). (2) Two clippy warnings introduced by this change were cleared rather than left: a hand-written Default for SpanStatus became #[derive(Default)] + #[default], and the rollup accumulator's nested tuple type became the `RollupKey` alias (store.rs:184). cargo clippy -p observation -p yah-scryer --all-targets now reports ZERO warnings anchored in either file; the warnings that remain in the workspace are pre-existing and in task-runs / yah-object-store / other scryer modules, untouched here. NOT DONE, and deliberately out of scope: nothing EMITS a span yet (that is F16) and nothing READS the rollups yet (that is F19). ingestion::IngestionServer still speaks events only - wiring a span ingestion path through it is F16's call, since only F16 knows what the emitter can send.")
//! @yah:verify("MEASURED BY THIS COURIER, baseline taken before the first edit on the same tree. `cd oss/qed && cargo test -p observation` = 10 passed / 0 failed, baseline 1 passed / 0 failed (9 new, in types.rs mod span: trace_and_span_ids_are_lowercase_hex, span_serde_round_trip, status_round_trips_through_its_column_pair, attr_values_keep_their_type, peer_ident_prefers_the_logical_name, rollup_windows_are_60s_tumbling, histogram_merge_is_exact_and_quantiles_are_bounded, histogram_interpolates_inside_a_bucket, hop_rollup_rates_use_the_requested_window). `cd oss/qed && cargo test -p yah-scryer --lib` = 86 passed / 0 failed, baseline 82 passed / 0 failed (4 new in store::tests: spans_round_trip_through_sqlite, span_reflush_is_idempotent_in_both_tables, rollups_key_on_peer_and_kind_and_merge_across_windows, pruning_raw_spans_leaves_the_rollups_answering). Note the package is `yah-scryer`, not `scryer` - `cargo test -p scryer` exits 1 with \"did not match any packages\".")
//! @yah:verify("WIDER GATES. `cd oss/qed && cargo check --workspace --all-targets` exits 0 (two unused-import warnings remain, both pre-existing and in beholders / a task-runs re-export, neither in a file this ticket touched). Root workspace: `cargo check -p yah-hub -p yah-agent-tools -p gnomes --lib` reaches Finished with warnings only (yah-agent-tools dead_code, pre-existing) - those are the root-side consumers of yah-scryer / observation, and the change is purely additive so none needed editing. HONEST CAVEAT ON THAT LAST RUN: the camp build rail reported a deferred skew verdict - three files (crates/yah/kg-store/src/camp_config.rs, oss/yubaba/crates/tenant-streamer/src/rpo_report.rs, oss/yubaba/crates/yubaba/src/lib.rs) were edited by live peers WHILE it ran, so it describes a tree that no longer exists. None of the three is in observation's or scryer's dependency path, and the two crate-scoped test runs above were both skew-clean (\"input closure unchanged across the whole run\"), so the green is trustworthy for THIS change - but a reviewer re-running the root check should expect to re-run it rather than treat mine as authoritative.")
//! @yah:handoff("Tree anchor at handoff: e0530813af8f7d86f5eb7ea9a6b5a57d386bf30b — the shared tree as I left it. Diff against it (`git diff e0530813af8f7d86f5eb7ea9a6b5a57d386bf30b..HEAD`) to see what landed under you, and quote this SHA rather than 'HEAD' in any revert/restore instruction.")
//! @yah:next("FOR R893-F16 (passway span emission): the contract is the LANDED and ATTRIBUTE VOCABULARY handoff entries above. Emit `observation::Span` with EventScope::Service(MeshIdent) alongside it; adopt-or-mint traceparent in request_filter, write it in upstream_request_filter, close the SERVER span in response_filter. Use the ATTR_* consts. YOU MUST EMIT yah.peer.service - `server.address` alone makes the hop matrix key on an IP and collapses tenants sharing a mesh address. Status: mark SpanStatus::Error deliberately; the rollup's error_count counts ONLY explicit Error, and an un-marked HTTP 500 will read as a success, which is intentional (the emitter owns that call). Still open from the spike and NOT settled here: how spans reach scryer on the wire - ingestion::IngestionServer speaks events only today, and only F16 knows what the emitter can send.")
//! @yah:next("FOR R893-F19 (the Analytics hop matrix): read EventStore::query_hop_rollups (store.rs:563), not the raw spans table - that is the whole point of the cadence decision recorded above. One HopRollup per (service, peer, kind) already merged over the requested window, carrying count(), error_rate(), rate_per_sec() and latency.p50()/p95()/p99(). Three things not to re-litigate: quantiles come from merged explicit buckets and are bucket-resolution estimates, not exact order statistics (documented at LatencyHistogram::quantile); `kind` is part of the cell identity because a hop has a Client span AND a Server span and summing them double-counts, so render them separately or pick a side; and the window bounds returned are the range ASKED FOR, so a silent hop correctly shows a lower rate. Per W346 section 4 and the spike's ANSWER 4, this is aggregates-over-a-window only - no now-values, no single-trace waterfall.")
//! @yah:handoff("LEADER SIGN-OFF (relay R893, @Ashguard:polaris). Accepted, and this is the span track's contract ticket — the LANDED / ATTRIBUTE VOCABULARY / ROLLUP CADENCE entries above are what R893-F16 and R893-F19 are written against, so a reviewer changing the Span field list, the ATTR_* consts, or the 60s write-time tumbling-window decision is changing two other tickets at the same time. The cadence call the ticket demanded was made and justified rather than deferred to the view: pre-aggregate because a 1h panel would otherwise scan up to 3.6M raw spans per service; 60s because it merges up to every LOOKBACK_OPTIONS entry and outlives raw-span pruning; at write time because a sweeper needs a schedule, a liveness story and a catch-up path that riding the flush transaction does not.")
//! @yah:verify("RE-VERIFIED BY THE LEADER, not taken on the courier's self-report: an independent read-only session (@Miravel:spade, session:40d5a613) re-ran all four gates. cargo test -p observation 10 pass / 0 fail (baseline 1); cargo test -p yah-scryer --lib 86 pass / 0 fail (baseline 82); cargo check --workspace --all-targets from inside oss/qed exits 0; root-side consumers cargo check -p yah-hub -p yah-agent-tools --all-targets exits 0. Zero failures, none attributable to Span / SpanKind / span_rollups or any other symbol this ticket introduced. This clears the courier's own honest caveat that its root-workspace check had run under deferred peer skew — the re-run was clean.")
//! @yah:gotcha("COMMAND FORM, for anyone re-running the gates above: oss/qed is a workspace EXCLUDED from the root one (CLAUDE.md, \"Co-developed OSS repos\"), so `cargo test -p yah-scryer --lib` from the repo root exits with \"package `yah-scryer` is not a member of the workspace\" — that is not a test failure. Run it as `cargo test --manifest-path oss/qed/Cargo.toml -p yah-scryer --lib`, or cd into oss/qed first. This bit the verifier on this ticket and bit a different verifier on R893-F18 with oss/yubaba, so it is a tree-wide trap, not a one-off. The package name is `yah-scryer`, not `scryer`.")

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;
use workload_spec::MeshIdent;

// ─── TaskRunId ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskRunId(pub Uuid);

impl TaskRunId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TaskRunId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskRunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TaskRunId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

// ─── ForgeId ──────────────────────────────────────────────────────────────────

/// Stable identity for a forge run (UUID), shared across all three forge
/// species (local, remote, integration).  Stable across yah restarts.
///
/// For local-forge runs `ForgeId` and `TaskRunId` share the same underlying
/// UUID — the `From` impls below are the identity conversion.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ForgeId(pub Uuid);

impl ForgeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ForgeId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ForgeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ForgeId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

/// For local-forge: the TaskRunId IS the ForgeId.  Same UUID, no translation.
impl From<ForgeId> for TaskRunId {
    fn from(id: ForgeId) -> Self {
        Self(id.0)
    }
}

impl From<TaskRunId> for ForgeId {
    fn from(id: TaskRunId) -> Self {
        Self(id.0)
    }
}

// ─── Initiator ────────────────────────────────────────────────────────────────

/// Who or what initiated a run.
///
/// Lives here rather than in `task-runs` for the same reason [`ForgeId`] does:
/// it is pure data that describers of work (queues, schedulers, wire clients)
/// need without dragging in a SQLite store. `task-runs` re-exports it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Initiator {
    Human { camp: String },
    Agent { camp: String, agent: String, session: String },
    Gnome { camp: String, shift: String },
    Cron { camp: String, schedule: String },
}

// ─── RunStatus ────────────────────────────────────────────────────────────────

/// Terminal + in-flight status of a run.
///
/// Hoisted alongside [`Initiator`] so the task vocabulary can describe a run's
/// lifecycle without depending on the store crate. `task-runs` re-exports it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Done { exit_code: i32, ended_at: u64 },
    Killed { signal: i32, ended_at: u64 },
    Lost { reason: String },
}

// ─── EventScope ───────────────────────────────────────────────────────────────

/// The scope an event row belongs to — stored as `(scope_kind, scope_id)` in
/// scryer's events.db so the index generalizes across all species.
///
/// `TaskRun` corresponds to per-run task-run.db rows (existing local-forge
/// store).  `Service` corresponds to long-lived service events keyed by mesh
/// identity.  `Forge` is the unified scope for all three forge species (local,
/// remote, integration); for local-forge runs `Forge(id)` and
/// `TaskRun(id.into())` refer to the same underlying UUID.  `TaskRun` is kept
/// as a backward-compatible alias so existing local-forge queries continue
/// working without a migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum EventScope {
    TaskRun(TaskRunId),
    Service(MeshIdent),
    /// Unified scope for forge runs.  Preferred over `TaskRun` for new code;
    /// scryer stores both so old queries still resolve.
    Forge(ForgeId),
}

impl EventScope {
    pub fn kind_str(&self) -> &'static str {
        match self {
            EventScope::TaskRun(_) => "task_run",
            EventScope::Service(_) => "service",
            EventScope::Forge(_) => "forge",
        }
    }

    pub fn id_str(&self) -> String {
        match self {
            EventScope::TaskRun(id) => id.to_string(),
            EventScope::Service(ident) => ident.0.clone(),
            EventScope::Forge(id) => id.to_string(),
        }
    }
}

// ─── Level ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Trace => "trace",
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
            Level::Fatal => "fatal",
        }
    }
}

impl std::str::FromStr for Level {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trace" => Ok(Level::Trace),
            "debug" => Ok(Level::Debug),
            "info" => Ok(Level::Info),
            "warn" => Ok(Level::Warn),
            "error" => Ok(Level::Error),
            "fatal" => Ok(Level::Fatal),
            other => Err(format!("unknown level: {other}")),
        }
    }
}

// ─── EventSource ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventSource {
    Beholder { name: String, version: String },
    Shim { lib: String, version: String },
    Synth,
}

impl EventSource {
    pub fn kind_str(&self) -> &'static str {
        match self {
            EventSource::Beholder { .. } => "beholder",
            EventSource::Shim { .. } => "shim",
            EventSource::Synth => "synth",
        }
    }

    pub fn name_str(&self) -> &str {
        match self {
            EventSource::Beholder { name, .. } => name,
            EventSource::Shim { lib, .. } => lib,
            EventSource::Synth => "synth",
        }
    }
}

// ─── ChunkRef + Event ─────────────────────────────────────────────────────────

/// Back-pointer from an event into the raw byte stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub seq: u32,
}

/// A structured event row — written by beholders (Tier 1.5) or shims (Tier 2).
///
/// `fields` holds open-shape JSON; reserved key prefixes follow a loose
/// OTel-semconv flavor (`error.*`, `file.*`, `test.*`, `build.*`, etc.).
/// See `RESERVED_FIELD_PATHS` for the canonical list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub run_id: TaskRunId,
    pub seq: u32,
    pub offset_ms: u32,
    pub level: Level,
    /// Dot-namespaced producer identity, e.g. `"cargo::rustc"`, `"tsc"`.
    pub target: String,
    pub msg: String,
    /// Freeform JSON object. Reserved key prefixes are listed in `RESERVED_FIELD_PATHS`.
    pub fields: serde_json::Value,
    pub anchor: Option<ChunkRef>,
    pub source: EventSource,
}

/// Normalized diagnostic shape — produced by beholders and shims where it makes sense.
/// Agents query `task.diagnostics` rather than parsing raw events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostic {
    pub severity: Level,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub col: Option<u32>,
    pub code: Option<String>,
    pub message: String,
    pub source: String,
    pub run_id: TaskRunId,
    pub event_seq: u32,
}

// ─── Reserved field paths (OTel-semconv-lite) ─────────────────────────────────

/// Field paths in `Event.fields` that have defined semantics.
pub const RESERVED_FIELD_PATHS: &[&str] = &[
    "$.error.kind",
    "$.error.code",
    "$.file.path",
    "$.file.line",
    "$.file.col",
    "$.http.status_code",
    "$.db.statement",
    "$.test.name",
    "$.test.suite",
    "$.build.unit",
];

// ─── Span (OTel data model, no OTel SDK) ─────────────────────────────────────

// This section is the SECOND application of the position reasoned out at
// `oss/yubaba/crates/yubaba/src/node.rs:18-58`: **we adopt OpenTelemetry's data
// model and semantic-convention attribute names; we do not link the OTel SDK.**
// No `opentelemetry`, `opentelemetry_sdk` or `opentelemetry-otlp` dependency
// appears here or in this crate's manifest, and none should be added — the
// binary-size argument that justified skipping the SDK for node metrics applies
// unchanged to yubaba/kamaji/passway, which all ship as curl-fetched musl-static
// artifacts.
//
// What that buys: [`TraceId`]/[`SpanId`] serialize as the lowercase hex the
// `traceparent` header and OTLP/JSON both use, and [`Span::attributes`] keys are
// already valid OTLP attribute keys. A real OTLP exporter later is a rename-free
// loop over these keys, not a re-modelling.
//
// A `Span` is deliberately NOT an [`Event`] with extra fields. `Event` is a log
// line keyed to a [`TaskRunId`] with no span identity and no duration, and
// [`RESERVED_FIELD_PATHS`] has no duration slot. Scoping IS shared: spans are
// stored as `(EventScope, Span)` pairs exactly as events are, so a span and a
// log line agree on what a service is.

/// W3C Trace Context trace id — 16 bytes. All-zero is the spec's "invalid"
/// sentinel, so [`TraceId::is_valid`] is the check a receiver makes before
/// adopting an inbound `traceparent`.
///
/// Serializes as a 32-char lowercase hex string: the same text that appears in
/// the `traceparent` header and in OTLP/JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TraceId(pub [u8; 16]);

/// W3C Trace Context span id — 8 bytes. All-zero is the "invalid" sentinel;
/// a root span carries `parent_span_id: None` rather than an all-zero parent.
///
/// Serializes as a 16-char lowercase hex string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpanId(pub [u8; 8]);

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

fn hex_decode<const N: usize>(s: &str, what: &str) -> Result<[u8; N], String> {
    let bytes = s.as_bytes();
    if bytes.len() != N * 2 {
        return Err(format!("{what}: expected {} hex chars, got {}", N * 2, bytes.len()));
    }
    let mut out = [0u8; N];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = (bytes[i * 2] as char)
            .to_digit(16)
            .ok_or_else(|| format!("{what}: not hex: {s}"))?;
        let lo = (bytes[i * 2 + 1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("{what}: not hex: {s}"))?;
        *slot = ((hi << 4) | lo) as u8;
    }
    Ok(out)
}

macro_rules! hex_id {
    ($ty:ident, $n:expr, $what:literal) => {
        impl $ty {
            /// The spec's all-zero "invalid" value.
            pub const INVALID: Self = Self([0u8; $n]);

            /// False for the all-zero sentinel, which the W3C spec forbids
            /// propagating.
            pub fn is_valid(&self) -> bool {
                self.0 != [0u8; $n]
            }

            /// Lowercase hex, as it appears in `traceparent` and OTLP/JSON.
            pub fn to_hex(&self) -> String {
                hex_encode(&self.0)
            }

            pub fn from_hex(s: &str) -> Result<Self, String> {
                Ok(Self(hex_decode::<$n>(s, $what)?))
            }
        }

        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl std::str::FromStr for $ty {
            type Err = String;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::from_hex(s)
            }
        }

        impl Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
                ser.serialize_str(&self.to_hex())
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
                let s = String::deserialize(de)?;
                Self::from_hex(&s).map_err(serde::de::Error::custom)
            }
        }
    };
}

hex_id!(TraceId, 16, "trace_id");
hex_id!(SpanId, 8, "span_id");

/// OTel `SpanKind` — which side of a hop this span describes.
///
/// One hop produces TWO spans: a `Client` span on the caller and a `Server`
/// span on the callee. They are not interchangeable and must not be merged,
/// which is why `kind` is part of the rollup key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SpanKind::Internal => "internal",
            SpanKind::Server => "server",
            SpanKind::Client => "client",
            SpanKind::Producer => "producer",
            SpanKind::Consumer => "consumer",
        }
    }
}

impl std::str::FromStr for SpanKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "internal" => Ok(SpanKind::Internal),
            "server" => Ok(SpanKind::Server),
            "client" => Ok(SpanKind::Client),
            "producer" => Ok(SpanKind::Producer),
            "consumer" => Ok(SpanKind::Consumer),
            other => Err(format!("unknown span kind: {other}")),
        }
    }
}

/// OTel span status. `Unset` is the default and is NOT an error — only an
/// explicit `Error` counts toward a hop's error rate.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum SpanStatus {
    #[default]
    Unset,
    Ok,
    Error { message: Option<String> },
}

impl SpanStatus {
    pub fn code_str(&self) -> &'static str {
        match self {
            SpanStatus::Unset => "unset",
            SpanStatus::Ok => "ok",
            SpanStatus::Error { .. } => "error",
        }
    }

    pub fn message(&self) -> Option<&str> {
        match self {
            SpanStatus::Error { message } => message.as_deref(),
            _ => None,
        }
    }

    /// Rebuild from the `(status_code, status_message)` column pair a store writes.
    pub fn from_parts(code: &str, message: Option<String>) -> Result<Self, String> {
        match code {
            "unset" => Ok(SpanStatus::Unset),
            "ok" => Ok(SpanStatus::Ok),
            "error" => Ok(SpanStatus::Error { message }),
            other => Err(format!("unknown span status code: {other}")),
        }
    }
}

/// An OTel attribute value.
///
/// Typed rather than freeform `serde_json::Value` because consumers compare
/// these: `http.response.status_code` has to be an integer for a `>= 500`
/// predicate to mean anything. Arrays are deliberately absent — nothing emits
/// one yet, and adding a variant later is a non-breaking widening.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttrValue {
    Bool(bool),
    Int(i64),
    Double(f64),
    Str(String),
}

impl From<bool> for AttrValue {
    fn from(v: bool) -> Self {
        AttrValue::Bool(v)
    }
}
impl From<i64> for AttrValue {
    fn from(v: i64) -> Self {
        AttrValue::Int(v)
    }
}
impl From<u16> for AttrValue {
    fn from(v: u16) -> Self {
        AttrValue::Int(v as i64)
    }
}
impl From<f64> for AttrValue {
    fn from(v: f64) -> Self {
        AttrValue::Double(v)
    }
}
impl From<&str> for AttrValue {
    fn from(v: &str) -> Self {
        AttrValue::Str(v.to_string())
    }
}
impl From<String> for AttrValue {
    fn from(v: String) -> Self {
        AttrValue::Str(v)
    }
}

impl AttrValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            AttrValue::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            AttrValue::Int(i) => Some(*i),
            _ => None,
        }
    }
}

/// Semconv attribute keys this codebase emits, by their standard OTel spelling.
///
/// Keys with no standard equivalent are namespaced under `yah.`, which is what
/// the conventions prescribe for non-standard attributes. Emitters should use
/// these consts rather than re-spelling the strings.
pub const ATTR_SERVICE_NAME: &str = "service.name";
pub const ATTR_SERVER_ADDRESS: &str = "server.address";
pub const ATTR_SERVER_PORT: &str = "server.port";
pub const ATTR_CLIENT_ADDRESS: &str = "client.address";
pub const ATTR_HTTP_REQUEST_METHOD: &str = "http.request.method";
pub const ATTR_HTTP_RESPONSE_STATUS_CODE: &str = "http.response.status_code";
pub const ATTR_HTTP_ROUTE: &str = "http.route";
pub const ATTR_URL_PATH: &str = "url.path";
pub const ATTR_DB_SYSTEM_NAME: &str = "db.system.name";
pub const ATTR_DB_QUERY_TEXT: &str = "db.query.text";
pub const ATTR_ERROR_TYPE: &str = "error.type";

/// yah-specific: the LOGICAL service name of the other end of the hop.
///
/// `server.address` is a host or IP, which is not a service — two tenants
/// behind one mesh address are one row in a hop matrix keyed on it. The mesh
/// knows the logical name, so it is recorded separately and preferred by
/// [`Span::peer_ident`].
pub const ATTR_YAH_PEER_SERVICE: &str = "yah.peer.service";
/// yah-specific: the tenant a span was produced on behalf of.
pub const ATTR_YAH_TENANT: &str = "yah.tenant";

/// Attribute keys with defined semantics, the span-side analogue of
/// [`RESERVED_FIELD_PATHS`].
pub const RESERVED_SPAN_ATTRIBUTES: &[&str] = &[
    ATTR_SERVICE_NAME,
    ATTR_SERVER_ADDRESS,
    ATTR_SERVER_PORT,
    ATTR_CLIENT_ADDRESS,
    ATTR_HTTP_REQUEST_METHOD,
    ATTR_HTTP_RESPONSE_STATUS_CODE,
    ATTR_HTTP_ROUTE,
    ATTR_URL_PATH,
    ATTR_DB_SYSTEM_NAME,
    ATTR_DB_QUERY_TEXT,
    ATTR_ERROR_TYPE,
    ATTR_YAH_PEER_SERVICE,
    ATTR_YAH_TENANT,
];

/// One OTel span — a timed operation with a caller and a callee.
///
/// Carries no scope of its own: stores and transports pair it with an
/// [`EventScope`] exactly as they do [`Event`], so `Service(MeshIdent)` means
/// the same thing for both signals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    /// `None` for a root span. The all-zero [`SpanId::INVALID`] is not used here.
    pub parent_span_id: Option<SpanId>,
    /// Low-cardinality operation name — `GET /orders/{id}`, not `GET /orders/42`.
    pub name: String,
    pub kind: SpanKind,
    /// Wall-clock start, nanoseconds since the Unix epoch.
    pub start_unix_nanos: u64,
    /// Elapsed time. Stored rather than an end timestamp because that is the
    /// number every aggregate wants and it cannot go negative across a clock step.
    pub duration_nanos: u64,
    #[serde(default)]
    pub status: SpanStatus,
    /// Flat, dotted, already-valid-OTLP attribute keys. See
    /// [`RESERVED_SPAN_ATTRIBUTES`].
    #[serde(default)]
    pub attributes: BTreeMap<String, AttrValue>,
}

impl Span {
    pub fn end_unix_nanos(&self) -> u64 {
        self.start_unix_nanos.saturating_add(self.duration_nanos)
    }

    /// Only an explicit `Error` status counts. An HTTP 500 that the emitter did
    /// not mark `Error` is not an error here — the emitter owns that call, so
    /// the aggregate does not second-guess it.
    pub fn is_error(&self) -> bool {
        matches!(self.status, SpanStatus::Error { .. })
    }

    pub fn attr(&self, key: &str) -> Option<&AttrValue> {
        self.attributes.get(key)
    }

    /// The other end of the hop, for the rollup key and the hop matrix.
    ///
    /// `yah.peer.service` (logical) wins over `server.address` (an address).
    /// `None` when neither is present — the caller stores that as the empty
    /// string so the key stays NOT NULL.
    pub fn peer_ident(&self) -> Option<&str> {
        self.attr(ATTR_YAH_PEER_SERVICE)
            .and_then(AttrValue::as_str)
            .or_else(|| self.attr(ATTR_SERVER_ADDRESS).and_then(AttrValue::as_str))
    }
}

// ─── Rollup (windowed aggregates over spans) ─────────────────────────────────

/// Rollup cadence: **60-second tumbling windows**, computed at write time.
///
/// Decided in R893-F15 rather than left to the view, because the view cannot
/// fix it: at `quota::ServiceQuotaManager`'s 1000 ev/s per-`MeshIdent` ceiling a
/// one-hour panel load would scan up to 3.6M raw spans per service. Sixty
/// seconds is the floor that serves every entry in the Analytics
/// `LOOKBACK_OPTIONS` list by merging buckets, and it makes the rollup
/// independent of the raw-span retention clock — the panel keeps answering for
/// windows whose raw spans have already been pruned to the Parquet tier.
pub const ROLLUP_WINDOW_NANOS: u64 = 60 * 1_000_000_000;

/// Floor a timestamp to the start of its rollup window.
pub fn rollup_window_start(unix_nanos: u64) -> u64 {
    unix_nanos - (unix_nanos % ROLLUP_WINDOW_NANOS)
}

/// Explicit bucket boundaries (inclusive upper bounds, nanoseconds) for latency
/// histograms — OTel semconv's advised `http.server.request.duration` buckets,
/// converted from seconds.
///
/// Changing these is a SCHEMA change, not a tuning knob: stored histograms are
/// arrays positional against this list and old rows become unmergeable. A
/// length mismatch fails deserialization loudly rather than silently
/// misattributing counts.
pub const LATENCY_BUCKET_BOUNDS_NANOS: [u64; 14] = [
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    75_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    750_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    7_500_000_000,
    10_000_000_000,
];

/// Number of histogram buckets — one per boundary plus the overflow bucket.
pub const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKET_BOUNDS_NANOS.len() + 1;

/// An OTel explicit-bucket histogram of span durations.
///
/// Buckets rather than stored quantiles because **quantiles do not merge**:
/// averaging two windows' p95 is not the p95 of their union, so a panel that
/// spans 60 windows would be reading a fabricated number. Element-wise bucket
/// addition IS exact, which is what makes a 60s cadence usable for a 30-day
/// lookback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyHistogram {
    /// Positional against [`LATENCY_BUCKET_BOUNDS_NANOS`]; last entry is the
    /// overflow bucket for durations above the final boundary.
    pub counts: [u64; LATENCY_BUCKET_COUNT],
    pub sum_nanos: u64,
    pub min_nanos: Option<u64>,
    pub max_nanos: Option<u64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: [0u64; LATENCY_BUCKET_COUNT],
            sum_nanos: 0,
            min_nanos: None,
            max_nanos: None,
        }
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn record(&mut self, duration_nanos: u64) {
        let idx = LATENCY_BUCKET_BOUNDS_NANOS
            .iter()
            .position(|b| duration_nanos <= *b)
            .unwrap_or(LATENCY_BUCKET_BOUNDS_NANOS.len());
        self.counts[idx] += 1;
        self.sum_nanos = self.sum_nanos.saturating_add(duration_nanos);
        self.min_nanos = Some(self.min_nanos.map_or(duration_nanos, |m| m.min(duration_nanos)));
        self.max_nanos = Some(self.max_nanos.map_or(duration_nanos, |m| m.max(duration_nanos)));
    }

    /// Exact element-wise merge — this is why the buckets are stored.
    pub fn merge(&mut self, other: &LatencyHistogram) {
        for (slot, add) in self.counts.iter_mut().zip(other.counts.iter()) {
            *slot += add;
        }
        self.sum_nanos = self.sum_nanos.saturating_add(other.sum_nanos);
        self.min_nanos = match (self.min_nanos, other.min_nanos) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max_nanos = match (self.max_nanos, other.max_nanos) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
    }

    pub fn mean_nanos(&self) -> Option<u64> {
        let n = self.count();
        (n > 0).then(|| self.sum_nanos / n)
    }

    /// Quantile by linear interpolation inside the containing bucket, clamped
    /// to the observed `[min, max]`.
    ///
    /// This is bucket-resolution estimation, the same semantics Prometheus's
    /// `histogram_quantile` has: 100 spans that were all exactly 1ms report a
    /// p50 of 2.55ms, the midpoint of the `<= 5ms` bucket they share. The
    /// clamp only recovers an exact value where a bucket's samples are also the
    /// histogram's own min or max. Landing in the unbounded overflow bucket
    /// returns `max_nanos`, the only defensible value available.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        let target = ((q * total as f64).ceil() as u64).max(1);
        let mut cumulative = 0u64;
        for (i, bucket) in self.counts.iter().enumerate() {
            if *bucket == 0 {
                continue;
            }
            cumulative += bucket;
            if cumulative < target {
                continue;
            }
            let Some(upper) = LATENCY_BUCKET_BOUNDS_NANOS.get(i).copied() else {
                return self.max_nanos;
            };
            let lower = if i == 0 { 0 } else { LATENCY_BUCKET_BOUNDS_NANOS[i - 1] };
            let rank_in_bucket = target - (cumulative - bucket);
            let frac = rank_in_bucket as f64 / *bucket as f64;
            let value = lower as f64 + (upper - lower) as f64 * frac;
            let value = value.round() as u64;
            return Some(value.clamp(
                self.min_nanos.unwrap_or(0),
                self.max_nanos.unwrap_or(u64::MAX),
            ));
        }
        self.max_nanos
    }

    pub fn p50(&self) -> Option<u64> {
        self.quantile(0.50)
    }
    pub fn p95(&self) -> Option<u64> {
        self.quantile(0.95)
    }
    pub fn p99(&self) -> Option<u64> {
        self.quantile(0.99)
    }
}

/// One cell of the hop matrix: a `(service, peer, kind)` edge aggregated over a
/// window range.
///
/// `kind` is part of the identity on purpose — the same hop shows up as a
/// `Client` span on the caller and a `Server` span on the callee, and summing
/// them double-counts every call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HopRollup {
    pub scope: EventScope,
    /// [`Span::peer_ident`], or the empty string when the span named no peer.
    pub peer: String,
    pub kind: SpanKind,
    /// Aligned start of the merged range, inclusive.
    pub window_start_unix_nanos: u64,
    /// Aligned end of the merged range, exclusive. Reflects the RANGE ASKED
    /// FOR, not the windows that happened to have data, so a hop that went
    /// silent reads as a genuine drop in [`HopRollup::rate_per_sec`].
    pub window_end_unix_nanos: u64,
    pub error_count: u64,
    pub latency: LatencyHistogram,
}

impl HopRollup {
    pub fn count(&self) -> u64 {
        self.latency.count()
    }

    pub fn error_rate(&self) -> f64 {
        let n = self.count();
        if n == 0 {
            0.0
        } else {
            self.error_count as f64 / n as f64
        }
    }

    pub fn rate_per_sec(&self) -> f64 {
        let span_nanos = self
            .window_end_unix_nanos
            .saturating_sub(self.window_start_unix_nanos);
        if span_nanos == 0 {
            0.0
        } else {
            self.count() as f64 / (span_nanos as f64 / 1e9)
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod scope {
    use super::*;

    /// Verify that EventScope::Forge round-trips through the (scope_kind,
    /// scope_id) column representation used by scryer's events.db.
    #[test]
    fn forge() {
        let id = ForgeId::new();
        let scope = EventScope::Forge(id.clone());

        // Column values are what the store writes and reads back.
        assert_eq!(scope.kind_str(), "forge");
        assert_eq!(scope.id_str(), id.to_string());

        // The id_str must parse back to an identical ForgeId.
        let recovered: ForgeId = scope.id_str().parse().unwrap();
        assert_eq!(recovered, id);

        // Serde round-trip (used by RPC wire format).
        let json = serde_json::to_string(&scope).unwrap();
        let back: EventScope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, scope);

        // TaskRun and Forge with the same underlying UUID produce distinct scopes.
        let task_scope = EventScope::TaskRun(TaskRunId(id.0));
        assert_ne!(task_scope.kind_str(), scope.kind_str());
        assert_eq!(task_scope.id_str(), scope.id_str()); // same UUID, different kind
    }
}

#[cfg(test)]
mod span {
    use super::*;

    fn sample() -> Span {
        Span {
            trace_id: TraceId([
                0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e,
                0x47, 0x36,
            ]),
            span_id: SpanId([0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]),
            parent_span_id: Some(SpanId([1, 2, 3, 4, 5, 6, 7, 8])),
            name: "GET /orders/{id}".to_string(),
            kind: SpanKind::Server,
            start_unix_nanos: 1_757_000_000_000_000_000,
            duration_nanos: 12_345_678,
            status: SpanStatus::Error { message: Some("upstream refused".to_string()) },
            attributes: BTreeMap::from([
                (ATTR_HTTP_REQUEST_METHOD.to_string(), AttrValue::from("GET")),
                (ATTR_HTTP_RESPONSE_STATUS_CODE.to_string(), AttrValue::from(503u16)),
                (ATTR_SERVER_ADDRESS.to_string(), AttrValue::from("100.64.0.2")),
                (ATTR_YAH_PEER_SERVICE.to_string(), AttrValue::from("orders")),
            ]),
        }
    }

    /// The hex form is the wire contract shared with `traceparent` and OTLP/JSON,
    /// so it is asserted literally rather than only round-tripped.
    #[test]
    fn trace_and_span_ids_are_lowercase_hex() {
        let span = sample();
        assert_eq!(span.trace_id.to_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(span.span_id.to_hex(), "00f067aa0ba902b7");

        assert_eq!(TraceId::from_hex(&span.trace_id.to_hex()).unwrap(), span.trace_id);
        assert_eq!(SpanId::from_hex(&span.span_id.to_hex()).unwrap(), span.span_id);

        // Serde sees a bare string, not a byte array.
        assert_eq!(
            serde_json::to_string(&span.span_id).unwrap(),
            "\"00f067aa0ba902b7\""
        );

        // Wrong length and non-hex both fail rather than truncating.
        assert!(TraceId::from_hex("4bf92f35").is_err());
        assert!(SpanId::from_hex("zzf067aa0ba902b7").is_err());

        // All-zero is the spec's invalid sentinel.
        assert!(!TraceId::INVALID.is_valid());
        assert!(!SpanId::INVALID.is_valid());
        assert!(span.trace_id.is_valid());
    }

    #[test]
    fn span_serde_round_trip() {
        let span = sample();
        let json = serde_json::to_string(&span).unwrap();
        let back: Span = serde_json::from_str(&json).unwrap();
        assert_eq!(back, span);

        // A root span with no parent, no attributes and an unset status is the
        // other extreme of the shape.
        let root = Span {
            parent_span_id: None,
            status: SpanStatus::Unset,
            attributes: BTreeMap::new(),
            kind: SpanKind::Client,
            ..sample()
        };
        let back: Span = serde_json::from_str(&serde_json::to_string(&root).unwrap()).unwrap();
        assert_eq!(back, root);
    }

    #[test]
    fn status_round_trips_through_its_column_pair() {
        for status in [
            SpanStatus::Unset,
            SpanStatus::Ok,
            SpanStatus::Error { message: None },
            SpanStatus::Error { message: Some("boom".to_string()) },
        ] {
            let back: SpanStatus =
                serde_json::from_str(&serde_json::to_string(&status).unwrap()).unwrap();
            assert_eq!(back, status);

            // The (status_code, status_message) representation a store writes.
            let rebuilt =
                SpanStatus::from_parts(status.code_str(), status.message().map(str::to_string))
                    .unwrap();
            assert_eq!(rebuilt, status);
        }
        assert!(SpanStatus::from_parts("weird", None).is_err());
        assert!(sample().is_error());
        assert!(!Span { status: SpanStatus::Ok, ..sample() }.is_error());
    }

    /// Attribute values must keep their JSON type — a status code compared as a
    /// string sorts "1000" before "500".
    #[test]
    fn attr_values_keep_their_type() {
        let attrs: BTreeMap<String, AttrValue> = serde_json::from_str(
            r#"{"a": 503, "b": "503", "c": 1.5, "d": true, "e": 2.0}"#,
        )
        .unwrap();
        assert_eq!(attrs["a"], AttrValue::Int(503));
        assert_eq!(attrs["b"], AttrValue::Str("503".to_string()));
        assert_eq!(attrs["c"], AttrValue::Double(1.5));
        assert_eq!(attrs["d"], AttrValue::Bool(true));
        assert_eq!(attrs["e"], AttrValue::Double(2.0));
        assert_eq!(attrs["a"].as_i64(), Some(503));
        assert_eq!(attrs["a"].as_str(), None);
    }

    #[test]
    fn peer_ident_prefers_the_logical_name() {
        let span = sample();
        assert_eq!(span.peer_ident(), Some("orders"));

        // Falls back to the address when no logical name was recorded.
        let mut attrs = span.attributes.clone();
        attrs.remove(ATTR_YAH_PEER_SERVICE);
        let addr_only = Span { attributes: attrs, ..sample() };
        assert_eq!(addr_only.peer_ident(), Some("100.64.0.2"));

        let bare = Span { attributes: BTreeMap::new(), ..sample() };
        assert_eq!(bare.peer_ident(), None);
    }

    #[test]
    fn rollup_windows_are_60s_tumbling() {
        assert_eq!(ROLLUP_WINDOW_NANOS, 60_000_000_000);
        assert_eq!(rollup_window_start(0), 0);
        assert_eq!(rollup_window_start(59_999_999_999), 0);
        assert_eq!(rollup_window_start(60_000_000_000), 60_000_000_000);
        assert_eq!(rollup_window_start(60_000_000_001), 60_000_000_000);
    }

    #[test]
    fn histogram_merge_is_exact_and_quantiles_are_bounded() {
        assert_eq!(LATENCY_BUCKET_COUNT, LATENCY_BUCKET_BOUNDS_NANOS.len() + 1);

        let mut a = LatencyHistogram::new();
        assert!(a.is_empty());
        assert_eq!(a.quantile(0.5), None);

        // 100 spans at 1ms, one at 30s (the unbounded overflow bucket).
        for _ in 0..100 {
            a.record(1_000_000);
        }
        let mut b = LatencyHistogram::new();
        b.record(30_000_000_000);

        let mut merged = a.clone();
        merged.merge(&b);
        assert_eq!(merged.count(), 101);
        assert_eq!(merged.count(), a.count() + b.count());
        assert_eq!(merged.sum_nanos, a.sum_nanos + b.sum_nanos);
        assert_eq!(merged.min_nanos, Some(1_000_000));
        assert_eq!(merged.max_nanos, Some(30_000_000_000));

        // Merging is order-independent and element-wise exact.
        let mut other_way = b.clone();
        other_way.merge(&a);
        assert_eq!(other_way, merged);

        // p50 and p99 both land in the `<= 5ms` bucket the 1ms crowd shares, so
        // they report that bucket's interpolated position — 2.55ms at rank 51
        // of 100, its 5ms upper bound at rank 100. Bucket resolution, not a
        // bug: the histogram cannot distinguish 1ms from 4ms after the fact.
        assert_eq!(merged.p50(), Some(2_550_000));
        assert_eq!(merged.p99(), Some(5_000_000));
        // The 30s outlier is only reachable at q=1.0, and comes back exact
        // because the overflow bucket has no upper bound to interpolate against
        // and falls back to the recorded max.
        assert_eq!(merged.quantile(1.0), Some(30_000_000_000));

        // The clamp does recover an exact value when the bucket's sample is
        // also the histogram's own extreme.
        let mut lone = LatencyHistogram::new();
        lone.record(1_000_000);
        assert_eq!(lone.p50(), Some(1_000_000));

        // An empty merge changes nothing.
        let before = merged.clone();
        merged.merge(&LatencyHistogram::new());
        assert_eq!(merged, before);
    }

    #[test]
    fn histogram_interpolates_inside_a_bucket() {
        let mut h = LatencyHistogram::new();
        // Straddle the 25ms boundary: 1 fast, 1 slow.
        h.record(1_000_000);
        h.record(200_000_000);
        assert_eq!(h.count(), 2);
        assert_eq!(h.mean_nanos(), Some(100_500_000));
        // p50 is rank 1 → the fast sample's `<= 5ms` bucket, reported at its
        // upper bound. The observed min does not clamp it, because 1ms is not
        // the value being estimated — the bucket is.
        assert_eq!(h.p50(), Some(5_000_000));
        // p95 is rank 2 → the slow sample's `<= 250ms` bucket, clamped back
        // down to the observed max of 200ms.
        assert_eq!(h.p95(), Some(200_000_000));

        let json = serde_json::to_string(&h).unwrap();
        let back: LatencyHistogram = serde_json::from_str(&json).unwrap();
        assert_eq!(back, h);

        // A bucket-count change is a schema change and must fail loudly, not
        // silently misattribute counts to shifted boundaries.
        let short = r#"{"counts":[1,2,3],"sum_nanos":6,"min_nanos":1,"max_nanos":3}"#;
        assert!(serde_json::from_str::<LatencyHistogram>(short).is_err());
        let long = format!(
            r#"{{"counts":[{}],"sum_nanos":6,"min_nanos":1,"max_nanos":3}}"#,
            vec!["0"; LATENCY_BUCKET_COUNT + 1].join(",")
        );
        assert!(serde_json::from_str::<LatencyHistogram>(&long).is_err());
    }

    #[test]
    fn hop_rollup_rates_use_the_requested_window() {
        let mut latency = LatencyHistogram::new();
        for _ in 0..120 {
            latency.record(2_000_000);
        }
        let hop = HopRollup {
            scope: EventScope::Service(MeshIdent("passway".to_string())),
            peer: "orders".to_string(),
            kind: SpanKind::Client,
            window_start_unix_nanos: 0,
            window_end_unix_nanos: ROLLUP_WINDOW_NANOS,
            error_count: 12,
            latency,
        };
        assert_eq!(hop.count(), 120);
        assert_eq!(hop.rate_per_sec(), 2.0); // 120 calls over a 60s window
        assert!((hop.error_rate() - 0.1).abs() < f64::EPSILON);

        let back: HopRollup =
            serde_json::from_str(&serde_json::to_string(&hop).unwrap()).unwrap();
        assert_eq!(back, hop);

        // An empty hop reports zeroes rather than dividing by zero.
        let empty = HopRollup {
            error_count: 0,
            latency: LatencyHistogram::new(),
            window_end_unix_nanos: 0,
            ..hop
        };
        assert_eq!(empty.error_rate(), 0.0);
        assert_eq!(empty.rate_per_sec(), 0.0);
    }
}
