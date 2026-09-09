# ES|QL Remote Fetch Post-MVP Roadmap

**Date:** 2026-09-09

**Status:** Approved direction; pending review of this written specification

## Objective

Turn the merged TopN remote-fetch prototype into a safe experimental platform, collect enough evidence to define conservative automatic
selection, and then expand support without adding more plan-shape special cases.

The intended product outcome remains
[`elastic/elasticsearch#134804`](https://github.com/elastic/elasticsearch/issues/134804): ES|QL timestamp sorting should approach or beat the
equivalent Query DSL workload. The broader program is
[`elastic/esql-planning#127`](https://github.com/elastic/esql-planning/issues/127): load document fields at the latest safe and useful point
in multi-stage searches.

## Merged Baseline

The following work is complete:

| Pull request | Delivered capability |
|---|---|
| [`#148028`](https://github.com/elastic/elasticsearch/pull/148028) | Initial transport and retained-context foundation |
| [`#150484`](https://github.com/elastic/elasticsearch/pull/150484) | Remote-fetch plans, document handles, and serialization |
| [`#150485`](https://github.com/elastic/elasticsearch/pull/150485) | Fetch service, field loading, and exchange execution |
| [`#150486`](https://github.com/elastic/elasticsearch/pull/150486) | Ref-counted retained contexts and cancellation-safe compute leases |
| [`#150487`](https://github.com/elastic/elasticsearch/pull/150487) | Streaming coordinator operator, batching, backpressure, and profiles |
| [`#155374`](https://github.com/elastic/elasticsearch/pull/155374) | Operator documentation and corrected test breaker accounting |
| [`#155628`](https://github.com/elastic/elasticsearch/pull/155628) | TopN planner integration, boundary contract, compatibility gates, and end-to-end tests |

The resulting implementation is a coherent, fail-closed prototype:

- The dynamic cluster setting `esql.query.remote_fetch_topn.enabled` is off by default.
- Planning requires node-level reduction, compatible field extraction, and a compatible cluster transport version.
- One distributed TopN fetch point is supported in a local cluster.
- Sort inputs remain eager; supported output fields are deferred.
- The coordinator runs the global TopN and fetches fields only for winning document handles.
- Unsupported plans keep eager loading.
- A transport-safe boundary validates the data-node and coordinator schemas.
- Binary and duplicate document handles survive TopN and exchanges.
- Single-node, multi-node, profile, negative-plan, and mixed-version fallback tests exist.

This baseline is suitable for controlled experiments. It is not ready for automatic or default enablement.

## Findings

### Sound architectural choices

The boundary, document handle, retained-context registry, and batched fetch operator form a useful reusable foundation. The implementation
also has strong defensive properties:

- planning falls back before rewriting an unsupported plan;
- execution fails explicitly if a rewritten plan reaches an incompatible node;
- retained-context release is idempotent;
- a compute-scoped lease prevents cancellation from closing contexts under active drivers;
- query-level cleanup reaches data nodes that contributed no final rows.

### Correctness blockers

[`elastic/elasticsearch#157270`](https://github.com/elastic/elasticsearch/issues/157270) is the primary blocker. After a fast data node
finishes initial compute, its registration becomes idle. A global TopN that waits more than the five-minute keep-alive for another node can
lose the fast node's contexts before fetch begins.

[`elastic/elasticsearch#154720`](https://github.com/elastic/elasticsearch/issues/154720) is the second blocker. Unit coverage exercises the
registry state machine, but there is no end-to-end proof that cancellation at different execution stages returns retained and open search
contexts to zero after drivers terminate.

Fetch-phase failures currently fail the query even when partial results are allowed. That behavior is acceptable for an off-by-default
prototype, but it must be specified before automatic selection. The initial partial-results policy should omit unavailable winners,
preserve the relative order of complete rows, report affected failures, and avoid backfilling replacement rows. Queries that disallow
partial results should fail.

### Planner and API debt

[`elastic/esql-planning#1875`](https://github.com/elastic/esql-planning/issues/1875) captures focused cleanup:

- supported fetch-plan validation has more than one source of truth;
- nullable values and booleans encode states that should be represented by one object;
- generic TopN planning scans for a remote-fetch-specific handle;
- production planner dependencies are nullable for test convenience;
- fetch-plan accessors accept shapes that the current planner does not produce.

The planner also has a manual physical pipeline-breaker list, a hard-coded fetchability allowlist, and shape-specific eager/deferred field
analysis. Extending these lists directly would make each new query shape more difficult to reason about.

### Evidence and observability gaps

The pilot in [`elastic/esql-planning#834`](https://github.com/elastic/esql-planning/issues/834) supports the performance hypothesis for
wide source-backed output:

- one node improved by 2.4x to 4.5x at p50;
- three nodes improved by 1.7x to 2.2x at p50 for completed comparisons.

Those results predate the merged planner code and do not establish a safe automatic-selection rule. Missing evidence includes narrow
doc-value controls, concurrent queries, warm and cold caches, resource measurements, exchange payload size, and retained-context count and
duration.

Profiles expose pages, rows, batches, exchanges, and which fields loaded before fetch. They do not expose selection reasons, exchange
payload bytes, or retained-context lifetime. The earlier profile array-index failure also lacks a named regression test.

## Chosen Strategy

Use three parallel workstreams immediately, followed by lineage-led expansion:

1. harden lifecycle and failure semantics;
2. rerun and complete performance validation on merged code;
3. clean the planning APIs and prepare common analysis.

This is preferable to either extreme:

- Adding query shapes directly to the current rule would be faster initially but compound shape-specific debt.
- Building the complete generalized planner first would delay correctness fixes and merged-code evidence.

The workstreams converge at an enablement gate. Generalized query-shape work begins after the lifecycle contract is safe and the current
TopN implementation has a reliable baseline.

## Workstream A: Reconcile the Issue Graph

Perform the following issue updates:

1. Update and close [`elastic/esql-planning#802`](https://github.com/elastic/esql-planning/issues/802). Its pull request, schema checks,
   eager fallback, and profile verification landed in `#155628`. Move residual work to the dedicated linked issues.
2. Update [`elastic/esql-planning#225`](https://github.com/elastic/esql-planning/issues/225) to mark `#802` complete and explicitly list
   `elastic/elasticsearch#154720`, `elastic/elasticsearch#157270`, `#803`, and `#1875` as remaining TopN work.
3. Move [`elastic/esql-planning#803`](https://github.com/elastic/esql-planning/issues/803) from blocked to in progress.
4. Keep [`elastic/esql-planning#834`](https://github.com/elastic/esql-planning/issues/834) in progress until the merged-code matrix is
   complete.
5. Keep [`elastic/esql-planning#226`](https://github.com/elastic/esql-planning/issues/226) blocked on the completed matrix, then use its
   results to define selection.
6. Assign `#1875` to the owner of the post-MVP cleanup and move it into the active TopN milestone.
7. Keep [`elastic/esql-planning#1673`](https://github.com/elastic/esql-planning/issues/1673) as the separate post-MVP generalization branch.
8. Keep `elastic/elasticsearch#134804` open as the product outcome and add an explicit benchmark link once its exact workload is measured.

## Workstream B: Make Context Ownership Query-Scoped

Adopt a renewable coordinator ownership model rather than an indefinite remote reference.

### Lifecycle

1. Data-node initial compute registers contexts under the node-reduce session.
2. Producer activity and compute leases protect contexts while local drivers run.
3. When initial compute completes, the registration becomes coordinator-owned and renewable.
4. A query-scoped coordinator tracker refreshes every participating node before its keep-alive expires, including nodes that contribute no
   final rows.
5. Each fetch exchange acquires and releases only its execution lease.
6. Fetch operators do not close the underlying registration; this permits later fetch stages to reuse it.
7. Root-query success, failure, or cancellation sends an idempotent release to every tracked node.
8. If the coordinator disappears or release delivery fails, missed renewals make the registration eligible for bounded reaping.

The renewal interval must be comfortably below the keep-alive and use the thread-pool relative clock. Tests inject a clock or short
keep-alive; they do not sleep for the production timeout.

This model satisfies both current TopN and future multi-stage requirements. A non-expiring coordinator reference is rejected because it
would leak indefinitely after coordinator loss.

### Required tests

- A fast data node remains fetchable while another node delays global TopN beyond the test keep-alive.
- Renewal stops and contexts expire after simulated coordinator abandonment.
- Success, planning failure, compute failure, fetch failure, and cancellation release all registrations.
- Cancellation is injected before registration, during data compute, during global TopN, during exchange setup, and during fetch.
- Existing fetch leases remain valid while registration closes; new leases are rejected.
- Assertions wait for driver teardown and then verify retained sessions and open search contexts return to zero.

## Workstream C: Complete Evidence and Observability

### Minimal instrumentation

Add enough data to explain both wins and regressions:

- estimated input and fetched page bytes, named as estimates unless actual serialized exchange byte accounting is available;
- retained-session count, peak count, hold duration, and release reason;
- registration-to-first-fetch latency;
- remote-fetch planning decision and rejection reason;
- existing rows, pages, batches, exchanges, and field-load placement.

Release transport failures should increment a metric and remain bounded by the reaper. Logging level should follow existing ES|QL transport
failure conventions; the metric is the reliable signal.

### Experimental controls

Keep the cluster boolean as a safety kill switch. Add a hidden query-level mode with these values:

- `off`: force eager loading;
- `auto`: apply safety eligibility and the cost rule;
- `force`: bypass the cost rule but never bypass correctness, transport, or field-semantics gates.

This permits paired runs without changing cluster state. The chosen mode and decision reason appear in explain or profile output.

### Benchmark matrix

Repeat the existing Wikipedia matrix on merged code and add:

- one and multiple data nodes;
- one and multiple shards;
- narrow doc-value and wide source-backed output;
- small and large candidate limits;
- field and score sorts;
- warm and cold caches;
- single and concurrent queries;
- coordinator and data-node breaker use;
- exchange rows and payload bytes;
- retained-session count and duration;
- cases where remote fetch is slower.

Add the exact `FROM logs-*-* | SORT @timestamp` workload from `elastic/elasticsearch#134804`, comparing Query DSL, eager ES|QL, and
remote-fetch ES|QL with equivalent filters and response fields. Wikipedia results alone do not close the timestamp-sort issue.

Published results must come from merged code, distinguish positive and negative cases, identify experimental controls, and include enough
configuration to reproduce each run.

## Workstream D: Clean APIs Before Generalization

Complete `elastic/esql-planning#1875` as a focused stack:

1. centralize supported fetch-plan validation;
2. make retained-context coordinator state unambiguous;
3. remove remote-fetch discovery from generic TopN and TopNBy planning, using explicit unsortable channels as the smaller initial change;
4. make `RemoteFetchService` non-null in production and test planners;
5. align accepted fetch-plan shapes with actual producers;
6. consolidate duplicated eager/deferred setup logic where doing so does not obscure coordinator and data-node responsibilities.

A native document-handle block type remains a possible later optimization. It is not required merely to remove the current generic TopN
scan.

## Workstream E: Generalize Through Lineage and Demand

Do not add each new operation as another special case in `PlanRemoteFetch`.

Start with [`elastic/esql-planning#1677`](https://github.com/elastic/esql-planning/issues/1677):

- classify operators by document identity behavior: preserve, duplicate, select, merge, or destroy;
- track the first operation that demands each field;
- retain a hidden handle while a deferred field is still needed;
- select the latest safe and useful fetch point;
- use eager loading when identity or field semantics are ambiguous;
- migrate the current TopN path onto this analysis before adding broad new shapes.

Then expand in this order:

1. [`#1674`](https://github.com/elastic/esql-planning/issues/1674): LIMIT, LIMIT BY, and TopNBy. Existing reduction planners provide a
   bounded first vertical extension.
2. [`#1676`](https://github.com/elastic/esql-planning/issues/1676): multiple stages for EVAL, WHERE, and RERANK. This depends on query-scoped
   retained contexts.
3. [`#1675`](https://github.com/elastic/esql-planning/issues/1675): FORK and FUSE. This requires handle reconciliation and multiple exchange
   planning.
4. [`#1678`](https://github.com/elastic/esql-planning/issues/1678): full-text rescoring. Keep this separate because Lucene scoring semantics
   are not ordinary field loading.
5. [`#1672`](https://github.com/elastic/esql-planning/issues/1672): cross-cluster search. Keep this separate because handles, authorization,
   compatibility, partial results, and routing must become cluster-aware.

Small safe coverage improvements, such as a dedicated remote-fetch test for WHERE before TopN or an additional directly loadable field
type, may land earlier when they do not introduce a new planner special case.

## Enablement Gates

### Safe experimental platform

- Active queries cannot lose retained contexts to idle expiry.
- Cancellation and all terminal paths have end-to-end leak coverage.
- Retention after coordinator loss remains bounded.
- Partial-fetch failure semantics are specified and tested.
- Release failures and retention lifetime are observable.

### Automatic selection

- The merged-code benchmark matrix contains positive and negative controls.
- The exact timestamp-sort workload is measured.
- Explain or profile identifies selection and rejection reasons.
- Query-level force-on and force-off comparisons are available.
- `#226` defines a conservative rule with no known correctness changes.
- Resource use remains within existing limits, or evidence justifies a specific additional limit.

### Query-shape expansion

- Current TopN uses common lineage and field-demand analysis.
- Every supported operator has an explicit document-identity classification.
- Fetch-on and fetch-off results match for null, multivalue, unmapped, mixed-index, alias, and duplicate-handle cases.
- Multiple fetch stages share query-owned contexts without premature release.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Shape expansion outruns lifecycle safety | Query-scoped ownership is a gate for multi-stage work |
| Planner special cases become unmaintainable | Migrate TopN to lineage and demand before broad expansion |
| Wide-field wins hide narrow-field regressions | Require narrow controls and publish negative cases |
| Retained contexts amplify resource use | Measure count and duration first; add a limit only with evidence |
| Heartbeats retain abandoned sessions | Renewable deadlines expire after coordinator loss |
| Partial failures change user-visible behavior | Define and test omission, ordering, and failure reporting before automatic selection |
| Benchmark controls accidentally bypass safety | Force mode bypasses cost only, never correctness or compatibility gates |
| The issue graph continues to report completed work as active | Reconcile `#802`, `#225`, and `#803` before starting the next stack |

## Proposed Milestones

1. **Graph and baseline:** reconcile issues and capture a merged-code performance baseline.
2. **Safe prototype:** land cancellation coverage, renewable query ownership, and explicit partial-failure behavior.
3. **Measurable prototype:** add selection, payload, and retention observability; complete the benchmark matrix.
4. **Selectable optimization:** define the conservative rule and retain force-on/off experimentation.
5. **Generalized planner:** migrate TopN to lineage and demand, then add LIMIT-family support.
6. **Multi-stage search:** add multiple fetch points, FORK/FUSE, full-text rescoring, and CCS through their separate milestones.

Work in milestones 1 and 2 and the focused API cleanup can proceed in parallel. Automatic enablement waits for milestones 2, 3, and 4;
broad shape expansion waits for query-scoped ownership and the lineage/demand foundation.
