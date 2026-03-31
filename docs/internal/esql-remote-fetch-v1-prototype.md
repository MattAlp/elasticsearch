# ES|QL Remote Fetch V1 Prototype

This branch builds the first working coordinator-side remote fetch prototype on top of `main` without replacing
the existing node-reduce late-materialization path.

## Scope

The first slice focuses on a narrow but real distributed path for the linear TopN family:

- A serializable row handle that can cross transport boundaries.
- A retained-search-context registry so data nodes can serve a second fetch phase after the initial query finishes.
- A dedicated planner setting to gate the new cross-node path independently from existing reduction late materialization.
- A constrained planner/runtime rewrite that replaces local `_doc` continuation with remote fetch handles at the
  final coordinator exchange.

## Why These Pieces Come First

`_doc` is deliberately local-only today. It can drive `FieldExtractExec` on a node-reduce driver, but it does not
cross the wire to the coordinator. A real distributed fetch therefore needs an explicit remote handle, not just
another place to insert `FieldExtractExec`.

Similarly, the initial distributed query currently releases its `AcquiredSearchContexts` when the data-node compute
completes. A continuation round trip needs those contexts to survive long enough for a follow-up fetch request.

## Landed in This Slice

- `PlannerSettings.REMOTE_FETCH_LATE_MATERIALIZATION`
  - Separate gate for the new prototype path.
  - Now flows through `PlannerSettings.Holder`, so the data-node request path can actually use it.
- `RemoteFetchHandle`
  - Transport-safe `(nodeId, sessionId, shard, segment, doc)` handle.
  - Supports both `Writeable` serialization and `BytesRef` encoding for future page/block transport.
- `RetainedSearchContextsRegistry`
  - Registers a session for follow-up fetch.
  - Allows temporary leases while preventing new acquires after the registration closes.
  - Releases the underlying `AcquiredSearchContexts` only after the last active lease completes.
- `RemoteFetchService`
  - Registers an internal `esql/query/remote_fetch` transport handler on data nodes.
  - Validates node/session ownership, reconstructs a `DocVector` from `RemoteFetchHandle`s, and runs
    `ValuesSourceReaderOperator` to load the requested fields in handle order.
  - Returns transport-serializable `Page` batches that contain only the fetched field blocks.
  - Also owns a best-effort `esql/query/remote_fetch/release` transport handler so retained sessions can be
    closed explicitly after the coordinator is done with them.
- Handle emission and coordinator fetch operators
  - `EmitRemoteFetchHandleExec` / `RemoteFetchHandleOperator` replace local `_doc` rows with
    transport-safe `RemoteFetchHandle`s before the final coordinator exchange.
  - `RemoteFetchExec` / `RemoteFetchOperator` batch handles by `(nodeId, sessionId)`, issue remote fetch
    requests, and merge fetched columns back into the coordinator stream in input order.
- Constrained planner wiring for the linear TopN family
  - `RemoteFetchPlanner` now rewrites the local-cluster distributed TopN reduction path when the query shape
    is `Project -> TopN` and the missing coordinator columns are remote-fetchable fields/metadata.
  - The data side keeps only `_doc` plus ordering keys through the distributed narrowing phases, then emits a
    remote fetch handle instead of a local `_doc`.
  - The coordinator inserts `RemoteFetchExec` under the final `ProjectExec` and fetches only the deferred
    columns after the distributed TopN has finished narrowing candidates.
- Distributed request lifecycle wiring
  - `DataNodeRequest` now carries a transport-versioned `retainSearchContexts` flag.
  - `DataNodeComputeHandler` registers retained sessions only for flagged requests, keeps them alive after the
    first phase succeeds, and closes them immediately on failure/cancellation.
  - `ComputeService` tracks per-node retained sessions for the local-cluster distributed path and releases them
    at query teardown.
  - Retention now uses the node-reduce internal session id, which matches the session embedded into emitted
    remote fetch handles.
  - `ComputeSearchContext` now owns the underlying `SearchContext` via explicit reference counting so retained
    sessions keep index readers alive after the data-driver phase completes.
- Transport compatibility
  - Added `esql_remote_fetch_retained_contexts` via `generateTransportVersion` so the new request flag is
    properly version-gated on the wire.
- Verification
  - Added focused unit coverage for remote fetch handles, the fetch transport service, the planner rewrite,
    and retained-search-context lifecycle behavior.
  - Added a multi-node internal-cluster test that exercises the end-to-end remote fetch TopN path and verifies
    that deferred columns are fetched on the coordinator rather than the original data/node-reduce drivers.

## Next Steps

1. Broaden planner support beyond the current `Project -> TopN` family so more late-materialization shapes can
   use the same continuation substrate.
2. Extend the continuation contract from field fetch to deterministic late filters, reusing the same retained
   search-context lifecycle and per-owner routing model.
3. Decide how score-mutating late operations should fit the continuation model, since they change ranking
   semantics rather than just materializing columns.
4. Extend the retained-session and remote fetch lifecycle to remote-cluster execution once the local-cluster
   continuation flow is stable enough to generalize.
