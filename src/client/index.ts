// tanstack-durable-object-sync/client — browser entry.
//
// The TanStack DB collection-options creator and WebSocket transport. Depends
// on `@tanstack/db` (a peer): types throughout, plus its predicate compiler at
// runtime for the where-filter preflight.
//
//   - WebSocketTransport: one per DO; single appliedSeq cursor; awaitSeq.
//   - doCollectionOptions: build a TanStack DB CollectionConfig, optionally
//     server-filtered by a `where` predicate.

export {
  ConnectionLostError,
  defaultReconnectDelay,
  MutationRejectedError,
  TransportClosedError,
  TransportStoppedError,
  WebSocketTransport,
} from "./transport.ts"
export type { ReconnectDelayFn, SubHandler, Transport, TransportOptions, WebSocketLike } from "./transport.ts"
export { doCollectionOptions, WriteOutsideSubError } from "./do-collection.ts"
export type { CollectionName, DoApiCollectionOptions, DoSyncMeta, RowOf } from "./do-collection.ts"
// SSR (@tanstack/db ≥0.8.0 DbClient dehydrate/hydrate; ADR-0011). Create one
// SsrSnapshotTransport PER REQUEST and pass `(req) => stub.readSyncSnapshot(req, request)`
// — the same claims-bearing Request the WS upgrade gets (one auth gate, both paths).
export { SsrReadOnlyError, SsrSnapshotTransport } from "./ssr-transport.ts"
export type { SnapshotRead } from "./ssr-transport.ts"
