// The client entry alone, under a DOM-only program: its declarations must not
// reach for worker types.
import {
  defaultReconnectDelay,
  doCollectionOptions,
  MutationRejectedError,
  SsrReadOnlyError,
  SsrSnapshotTransport,
  TransportClosedError,
  WebSocketTransport,
  WriteOutsideSubError,
} from "tanstack-durable-object-sync/client"

const transport = new WebSocketTransport({ url: "wss://example.test/sync/1", reconnectDelay: defaultReconnectDelay(250) })
export const errors = [MutationRejectedError, SsrReadOnlyError, TransportClosedError, WriteOutsideSubError]
export { doCollectionOptions, SsrSnapshotTransport, transport }
