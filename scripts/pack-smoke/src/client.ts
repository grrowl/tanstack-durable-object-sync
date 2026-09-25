// Browser usage (README §3), type-checked in the worker program: it needs the
// real schema `Api`, whose type graph includes the server entry.
import { createCollection } from "@tanstack/db"
import {
  ConnectionLostError,
  doCollectionOptions,
  type RowOf,
  WebSocketTransport,
} from "tanstack-durable-object-sync/client"
import type { Api, Message } from "./schema.js"

export function connect(url: string) {
  const transport = new WebSocketTransport<Api>({ url })
  const messages = createCollection(
    doCollectionOptions<Api, "messages">({ transport, table: "messages", getKey: (m) => m.id }),
  )
  const row: Message | undefined = messages.get("some-id")
  const same: RowOf<Api, "messages"> | undefined = row
  const cleared: Promise<{ deleted: number }> = transport.call.clearRoom()
  return { messages, same, cleared, isLost: (e: unknown) => e instanceof ConnectionLostError }
}
