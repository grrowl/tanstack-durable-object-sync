// Worker-side SSR render (README "SSR"): per-request DbClient over a snapshot
// transport, dehydrated for the browser. This path pulls @tanstack/db's query
// runtime into the Worker's module graph — the path that crashed at module
// evaluation on @tanstack/db 0.8.5 (ADR-0022).
import { collectionOptions, DbClient } from "@tanstack/db"
import { doCollectionOptions, SsrSnapshotTransport, type SnapshotRead } from "tanstack-durable-object-sync/client"
import type { Api } from "./schema.js"

export async function render(read: SnapshotRead) {
  const transport = new SsrSnapshotTransport<Api>({ read })
  const db = new DbClient()
  const messages = db.collection(
    collectionOptions("messages", () =>
      doCollectionOptions<Api, "messages">({ transport, table: "messages", getKey: (m) => m.id }),
    ),
  )
  await messages.preload()
  return db.dehydrate()
}
