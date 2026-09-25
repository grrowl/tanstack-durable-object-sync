// The shared schema, as a consumer writes it (README §1). `Api` is the client
// contract; it is imported type-only by the client-side files.
import { defineSync } from "tanstack-durable-object-sync"
import type { MixedDO, SessionDO } from "./server.js"

export interface Claims { userId: string }
export interface Env {
  SESSION: DurableObjectNamespace<SessionDO>
  MIXED: DurableObjectNamespace<MixedDO>
}
export interface Message { id: string; author: string; content: string; created_at: number }

const sync = defineSync<Claims, Env>()

export const chatSchema = sync.schema({
  collections: {
    messages: sync.collection<Message>({
      pk: "id",
      mutations: {
        insert: {
          authorize: ({ user, op }) => {
            if (op.cols.author !== user.userId) throw new Error("author mismatch")
          },
          execute: ({ op, sql }) => {
            const m: Message = op.cols
            sql.exec("INSERT INTO messages VALUES (?, ?, ?, ?)", m.id, m.author, m.content, m.created_at)
          },
        },
        delete: { execute: ({ op, sql }) => void sql.exec("DELETE FROM messages WHERE id = ?", op.key) },
      },
    }),
  },
  commands: {
    clearRoom: sync.command()(({ sql }) => {
      sql.exec("DELETE FROM messages")
      return { deleted: 0 }
    }),
  },
})

export type Api = typeof chatSchema
