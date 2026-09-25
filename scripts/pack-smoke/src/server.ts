// Both server entries: the base class from `.` and the mixin from both `.` and
// `./server/mixin` (README §1 and "Syncable mixin").
import { DurableObject } from "cloudflare:workers"
import { SYNC_TAG, SyncDurableObject, Syncable } from "tanstack-durable-object-sync"
import { Syncable as SyncableFromMixin } from "tanstack-durable-object-sync/server/mixin"
import { chatSchema, type Claims, type Env, type Message } from "./schema.js"

const DDL = `CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY, author TEXT NOT NULL, content TEXT NOT NULL, created_at INTEGER NOT NULL)`

export class SessionDO extends SyncDurableObject<Env, Claims> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      this.sql.exec(DDL)
      this.registerSync(chatSchema)
    })
  }

  protected parseAttachment(req: Request): Claims {
    return JSON.parse(req.headers.get("x-claims") ?? "{}") as Claims
  }

  seed(m: Message): void {
    this.runSyncedWrite((sql) => {
      sql.exec("INSERT INTO messages VALUES (?, ?, ?, ?)", m.id, m.author, m.content, m.created_at)
    })
  }
}

export class MixedDO extends Syncable<Env, Claims>()(DurableObject<Env>) {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sync.configure({ parseAttachment: () => ({ userId: "u" }) })
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(DDL)
      this.sync.registerSync(chatSchema)
    })
  }
}

// Both entries must hand out the same mixin (one module instance).
export const mixinEntriesAgree: boolean = (Syncable as unknown) === SyncableFromMixin
export const syncTag: string = SYNC_TAG
