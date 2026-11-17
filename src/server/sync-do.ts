// SyncDurableObject — the trivial application of the `Syncable` mixin over a
// bare `DurableObject` (ADR-0001 D13, ADR-0015).
//
// The sync machinery now lives in `Syncable(Base)` (mixin.ts). This module keeps
// `SyncDurableObject` as the zero-config base class it has always been, so every
// existing `extends SyncDurableObject<Env, Claims>` keeps compiling and behaving
// identically to 0.4.0: sockets are the mixin's sync sockets, and the two
// DO-global side effects (`ping/pong` auto-response and `PRAGMA
// case_sensitive_like = ON`) default ON because the base IS `DurableObject`.
//
// The legacy protected surface (`this.sql`, `this.registerSync`,
// `this.runSyncedWrite`, an overridable `parseAttachment`) is re-exposed here as
// thin aliases over the `this.sync` facade. `this.sql` is safe on this base
// because a bare `DurableObject` defines no `sql` member to shadow — on a
// non-trivial host reach `this.ctx.storage.sql` directly (ADR-0015).

import { DurableObject } from "cloudflare:workers"
import { Syncable } from "./mixin.ts"
import type { CompiledSync, SyncSchema } from "./registry.ts"

// Generics are erased at runtime, so the base VALUE is the plain application of
// the factory over DurableObject; Env/TUser are re-exposed through this class's
// own typed shims below (a base-class expression cannot reference a class's own
// type parameters — TS2562). The factory already exposes a `(...args: any[])`
// construct signature, so this generic subclass can forward `super(ctx, env)`
// for any `Env`.
const SyncableBase = Syncable()(DurableObject)

export abstract class SyncDurableObject<Env = unknown, TUser = unknown> extends SyncableBase {
  // The mixin's return type hides the tuning knobs (they must not widen the
  // host-collision surface). They ARE protected fields on the mixin at runtime;
  // re-declare them here (ambient — no runtime field, no shadow) so existing
  // `protected override readonly tickMs = …` subclasses keep compiling. Behind a
  // non-DO host, tune with `this.sync.configure` instead.
  declare protected readonly tickMs: number
  declare protected readonly compactionEvery: number
  declare protected readonly changelogRetentionMs: number | null
  declare protected readonly dedupRetentionMs: number
  declare protected readonly maxOpsPerMutation: number
  declare protected readonly maxSubsPerSocket: number
  declare protected readonly maxFrameBytes: number

  constructor(ctx: ConstructorParameters<typeof DurableObject>[0], env: Env) {
    super(ctx, env)
    // Bridge the overridable protected `parseAttachment` into the facade so an
    // override on a subclass is honoured at upgrade time (resolved dynamically).
    this.sync.configure({ parseAttachment: (req) => this.parseAttachment(req) })
  }

  /** SQLite handle. Safe on this base (a bare `DurableObject` defines no `sql`
   *  member); on a non-trivial host reach `this.ctx.storage.sql` directly to
   *  avoid shadowing the host's `sql` tagged template (ADR-0015). */
  protected get sql(): SqlStorage {
    return this.ctx.storage.sql
  }

  /**
   * Wire collections for sync (ADR-0007). Legacy alias for
   * `this.sync.registerSync`. Call in your constructor's `blockConcurrencyWhile`,
   * after your tables exist.
   */
  protected registerSync(schema: SyncSchema<TUser, Env>): void {
    // The base facade is generics-erased (SyncApi<unknown, unknown>); the runtime
    // is type-agnostic, so re-narrowing the schema here is sound.
    this.sync.registerSync(schema as SyncSchema<unknown, unknown>)
  }

  /**
   * Apply a server-originated write and broadcast it (ADR-0006). Legacy alias for
   * `this.sync.runSyncedWrite`.
   */
  protected runSyncedWrite<T>(fn: (sql: SqlStorage) => T): T {
    return this.sync.runSyncedWrite(fn)
  }

  /** The compiled schema; throws (ADR-0007) if `registerSync` hasn't run yet.
   *  Legacy alias for `this.sync.registry`. */
  protected get registry(): CompiledSync<TUser, Env> {
    return this.sync.registry as CompiledSync<TUser, Env>
  }

  /** Drain the CDC log and broadcast pending deltas (ADR-0006). Legacy alias for
   *  `this.sync.drainAndBroadcast`. */
  protected drainAndBroadcast(): void {
    this.sync.drainAndBroadcast()
  }

  /**
   * Validate the upgrade and produce the attachment bound to the WebSocket
   * (read via `deserializeAttachment` in handlers). Override to read a
   * Worker-forged claims header and/or reject by throwing a `Response`.
   * Default: no identity.
   */
  protected parseAttachment(_req: Request): TUser | Promise<TUser> {
    return undefined as TUser
  }
}
