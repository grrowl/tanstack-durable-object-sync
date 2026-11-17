// Sync schema — the object-shaped authoring API (ADR-0014).
//
// An app authors its DO's sync surface with `defineSync<User, Env>()`, which
// binds the identity (User) and binding-env (Env) once and hands back three
// co-located helpers:
//
//   const sync = defineSync<Claims, Env>()
//   const schema = sync.schema({ collections: { … }, commands: { … } })
//   export type Api = typeof schema      // the WHOLE client contract
//
// `schema(...)` is a VALUE: the DO registers it (`registerSync`), and the client
// imports it *as a type only* (`typeof schema`) to drive `transport.call.*` and
// `doCollectionOptions`. Each collection entry carries its Row in the type — via
// an explicit generic (`collection<Row>({ … })`) OR inferred from the row schema
// on its insert mutation (`collection({ mutations: { insert: { schema } } })`).
// The collection KEY is the DB table name.
//
// `registerSync` compiles the schema value into the flat dispatch structures the
// DO consumes (collections `{table,pk}`, mutations keyed `${table}:${type}`,
// commands keyed by name) — see `compileSchema`. The wire/dispatch semantics are
// unchanged; this file only changes how the author DECLARES them.

import type { MutOp, RowOp } from "../wire/frames.ts"
import { SYNC_PREFIX } from "./changes.ts"
import type { StandardSchemaV1 } from "./standard-schema.ts"

export type { StandardSchemaV1 }

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/

// ---------------------------------------------------------------------------
// Standard Schema — the dependency-free `~standard` interface that zod/valibot/
// arktype all satisfy, vendored verbatim in `./standard-schema.ts` (we import no
// validator and add no dependency).
//
// A schema does double duty: it infers types (a row schema on `insert.schema`
// infers a collection's Row, `command(schema, …)` infers a command's Args) AND it
// validates at runtime. `compileSchema` calls `~standard.validate` before the
// handler runs and throws on issues, rejecting the frame (fail loud).
//
// It is a validation GATE, not a parser: the handler receives the original wire
// value — the schema's INPUT — never its parsed output. We infer from input
// (`StandardSchemaV1.InferInput`), so `op.cols`/`args` describe exactly what the
// handler gets; a transforming schema's output is validated but discarded, never
// applied. Applying it would rewrite a row the client already holds optimistically
// (manufacturing divergence), and a pk rewrite would break optimistic-id ==
// confirmed-id (ADR-0001 D9).
// ---------------------------------------------------------------------------
type InferSchema<S extends StandardSchemaV1> = StandardSchemaV1.InferInput<S>

// ---------------------------------------------------------------------------
// Collection identity (runtime) + per-op shapes.
// ---------------------------------------------------------------------------
export interface CollectionDef {
  /** Table name; also the collection's wire identity. The author creates the
   *  table themselves (migration); the framework only reads + emits it. */
  table: string
  /** Primary-key column — a client-supplied TEXT key (ULID/UUIDv7). Enforced
   *  against the actual table by `registerSync` (ADR-0007). */
  pk: string
}

/** Per-op shape of `op`, discriminated by `type`: an insert carries the full row
 *  (ADR-0001 D19), an update a top-level partial patch (ADR-0002 C6), a delete
 *  only the key. */
export type InsertOp<Row> = { type: "insert"; key: string; cols: Row }
export type UpdateOp<Row> = { type: "update"; key: string; cols: Partial<Row> }
export type DeleteOp = { type: "delete"; key: string; cols?: undefined }

/** Context for a mutation handler. `env` is the DO's binding env; `execute` runs
 *  inside `transactionSync`, so it must be synchronous (ADR-0001 D11/C6).
 *  `TOp` is the typed `op` (defaults to the erased wire `MutOp`). */
export interface MutationCtx<TUser, Env = unknown, TOp = MutOp> {
  user: TUser
  op: TOp
  sql: SqlStorage
  env: Env
}

/** Context for a command handler. `execute` runs OUTSIDE any transaction (so it
 *  may be async). `Args` defaults to `unknown` (the erased runtime view). */
export interface CommandCtx<TUser, Env = unknown, Args = unknown> {
  user: TUser
  args: Args
  sql: SqlStorage
  env: Env
}

// ---------------------------------------------------------------------------
// Authoring types — the closed mutation trio + collection/command entries.
//
// `insert` and `update` each carry an optional `schema` (the row schema, and the
// patch schema). The schema validates `op.cols` at runtime AND types it — for
// `insert`, it is also the inference source for the collection's Row.
// ---------------------------------------------------------------------------
export interface InsertDef<TUser, Env, Row> {
  /** Row schema. Validates the full-row `cols` and, when used as the inference
   *  source, types the collection's Row. `<Row, unknown>`: the schema's INPUT is
   *  the Row (what the handler receives); its output is discarded (gate, not parser). */
  schema?: StandardSchemaV1<Row, unknown>
  authorize?: (ctx: MutationCtx<TUser, Env, InsertOp<Row>>) => void | Promise<void>
  execute: (ctx: MutationCtx<TUser, Env, InsertOp<Row>>) => void
  afterCommit?: (ctx: MutationCtx<TUser, Env, InsertOp<Row>>) => unknown | Promise<unknown>
}
export interface UpdateDef<TUser, Env, Row> {
  /** Patch schema. The author supplies a PARTIAL schema (e.g. `Row.partial()`),
   *  since an update carries a top-level partial patch, not a full row. Output is
   *  discarded — only the INPUT (the patch shape the handler receives) is typed. */
  schema?: StandardSchemaV1<Partial<Row>, unknown>
  authorize?: (ctx: MutationCtx<TUser, Env, UpdateOp<Row>>) => void | Promise<void>
  execute: (ctx: MutationCtx<TUser, Env, UpdateOp<Row>>) => void
  afterCommit?: (ctx: MutationCtx<TUser, Env, UpdateOp<Row>>) => unknown | Promise<unknown>
}
export interface DeleteDef<TUser, Env> {
  authorize?: (ctx: MutationCtx<TUser, Env, DeleteOp>) => void | Promise<void>
  execute: (ctx: MutationCtx<TUser, Env, DeleteOp>) => void
  afterCommit?: (ctx: MutationCtx<TUser, Env, DeleteOp>) => unknown | Promise<unknown>
}

/** The CLOSED mutation trio. A fourth key is unrepresentable, so excess-property
 *  checking on the object literal rejects e.g. an `archive` mutation. */
export interface Mutations<TUser, Env, Row> {
  insert?: InsertDef<TUser, Env, Row>
  update?: UpdateDef<TUser, Env, Row>
  delete?: DeleteDef<TUser, Env>
}

/** What the type-only `collection<Row>(...)` form accepts. */
export interface CollectionInput<TUser, Env, Row> {
  pk: keyof Row & string
  mutations?: Mutations<TUser, Env, Row>
}

/** An insert def whose `schema` is REQUIRED — the Row inference source for the
 *  schema-first `collection({ mutations: { insert: { schema } } })` form. */
interface InsertWithSchema<TUser, Env, S extends StandardSchemaV1> {
  schema: S
  authorize?: (ctx: MutationCtx<TUser, Env, InsertOp<InferSchema<S>>>) => void | Promise<void>
  execute: (ctx: MutationCtx<TUser, Env, InsertOp<InferSchema<S>>>) => void
  afterCommit?: (ctx: MutationCtx<TUser, Env, InsertOp<InferSchema<S>>>) => unknown | Promise<unknown>
}

/** What the schema-first `collection(...)` form accepts: Row is inferred from
 *  `mutations.insert.schema`, and flows to `pk`, `update`, and the entry's Row. */
interface CollectionInputFromInsert<TUser, Env, S extends StandardSchemaV1> {
  pk: keyof InferSchema<S> & string
  mutations: {
    insert: InsertWithSchema<TUser, Env, S>
    update?: UpdateDef<TUser, Env, InferSchema<S>>
    delete?: DeleteDef<TUser, Env>
  }
}

/** A collection entry: the authored config plus the Row carried in the type. The
 *  phantom `__row` lets the client recover Row from `typeof schema`. */
export interface CollectionEntry<TUser, Env, Row> {
  pk: keyof Row & string
  mutations?: Mutations<TUser, Env, Row>
  /** phantom — type-only carrier of Row for client inference. */
  readonly __row?: Row
}

/** What `command(...)` accepts: a bare execute fn, or `{ authorize?, execute }`. */
export type CommandInput<TUser, Env, Args, Result> =
  | ((ctx: CommandCtx<TUser, Env, Args>) => Result)
  | {
      authorize?: (ctx: CommandCtx<TUser, Env, Args>) => void | Promise<void>
      execute: (ctx: CommandCtx<TUser, Env, Args>) => Result
    }

/** A command entry: carries Args and the (awaited) Result in the type. The
 *  phantoms let the client recover both from `typeof schema`. */
export interface CommandEntry<TUser, Env, Args, Result> {
  /** Args schema. `<Args, unknown>`: the schema's INPUT is the args the handler
   *  receives; its output is discarded (gate, not parser). */
  schema?: StandardSchemaV1<Args, unknown>
  authorize?: (ctx: CommandCtx<TUser, Env, Args>) => void | Promise<void>
  execute: (ctx: CommandCtx<TUser, Env, Args>) => Result | Promise<Result>
  /** phantoms — type-only carriers for client inference. */
  readonly __args?: Args
  readonly __result?: Result
}

/** The schema VALUE produced by `defineSync().schema(...)`; `typeof` it for the
 *  client Api. Structurally typed so a concrete schema value is assignable. */
export interface SyncSchema<TUser = unknown, Env = unknown> {
  collections: Record<string, CollectionEntry<TUser, Env, any>>
  commands: Record<string, CommandEntry<TUser, Env, any, any>>
}

// ---------------------------------------------------------------------------
// The bound factory.
// ---------------------------------------------------------------------------
/**
 * Bind `User`/`Env` once and get the three co-located authoring helpers.
 *
 *   const sync = defineSync<Claims, Env>()
 *   sync.collection<Message>({ pk: "id", mutations: { insert: { … } } })   // explicit Row
 *   sync.collection({ pk: "id", mutations: { insert: { schema: zMessage, … } } }) // Row inferred
 *   sync.command<{ before?: number }>()(({ args }) => { … })   // Result inferred
 *   sync.command(zArgs, ({ args }) => { … })                    // Args inferred
 *   sync.schema({ collections, commands })
 */
export function defineSync<User, Env = unknown>(): {
  collection: CollectionFactory<User, Env>
  command: CommandFactory<User, Env>
  schema: SchemaFactory<User, Env>
} {
  // --- collection: Row inferred from insert.schema, OR explicit type-only Row ---
  function collection<S extends StandardSchemaV1>(
    def: CollectionInputFromInsert<User, Env, S>,
  ): CollectionEntry<User, Env, InferSchema<S>>
  function collection<Row extends object>(def: CollectionInput<User, Env, Row>): CollectionEntry<User, Env, Row>
  function collection(def: unknown): CollectionEntry<User, Env, any> {
    return { ...(def as object) } as CollectionEntry<User, Env, any>
  }

  // --- command: type-only Args (curried so Result infers) OR Schema-inferred Args ---
  function command<Args = void>(): <Result>(
    input: CommandInput<User, Env, Args, Result>,
  ) => CommandEntry<User, Env, Args, Awaited<Result>>
  function command<S extends StandardSchemaV1, Result>(
    schema: S,
    input: CommandInput<User, Env, InferSchema<S>, Result>,
  ): CommandEntry<User, Env, InferSchema<S>, Awaited<Result>>
  function command(a?: unknown, b?: unknown): unknown {
    if (a !== undefined && b !== undefined) {
      return { ...normalizeCommand(b), schema: a } as unknown
    }
    return (input: unknown) => normalizeCommand(input)
  }

  // Overloaded so a commandless schema gets an EMPTY command map (keyof = never)
  // rather than the loose `Record<string, …>` an optional generic would infer —
  // otherwise `transport.call.anything()` would type-check against a DO that has
  // no commands.
  function schema<Cols extends Record<string, CollectionEntry<User, Env, any>>>(config: {
    collections: Cols
    commands?: undefined
  }): { collections: Cols; commands: Record<never, never> }
  function schema<
    Cols extends Record<string, CollectionEntry<User, Env, any>>,
    Cmds extends Record<string, CommandEntry<User, Env, any, any>>,
  >(config: { collections: Cols; commands: Cmds }): { collections: Cols; commands: Cmds }
  function schema(config: {
    collections: Record<string, CollectionEntry<User, Env, any>>
    commands?: Record<string, CommandEntry<User, Env, any, any>>
  }): { collections: unknown; commands: unknown } {
    return { collections: config.collections, commands: config.commands ?? {} }
  }

  return { collection, command, schema }
}

/** Normalize a `CommandInput` (bare fn | object) to a `{ authorize?, execute }`. */
function normalizeCommand(input: unknown): { authorize?: unknown; execute: unknown } {
  if (typeof input === "function") return { execute: input }
  return input as { authorize?: unknown; execute: unknown }
}

// Helper aliases so the factory return is nameable (and re-exportable).
interface CollectionFactory<User, Env> {
  <S extends StandardSchemaV1>(def: CollectionInputFromInsert<User, Env, S>): CollectionEntry<User, Env, InferSchema<S>>
  <Row extends object>(def: CollectionInput<User, Env, Row>): CollectionEntry<User, Env, Row>
}
interface CommandFactory<User, Env> {
  <Args = void>(): <Result>(
    input: CommandInput<User, Env, Args, Result>,
  ) => CommandEntry<User, Env, Args, Awaited<Result>>
  <S extends StandardSchemaV1, Result>(
    schema: S,
    input: CommandInput<User, Env, InferSchema<S>, Result>,
  ): CommandEntry<User, Env, InferSchema<S>, Awaited<Result>>
}
interface SchemaFactory<User, Env> {
  <Cols extends Record<string, CollectionEntry<User, Env, any>>>(config: {
    collections: Cols
    commands?: undefined
  }): { collections: Cols; commands: Record<never, never> }
  <
    Cols extends Record<string, CollectionEntry<User, Env, any>>,
    Cmds extends Record<string, CommandEntry<User, Env, any, any>>,
  >(config: {
    collections: Cols
    commands: Cmds
  }): { collections: Cols; commands: Cmds }
}

// ---------------------------------------------------------------------------
// Runtime dispatch structures — what the DO consumes. The authoring types above
// are erased to these by `compileSchema`; dispatch is untyped and the erasure is
// sound (the wire delivers exactly the per-op shape the handler expects).
// ---------------------------------------------------------------------------
export interface RuntimeMutationDef<TUser, Env = unknown> {
  authorize?: (ctx: MutationCtx<TUser, Env>) => void | Promise<void>
  execute: (ctx: MutationCtx<TUser, Env>) => void
  afterCommit?: (ctx: MutationCtx<TUser, Env>) => unknown | Promise<unknown>
}
export interface RuntimeCommandDef<TUser, Env = unknown> {
  authorize?: (ctx: CommandCtx<TUser, Env>) => void | Promise<void>
  execute: (ctx: CommandCtx<TUser, Env>) => unknown | Promise<unknown>
}

/** The compiled dispatch tables the DO holds. */
export interface CompiledSync<TUser = unknown, Env = unknown> {
  readonly collections: Map<string, CollectionDef>
  /** Keyed by `${table}:${type}`. */
  readonly mutations: Map<string, RuntimeMutationDef<TUser, Env>>
  readonly commands: Map<string, RuntimeCommandDef<TUser, Env>>
}

const ROW_OPS = ["insert", "update", "delete"] as const

/**
 * Compile an authored schema value into the flat dispatch tables, validating each
 * collection's identifiers (ADR-0007/0008).
 *
 * When a per-op Standard Schema is present it is enforced at runtime:
 * `insert.schema` validates the full-row `cols`, `update.schema` validates the
 * partial patch, and a command `schema` validates `args` — each before the
 * handler runs, throwing on issues (fail loud). A `delete` carries no cols.
 */
export function compileSchema<TUser, Env>(schema: SyncSchema<TUser, Env>): CompiledSync<TUser, Env> {
  const collections = new Map<string, CollectionDef>()
  const mutations = new Map<string, RuntimeMutationDef<TUser, Env>>()
  const commands = new Map<string, RuntimeCommandDef<TUser, Env>>()

  for (const [table, entry] of Object.entries(schema.collections)) {
    const def: CollectionDef = { table, pk: entry.pk }
    assertValidCollection(def)
    if (collections.has(table)) throw new Error(`collection '${table}' is already defined`)
    collections.set(table, def)
    const muts = entry.mutations
    if (!muts) continue
    for (const type of ROW_OPS) {
      const m = muts[type]
      if (!m) continue
      const perOpSchema = (m as { schema?: StandardSchemaV1 }).schema
      mutations.set(`${table}:${type}`, compileMutation(type, m as RuntimeMutationDef<TUser, Env>, perOpSchema))
    }
  }

  for (const [name, entry] of Object.entries(schema.commands)) {
    if (commands.has(name)) throw new Error(`command '${name}' is already defined`)
    commands.set(name, compileCommand(entry as CommandEntry<TUser, Env, unknown, unknown>))
  }

  return { collections, mutations, commands }
}

function compileMutation<TUser, Env>(
  type: RowOp,
  m: RuntimeMutationDef<TUser, Env>,
  schema: StandardSchemaV1 | undefined,
): RuntimeMutationDef<TUser, Env> {
  // Gate the write on the per-op schema when present: insert.schema validates the
  // full-row cols, update.schema the partial patch. A delete carries no cols.
  // Validation runs in authorize (before the transaction) and throws on issues.
  if (!schema || type === "delete") return m
  const userAuthorize = m.authorize
  return {
    authorize: async (ctx) => {
      await validateStandard(schema, ctx.op.cols)
      if (userAuthorize) await userAuthorize(ctx)
    },
    execute: m.execute,
    afterCommit: m.afterCommit,
  }
}

function compileCommand<TUser, Env>(entry: CommandEntry<TUser, Env, unknown, unknown>): RuntimeCommandDef<TUser, Env> {
  const userAuthorize = entry.authorize
  const schema = entry.schema
  if (!schema) return { authorize: userAuthorize, execute: entry.execute }
  return {
    authorize: async (ctx) => {
      await validateStandard(schema, ctx.args)
      if (userAuthorize) await userAuthorize(ctx)
    },
    execute: entry.execute,
  }
}

/** Thrown by the validation gate on a schema failure. Carries the issue detail
 *  and is surfaced to the client with a `VALIDATION` code, since bad input is the
 *  caller's to fix. An `execute` error, by contrast, stays sanitized. */
export class ValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "ValidationError"
  }
}

/** Run a Standard Schema's validator and throw `ValidationError` on issues. */
async function validateStandard(schema: StandardSchemaV1, value: unknown): Promise<void> {
  const result = await schema["~standard"].validate(value)
  if ("issues" in result && result.issues) {
    const detail = result.issues.map((i) => i.message).join("; ")
    throw new ValidationError(`validation failed: ${detail}`)
  }
}

/**
 * Validate the `{table, pk}` identifiers at registration. These strings are
 * interpolated raw into trigger DDL and `SELECT`s, so they must be safe
 * identifiers and must not collide with framework tables. The structural
 * constraint — the pk is a sole TEXT client-supplied key (no AUTOINCREMENT,
 * D9) — is enforced against the ACTUAL table by `registerSync` (ADR-0007).
 */
export function assertValidCollection(def: CollectionDef): void {
  const { table, pk } = def
  if (!IDENT.test(table)) throw new Error(`invalid table name '${table}'`)
  if (!IDENT.test(pk)) throw new Error(`invalid pk name '${pk}'`)
  if (table.startsWith(SYNC_PREFIX)) {
    throw new Error(`table name '${table}' uses the reserved '${SYNC_PREFIX}' prefix`)
  }
}
