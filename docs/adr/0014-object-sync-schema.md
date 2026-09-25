# 0014 — `defineSync`: one schema value, mutations on the collection, commands on the connection

**Status:** Accepted. Supersedes [ADR-0001](./0001-sync-architecture.md) D11's
`defineMutation`/`defineCommand` builder and the [ADR-0007](./0007-author-owned-schema-register-sync.md)
`new SyncRegistry().defineCollection(…)` chain; closes the ADR-0010 manifest and
its "typed command args" out-of-scope follow-up; revises [ADR-0012](./0012-wire-input-hardening.md)
D3 so a command's `authorize`/validation errors surface like a mutation's. Hard
breaking change to the DO authoring API and the typed client surface (pre-1.0,
clean break).

## Context

Three threads converged. ADR-0007 inverted control so the author owns migration
and `registerSync`es a `Registry`. ADR-0010 wanted `op.cols` typed per op, and
landed on a constructor **manifest** (`new SyncRegistry<Claims, Env, { messages: Message }>()`)
because TypeScript has no partial type-argument inference (microsoft/TypeScript#26242):
the row type had no runtime witness in `{ table, pk }`, so it had to be an explicit
annotation, and an explicit `<Message>` killed inference of every other slot. ADR-0010
explicitly deferred typed command `args`, and noted the manifest "sits apart from the
`.defineCollection` calls."

That manifest is the seam that hurt. The row type lives in a generic on the
constructor; the `pk` and the mutations live in chained `.defineCollection` /
`.defineMutation` calls somewhere below; the client recovers nothing — it imports
no shared type, so `op.cols` on the wire and `transport.sendCall(frame)` on the
client are both `unknown`/hand-built. Authoring is split across three sites
(manifest, collection, mutation) and the end-to-end type story stops at the DO.

We also had two genuinely different shapes wearing one builder. A **mutation**
mirrors a row write — TanStack DB already names exactly `onInsert`/`onUpdate`/`onDelete`,
and a synced write *is* one of those three. A **command** is an RPC: free-form
args, a return value, its own atomicity, no collection. ADR-0010 lumped them as
sibling `defineX` methods, but they stay distinct in what's structural: a command
owns its atomicity and may be async; a mutation is synchronous inside the
transaction. (ADR-0012 D3 had also split their error surfacing — command
`authorize` sanitized, mutation user-facing — but D2 below drops that, so
surfacing is now uniform.)

## Decision

One factory, `defineSync<User, Env>()`, binds identity and binding-env **once**
and returns three co-located helpers `{ collection, command, schema }`. The output
of `sync.schema({ collections, commands })` is a single **value** that is *both*
the DO registration and the client contract: `this.registerSync(chatSchema)` on
the server, `export type Api = typeof chatSchema` for the client.

```ts
const sync = defineSync<Claims, Env>()
export const chatSchema = sync.schema({
  collections: {
    messages: sync.collection<Message>({       // KEY "messages" === table name
      pk: "id",                                  // keyof Row & string (ADR-0007 D9)
      mutations: {                               // CLOSED trio
        insert: { authorize, execute, afterCommit },   // op.cols: Message
        update: { execute },                            // op.cols: Partial<Message>
        delete: { execute },                            // op.key only, no cols
      },
    }),
  },
  commands: {                                    // OPEN, named
    clearRoom: sync.command<{ before?: number }>()(({ args, sql }) => ({ deleted: 0 })),
    purge:     sync.command(zArgs, ({ args }) => ({ ok: args.hard })),
    ping:      sync.command()(() => ({ pong: true as const })),
  },
})
export type Api = typeof chatSchema
```

### D1: Mutations are the closed `insert`/`update`/`delete` trio, co-located on the collection

Mutations move from sibling `.defineMutation({ collection, type, … })` calls onto
a `mutations` object inside the collection, keyed by op type. This mirrors TanStack
DB's `onInsert`/`onUpdate`/`onDelete` — the surface authors already know, and the
surface a synced write maps onto one-for-one. The trio is **closed**: a 4th key
(e.g. `archive`) is an excess-property type error.

It is closed *because it has to be*. The op `type` bridges TanStack's `OperationType`,
and the three op shapes are structurally distinct — `insert` carries `cols: Row`
(the full row, ADR-0001 D19), `update` carries `cols: Partial<Row>` (top-level
patch, ADR-0002 C6), `delete` carries no `cols`, just `key`. A custom op type has
no `OperationType` to bridge and no defined `cols` shape, so it is structurally
unrepresentable, not merely disallowed. Co-locating the trio is what lets `op` be
typed per kind from one row annotation, replacing ADR-0010's manifest+chain split.
Named, open-ended writes are not a missing mutation kind — they are commands (D2).

This **supersedes ADR-0001 D11's `defineMutation` builder** and absorbs ADR-0010:
the constructor manifest is gone, the row type lives on its own collection, and
`op.cols`/`op.key` are still precise per op — now from a single co-located site.

### D2: Commands are the open, named RPC escape hatch on the connection, not a collection

A command is keyed by a free name under `commands`, takes free-form `args`, and
returns a `Result`. It is **not** a collection operation: it lives on the
connection (the transport / DO), can write rows of *any* collection — those writes
broadcast through the same CDC triggers (ADR-0001), so subscribers see them like
any mutation — **and** returns a value to the caller. Crucially a command **owns
its own atomicity**: its `execute` runs *outside* `transactionSync` (so it may be
`async`), whereas a mutation's `execute` is synchronous inside the transaction.
This is the open counterpart to the closed trio: anything that isn't one-row
insert/update/delete (multi-row, cross-collection, compute-and-return, no write at
all) is a command.

They stay separate helpers because their shapes differ — a command is RPC with
free args and a return value, a mutation is a typed row op — not because their
errors differ. Error surfacing is now the **same** for both (revising ADR-0012
D3, which had sanitized a command's `authorize`): a mutation and a command both
surface their `authorize` and validation errors to the client, and both sanitize
their `execute` errors. See D3.

### D3: The row schema lives on the insert mutation; it infers the row and validates writes

A collection's row type lives on the collection two ways. Type-only —
`sync.collection<Message>({ pk, mutations })` — recovers ADR-0010's precise
`op.cols` with no runtime cost (`pk` is checked against `keyof Row & string`).
Or **from a [Standard Schema](https://standardschema.dev/) on the insert
mutation** — `sync.collection({ pk, mutations: { insert: { schema: zMessage } } })`
— where `Row` is inferred from `insert.schema` and flows to `pk`, to `update`'s
`Partial<Row>`, and to the client. The "default" row schema and the insert
validator are the same thing, so the insert mutation is its one home. There is no
separate positional schema argument.

Each op may carry its own schema, and when present it is checked at runtime inside
the compiled `authorize`, before the author's `authorize`/`execute`, throwing
(fail-loud, rejecting the frame) on issues:

- `insert.schema` validates the full-row `cols`.
- `update.schema` validates the partial patch. The author supplies a partial
  schema (e.g. `Message.partial()`), because an update carries a top-level partial,
  not a full row, and a full-row schema would reject every valid partial.
- a command's schema validates its `args`.
- a `delete` has no schema. It carries only the key, the wire layer already checks
  the key is a non-empty string (ADR-0012), and the pk was validated at insert.
  *(2026-09-25: this check was not enforced until 0.7.0; see
  [ADR-0025](./0025-empty-op-key-rejection.md).)*

This reverses ADR-0010's B3 rejection narrowly and deliberately. ADR-0010 rejected
a schema slot because it bought no *injection* safety (parameterised binding
already covers that) at a per-mutation hot-path cost. That still holds, so
validation is **opt-in**: no schema means no validator runs. The slot's primary
payoff is inference and a typed client contract, with runtime validation as the
opt-in bonus.

It is a validation **gate, not a parser**: the handler receives the original wire
value, never the schema's parsed output, so a schema must not rely on
transforms/defaults/coercion (input must equal output). Rewriting a row the client
already applied optimistically would manufacture divergence, and a pk rewrite would
break optimistic-id == confirmed-id (ADR-0001 D9). The interface is the
dependency-free `~standard` shape (`StandardSchemaV1`, exported); **no validator
runtime is imported** — zod/valibot/arktype all satisfy it, and an author who wants
none pays nothing.

### D4: `type Api = typeof schema` carries end-to-end typing to the client

Because the schema is a value whose type captures every collection's row and every
command's args/result, the client recovers the whole contract from a **type-only**
import (nothing server-side is bundled; Row/Args/Result ride phantom carriers):

- `new WebSocketTransport<Api>({ url })` — typed connection.
- `transport.call.clearRoom({ before })` — a Proxy; name autocompletes, args and
  result are checked. A void-args command is `transport.call.ping()`.
- `transport.sendCall("purge", { hard: true })` — the low-level typed call;
  it builds the `call` frame and generates the txId via `crypto.randomUUID()`
  internally (the app supplies no ulid/txId), and resolves with the command's
  `result` on its `committed` receipt. The old `sendCall(frame)` signature is gone.
- `doCollectionOptions<Api, "messages">({ transport, table, getKey })` — infers the
  row from `Api` + table, no runtime schema value needed; a zero-type-arg call
  infers `Api` from the transport too.

This is the thread ADR-0010 left dangling: the manifest typed the *server* alone;
the schema value types both ends from one source.

### D5: Multi-DO is one transport per DO; commands are keyed by DO

A schema (and thus an `Api`) describes exactly one DO. Two DOs means two schema
values, two `Api` types, two `WebSocketTransport`s — so `transport.call.*` is
naturally scoped to the commands of *that* DO, with no cross-DO command namespace
to disambiguate. This keeps the single-ordered-stream-per-DO model (ADR-0001)
intact: the client contract is per-connection, like the cursor.

## DO wiring

`registerSync` now takes the schema **value**, not a builder instance. It calls
`compileSchema(schema)` → `CompiledSync { collections, mutations, commands }`
(Maps keyed `table` / `` `${table}:${type}` `` / `name`), then runs the same
`initSchema` + `ensureTriggers` path as ADR-0007 — preserving D9 pk validation,
the reserved-`_sync_`-prefix guard, and the AFTER triggers. Dispatch is unchanged
in spirit: `handleMut` reads `registry.mutations.get(\`${collection}:${op.type}\`)`
and runs `authorize` → `execute` (inside `transactionSync`) → `afterCommit`;
`handleCall` reads `registry.commands.get(name)` and awaits `authorize` →
`execute`. The collection KEY is strictly the table name (interpolated into trigger
DDL; `/^[A-Za-z_][A-Za-z0-9_]*$/`, non-reserved). For synced writes **outside** a
mut/call handler, `runSyncedWrite` is still the path (ADR-0006).

## Consequences

- **One authoring site, one contract.** Row, `pk`, and the mutation trio sit
  together on the collection; commands sit together under `commands`; the whole
  thing is one value. ADR-0010's manifest/collection/mutation three-way split and
  its `as Message` casts are gone, and the client is typed from the same value.
- **Hard break (pre-1.0, clean).** `new SyncRegistry().defineCollection/defineMutation/defineCommand`
  is **removed** — no compatibility shim. Every DO subclass moves to
  `defineSync` + `sync.schema` + `registerSync(schema)`; every typed client moves
  to `transport.call` / the new `sendCall(name, args)`. Examples and the test-worker
  move with it.
- **Closed mutations, open commands** — the shape now encodes the rule: if a write
  isn't one-row insert/update/delete it is a command, and the compiler says so
  (excess-property error on a 4th mutation key).
- **Opt-in validation, scoped, gate-not-parser.** No schema → zero validator on
  the hot path (ADR-0010's objection preserved). Schema present → `insert.schema`
  validates the full row and infers `Row`, `update.schema` validates the partial
  patch, a command schema validates `args`; a `delete` is unvalidated (no cols, pk
  validated at insert). It validates but does not transform — the original value
  flows to handlers. Dependency-free via Standard Schema; no zod runtime pulled in.
- **Uniform error surfacing (revises ADR-0012 D3).** Mutations and commands both
  surface `authorize` and validation errors (a schema failure carries a
  `VALIDATION` code) and both sanitize `execute` errors. ADR-0012 D3's
  command-`authorize` sanitization is dropped, so the two behave the same.
- **Per-DO contract.** Multi-DO apps run one transport and one `Api` per DO; there
  is no global command registry. Consistent with the per-connection cursor (0002).
