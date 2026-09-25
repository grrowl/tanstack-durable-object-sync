# Validate collections and commands with a schema

Use this when you want the Durable Object to reject a malformed write at the edge,
such as a bad insert row or bad command arguments, using a schema library you
already have. It works with zod, valibot, or arktype, and the framework does not
depend on any of them.

The schema is optional. If you leave it off, you pass the row type as a generic
(`collection<Message>({ ... })`) and nothing is checked at runtime. Add a schema
when you want the runtime check as well.

## Recipe

```ts
// server
import { z } from "zod" // 3.24 or later; any Standard Schema library works
import { defineSync } from "tanstack-durable-object-sync"

const Message = z.object({
  id: z.string(),
  author: z.string(),
  content: z.string().min(1).max(4000),
  created_at: z.number().int(),
})

const sync = defineSync<Claims, Env>()

export const schema = sync.schema({
  collections: {
    messages: sync.collection({
      pk: "id",
      mutations: {
        // The insert schema checks the full row. Its type also becomes the
        // collection's Row, so you do not pass a <Row> generic.
        insert: {
          schema: Message,
          execute: ({ op, sql }) =>
            sql.exec(
              "INSERT INTO messages(id, author, content, created_at) VALUES (?, ?, ?, ?)",
              op.cols.id,
              op.cols.author,
              op.cols.content,
              op.cols.created_at,
            ),
        },
        // An update sends a partial patch, so pass a partial schema.
        update: {
          schema: Message.partial(),
          execute: ({ op, sql }) => {
            if (op.cols.content !== undefined) {
              sql.exec("UPDATE messages SET content = ? WHERE id = ?", op.cols.content, op.key)
            }
          },
        },
        delete: { execute: ({ op, sql }) => sql.exec("DELETE FROM messages WHERE id = ?", op.key) },
      },
    }),
  },
  commands: {
    // A command checks its arguments against the schema you pass first.
    clearOlderThan: sync.command(z.object({ before: z.number().int() }), ({ args, sql }) => {
      sql.exec("DELETE FROM messages WHERE created_at < ?", args.before)
      return { ok: true }
    }),
  },
})
export type Api = typeof schema
```

## What gets checked

- **Insert.** The insert schema checks the full row before the write runs.
- **Update.** The update schema checks the partial patch. You pass a partial
  schema because an update sends only the fields that changed, not the whole row.
  A full row schema would reject every valid partial.
- **Delete.** A delete needs no schema. It carries only the key, the framework
  already checks that the key is a non-empty string, and the primary key was
  checked when the row was inserted.
- **Command.** A command checks its arguments against the schema you pass.

When a check fails, the write is rejected and nothing is applied.

## The schema is a gate, not a parser

The schema checks the value and rejects it on failure. It does not change the
value. Your handler receives the original value from the wire, not the schema's
parsed output. So do not rely on `.transform()`, `.default()`, `z.coerce`, or
unknown-key stripping to reshape what gets written, because the parsed result is
thrown away. Use schemas where the input equals the output. Constraints like
`.min()`, `.max()`, and `.refine()` are fine, because they check the value and do
not reshape it.

The reason is that a synced row is applied on the client the moment it is sent. If
the server changed the row during a write, the stored row would no longer match
the client's copy, and the correction would overwrite it. Changing the primary
key would also break the rule that the optimistic id equals the confirmed id
(ADR-0001 D9).

## Any Standard Schema works

The slot accepts any library that implements the
[Standard Schema](https://github.com/standard-schema/standard-schema) interface,
such as zod 3.24 or later, valibot, or arktype. The framework imports no
validator, so you bring your own. To switch libraries, replace `z.object({ ... })`
with the equivalent from your library and change nothing else.

## Notes

- **Types follow the schema.** The row type and the argument type come from the
  schema you pass, so `op.cols` and `args` are typed exactly as the schema
  describes.
- **A failed validation tells the client why.** For both mutations and commands,
  a schema failure is surfaced to the client with its reason and a `VALIDATION`
  code, so you can show the user what was wrong. An `authorize` throw also
  surfaces its reason, without that code. Only an `execute` error is sanitized to
  a generic message.
- **It runs on every matching write,** so keep schemas cheap.

## See also

- ADR-0014 for the object schema and why the slot is a gate and not a parser.
- ADR-0012 for wire input hardening; ADR-0014 for how authorize/validation errors
  are surfaced (revising ADR-0012 D3).
- `examples/chat` for a collection and command with no schema, as a baseline to
  compare against.
