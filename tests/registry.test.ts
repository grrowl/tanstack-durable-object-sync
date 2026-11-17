import { describe, expect, it } from "vitest"
import { compileSchema, type SyncSchema } from "../src/server/registry.ts"

// WHY: `{table, pk}` are interpolated raw into trigger DDL and SELECTs, so the
// registration path (registerSync -> compileSchema) rejects unsafe identifiers
// and the reserved `_sync_` prefix. The structural D9 rule (the pk is a sole
// TEXT client-supplied key) is now enforced against the REAL table by
// registerSync -> assertSyncCompatible (see assert-sync-compatible.test.ts),
// since the author owns the schema (ADR-0007).
//
// The object-schema API (ADR-0014) keys a collection by its table name, so the
// table IS the object key: `compileSchema` is the guard site, and a one-entry
// schema value is the unit under test. (The old imperative "define the same
// collection twice" case is structurally unrepresentable now — duplicate object
// keys collapse — so that runtime guard can no longer be reached or asserted.)

const compile = (table: string, pk: string) => () =>
  compileSchema({ collections: { [table]: { pk } }, commands: {} } as SyncSchema)

describe("compileSchema — identifier + registration guards", () => {
  it("accepts a valid { table, pk }", () => {
    expect(compile("messages", "id")).not.toThrow()
  })

  it("rejects the reserved _sync_ table prefix", () => {
    expect(compile("_sync_x", "id")).toThrow(/reserved/)
  })

  it("rejects an invalid table identifier (no SQL injection into trigger DDL)", () => {
    expect(compile("bad name", "id")).toThrow(/invalid table/)
  })

  it("rejects an invalid pk identifier", () => {
    expect(compile("t", "1bad")).toThrow(/invalid pk/)
  })

  // ADR-0012 amendment 2026-11-17: the server refuses a frame whose collection or
  // command name is over 128 UTF-8 bytes, and the library's client builds its
  // sub and fetch ids from the table name. So a schema must not register a
  // longer name, or the client would trip the server's limits.
  it("accepts a table name of 128 bytes and refuses 129, naming the limit", () => {
    expect(compile("t" + "x".repeat(127), "id")).not.toThrow()
    expect(compile("t" + "x".repeat(128), "id")).toThrow(/is over 128 bytes/)
  })

  it("accepts a command name of 128 UTF-8 bytes and refuses 129, counting bytes", () => {
    const command = (name: string) => () =>
      compileSchema({ collections: {}, commands: { [name]: { execute: () => null } } } as unknown as SyncSchema)
    expect(command("c".repeat(128))).not.toThrow()
    expect(command("c".repeat(129))).toThrow(/is over 128 bytes/)
    // 64 two-byte characters are 128 bytes; one more ASCII character is 129.
    expect(command("é".repeat(64))).not.toThrow()
    expect(command("é".repeat(64) + "c")).toThrow(/is over 128 bytes/)
  })
})
