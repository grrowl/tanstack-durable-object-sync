import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { recordTx, sweepDedup } from "../src/server/dedup.ts"

// WHY (bugbash F7 cutoff, ADR-0002 C5 / ADR-0021): a receipt is stamped with
// `unixepoch()*1000` (whole seconds, truncated), so its stamp can be up to 999 ms
// EARLIER than the true write time. Sweeping on `ts < now - retention` then deletes
// a receipt whose true age is as little as `retention - 999 ms`, breaking the
// promise that a retry inside `dedupRetentionMs` replays instead of re-executing.
// Retention must be a true lower bound. The times are passed in, so this is
// deterministic; the stamp itself comes from the real `recordTx`.

const RETENTION = 3_600_000

async function stampOf(txId: string, then: (sql: SqlStorage, ts: number) => void): Promise<void> {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(`dedup-bound-${crypto.randomUUID()}`))
  await runInDurableObject(stub, (_i, state) => {
    const sql = state.storage.sql
    recordTx(sql, txId, false, null, "x", null, null)
    const ts = sql.exec<{ ts: number }>("SELECT ts FROM _sync_seen_tx WHERE tx_id = ?", txId).one().ts
    then(sql, ts)
  })
}

const present = (sql: SqlStorage, txId: string): boolean =>
  sql.exec("SELECT 1 FROM _sync_seen_tx WHERE tx_id = ?", txId).toArray().length > 0

describe("dedup sweep keeps retention a true lower bound (bugbash F7)", () => {
  it("stamps whole seconds, so the true write time is anywhere in [ts, ts + 999 ms]", async () => {
    await stampOf("whole-seconds", (_sql, ts) => expect(ts % 1000).toBe(0))
  })

  it("does not sweep a receipt whose true age is under retention, even at worst-case truncation", async () => {
    await stampOf("young", (sql, ts) => {
      const trueWrite = ts + 999 // worst case: stamped 999 ms before it happened
      sweepDedup(sql, RETENTION, trueWrite + RETENTION - 1) // true age: retention - 1 ms
      expect(present(sql, "young")).toBe(true)
    })
  })

  it("does sweep once the true age exceeds retention at best-case stamping, by at most one second more", async () => {
    await stampOf("old", (sql, ts) => {
      // Best case: stamped exactly when it happened. A second past retention it must go.
      sweepDedup(sql, RETENTION, ts + RETENTION + 1000 + 1)
      expect(present(sql, "old")).toBe(false)
    })
  })
})
