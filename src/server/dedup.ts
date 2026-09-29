// Mutation/command dedup (exactly-once under client retry). Every mut/call
// outcome is recorded by client-generated txId; a retry replays the stored
// outcome instead of re-running the handler (and re-doing its side effects).
//
// Retention is INDEPENDENT of changelog retention (ADR-0002 C5): a fully
// current client can still retry an old txId, so this table is swept on its
// own time-based horizon (M7), not tied to the compaction floor.

import { decode as decodeValue, encode as encodeValue } from "../wire/codec.ts"

export interface SeenTx {
  ok: boolean
  cursor: string | null
  error: string | null
  /** Machine-readable rejection code (e.g. VALIDATION); null when the original
   *  rejection carried none. Persisted so a replay is shaped identically to the
   *  first response (issue #21). */
  errorCode: string | null
  /** Value-codec-encoded result for commands; null for none. */
  result: string | null
}

export function lookupTx(sql: SqlStorage, txId: string): SeenTx | null {
  const rows = Array.from(
    sql.exec<{ ok: number; cursor: string | null; error: string | null; error_code: string | null; result: string | null }>(
      "SELECT ok, cursor, error, error_code, result FROM _sync_seen_tx WHERE tx_id = ?",
      txId,
    ),
  )
  if (rows.length === 0) return null
  const r = rows[0]!
  return { ok: r.ok === 1, cursor: r.cursor, error: r.error, errorCode: r.error_code, result: r.result }
}

export function recordTx(
  sql: SqlStorage,
  txId: string,
  ok: boolean,
  cursor: string | null,
  error: string | null,
  errorCode: string | null,
  result: string | null,
): void {
  // INSERT OR IGNORE: the first recorded outcome wins; a racing retry never
  // clobbers it.
  sql.exec(
    "INSERT OR IGNORE INTO _sync_seen_tx(tx_id, ok, cursor, error, error_code, result, ts) VALUES (?, ?, ?, ?, ?, ?, unixepoch()*1000)",
    txId,
    ok ? 1 : 0,
    cursor,
    error,
    errorCode,
    result,
  )
}

/** Encode a command result for storage; null for `undefined`. Throws (via the
 *  value codec) on non-serializable values, surfaced as a rejected mutation. */
export function encodeResult(value: unknown): string | null {
  return value === undefined ? null : encodeValue(value)
}

export function decodeResult(stored: string | null): unknown {
  return stored === null ? undefined : decodeValue(stored)
}

/**
 * Drop dedup entries older than `olderThanMs`. Retention is INDEPENDENT of the
 * changelog (ADR-0002 C5): sized to the maximum client retry/outbox window, not
 * the compaction floor — a fully-current client can still retry an old txId.
 *
 * `ts` is stamped in whole seconds (`unixepoch()*1000`), truncated, so it can
 * trail the true write time by up to 999 ms. The extra 1000 ms in the cutoff
 * keeps `olderThanMs` a true lower bound (a receipt is never swept before it
 * has aged that long); the cost is retention of up to 1 s longer.
 */
export function sweepDedup(sql: SqlStorage, olderThanMs: number, nowMs: number): void {
  sql.exec("DELETE FROM _sync_seen_tx WHERE ts < ?", nowMs - olderThanMs - 1000)
}
