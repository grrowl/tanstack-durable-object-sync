// The one shape of a column reference the server reads (ADR-0013 amendment
// 2026-11-17, column references). Shared by the compiler, which refuses any
// other shape, and the on-demand adapter, which does not send an `orderBy` the
// server would refuse (ADR-0023 amendment 2026-11-17).

/** A name the server can quote as an SQL identifier. */
export const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/

/** Why the server cannot read `node` as one column the way @tanstack/db's
 *  evaluator reads it, or null when it can. A ref must be exactly
 *  `{ type: "ref", path: [name] }`, so the evaluator reads `row[name]` as SQL
 *  does. It reads any other shape differently: on 1.0 it drops `path[0]` when it
 *  equals `sourceAlias`, and it walks a longer path into the value. */
export function columnRefProblem(node: unknown): string | null {
  if (typeof node !== "object" || node === null || (node as { type?: unknown }).type !== "ref") {
    return "expected a column reference"
  }
  const extra = Object.keys(node).find((k) => k !== "type" && k !== "path")
  if (extra !== undefined) return `a column reference carries only 'type' and 'path'; got '${extra}'`
  const path: unknown = (node as { path?: unknown }).path
  if (!Array.isArray(path)) return "a column reference's path must be an array"
  if (path.length !== 1) {
    const shown = path.map((p) => (typeof p === "string" ? p : typeof p)).join(", ")
    return `nested/aliased column paths are not supported: [${shown}]`
  }
  const name: unknown = path[0]
  if (typeof name !== "string" || !IDENT.test(name)) {
    return `invalid column name: ${typeof name === "string" ? name : typeof name}`
  }
  return null
}
