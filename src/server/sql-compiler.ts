// IR -> SQLite compiler (ADR-0001 D5/D6). Lowers a @tanstack/db BasicExpression
// `where` plus orderBy/limit/offset into a parameterised SELECT, so subset
// shaping runs in SQLite rather than scanning every row in memory.
//
// Supported operator floor (D6): eq, gt, gte, lt, lte, like, in, and, or,
// not. Anything outside it — ne, ilike, isNull, functional/nested-path
// predicates — throws UnsupportedPredicateError. The caller rejects such a
// subscription rather than silently falling back to a full scan (fail loud).
//
// The floor is exactly the set of operators that the SQL snapshot path and
// @tanstack/db's JS evaluator (delta/catch-up path) agree on, row-for-row —
// see ADR-0013. `ne` is excluded because @tanstack/db has no `ne` (it only
// knows `not(eq(...))`); accepting it here desynced the two paths and crashed
// the delta path. `like` stays in the floor but is only correct because the DO
// sets `PRAGMA case_sensitive_like = ON` (SyncDurableObject constructor), which
// makes SQLite LIKE case-sensitive to match @tanstack/db's case-sensitive `like`.

import { columnRefProblem, IDENT } from "../wire/column-ref.ts"

export class UnsupportedPredicateError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "UnsupportedPredicateError"
  }
}

// SQLite limits in a Durable Object, set by workerd's `SqliteDatabase::setupSecurity`
// (Cloudflare's Durable Objects limits page lists the same). A query over one is
// refused when it is compiled (ADR-0013 amendment 2026-11-17). The checks are
// one-sided and may refuse a little early. The depth bound covers top-level
// conjunctions; a conjunction nested inside an `or` can still plan deeper, and
// `SQLITE_LIMIT_ERRORS` below is the backstop for that. tests/sqlite-limits.test.ts
// pins this in workerd.
/** SQLITE_LIMIT_VARIABLE_NUMBER: bound parameters per statement. */
const MAX_BOUND_PARAMS = 100
/** SQLITE_LIMIT_EXPR_DEPTH: height of an expression tree. */
const MAX_EXPR_HEIGHT = 100
/** SQLITE_LIMIT_COLUMN: also caps the terms of an ORDER BY. */
const MAX_ORDER_BY_TERMS = 100
/** SQLITE_LIMIT_SQL_LENGTH: bytes in one statement. */
const MAX_SQL_BYTES = 100_000
/** SQLITE_LIMIT_LENGTH: a bound string must be shorter than this many bytes.
 *  workerd's main branch sets 8 MiB + 34; the workerd this package is tested on
 *  rejects a plain string over 2 200 000 bytes and a `json_each` list of exactly
 *  2 200 000. The smaller holds on both. */
const MAX_VALUE_BYTES = 2_200_000
/** SQLITE_LIMIT_LIKE_PATTERN_LENGTH: bytes in a LIKE pattern. SQLite only
 *  throws when the LIKE is evaluated on a row, so this is refused up front. */
const MAX_LIKE_PATTERN_BYTES = 50
const utf8Bytes = (s: string): number => new TextEncoder().encode(s).length

/** True when a value bound as one SQL parameter would be over SQLITE_LIMIT_LENGTH:
 *  a string by its UTF-8 bytes, bytes by their length. */
export function valueTooLarge(v: unknown): boolean {
  if (typeof v === "string") return v.length >= MAX_VALUE_BYTES / 3 && utf8Bytes(v) >= MAX_VALUE_BYTES
  if (v instanceof ArrayBuffer || ArrayBuffer.isView(v)) return v.byteLength >= MAX_VALUE_BYTES
  return false
}

/** SQLite's messages for a statement over one of its limits, as workerd reports
 *  them. Deterministic for a given statement and schema, so a retry cannot help: the
 *  compiler's checks come first, and these are the backstop (ADR-0012 amendment
 *  2026-11-17). tests/sqlite-limits.test.ts provokes each one for real. */
const SQLITE_LIMIT_ERRORS = [
  "too many SQL variables", // SQLITE_LIMIT_VARIABLE_NUMBER
  "Expression tree is too large", // SQLITE_LIMIT_EXPR_DEPTH
  "statement too long", // SQLITE_LIMIT_SQL_LENGTH
  "string or blob too big", // SQLITE_LIMIT_LENGTH
  "too many terms in ORDER BY clause", // SQLITE_LIMIT_COLUMN
  "LIKE or GLOB pattern too complex", // SQLITE_LIMIT_LIKE_PATTERN_LENGTH
]

/** True for an error SQLite gives for a statement over one of its limits. */
export function isSqliteLimitError(e: unknown): boolean {
  const message = e instanceof Error ? e.message : String(e)
  return SQLITE_LIMIT_ERRORS.some((m) => message.includes(m))
}
const COMPARATORS: Record<string, string> = {
  eq: "=",
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
  // LIKE is case-sensitive only because the DO sets PRAGMA case_sensitive_like
  // = ON; see the floor note above and ADR-0013.
  like: "LIKE",
}

type Node =
  | { type: "ref"; path: Array<string> }
  | { type: "val"; value: unknown }
  | { type: "func"; name: string; args: Array<Node> }

function isNode(x: unknown): x is Node {
  return typeof x === "object" && x !== null && typeof (x as { type?: unknown }).type === "string"
}

/** Validate a column reference, record its name in `names`, and quote it. The
 *  quoting alone does not make an unknown name fail: SQLite reads a double quoted
 *  name that matches no column as a string. The caller checks `names` against
 *  the table (ADR-0013 amendment 2026-11-17).
 *
 *  The ref must have the one shape `columnRefProblem` accepts (ADR-0013
 *  amendment 2026-11-17, column references). `where` and `orderBy` both come
 *  here, and the on-demand adapter uses the same rule to decide which `orderBy`
 *  it can send. */
function column(node: unknown, names: Set<string>): string {
  const problem = columnRefProblem(node)
  if (problem !== null) throw new UnsupportedPredicateError(problem)
  const name = (node as { path: [string] }).path[0]
  names.add(name)
  return `"${name}"`
}

function literal(node: unknown, params: Array<unknown>): string {
  if (!isNode(node) || node.type !== "val") throw new UnsupportedPredicateError("comparison value must be a literal")
  // workerd cannot bind a bigint (README: not supported in Durable Object storage).
  // Not dropped as `in` drops it: `gt`/`lt` compare a bigint with a number.
  if (typeof node.value === "bigint") throw new UnsupportedPredicateError("a bigint comparison value is not supported")
  params.push(node.value)
  return "?"
}

/** Compiled SQL for one expression, with what the depth bound needs: its height
 *  in SQLite's expression tree (`exprSetHeight`: a leaf is 1, any other node 1 +
 *  its tallest child; `a OR b OR c` is parsed left-deep, so each further operand
 *  adds one), the heights of its conjuncts when it is an `and` (flattened through
 *  nested `and`s), and whether it holds a subquery. */
interface Compiled {
  sql: string
  height: number
  conjuncts?: Array<number>
  subquery?: boolean
}

/** `col IN list` without one bound parameter per value: Durable Object SQLite
 *  allows 100 per statement, and @tanstack/db's join loader sends one `in` over
 *  every join key (ADR-0013 amendment 2026-11-17). Strings and safe integers
 *  travel as one JSON array each, read with `json_each` (exact for both); other
 *  numbers bind one each, because JSON text does not carry every double exactly.
 *  One OR term per kind keeps the pk index (SQLite's MULTI-INDEX OR).
 *
 *  @tanstack/db's `in` compares by kind (a string never equals a number) where
 *  SQLite converts by column affinity (`1` would match the text "1.0" in a TEXT
 *  column, "5" the integer 5 in an INTEGER one), so each kind matches only
 *  stored values of that kind. Allowing `typeof … 'null'` keeps a NULL column
 *  NULL (unknown), as the evaluator does, so `not` leaves it out too.
 *
 *  The evaluator compares a Date by its time value (`normalizeValue`, 0.8.6 to
 *  1.0), so a Date travels as that number. A value that can equal nothing a
 *  stored row holds is dropped: null and undefined never match, SQLite stores
 *  NaN (and so an invalid Date) as NULL, a row holds no boolean, bigint, object
 *  or array, and its BLOBs hydrate as ArrayBuffer, which the evaluator never
 *  equates with a Uint8Array. */
function compileIn(col: string, list: Array<unknown>, params: Array<unknown>): Compiled {
  const texts: Array<string> = []
  const ints: Array<number> = []
  const others: Array<number> = []
  for (const item of list) {
    const v = item instanceof Date ? item.getTime() : item
    if (typeof v === "string") texts.push(v)
    else if (Number.isSafeInteger(v)) ints.push(v as number)
    else if (typeof v === "number" && !Number.isNaN(v)) others.push(v)
  }
  const textGuard = `typeof(${col}) IN ('text', 'null')`
  const numberGuard = `typeof(${col}) IN ('integer', 'real', 'null')`
  // AND(col IN …, typeof(col) IN …): 1 + typeof's 3 = 4. A `json_each` term also
  // marks the `where` as holding a subquery (see `depthBound`).
  const terms: Array<Compiled> = []
  if (texts.length > 0) {
    params.push(JSON.stringify(texts))
    terms.push({ sql: `(${col} IN (SELECT value FROM json_each(?)) AND ${textGuard})`, height: 4, subquery: true })
  }
  if (ints.length > 0) {
    params.push(JSON.stringify(ints))
    terms.push({ sql: `(${col} IN (SELECT value FROM json_each(?)) AND ${numberGuard})`, height: 4, subquery: true })
  }
  if (others.length > 0) {
    params.push(...others)
    terms.push({ sql: `(${col} IN (${others.map(() => "?").join(", ")}) AND ${numberGuard})`, height: 4 })
  }
  // Nothing can match: false, or NULL for a NULL column (as above).
  if (terms.length === 0) return { sql: `CASE WHEN ${col} IS NULL THEN NULL ELSE 0 END`, height: 3 }
  if (terms.length === 1) return terms[0]!
  return { sql: `(${terms.map((t) => t.sql).join(" OR ")})`, height: orHeight(terms), subquery: hasSubquery(terms) }
}

const hasSubquery = (parts: Array<Compiled>): boolean => parts.some((p) => p.subquery)

/** A height SQLite's plan of this `where` does not exceed at the top level
 *  (one-sided; ADR-0013 amendment 2026-11-17). Two things add depth beyond the
 *  tree's own height:
 *  - a MULTI-INDEX OR plan rebuilds the other top-level conjuncts as one
 *    left-deep AND and wraps it again, which can reach the tallest conjunct plus
 *    the number of conjuncts, whatever the table's indexes; the bound allows one
 *    more;
 *  - a `where` holding a subquery (`json_each`) is one level taller as a whole.
 *  The tree's own height never exceeds tallest conjunct + count - 1. SQLite does
 *  the same rebuild for a conjunction inside an `or` operand, which this does not
 *  count; that hand-built shape is left to the limit-error backstop. */
function depthBound(w: Compiled): number {
  const conjuncts = w.conjuncts ?? [w.height]
  const tallest = conjuncts.reduce((a, b) => Math.max(a, b), 0)
  return tallest + conjuncts.length + 1 + (w.subquery ? 1 : 0)
}

/** Height of `a OR b OR c` (or AND): left-deep, so each further operand adds one. */
function orHeight(parts: Array<Compiled>): number {
  return parts.reduce((h, p, i) => (i === 0 ? p.height : 1 + Math.max(h, p.height)), 0)
}

function compileExpr(node: unknown, params: Array<unknown>, names: Set<string>): Compiled {
  if (!isNode(node) || node.type !== "func") {
    throw new UnsupportedPredicateError(
      `expected a boolean function expression${isNode(node) ? `, got '${node.type}'` : ""}`,
    )
  }
  const { name, args } = node

  if (name in COMPARATORS) {
    if (args.length !== 2) throw new UnsupportedPredicateError(`'${name}' expects 2 arguments`)
    if (name === "like") {
      // @tanstack/db's evaluateLike returns false unless BOTH operands are
      // strings, whereas SQLite LIKE coerces a non-string pattern (e.g. 123 →
      // '123') and could match rows the JS delta/catch-up path rejects. Restrict
      // the pattern to a string literal so the two paths stay in lockstep — the
      // floor must be verified-agreeing, not merely "usually agreeing" (ADR-0013).
      const lit = args[1]
      if (!isNode(lit) || lit.type !== "val" || typeof lit.value !== "string") {
        throw new UnsupportedPredicateError("'like' pattern must be a string literal")
      }
      if (utf8Bytes(lit.value) > MAX_LIKE_PATTERN_BYTES) {
        throw new UnsupportedPredicateError(`'like' pattern is over ${MAX_LIKE_PATTERN_BYTES} bytes`)
      }
    }
    return { sql: `${column(args[0], names)} ${COMPARATORS[name]} ${literal(args[1], params)}`, height: 2 }
  }
  if (name === "and" || name === "or") {
    if (args.length === 0) throw new UnsupportedPredicateError(`'${name}' expects at least one argument`)
    const parts = args.map((a) => compileExpr(a, params, names))
    return {
      sql: `(${parts.map((p) => p.sql).join(` ${name.toUpperCase()} `)})`,
      height: orHeight(parts),
      conjuncts: name === "and" ? parts.flatMap((p) => p.conjuncts ?? [p.height]) : undefined,
      subquery: hasSubquery(parts),
    }
  }
  if (name === "not") {
    if (args.length !== 1) throw new UnsupportedPredicateError("'not' expects 1 argument")
    const inner = compileExpr(args[0], params, names)
    return { sql: `(NOT ${inner.sql})`, height: 1 + inner.height, subquery: inner.subquery }
  }
  if (name === "isNull" || name === "isUndefined") {
    if (args.length !== 1) throw new UnsupportedPredicateError(`'${name}' expects 1 argument`)
    const col = column(args[0], names)
    // A stored column is NULL, never undefined: @tanstack/db's `isUndefined`
    // (=== undefined) is false for every row hydrated from SQLite, and `isNull`
    // (=== null) is exactly IS NULL. Needed for the nulls-last cursor's whereFrom
    // (`gt OR isNull OR isUndefined`). ADR-0013 amendment 2026-09-29.
    // Only a column the table lacks is undefined in JS (true there, false here), and
    // such a query is refused (ADR-0013 amendment 2026-11-17).
    return name === "isNull" ? { sql: `${col} IS NULL`, height: 2 } : { sql: "0", height: 1 }
  }
  if (name === "in") {
    const arr = isNode(args[1]) && args[1].type === "val" ? (args[1] as { value: unknown }).value : undefined
    if (args.length !== 2 || !Array.isArray(arr)) {
      throw new UnsupportedPredicateError("'in' expects a column and an array literal")
    }
    return compileIn(column(args[0], names), arr, params)
  }
  throw new UnsupportedPredicateError(
    `operator '${name}' is not supported for server-side filtering ` +
      `(floor: eq, gt, gte, lt, lte, like, in, isNull, isUndefined, and, or, not)`,
  )
}

/** Combine two `where` IR predicates with AND, mirroring @tanstack/db's `and()`
 *  node shape (`{ type: "func", name: "and", args: [a, b] }`) so the existing
 *  compiler path handles it. Either side may be absent. Used to compose a base
 *  `where` with a cursor expression server-side (the cursor expressions arrive
 *  raw, excluding the base, exactly as TanStack's CursorExpressions define). */
export function andPredicates(a: unknown, b: unknown): unknown {
  if (a == null) return b
  if (b == null) return a
  return { type: "func", name: "and", args: [a, b] }
}

/** Compile a `where` IR to a SQL boolean expression + bound params. */
export function compileWhere(where: unknown): { sql: string; params: Array<unknown> } {
  const params: Array<unknown> = []
  return { sql: compileExpr(where, params, new Set()).sql, params }
}

/** Extract (quoted column, descending, nulls last) from an orderBy clause, recording
 *  the column in `names`. Accepts @tanstack/db's OrderByClause ({ expression:
 *  PropRef, compareOptions: { direction, nulls } }), the real wire shape from a
 *  live query, whose expression is read by the same rule as a ref in `where`; or a
 *  simple { col, dir }. Nulls default to first, as in TanStack's comparator, in
 *  either direction. */
function orderByTerm(item: unknown, names: Set<string>): { col: string; desc: boolean; nullsLast: boolean } {
  const o = item as {
    expression?: unknown
    compareOptions?: { direction?: string; nulls?: string }
    col?: unknown
    dir?: unknown
  }
  if (o.expression !== undefined) {
    const nullsLast = o.compareOptions?.nulls === "last"
    return { col: column(o.expression, names), desc: o.compareOptions?.direction === "desc", nullsLast }
  }
  if (typeof o.col === "string") {
    if (!IDENT.test(o.col)) throw new UnsupportedPredicateError(`invalid orderBy column: ${o.col}`)
    names.add(o.col)
    return { col: `"${o.col}"`, desc: o.dir === "desc", nullsLast: false }
  }
  throw new UnsupportedPredicateError(`unsupported orderBy clause: ${JSON.stringify(item)}`)
}

function compileOrderBy(orderBy: unknown, pk: string, names: Set<string>): string {
  if (!Array.isArray(orderBy) || orderBy.length === 0) return ""
  if (!IDENT.test(pk)) throw new UnsupportedPredicateError(`invalid pk column: ${pk}`)
  if (orderBy.length + 1 > MAX_ORDER_BY_TERMS) {
    throw new UnsupportedPredicateError(`orderBy has ${orderBy.length} terms; Durable Object SQLite allows ${MAX_ORDER_BY_TERMS - 1} beside the pk`)
  }
  const terms = orderBy.map((item) => {
    const { col, desc, nullsLast } = orderByTerm(item, names)
    return `${col} ${desc ? "DESC" : "ASC"} NULLS ${nullsLast ? "LAST" : "FIRST"}`
  })
  // Final tie-break by pk, ascending whatever the order direction: @tanstack/db
  // breaks value ties by row key ascending (db-ivm `createKeyedComparator`, same in
  // db-ivm 0.1.19 and 0.1.22), comparing strings with `<`. Forced to BINARY: the pk
  // may declare another collation (NOCASE), which would order `a` before `B`. Without it a LIMIT over
  // tied rows picks by scan order and the window differs from the client's own.
  terms.push(`"${pk}" COLLATE BINARY ASC`)
  return terms.join(", ")
}

function nonNegInt(n: unknown, label: string): number {
  // SQLite needs an integer: 2^63 and above bind as a REAL it cannot convert.
  if (typeof n !== "number" || !Number.isInteger(n) || n < 0 || n >= 2 ** 63) {
    throw new UnsupportedPredicateError(`${label} must be a non-negative integer under 2^63`)
  }
  return n
}

export interface SubsetQuery {
  /** The collection's pk column: the final ORDER BY tie-break. */
  pk: string
  where?: unknown
  orderBy?: unknown
  limit?: number
  offset?: number
}

/** Compile a full subset SELECT for a collection table (validated identifier).
 *  `columns` lists every column the `where` and `orderBy` name, for the caller to
 *  check against the table: this module has no handle on SQLite. */
export function compileSubsetQuery(
  tbl: string,
  opts: SubsetQuery,
): { sql: string; params: Array<unknown>; columns: Array<string> } {
  if (!IDENT.test(tbl)) throw new UnsupportedPredicateError(`invalid table name: ${tbl}`)
  let sql = `SELECT * FROM "${tbl}"`
  const params: Array<unknown> = []
  const names = new Set<string>()

  if (opts.where != null) {
    const w = compileExpr(opts.where, params, names)
    const depth = depthBound(w)
    if (depth > MAX_EXPR_HEIGHT) {
      throw new UnsupportedPredicateError(
        `predicate may be ${depth} levels deep; Durable Object SQLite allows ${MAX_EXPR_HEIGHT}`,
      )
    }
    sql += ` WHERE ${w.sql}`
  }

  const orderBy = compileOrderBy(opts.orderBy, opts.pk, names)
  if (orderBy) {
    sql += ` ORDER BY ${orderBy}`
  } else {
    // No client-specified order: default to `rowid` so a subset read is
    // deterministic instead of an accident of SQLite's query plan (a bare scan
    // happens to walk rowid order, but a WHERE clause touching the pk can make
    // the planner pick the pk's autoindex instead, returning pk-sorted rows).
    // `rowid` also matches insertion order among currently-live rows: a rowid
    // table assigns each new row 1 + the current max, which is monotonic
    // across inserts regardless of intervening deletes. Every synced table
    // exposes a real internal rowid: `assertSyncCompatible` (changes.ts) forbids
    // an `INTEGER PRIMARY KEY` pk (which aliases it), a `WITHOUT ROWID` table
    // (which has none), and a column named `rowid`, generated or not (which
    // shadows it), so this reference always resolves to insertion order.
    sql += ` ORDER BY rowid`
  }

  if (opts.limit != null) {
    sql += ` LIMIT ?`
    params.push(nonNegInt(opts.limit, "limit"))
  } else if (opts.offset != null) {
    sql += ` LIMIT -1` // SQLite requires a LIMIT before OFFSET
  }
  if (opts.offset != null) {
    sql += ` OFFSET ?`
    params.push(nonNegInt(opts.offset, "offset"))
  }

  // Over a limit SQLite would throw at run time; refuse here instead. The SQL is
  // ASCII (identifiers are validated, values are bound), so length is bytes.
  if (params.length > MAX_BOUND_PARAMS) {
    throw new UnsupportedPredicateError(
      `query binds ${params.length} values; Durable Object SQLite allows ${MAX_BOUND_PARAMS} per statement`,
    )
  }
  if (sql.length > MAX_SQL_BYTES) {
    throw new UnsupportedPredicateError(`query is ${sql.length} bytes; Durable Object SQLite allows ${MAX_SQL_BYTES}`)
  }
  if (params.some(valueTooLarge)) {
    throw new UnsupportedPredicateError(`a bound value is ${MAX_VALUE_BYTES} bytes or more`)
  }
  return { sql, params, columns: [...names] }
}
