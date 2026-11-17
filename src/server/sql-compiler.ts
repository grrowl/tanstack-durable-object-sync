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

export class UnsupportedPredicateError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "UnsupportedPredicateError"
  }
}

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/
// SQLite limits in a Durable Object, set by workerd's `SqliteDatabase::setupSecurity`
// (Cloudflare's Durable Objects limits page lists the same). A query over one is
// refused when it is compiled, so none of them throws from SQLite (ADR-0013
// amendment 2026-11-17). Each edge is pinned in workerd by tests/sqlite-limits.test.ts.
/** SQLITE_LIMIT_VARIABLE_NUMBER: bound parameters per statement. */
const MAX_BOUND_PARAMS = 100
/** SQLITE_LIMIT_EXPR_DEPTH: height of an expression tree. */
const MAX_EXPR_HEIGHT = 100
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

function column(node: unknown): string {
  if (!isNode(node) || node.type !== "ref") throw new UnsupportedPredicateError("expected a column reference")
  if (node.path.length !== 1) {
    throw new UnsupportedPredicateError(`nested/aliased column paths are not supported: [${node.path.join(", ")}]`)
  }
  const name = node.path[0]!
  if (!IDENT.test(name)) throw new UnsupportedPredicateError(`invalid column name: ${name}`)
  return `"${name}"`
}

function literal(node: unknown, params: Array<unknown>): string {
  if (!isNode(node) || node.type !== "val") throw new UnsupportedPredicateError("comparison value must be a literal")
  params.push(node.value)
  return "?"
}

/** Compiled SQL for one expression, with its height in SQLite's expression tree
 *  (SQLite's `exprSetHeight`: a leaf is 1, any other node 1 + its tallest child;
 *  `a OR b OR c` is parsed left-deep, so each further operand adds one). The
 *  heights of the `in` terms are measured, not derived: a `json_each` term
 *  counts one more than this rule gives. tests/sqlite-limits.test.ts holds the
 *  model to SQLite's verdict across shapes. */
interface Compiled {
  sql: string
  height: number
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
  // Heights measured in workerd: a `json_each` term is 5, a bound-list term 4.
  const terms: Array<Compiled> = []
  if (texts.length > 0) {
    params.push(JSON.stringify(texts))
    terms.push({ sql: `(${col} IN (SELECT value FROM json_each(?)) AND ${textGuard})`, height: 5 })
  }
  if (ints.length > 0) {
    params.push(JSON.stringify(ints))
    terms.push({ sql: `(${col} IN (SELECT value FROM json_each(?)) AND ${numberGuard})`, height: 5 })
  }
  if (others.length > 0) {
    params.push(...others)
    terms.push({ sql: `(${col} IN (${others.map(() => "?").join(", ")}) AND ${numberGuard})`, height: 4 })
  }
  // Nothing can match: false, or NULL for a NULL column (as above).
  if (terms.length === 0) return { sql: `CASE WHEN ${col} IS NULL THEN NULL ELSE 0 END`, height: 3 }
  return terms.length === 1 ? terms[0]! : { sql: `(${terms.map((t) => t.sql).join(" OR ")})`, height: orHeight(terms) }
}

/** Height of `a OR b OR c` (or AND): left-deep, so each further operand adds one. */
function orHeight(parts: Array<Compiled>): number {
  return parts.reduce((h, p, i) => (i === 0 ? p.height : 1 + Math.max(h, p.height)), 0)
}

function compileExpr(node: unknown, params: Array<unknown>): Compiled {
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
    return { sql: `${column(args[0])} ${COMPARATORS[name]} ${literal(args[1], params)}`, height: 2 }
  }
  if (name === "and" || name === "or") {
    if (args.length === 0) throw new UnsupportedPredicateError(`'${name}' expects at least one argument`)
    const parts = args.map((a) => compileExpr(a, params))
    return { sql: `(${parts.map((p) => p.sql).join(` ${name.toUpperCase()} `)})`, height: orHeight(parts) }
  }
  if (name === "not") {
    if (args.length !== 1) throw new UnsupportedPredicateError("'not' expects 1 argument")
    const inner = compileExpr(args[0], params)
    return { sql: `(NOT ${inner.sql})`, height: 1 + inner.height }
  }
  if (name === "isNull" || name === "isUndefined") {
    if (args.length !== 1) throw new UnsupportedPredicateError(`'${name}' expects 1 argument`)
    const col = column(args[0])
    // A stored column is NULL, never undefined: @tanstack/db's `isUndefined`
    // (=== undefined) is false for every row hydrated from SQLite, and `isNull`
    // (=== null) is exactly IS NULL. Needed for the nulls-last cursor's whereFrom
    // (`gt OR isNull OR isUndefined`). ADR-0013 amendment 2026-09-29.
    // Only a column the table lacks is undefined in JS (true there, false here); the
    // schema is author-owned and shared with the client, so that is out of contract.
    return name === "isNull" ? { sql: `${col} IS NULL`, height: 2 } : { sql: "0", height: 1 }
  }
  if (name === "in") {
    const arr = isNode(args[1]) && args[1].type === "val" ? (args[1] as { value: unknown }).value : undefined
    if (args.length !== 2 || !Array.isArray(arr)) {
      throw new UnsupportedPredicateError("'in' expects a column and an array literal")
    }
    return compileIn(column(args[0]), arr, params)
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
  return { sql: compileExpr(where, params).sql, params }
}

/** Extract (column, descending, nulls last) from an orderBy clause. Accepts @tanstack/db's
 *  OrderByClause ({ expression: PropRef, compareOptions: { direction, nulls } }) — the
 *  real wire shape from a live query — or a simple { col, dir }. Nulls default to
 *  first, as in TanStack's comparator, in either direction. */
function orderByTerm(item: unknown): { col: string; desc: boolean; nullsLast: boolean } {
  const o = item as {
    expression?: { type?: string; path?: Array<string> }
    compareOptions?: { direction?: string; nulls?: string }
    col?: unknown
    dir?: unknown
  }
  if (o.expression?.type === "ref" && Array.isArray(o.expression.path) && o.expression.path.length > 0) {
    const path = o.expression.path
    const nullsLast = o.compareOptions?.nulls === "last"
    return { col: path[path.length - 1]!, desc: o.compareOptions?.direction === "desc", nullsLast }
  }
  if (typeof o.col === "string") return { col: o.col, desc: o.dir === "desc", nullsLast: false }
  throw new UnsupportedPredicateError(`unsupported orderBy clause: ${JSON.stringify(item)}`)
}

function compileOrderBy(orderBy: unknown, pk: string): string {
  if (!Array.isArray(orderBy) || orderBy.length === 0) return ""
  if (!IDENT.test(pk)) throw new UnsupportedPredicateError(`invalid pk column: ${pk}`)
  const terms = orderBy.map((item) => {
    const { col, desc, nullsLast } = orderByTerm(item)
    if (!IDENT.test(col)) throw new UnsupportedPredicateError(`invalid orderBy column: ${col}`)
    return `"${col}" ${desc ? "DESC" : "ASC"} NULLS ${nullsLast ? "LAST" : "FIRST"}`
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
  if (typeof n !== "number" || !Number.isInteger(n) || n < 0) {
    throw new UnsupportedPredicateError(`${label} must be a non-negative integer`)
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

/** Compile a full subset SELECT for a collection table (validated identifier). */
export function compileSubsetQuery(tbl: string, opts: SubsetQuery): { sql: string; params: Array<unknown> } {
  if (!IDENT.test(tbl)) throw new UnsupportedPredicateError(`invalid table name: ${tbl}`)
  let sql = `SELECT * FROM "${tbl}"`
  const params: Array<unknown> = []

  if (opts.where != null) {
    const w = compileExpr(opts.where, params)
    if (w.height > MAX_EXPR_HEIGHT) {
      throw new UnsupportedPredicateError(
        `predicate is ${w.height} levels deep; Durable Object SQLite allows ${MAX_EXPR_HEIGHT}`,
      )
    }
    sql += ` WHERE ${w.sql}`
  }

  const orderBy = compileOrderBy(opts.orderBy, opts.pk)
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
    // (which has none), and a declared `rowid` column (which shadows it), so this
    // reference always resolves to insertion order.
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
  for (const p of params) {
    if (typeof p === "string" && p.length >= MAX_VALUE_BYTES / 3 && utf8Bytes(p) >= MAX_VALUE_BYTES) {
      throw new UnsupportedPredicateError(`a bound value is ${MAX_VALUE_BYTES} bytes or more`)
    }
  }
  return { sql, params }
}
