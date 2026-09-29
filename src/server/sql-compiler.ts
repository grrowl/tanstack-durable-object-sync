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

function compileExpr(node: unknown, params: Array<unknown>): string {
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
    }
    return `${column(args[0])} ${COMPARATORS[name]} ${literal(args[1], params)}`
  }
  if (name === "and" || name === "or") {
    if (args.length === 0) throw new UnsupportedPredicateError(`'${name}' expects at least one argument`)
    return `(${args.map((a) => compileExpr(a, params)).join(` ${name.toUpperCase()} `)})`
  }
  if (name === "not") {
    if (args.length !== 1) throw new UnsupportedPredicateError("'not' expects 1 argument")
    return `(NOT ${compileExpr(args[0], params)})`
  }
  if (name === "in") {
    const arr = isNode(args[1]) && args[1].type === "val" ? (args[1] as { value: unknown }).value : undefined
    if (args.length !== 2 || !Array.isArray(arr)) {
      throw new UnsupportedPredicateError("'in' expects a column and an array literal")
    }
    if (arr.length === 0) return "0" // IN () is always false
    const placeholders = arr.map((v) => {
      params.push(v)
      return "?"
    })
    return `${column(args[0])} IN (${placeholders.join(", ")})`
  }
  throw new UnsupportedPredicateError(
    `operator '${name}' is not supported for server-side filtering ` +
      `(floor: eq, gt, gte, lt, lte, like, in, and, or, not)`,
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
  return { sql: compileExpr(where, params), params }
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

function compileOrderBy(orderBy: unknown): string {
  if (!Array.isArray(orderBy) || orderBy.length === 0) return ""
  return orderBy
    .map((item) => {
      const { col, desc, nullsLast } = orderByTerm(item)
      if (!IDENT.test(col)) throw new UnsupportedPredicateError(`invalid orderBy column: ${col}`)
      return `"${col}" ${desc ? "DESC" : "ASC"} NULLS ${nullsLast ? "LAST" : "FIRST"}`
    })
    .join(", ")
}

function nonNegInt(n: unknown, label: string): number {
  if (typeof n !== "number" || !Number.isInteger(n) || n < 0) {
    throw new UnsupportedPredicateError(`${label} must be a non-negative integer`)
  }
  return n
}

export interface SubsetQuery {
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
    const w = compileWhere(opts.where)
    sql += ` WHERE ${w.sql}`
    params.push(...w.params)
  }

  const orderBy = compileOrderBy(opts.orderBy)
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

  return { sql, params }
}
