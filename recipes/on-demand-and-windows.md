# Load only part of a collection on demand

By default a collection syncs every row up front. This is the eager mode. With
`syncMode: "on-demand"`, the collection syncs only the rows that a live query
asks for. Use it when a collection is large and a client needs only part of it at
a time.

There are two common shapes. In the first, each query loads the rows that match
its filter. In the second, a bounded list grows as the user scrolls.

## Load a subset per query

Create the collection in on-demand mode. Each live query then loads its rows when
it mounts and releases them when it unmounts.

```tsx
const items = createCollection(
  doCollectionOptions<ItemsApi, "items">({
    transport,
    table: "items",
    getKey: (i) => i.id,
    syncMode: "on-demand",
  }),
)

function CategoryPanel({ category }: { category: string }) {
  // Mounting this query loads the matching rows. Unmounting releases them.
  const { data } = useLiveQuery((q) =>
    q.from({ i: items }).where(({ i }) => eq(i.category, category)).orderBy(({ i }) => i.created_at, "asc"),
  )
  return <ul>{data.map((i) => <li key={i.id}>{i.text}</li>)}</ul>
}
```

A category you never open is never synced. See `examples/on-demand` for the full
app.

## Grow a window as you scroll

For a long ordered list, use `useLiveInfiniteQuery`. It keeps a bounded window
and loads the next page when you call `fetchNextPage`. The order column needs a
range index. Without one, the window can't page lazily: each time it grows, the
query requests the whole window again from the start instead of fetching only
the next page. A range index is a sorted index that supports fetching rows by a range of values.

```ts
const tasks = createCollection(
  doCollectionOptions<BoardApi, "tasks">({
    transport,
    table: "tasks",
    getKey: (t) => t.id,
    syncMode: "on-demand",
  }),
)
tasks.createIndex((t) => t.updated_at, { indexType: BTreeIndex })

const { data, fetchNextPage, hasNextPage } = useLiveInfiniteQuery(
  (q) => q.from({ t: tasks }).orderBy(({ t }) => t.updated_at, "desc"),
  { pageSize: 50 },
)
```

On join the client loads about one page, even when the table holds thousands of
rows, plus any rows tied at the page boundary. Scrolling loads older pages. See
`examples/board` for the full app.

## How it works

Each subset request loads exactly the rows it asks for. Queries with identical
subset requests share one load. When a subscription on the Durable Object already
keeps a request's rows current, for example the same filter with a different
order or limit, the request is a one-off read. Any other request opens its own
subscription, with a bounded first snapshot when it has a limit. The Durable
Object releases a subscription when the last query using it is released, and the
rows only it kept current leave the collection. Each scroll fetch is a one-off
read, not a subscription, and returns the next bounded page plus every row that
ties with the boundary value. A query with no limit loads every row that matches
its filter. The client applies ordering and limits over the rows it has loaded.

## Notes

- A write can land outside every loaded subset, e.g. you insert a row in a
  category that no open panel is showing. The write is still confirmed, and the
  client retires its optimistic copy with a follow-up sync commit, so the row
  does not stay unconfirmed.
- Under heavy change the loaded row count can grow past the visible window. A row
  that moves into the window is added and is not removed later. Keeping the
  loaded set as small as the window is a known limitation, and `examples/board`
  shows the gap as a live number.
- A released query's rows leave the collection unless another loaded query still
  holds them. A query is released after it unmounts and its `gcTime` passes. A row
  with a pending optimistic write stays visible until the write settles.
- With `@tanstack/db` 0.9, the first time a row in a window is deleted or moves
  in the order (for example a vote that bumps `updated_at`), the query loads every
  row that matches its filter once, then pages from local rows. A query whose order
  0.9 can't express as a cursor, such as a string order with the default locale
  comparison, does the same when it first loads. This is upstream behaviour, and
  the Durable Object answers the request in full.
- Eager mode with a static `where` also filters, but it loads every matching row
  up front. Use on-demand when even the filtered set is too large to load at
  once.

## See also

- `examples/on-demand` loads one category subset at a time.
- `examples/board` is a windowed list over thousands of rows.
- ADR-0002 and ADR-0005 cover the subset and page-fetch design; ADR-0023 covers
  how subset requests, subscriptions and released rows fit together.
