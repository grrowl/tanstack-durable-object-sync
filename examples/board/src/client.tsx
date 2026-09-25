// Stress example — board client. A live INFINITE query over `tasks` ordered by
// the MUTABLE `updated_at` key. `useLiveInfiniteQuery` grows the window via
// `setWindow` → the orderBy operator requests the next page through our cursor
// `loadSubset` (the real scroll-back path). Voting/starring sets updated_at=now,
// bumping a task to the top.
//
// Watch three numbers: window (visible) / loaded (collection.size) / total.
// On join, `loaded ≈ window` even though `total` is thousands — bounded load.
// Under the firehose, `loaded` climbs past `window`: every cold task that gets
// bumped upserts into the collection and is never evicted. That gap IS the
// deferred "bounded-window maintenance under churn" limitation, made visible.

import { BTreeIndex, createCollection } from "@tanstack/db"
import { useLiveInfiniteQuery } from "@tanstack/react-db"
import { useEffect, useRef, useState } from "react"
import { createRoot } from "react-dom/client"
import { ulid } from "ulid"
import { doCollectionOptions, WebSocketTransport } from "../../../src/client/index.ts"
import type { BoardApi } from "./worker.ts"

interface Task {
  id: string
  title: string
  status: string
  votes: number
  updated_at: number
}

const room = new URLSearchParams(location.search).get("room") ?? "demo"
const qs = `room=${encodeURIComponent(room)}`
const wsProto = location.protocol === "https:" ? "wss:" : "ws:"
const transport = new WebSocketTransport<BoardApi>({ url: `${wsProto}//${location.host}/sync?${qs}` })
const tasks = createCollection(
  // Row (Task) is inferred from BoardApi + the "tasks" table — no runtime schema.
  doCollectionOptions({ transport, table: "tasks", getKey: (t) => t.id, syncMode: "on-demand" }),
)
// A range index on the order column is what lets the window page lazily via
// the cursor: without it the window loads its first page and can never grow.
// BTreeIndex suits this write-heavy (firehose) collection.
tasks.createIndex((t) => t.updated_at, { indexType: BTreeIndex })

const PAGE = 50

function Board({ total }: { total: number }): JSX.Element {
  const [firehose, setFirehose] = useState(false)
  const scrollRef = useRef<HTMLDivElement>(null)

  // Window grows via setWindow -> cursor loadSubset (scroll-back). No .limit():
  // the hook owns the window. The table is already seeded before this mounts
  // (see boot()), so the initial bounded snapshot has data to return.
  const { data, fetchNextPage, hasNextPage, isFetchingNextPage } = useLiveInfiniteQuery(
    (q) => q.from({ t: tasks }).orderBy(({ t }) => t.updated_at, "desc"),
    { pageSize: PAGE },
  )

  // Server-side firehose: bump random (mostly cold) tasks; the OTHER tab sees
  // them move into its window, and `loaded` climbs.
  useEffect(() => {
    if (!firehose) return
    const id = setInterval(() => void fetch(`/bump?n=5&${qs}`), 200)
    return () => clearInterval(id)
  }, [firehose])

  const onScroll = (): void => {
    const el = scrollRef.current
    if (!el || isFetchingNextPage || !hasNextPage) return
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 80) fetchNextPage()
  }

  const vote = (t: Task): void => void tasks.update(t.id, (d) => { d.votes += 1; d.updated_at = Date.now() })
  const star = (t: Task): void =>
    void tasks.update(t.id, (d) => {
      d.title = d.title.startsWith("★ ") ? d.title : `★ ${d.title}`
      d.updated_at = Date.now()
    })
  const del = (t: Task): void => void tasks.delete(t.id)
  const add = (): void =>
    void tasks.insert({ id: ulid(), title: `New ${ulid().slice(-4)}`, status: "open", votes: 0, updated_at: Date.now() })

  const rows = data ?? []

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", maxWidth: 720, margin: "1.5rem auto", padding: "0 1rem" }}>
      <h2 style={{ marginBottom: 2 }}>tanstack-do-db — board (stress)</h2>
      <p style={{ color: "#666", marginTop: 0, fontSize: 14 }}>
        Bounded window over a big board; voting/★ bumps a task to the top (move-in).
        Toggle the firehose and watch <b>loaded</b> climb past <b>window</b> — the
        deferred bounded-eviction limitation, visualized.
      </p>

      <div style={{ display: "flex", gap: 16, alignItems: "center", marginBottom: 8, fontSize: 14 }}>
        <span>window <b data-testid="window">{rows.length}</b></span>
        <span>loaded <b data-testid="loaded">{tasks.size}</b></span>
        <span>total <b data-testid="total">{total}</b></span>
        <button data-testid="firehose" onClick={() => setFirehose((f) => !f)} style={{ padding: "4px 12px", borderRadius: 6 }}>
          firehose {firehose ? "■ stop" : "▶ start"}
        </button>
        <button data-testid="add" onClick={add} style={{ padding: "4px 12px", borderRadius: 6 }}>
          + task
        </button>
        <button data-testid="more" onClick={() => fetchNextPage()} disabled={!hasNextPage || isFetchingNextPage} style={{ padding: "4px 12px", borderRadius: 6 }}>
          load older
        </button>
        <span data-testid="hasnext" style={{ color: "#aaa" }}>{hasNextPage ? "more" : "end"}</span>
      </div>

      <div
        ref={scrollRef}
        onScroll={onScroll}
        data-testid="list"
        style={{ height: "62vh", overflow: "auto", border: "1px solid #ddd", borderRadius: 8 }}
      >
        {rows.map((t) => (
          <div
            key={t.id}
            data-id={t.id}
            style={{ display: "flex", gap: 10, alignItems: "center", padding: "6px 12px", borderBottom: "1px solid #f0f0f0" }}
          >
            <span style={{ width: 36, textAlign: "right", color: "#888", fontVariantNumeric: "tabular-nums" }}>
              {t.votes}
            </span>
            <button data-testid="vote" onClick={() => vote(t)} title="vote + bump">▲</button>
            <button data-testid="star" onClick={() => star(t)} title="edit + bump">★</button>
            <span style={{ flex: 1 }}>{t.title}</span>
            <span style={{ color: "#aaa", fontSize: 12 }}>{t.status}</span>
            <button data-testid="del" onClick={() => del(t)} title="delete">🗑</button>
          </div>
        ))}
        {rows.length === 0 && <div style={{ padding: 16, color: "#999" }}>loading window…</div>}
      </div>
      <p style={{ color: "#999", fontSize: 12 }}>
        scroll to load older pages (cursor fetch){hasNextPage ? "" : " · end"}
      </p>
    </div>
  )
}

// Seed BEFORE mounting the query: the live window loads its bounded snapshot on
// mount, so the data must already exist (a /seed after mount wouldn't reach the
// already-loaded window without a full broadcast, defeating bounded load).
async function boot(): Promise<void> {
  const c = (await (await fetch(`/count?${qs}`)).json()) as { count: number }
  if (c.count === 0) await fetch(`/seed?n=5000&${qs}`)
  const c2 = (await (await fetch(`/count?${qs}`)).json()) as { count: number }
  createRoot(document.getElementById("root")!).render(<Board total={c2.count} />)
}
void boot()
