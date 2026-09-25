// The loader-preload SSR path: the route loader materializes the todos
// collection on the request's DbClient and preloads it (one snapshot read from
// the DO). routerWithDbClient dehydrates the normalized rows PLUS the resume
// cursor (opaque syncMeta) into the router payload and hydrates the browser
// client from it — data is present from the first (server) render, then the
// WebSocket resumes from the dehydrated cursor and converges: catch-up applies
// exactly what changed while the HTML was in flight, updates AND deletes.
// Stale-while-revalidate, never a flash of empty.

import { useDbClient, useLiveQuery } from "@tanstack/react-db"
import { createFileRoute } from "@tanstack/react-router"
import * as React from "react"
import { todosCollection } from "../lib/todos.ts"

export const Route = createFileRoute("/live-query")({
  loader: async ({ context }) => {
    // Explicit collection preload → normalized rows + syncMeta dehydrate.
    // On client-side navigation this same line just awaits the live sync.
    await context.dbClient.collection(todosCollection).preload()
  },
  component: LiveQueryPage,
})

function LiveQueryPage() {
  // Materialize through the ambient DbClient for imperative writes; the same
  // descriptor in `from` resolves to the same collection instance.
  const todos = useDbClient().collection(todosCollection)
  const [hydrated, setHydrated] = React.useState(false)
  const [text, setText] = React.useState("")
  const { data, isReady } = useLiveQuery({
    query: (q) => q.from({ t: todosCollection }).orderBy(({ t }) => t.id, "asc"),
  })

  React.useEffect(() => setHydrated(true), [])

  const add = () => {
    const t = text.trim()
    if (!t) return
    // Optimistic: appears instantly, confirmed on the single stream.
    todos.insert({ id: crypto.randomUUID(), text: t, done: 0 })
    setText("")
  }

  return (
    <main style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "2rem auto", padding: "0 1rem" }}>
      <h2 style={{ marginBottom: 4 }}>useLiveQuery todos</h2>
      <p style={{ color: "#666", marginTop: 0 }}>
        <span data-testid="hydration-state">{hydrated ? "hydrated" : "ssr"}</span>
        {" · "}
        {/* The server's snapshot is "ready" too; gate on hydration so the first
            client render matches the server HTML ("catching up"). */}
        <span data-testid="ready-state">{hydrated && isReady ? "live" : "catching up"}</span>
        {" · "}
        rows: <b data-testid="ssr-row-count">{data.length}</b>
      </p>
      <ul data-testid="ssr-todo-list" style={{ listStyle: "none", padding: 0 }}>
        {data.map((t) => (
          <li key={t.id} data-testid={`ssr-todo-${t.id}`} style={{ padding: "4px 0" }}>
            <label style={{ textDecoration: t.done ? "line-through" : "none" }}>
              <input
                type="checkbox"
                checked={!!t.done}
                onChange={() => todos.update(t.id, (d) => void (d.done = d.done ? 0 : 1))}
              />{" "}
              {t.text}
            </label>
          </li>
        ))}
      </ul>
      <form
        onSubmit={(e) => {
          e.preventDefault()
          add()
        }}
        style={{ display: "flex", gap: 8 }}
      >
        <input
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="new todo…"
          style={{ flex: 1, padding: 8, borderRadius: 6, border: "1px solid #ccc" }}
        />
        <button type="submit" style={{ padding: "8px 16px", borderRadius: 6 }}>
          add
        </button>
      </form>
    </main>
  )
}
