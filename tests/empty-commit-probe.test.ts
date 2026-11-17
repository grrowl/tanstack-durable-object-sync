import { createCollection } from "@tanstack/db"
import { describe, expect, it } from "vitest"
import { settled } from "./settled.ts"

// PROBE (ADR-0002 C2, ADR-0028): a confirmed write that lands in NO loaded
// window gets no delta. Through @tanstack/db 0.11 its completed *direct*
// optimistic upsert is then retained until a later sync commit, so the adapter
// issues a post-mutation empty begin()/commit() to run the clear path. From
// 0.12 (TanStack/db#2030) the row is dropped when the mutation function
// settles, and that empty commit has nothing left to do. The adapter issues it
// on every version; this pins the one outcome it needs from all of them:
// nothing is left once it has run.

interface Row {
  id: string
  body: string
}

describe("@tanstack/db: empty sync commit clears a confirmed no-match direct insert", () => {
  it("nothing is left of a confirmed no-match direct insert after an empty begin/commit", async () => {
    type Controls = {
      begin: () => void
      write: (m: { type: string; value?: unknown; key?: string }) => void
      commit: () => void
      markReady: () => void
    }
    let controls: Controls
    const c = createCollection<Row, string>({
      id: "empty-commit-probe",
      getKey: (r: Row) => r.id,
      startSync: true,
      sync: {
        rowUpdateMode: "partial",
        sync: (p: Controls) => {
          controls = p
          p.markReady()
        },
      },
      // Confirmed, but no synced row will ever arrive (the no-window-match case).
      onInsert: async () => {},
    } as never)

    const tx = c.insert({ id: "x", body: "1" })
    expect(c.get("x")).toMatchObject({ id: "x", body: "1" }) // the optimistic row
    await settled(tx)
    // No sync confirmed it. Here 0.11 and older still show the row; 0.12+ do not.

    // The decisive step for the older versions: an empty sync commit.
    controls!.begin()
    controls!.commit()
    await Promise.resolve()
    await new Promise((r) => setTimeout(r, 0))

    expect(c.get("x")).toBeUndefined()
  })
})
