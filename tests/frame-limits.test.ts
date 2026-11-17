// WHY: oversize-frame handling (ADR-0018, issue #28b). Cloudflare's edge caps
// INBOUND WebSocket messages at ~1 MiB, so an oversize client frame may never
// reach the DO — a server-side rejection alone can't be the surface. The client
// transport therefore guards BEFORE sending: an oversize mut/call rejects
// immediately with a typed MutationRejectedError (FRAME_TOO_LARGE), rolling the
// optimistic overlay back promptly instead of dying into a confirmation
// timeout. Outbound (server->client) is NOT capped the same way: a large row
// must still arrive whole (no splitting, no dropping — column projection, #28a,
// is the real fix), with a console.warn as observability when a frame exceeds
// the fixed 1 MiB threshold. The limits are infrastructure facts (Cloudflare's
// edge cap), not application preferences — constants, not knobs. These tests
// pin all three surfaces.

import { createCollection } from "@tanstack/db";
import { env, runInDurableObject, SELF } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { doCollectionOptions } from "../src/client/do-collection.ts";
import {
  MutationRejectedError,
  WebSocketTransport,
  type WebSocketLike,
} from "../src/client/transport.ts";
import { createFrameCodec } from "../src/wire/frame-codec.ts";
import type { TestApi } from "./test-worker.ts";
import { settled } from "./settled.ts";

function makeTransport(
  room: string,
  timeoutMs?: number,
): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${room}`,
    timeoutMs,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${room}`, {
        headers: { Upgrade: "websocket" },
      });
      const ws = res.webSocket;
      if (!ws) throw new Error("no webSocket");
      ws.accept();
      return ws as unknown as WebSocketLike;
    },
  });
}

const BIG = 1_500_000; // ~1.5 MB, comfortably over the 1 MiB (1_048_576) boundary

describe("large TEXT values sync server->client whole", () => {
  it("delivers a ~1.5MB row server->client in the initial snapshot, whole", async () => {
    const room = "fl-big-snap";
    const t = makeTransport(room);
    await t.connect();

    const bigBody = "S".repeat(BIG);
    await runInDurableObject(
      env.SYNC_DO.get(env.SYNC_DO.idFromName(room)),
      (_i, s) => {
        // NB: the value must go through a bind parameter — interpolating a
        // 1.5MB literal into the statement text hits SQLITE_TOOBIG (DO SQLite
        // caps statement TEXT length, not bound-value size).
        s.storage.sql.exec(
          "INSERT INTO messages(id,body) VALUES(?,?)",
          "big",
          "S".repeat(BIG),
        );
      },
    );

    const messages = createCollection(
      doCollectionOptions({
        transport: t,
        table: "messages",
        getKey: (r) => r.id,
      }),
    );
    await messages.preload();

    const row = messages.get("big");
    expect(row).toBeDefined();
    expect((row as { body: string }).body.length).toBe(BIG);
    expect((row as { body: string }).body).toBe(bigBody);
    t.close();
  });

  it("delivers a ~1.5MB row server->client as a live delta (runSyncedWrite), whole", async () => {
    const room = "fl-big-delta";
    const t = makeTransport(room);
    await t.connect();

    const messages = createCollection(
      doCollectionOptions({
        transport: t,
        table: "messages",
        getKey: (r) => r.id,
      }),
    );
    await messages.preload();
    expect(messages.get("big2")).toBeUndefined();

    // Server-originated write: runSyncedWrite is protected — reach it at
    // runtime, as a DO subclass would call it.
    await runInDurableObject(
      env.SYNC_DO.get(env.SYNC_DO.idFromName(room)),
      (i) => {
        (
          i as unknown as {
            runSyncedWrite: (fn: (sql: SqlStorage) => void) => void;
          }
        ).runSyncedWrite((sql) => {
          sql.exec(
            "INSERT INTO messages(id,body) VALUES(?,?)",
            "big2",
            "D".repeat(BIG),
          );
        });
      },
    );

    const start = Date.now();
    while (!messages.get("big2")) {
      if (Date.now() - start > 3000) throw new Error("big delta never arrived");
      await new Promise((r) => setTimeout(r, 10));
    }
    expect((messages.get("big2") as { body: string }).body.length).toBe(BIG);
    t.close();
  });

  it("warns (console.warn) when an outbound frame exceeds the fixed 1 MiB threshold; small frames stay silent", async () => {
    const room = "fl-warn-outbound";
    const warn = vi.spyOn(console, "warn");
    try {
      const t = makeTransport(room);
      await t.connect();

      // A small row first: no outbound frame is near the threshold, so the
      // guard must stay silent (warn-on-everything would be noise, not signal).
      await runInDurableObject(
        env.SYNC_DO.get(env.SYNC_DO.idFromName(room)),
        (_i, s) => {
          s.storage.sql.exec(
            "INSERT INTO messages(id,body) VALUES('small','tiny')",
          );
        },
      );
      const messages = createCollection(
        doCollectionOptions({
          transport: t,
          table: "messages",
          getKey: (r) => r.id,
        }),
      );
      await messages.preload();
      expect(messages.get("small")).toBeDefined();
      expect(
        warn.mock.calls.filter((c) =>
          String(c[0]).includes("oversize outbound"),
        ),
      ).toHaveLength(0);

      // A >1 MiB live delta must still be DELIVERED whole (warn is
      // observability, not enforcement) AND produce exactly the warning.
      await runInDurableObject(
        env.SYNC_DO.get(env.SYNC_DO.idFromName(room)),
        (i) => {
          (
            i as unknown as {
              runSyncedWrite: (fn: (sql: SqlStorage) => void) => void;
            }
          ).runSyncedWrite((sql) => {
            sql.exec(
              "INSERT INTO messages(id,body) VALUES(?,?)",
              "warned",
              "W".repeat(BIG),
            );
          });
        },
      );
      const start = Date.now();
      while (!messages.get("warned")) {
        if (Date.now() - start > 3000)
          throw new Error("big delta never arrived");
        await new Promise((r) => setTimeout(r, 10));
      }
      const warned = warn.mock.calls.filter((c) =>
        String(c[0]).includes("oversize outbound"),
      );
      expect(warned.length).toBeGreaterThan(0);
      // The warning names the collection (or sub) so the operator can act on
      // it, and points at the real fix (column projection, issue #28).
      expect(String(warned[0]![0])).toMatch(/messages/);
      expect(String(warned[0]![0])).toMatch(/column projection/);
      t.close();
    } finally {
      warn.mockRestore();
    }
  });
});

describe("oversize client mutation frames (ADR-0018)", () => {
  it("rejects an oversize mut PROMPTLY with a typed FRAME_TOO_LARGE — never a confirmation timeout", async () => {
    const room = "fl-big-mut";
    // Generous confirmation timeout: if the old silent-drop path were still in
    // effect, this test would only fail after 5s with a generic timeout Error.
    const t = makeTransport(room, 5000);
    await t.connect();

    const start = Date.now();
    let err: unknown;
    try {
      await t.sendMut({
        t: "mut",
        txId: "fl-big-tx",
        collection: "messages",
        ops: [
          {
            type: "insert",
            key: "toolarge",
            cols: { id: "toolarge", body: "B".repeat(BIG) },
          },
        ],
      });
    } catch (e) {
      err = e;
    }
    // The pre-send guard rejects locally — typed, coded, and without waiting
    // out the confirmation timeout (the frame may never reach the DO in prod:
    // Cloudflare's edge caps inbound WS messages at ~1 MiB).
    expect(err).toBeInstanceOf(MutationRejectedError);
    expect((err as MutationRejectedError).code).toBe("FRAME_TOO_LARGE");
    expect(Date.now() - start).toBeLessThan(2000);

    // The row must not exist server-side.
    const rows = await runInDurableObject(
      env.SYNC_DO.get(env.SYNC_DO.idFromName(room)),
      (_i, s) =>
        Array.from(
          s.storage.sql.exec(
            "SELECT COUNT(*) AS c FROM messages WHERE id='toolarge'",
          ),
        ),
    );
    expect((rows[0] as { c: number }).c).toBe(0);
    t.close();
  });

  it("rolls the optimistic overlay back promptly when a collection insert is FRAME_TOO_LARGE", async () => {
    const room = "fl-big-rollback";
    const t = makeTransport(room, 5000);
    await t.connect();

    const messages = createCollection(
      doCollectionOptions({
        transport: t,
        table: "messages",
        getKey: (r) => r.id,
      }),
    );
    await messages.preload();

    const tx = messages.insert({ id: "huge", body: "H".repeat(BIG) });
    // Optimistic overlay applies immediately...
    expect(messages.get("huge")).toBeDefined();

    let err: unknown;
    try {
      await settled(tx);
    } catch (e) {
      err = e;
    }
    expect(err).toBeDefined();
    // TanStack may wrap the mutationFn error; the root cause is the transport's
    // MutationRejectedError with the FRAME_TOO_LARGE reason.
    const msg =
      err instanceof Error
        ? `${err.message} ${String((err as { cause?: unknown }).cause ?? "")}`
        : String(err);
    expect(msg).toMatch(/frame too large/);

    // ...and rolls back on the local rejection — no 5s timeout window.
    expect(messages.get("huge")).toBeUndefined();
    t.close();
  });

  it("measures string-codec frames in UTF-8 bytes, not UTF-16 code units", async () => {
    // The JSON debug codec emits a string; WebSocket sends strings as UTF-8.
    // Measuring `.length` (code units) would undercount non-ASCII payloads and
    // let them slip past the guard only to die at the edge cap (codex review).
    const fake: WebSocketLike = {
      send: () => {},
      close: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
    };
    const t = new WebSocketTransport({
      url: "https://example.com/unused",
      open: () => fake,
      codec: createFrameCodec({ binary: false }),
      timeoutMs: 500,
    });
    await t.connect();
    // "€" is 1 UTF-16 code unit but 3 UTF-8 bytes: 400k of them keep the
    // frame's code-unit length (~400k) under the 1 MiB cap while its byte
    // size (~1.2M) is over it.
    let err: unknown;
    try {
      await t.sendMut({
        t: "mut",
        txId: "fl-utf8-tx",
        collection: "messages",
        ops: [
          {
            type: "insert",
            key: "u",
            cols: { id: "u", body: "€".repeat(400_000) },
          },
        ],
      });
    } catch (e) {
      err = e;
    }
    // Code-unit measurement would send the frame and die on the 500ms timeout
    // with a generic Error instead.
    expect(err).toBeInstanceOf(MutationRejectedError);
    expect((err as MutationRejectedError).code).toBe("FRAME_TOO_LARGE");
    t.close();
  });

  it("surfaces a synchronous send() failure immediately with a clean waiter — never a confirmation timeout", async () => {
    // workerd refuses some sends synchronously (e.g. frames over its own cap).
    // The transport must reject with THAT error right away and clean up the
    // pending-receipt waiter/timer (codex review) — not sit on an armed
    // timeout and report a generic "confirmation timeout" later.
    const boom = new Error("socket refused the send");
    const fake: WebSocketLike = {
      send: () => {
        throw boom;
      },
      close: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
    };
    const t = new WebSocketTransport({
      url: "https://example.com/unused",
      open: () => fake,
      timeoutMs: 500,
    });
    await t.connect();
    const start = Date.now();
    let err: unknown;
    try {
      await t.sendMut({
        t: "mut",
        txId: "fl-syncfail-tx",
        collection: "messages",
        ops: [{ type: "insert", key: "k", cols: { id: "k", body: "small" } }],
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBe(boom);
    expect(Date.now() - start).toBeLessThan(400);
    t.close();
  });
});

// SqlStorage type for the runInDurableObject callbacks above.
type SqlStorage = import("@cloudflare/workers-types").SqlStorage;
