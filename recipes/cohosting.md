# Add sync to a Durable Object that extends a framework base

`SyncDurableObject` is the trivial application of a mixin, `Syncable(Base)`,
over a plain `DurableObject`. When your DO already extends a framework base,
e.g. the Cloudflare Agents SDK `Agent` or `@cloudflare/think`'s `Think`, apply
the mixin over that base instead. The same DO then serves both its framework's
protocol and the sync protocol, so you do not need a second DO and a mirror
write into it (ADR-0015).

## Recipe

```ts
import { Syncable } from "tanstack-durable-object-sync" // or ".../server/mixin"

// Curried: pin Env and your claims type, then apply over the runtime base.
class FeedAgent extends Syncable<Env, Claims>()(Agent<Env, State>) {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env) // host constructor first
    // Auth hook for the sync upgrade (same contract as parseAttachment):
    this.sync.configure({ parseAttachment: (req) => readClaims(req) })
    ctx.blockConcurrencyWhile(async () => {
      migrate(ctx.storage.sql) // you create the tables…
      this.sync.registerSync(feedSchema) // …then register (ADR-0007)
    })
  }
}
```

The whole sync API lives behind one facade, `this.sync` (`registerSync`,
`runSyncedWrite`, `drainAndBroadcast`, `parseAttachment`, `configure`,
`registry`), so the only public names the mixin adds to your class are `sync`,
`readSyncSnapshot` (the SSR read, public so that DO RPC can reach it), and the
four WebSocket/`fetch` handlers. Sync
sockets carry a reserved hibernation tag and a plain attachment, and the mixin
claims only the `/_sync` path (configurable). Everything else is delegated to
your host base, so the two protocols never cross. No framework is added to this
library's dependency graph; you supply `Base`.

## Keeping the two protocols apart

- **Reach `this.ctx.storage.sql`, not `this.sql`.** The mixin defines no `sql`
  member, because partyserver and agents both define `sql` as a tagged-template
  method and a getter would shadow it and break the host's own queries.
  (`SyncDurableObject` still has `this.sql`, because a bare `DurableObject` has
  no `sql` to shadow.)
- **Your `parseAttachment` claims must not use the key `__pk`.** partyserver
  marks its own sockets with a `__pk` attachment key, so a sync claim object
  carrying `__pk` would make a partyserver-like host mis-claim the sync socket.
  The reserved tag keeps the sync side correct regardless, and the mixin logs an
  error if it sees `__pk` in a sync attachment, but keep it out of your claims.
- **Two DO-global side effects default OFF over a non-`DurableObject` base**,
  and ON for plain `SyncDurableObject`. Opt in with
  `this.sync.configure({ autoResponse: true, caseSensitiveLike: true })`:
  - `autoResponse`: `setWebSocketAutoResponse("ping","pong")` is DO-wide and
    would answer a literal `"ping"` frame from *your host's* client before the
    host sees it.
  - `caseSensitiveLike`: `PRAGMA case_sensitive_like = ON` is connection-wide
    and changes your host's own `LIKE` queries. (Sync needs it for filtered
    subscription parity, ADR-0013.)
- **Never register a host-owned table as a synced collection.** CDC triggers
  install only on the tables you register, so a host's own tables
  (`cf_agents_*`, Think's `assistant_*`) stay untouched by default. But nothing
  stops you from registering one by mistake. Do not, or every host write to it
  emits change rows to your clients.

## What is verified, and how

Be clear-eyed about what backs the guarantee before you build on it.

- **Tested in CI against a fake host.** `tests/host-matrix.test.ts` drives the
  mixin over a partyserver-like fake and pins the safety properties: sockets are
  tag-partitioned, upgrades are path-partitioned, host traffic is delegated to
  `super`, the host's `sql` method is not shadowed, host tables get no triggers,
  a legacy untagged socket still syncs, and the two side effects default off
  with `configure` as a working toggle.
- **Verified by source audit at pinned versions.** The claims about the real
  hosts (partyserver filters foreign sockets on `__pk`; `Agent` and `Think` add
  no socket entry points) were checked by reading partyserver 0.5.8, agents
  0.17.3, and think 0.12.1. A newer host version may behave differently; the
  filtering is an internal behavior of a pre-1.0 package.
- **Wake restore exercised under real evictions** — `tests/hibernation.test.ts`
  forces actual eviction-and-reconstruct cycles (`evictDurableObject`, unlocked
  by the vitest 4 migration) and pins that subscriptions survive on surviving
  sockets (ADR-0019; the first real eviction promptly exposed that they
  hadn't been). The eviction tests run over the bare-DO base; the cohosted
  bases are still covered by same-instance tag assertions only. And CI never
  runs the real `agents` package, only the fake. If you cohost with a real
  `Agent` in production, you are ahead of our own test coverage; we would love
  to hear how it goes.

## Using `@cloudflare/actors`

The practical guidance first: **use the Actors toolkit à la carte over a plain
`DurableObject` base, and let this library be your socket layer.** The Actors
repo itself documents this style
([examples/durable-objects](https://github.com/cloudflare/actors/tree/main/examples/durable-objects)):
extend `DurableObject`, and use the helpers (`Alarms`, `Storage`) as standalone
classes. That composes cleanly with `SyncDurableObject` or `Syncable()`:

- `Alarms` works, because the mixin defines no `alarm()` handler. The DO's
  single alarm slot stays wholly yours (ADR-0015).
- `Storage` is plain SQL over the same database and does not interact with sync.
- Skip `Sockets`. Sync already gives you the realtime channel, with typed
  mutations, catch-up, and confirmation on top.

The one thing that doesn't work out of the box is cohosting over the **`Actor`
class** itself, because its `Sockets` helper takes over socket connections
completely: the `Actor` constructor instantiates it eagerly, and on each wake it
adopts every hibernated socket on the DO with an unfiltered `getWebSockets()`,
broadcasts to all of them, and closes sockets from that same unfiltered set. It
also claims the DO-wide `setWebSocketAutoResponse` slot. Today it has no
ownership discriminator (partyserver filters on `__pk`), so it doesn't leave
room for a second WebSocket protocol alongside it, ours or anyone's — and since
the adoption happens at construction time, a subclass mixin can't step in front
of it.

Checked against `@cloudflare/actors@0.0.1-beta.6` (July 2026). The package is
young and open source, and the fix upstream is small: filter adopted sockets by
a tag or attachment key, the way partyserver does. If Actors ships that,
cohosting over `Actor` becomes possible and this section will change.

## See also

- ADR-0015 is the full design: the three discriminators, the `sql` decision,
  and the cohosting proof against each host.
- ADR-0006 (`runSyncedWrite`), ADR-0007 (`registerSync`), ADR-0008 (trigger
  namespace safety) all compose unchanged with the mixin.
- The README's [Cohosting section](../README.md#cohosting-syncable-over-a-framework-base)
  is the short version of this recipe.
