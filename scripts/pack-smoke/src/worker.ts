// The consumer Worker the smoke test boots in real workerd.
import { render } from "./ssr.js"
import { mixinEntriesAgree } from "./server.js"
import type { Env } from "./schema.js"

export { MixedDO, SessionDO } from "./server.js"

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const session = env.SESSION.get(env.SESSION.idFromName("smoke"))
    await session.seed({ id: "m1", author: "u", content: "hello", created_at: 1 })
    const state = await render((req) => session.readSyncSnapshot(req, request))
    // Construct the mixin DO too: registerSync over a plain DurableObject base.
    const mixed = env.MIXED.get(env.MIXED.idFromName("smoke"))
    const mixedRows = (await mixed.readSyncSnapshot({ collection: "messages" }, request)).rows.length
    return Response.json({ state, mixedRows, mixinEntriesAgree })
  },
} satisfies ExportedHandler<Env>
