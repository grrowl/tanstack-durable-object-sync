import { cloudflareTest } from "@cloudflare/vitest-pool-workers"
import { defineConfig } from "vitest/config"

// Tests run inside workerd (via miniflare) so Durable Objects behave like
// production. Pure-TS units (e.g. the wire codec) run here too — workerd is a
// V8 isolate, so they execute identically and we keep a single runner.
export default defineConfig({
  plugins: [
    cloudflareTest({
      singleWorker: true,
      // WebSocket integration tests keep durable connections open across the
      // suite, which is incompatible with isolated-storage stacking. Tests
      // use unique DO names (random UUIDs / distinct rooms) for isolation
      // instead, so per-test storage rollback is unnecessary.
      isolatedStorage: false,
      main: "./tests/test-worker.ts",
      miniflare: {
        compatibilityDate: "2026-03-10",
        compatibilityFlags: ["nodejs_compat"],
        durableObjects: {
          TEST_DO: { className: "TestDO", useSQLite: true },
          SYNC_DO: { className: "SyncTestDO", useSQLite: true },
          UNREG_DO: { className: "UnregisteredDO", useSQLite: true },
          MAINT_DO: { className: "MaintTestDO", useSQLite: true },
          SLOW_DO: { className: "SlowTickDO", useSQLite: true },
          LIMITS_DO: { className: "LimitsTestDO", useSQLite: true },
          GATED_DO: { className: "GatedTestDO", useSQLite: true },
          HOST_DO: { className: "SyncOverHostDO", useSQLite: true },
          HOST_OPTIN_DO: { className: "SyncOverHostOptInDO", useSQLite: true },
        },
      },
    }),
  ],
  test: {
    include: ["tests/**/*.test.ts"],
  },
})
