/// <reference types="@cloudflare/vitest-pool-workers/types" />

declare namespace Cloudflare {
  interface Env {
    TEST_DO: DurableObjectNamespace
    SYNC_DO: DurableObjectNamespace
    UNREG_DO: DurableObjectNamespace
    MAINT_DO: DurableObjectNamespace
    SLOW_DO: DurableObjectNamespace
    LIMITS_DO: DurableObjectNamespace
    GATED_DO: DurableObjectNamespace
    HOST_DO: DurableObjectNamespace
    HOST_OPTIN_DO: DurableObjectNamespace
  }
}
