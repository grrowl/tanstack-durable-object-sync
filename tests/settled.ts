// The suite runs against every supported @tanstack/db (ADR-0022: floor, locked,
// latest), and 1.0 renamed how a write's outcome is awaited: `when("settled")`
// replaces `isPersisted.promise`, which 1.0 removed. Both resolve once the
// transaction's mutationFn has completed and reject with its error.

/** Await a direct write's outcome on any supported @tanstack/db. */
export function settled(tx: {
  when?: (state: "settled") => Promise<unknown>
  isPersisted?: { promise: Promise<unknown> }
}): Promise<unknown> {
  return tx.when ? tx.when("settled") : tx.isPersisted!.promise
}
