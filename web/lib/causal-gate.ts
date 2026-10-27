/**
 * CausalGate enforces strict monotonicity of an aggregate's sequence on the client.
 *
 *   null → no state materialized yet (any sequence, including 0, is accepted)
 *   N    → state materialized up to N (only sequences > N are accepted)
 *
 * Gaps (4 → 6) are accepted: gap recovery is a backend responsibility, the client
 * only guarantees it never regresses or re-applies a state.
 */
export type CausalGate = {
  offer(seq: bigint): boolean;
  current(): bigint | null;
  reset(): void;
};

export function createCausalGate(): CausalGate {
  let last: bigint | null = null;

  return {
    offer(seq) {
      if (last !== null && seq <= last) return false;
      last = seq;
      return true;
    },
    current: () => last,
    reset() {
      last = null;
    },
  };
}
