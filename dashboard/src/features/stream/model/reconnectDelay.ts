const BASE_MS = 1_000;
const CAP_MS = 30_000;

/**
 * HTTP-error backoff: 1s, 2s, 4s, … capped at 30s, with jitter in [0.5, 1.5].
 * `attempt` is 1-based. `random` returns a value in [0, 1].
 */
export function reconnectDelayMs(attempt: number, random: () => number = Math.random): number {
  const step = Math.max(1, Math.floor(attempt));
  const base = Math.min(CAP_MS, BASE_MS * 2 ** (step - 1));
  const span = 0.5 + random();
  return Math.min(CAP_MS, Math.round(base * span));
}
