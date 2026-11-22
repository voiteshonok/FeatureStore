import type { StreamConnection } from "./streamSlice";
import { consumeSseFrames, type SseFrame } from "./parseSse";
import { reconnectDelayMs } from "./reconnectDelay";

const DEFAULT_RETRY_MS = 3_000;

export type RunSseOptions = {
  url: string;
  signal: AbortSignal;
  fetchImpl?: typeof fetch;
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
  random?: () => number;
  now?: () => string;
  getLastEventId: () => string | null;
  setLastEventId: (id: string) => void;
  onStatus: (status: StreamConnection) => void;
  onActivity: (at: string) => void;
  onEvent: (event: string, data: unknown) => void;
  onInvalid: (event: string) => void;
  /** True when the operator session is still valid. */
  confirmSession: () => Promise<boolean>;
};

/**
 * One live stream for the whole app.
 * `fetch` is used instead of `EventSource` so `: ping` comments are visible
 * (they reset the staleness timer) and a reconnect after an HTTP error can
 * send `Last-Event-ID`. Reconnect policy matches the browser EventSource:
 * transport drops use the server `retry`, HTTP errors use exponential backoff
 * after `confirmSession`.
 */
export async function runSseStream(options: RunSseOptions): Promise<void> {
  const sleep = options.sleep ?? abortableDelay;
  const random = options.random ?? Math.random;
  const fetchImpl = options.fetchImpl ?? globalThis.fetch.bind(globalThis);
  const now = options.now ?? (() => new Date().toISOString());
  let serverRetryMs = DEFAULT_RETRY_MS;
  let httpAttempt = 0;

  const setStatus = (status: StreamConnection) => {
    if (!isAborted(options.signal)) {
      options.onStatus(status);
    }
  };
  const activity = () => {
    if (!isAborted(options.signal)) {
      options.onActivity(now());
    }
  };

  setStatus("connecting");

  while (!isAborted(options.signal)) {
    let response: Response;
    try {
      const headers = new Headers({
        Accept: "text/event-stream",
        "X-Requested-With": "fetch",
      });
      const lastId = options.getLastEventId();
      if (lastId) {
        headers.set("Last-Event-ID", lastId);
      }
      response = await fetchImpl(options.url, {
        credentials: "include",
        headers,
        signal: options.signal,
        cache: "no-store",
      });
    } catch (error: unknown) {
      if (isAborted(options.signal) || isAbortError(error)) {
        return;
      }
      setStatus("reconnecting");
      await sleep(serverRetryMs, options.signal);
      continue;
    }

    if (isAborted(options.signal)) {
      return;
    }

    if (!response.ok) {
      await cancelBody(response);
      let sessionOk = false;
      try {
        sessionOk = await options.confirmSession();
      } catch {
        sessionOk = false;
      }
      if (!sessionOk || isAborted(options.signal)) {
        return;
      }
      httpAttempt += 1;
      setStatus("reconnecting");
      await sleep(reconnectDelayMs(httpAttempt, random), options.signal);
      continue;
    }

    httpAttempt = 0;
    setStatus("open");
    activity();
    try {
      await readEventStream(response, options.signal, (frame) => {
        if (frame.kind === "retry") {
          serverRetryMs = frame.retryMs;
          return;
        }
        if (frame.kind === "ping") {
          activity();
          return;
        }
        activity();
        if (frame.id) {
          options.setLastEventId(frame.id);
        }
        const parsed = parseJson(frame.data);
        if (parsed === undefined) {
          options.onInvalid(frame.event);
          return;
        }
        options.onEvent(frame.event, parsed);
      });
    } catch (error: unknown) {
      if (isAborted(options.signal) || isAbortError(error)) {
        return;
      }
    }

    if (isAborted(options.signal)) {
      return;
    }
    setStatus("reconnecting");
    await sleep(serverRetryMs, options.signal);
  }
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return undefined;
  }
}

async function readEventStream(
  response: Response,
  signal: AbortSignal,
  onFrame: (frame: SseFrame) => void,
): Promise<void> {
  const reader = response.body?.getReader();
  if (!reader) {
    return;
  }
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (!isAborted(signal)) {
      const { done, value } = await reader.read();
      if (done) {
        buffer += decoder.decode();
        if (buffer.length > 0) {
          const consumed = consumeSseFrames(`${buffer}\n\n`);
          for (const frame of consumed.frames) {
            onFrame(frame);
          }
        }
        return;
      }
      buffer += decoder.decode(value, { stream: true });
      const consumed = consumeSseFrames(buffer);
      buffer = consumed.rest;
      for (const frame of consumed.frames) {
        onFrame(frame);
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}

async function cancelBody(response: Response): Promise<void> {
  await response.body?.cancel().catch(() => undefined);
}

function isAborted(signal: AbortSignal): boolean {
  return signal.aborted;
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

export function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (isAborted(signal)) {
      resolve();
      return;
    }
    let settled = false;
    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
  });
}
