export type SseFrame =
  | { kind: "ping" }
  | { kind: "retry"; retryMs: number }
  | { kind: "event"; event: string; data: string; id?: string };

/** Pull complete SSE frames out of a text buffer. `rest` is the incomplete tail. */
export function consumeSseFrames(buffer: string): { frames: SseFrame[]; rest: string } {
  const normalized = buffer.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const frames: SseFrame[] = [];
  let rest = normalized;
  while (rest.length > 0) {
    const splitAt = rest.indexOf("\n\n");
    if (splitAt === -1) {
      break;
    }
    const raw = rest.slice(0, splitAt);
    rest = rest.slice(splitAt + 2);
    const frame = parseFrame(raw);
    if (frame) {
      frames.push(frame);
    }
  }
  return { frames, rest };
}

function parseFrame(raw: string): SseFrame | null {
  if (raw.length === 0) {
    return null;
  }
  let eventName = "message";
  let id: string | undefined;
  let retryMs: number | undefined;
  const data: string[] = [];
  let comment = false;
  let field = false;

  for (const line of raw.split("\n")) {
    if (line.startsWith(":")) {
      comment = true;
      continue;
    }
    if (line.length === 0) {
      continue;
    }
    field = true;
    const colon = line.indexOf(":");
    const name = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }
    if (name === "event") {
      eventName = value;
    } else if (name === "data") {
      data.push(value);
    } else if (name === "id" && !value.includes("\0")) {
      id = value;
    } else if (name === "retry" && /^\d+$/.test(value)) {
      retryMs = Number(value);
    }
  }

  if (!field && comment) {
    return { kind: "ping" };
  }
  if (data.length === 0 && retryMs !== undefined) {
    return { kind: "retry", retryMs };
  }
  if (data.length === 0) {
    return null;
  }
  const frame: SseFrame = { kind: "event", event: eventName, data: data.join("\n") };
  if (id !== undefined && id.length > 0) {
    return { kind: "event", event: eventName, data: data.join("\n"), id };
  }
  return frame;
}
