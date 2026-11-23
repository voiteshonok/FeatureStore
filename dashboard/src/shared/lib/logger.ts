type LogLevel = "debug" | "info" | "warn" | "error";

const levelRank: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

const minLevel: LogLevel = import.meta.env.PROD ? "warn" : "debug";

function emit(level: LogLevel, message: string, detail?: unknown): void {
  if (levelRank[level] < levelRank[minLevel]) {
    return;
  }
  const line = `[dashboard] ${message}`;
  // Never log tokens or alert payloads — callers must pass safe strings only.
  const sink = console[level === "debug" ? "log" : level].bind(console);
  if (detail === undefined) {
    sink(line);
    return;
  }
  sink(line, detail);
}

export const logger = {
  debug: (message: string, detail?: unknown) => {
    emit("debug", message, detail);
  },
  info: (message: string, detail?: unknown) => {
    emit("info", message, detail);
  },
  warn: (message: string, detail?: unknown) => {
    emit("warn", message, detail);
  },
  error: (message: string, detail?: unknown) => {
    emit("error", message, detail);
  },
};
