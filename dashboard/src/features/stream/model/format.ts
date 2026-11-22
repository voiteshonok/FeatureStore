export function formatClock(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return date.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });
}

export function formatRate(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(value);
}

export function formatCount(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value);
}

export function formatLatency(value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  return `${formatCount(value)} ms`;
}

export function alertsInWindow(byRule: Record<string, number>): number {
  let total = 0;
  for (const value of Object.values(byRule)) {
    total += value;
  }
  return total;
}
