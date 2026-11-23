/** Accept only same-origin relative paths (blocks open redirects). */
export function safeNextPath(raw: string | null | undefined, fallback = "/"): string {
  if (!raw) {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!trimmed.startsWith("/")) {
    return fallback;
  }
  if (trimmed.startsWith("//") || trimmed.includes("://")) {
    return fallback;
  }
  return trimmed;
}
