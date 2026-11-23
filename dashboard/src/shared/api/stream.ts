import { z } from "zod";

/** SSE payloads for GET /api/v1/stream/alerts (AsyncAPI alerts-stream). */
export const alertStreamEventSchema = z
  .object({
    id: z.number().int(),
    op: z.enum(["created", "updated"]),
    alert_id: z.uuid(),
    rule_id: z.uuid(),
    rule_name: z.string(),
    status: z.enum(["open", "acknowledged", "resolved"]),
    entity: z.enum(["card", "merchant"]),
    entity_id: z.string(),
    created_at: z.iso.datetime({ offset: true }),
  })
  .strict();

export const statsStreamEventSchema = z
  .object({
    events_per_s: z.number(),
    events_per_s_avg_60s: z.number(),
    alerts_60s_by_rule: z.record(z.string(), z.number().int()),
    latency_p50_ms: z.number().nullable().optional(),
    latency_p95_ms: z.number().nullable().optional(),
    active_cards_60s: z.number().int(),
  })
  .strict();

export const resetStreamEventSchema = z
  .object({
    reason: z.enum(["gap", "reconnect"]).optional(),
  })
  .strict();

/** SSE payloads for GET /api/v1/stream/system (AsyncAPI system-stream). */
export const componentNameSchema = z.enum([
  "ingestion",
  "features-worker",
  "features-offline",
  "features-api",
  "detection",
  "gateway",
  "Kafka",
  "Redis",
  "PostgreSQL",
  "ClickHouse",
]);

export const componentStateSchema = z
  .object({
    name: componentNameSchema,
    status: z.enum(["up", "down", "unknown"]),
    checked_at: z.iso.datetime({ offset: true }),
    lag: z.number().int().nullable().optional(),
  })
  .strict();

export const systemStreamEventSchema = z
  .object({
    mode: z.enum(["normal", "degraded", "fallback"]),
    components: z.array(componentStateSchema),
  })
  .strict();

export type AlertStreamEvent = z.infer<typeof alertStreamEventSchema>;
export type StatsStreamEvent = z.infer<typeof statsStreamEventSchema>;
export type ResetStreamEvent = z.infer<typeof resetStreamEventSchema>;
export type SystemStreamEvent = z.infer<typeof systemStreamEventSchema>;

export function parseAlertStreamEvent(data: unknown): AlertStreamEvent | null {
  const result = alertStreamEventSchema.safeParse(data);
  return result.success ? result.data : null;
}

export function parseStatsStreamEvent(data: unknown): StatsStreamEvent | null {
  const result = statsStreamEventSchema.safeParse(data);
  return result.success ? result.data : null;
}

export function parseResetStreamEvent(data: unknown): ResetStreamEvent | null {
  const result = resetStreamEventSchema.safeParse(data);
  return result.success ? result.data : null;
}

export function parseSystemStreamEvent(data: unknown): SystemStreamEvent | null {
  const result = systemStreamEventSchema.safeParse(data);
  return result.success ? result.data : null;
}
