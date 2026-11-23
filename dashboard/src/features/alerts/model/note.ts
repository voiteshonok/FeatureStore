export const RESOLVE_OUTCOMES = [
  { id: "confirmed_fraud", label: "Confirmed fraud" },
  { id: "false_positive", label: "False positive" },
  { id: "other", label: "Other" },
] as const;

export type ResolveOutcome = (typeof RESOLVE_OUTCOMES)[number]["id"];

const NOTE_MIN = 3;
const NOTE_MAX = 500;

export function outcomeLabel(outcome: ResolveOutcome): string {
  return RESOLVE_OUTCOMES.find((item) => item.id === outcome)?.label ?? "Other";
}

export function resolutionNote(outcome: ResolveOutcome, comment: string): string {
  return `${outcomeLabel(outcome)}: ${comment.trim()}`;
}

export function noteError(note: string): string | null {
  if (note.length < NOTE_MIN) {
    return "Comment must be at least 3 characters";
  }
  if (note.length > NOTE_MAX) {
    return "Note must be at most 500 characters";
  }
  return null;
}

export function commentLimit(outcome: ResolveOutcome): number {
  return NOTE_MAX - outcomeLabel(outcome).length - 2;
}

const COMPARISON = {
  gt: ">",
  ge: "≥",
  lt: "<",
  le: "≤",
} as const;

export function formatCondition(condition: {
  feature_name: string;
  comparison: keyof typeof COMPARISON;
  threshold: number;
}): string {
  return `${condition.feature_name} ${COMPARISON[condition.comparison]} ${condition.threshold}`;
}
