import { EmptyState } from "@shared/ui/EmptyState";

export default function ForbiddenPage() {
  return (
    <EmptyState
      title="403 — Forbidden"
      description="Your role cannot open this page."
      actionLabel="Back to overview"
      actionTo="/"
    />
  );
}
