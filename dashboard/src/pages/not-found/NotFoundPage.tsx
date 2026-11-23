import { EmptyState } from "@shared/ui/EmptyState";

export default function NotFoundPage() {
  return (
    <EmptyState
      title="404 — Not found"
      description="This address does not exist."
      actionLabel="Back to overview"
      actionTo="/"
    />
  );
}
