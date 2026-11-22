import { useParams } from "react-router-dom";

import { PageStub } from "@shared/ui/PageStub";

export default function AlertDetailPage() {
  const { alertId } = useParams();
  return (
    <PageStub
      title="Alert"
      description={alertId ? `Stub for alert ${alertId}. Full card in T8.4.` : "Missing alert id."}
    />
  );
}
