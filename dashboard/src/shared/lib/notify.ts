import { notifications } from "@mantine/notifications";

type NotifyKind = "error" | "warning" | "info" | "success";

const colorByKind: Record<NotifyKind, string> = {
  error: "red",
  warning: "yellow",
  info: "blue",
  success: "green",
};

export function notify(message: string, kind: NotifyKind = "error"): void {
  notifications.show({
    message,
    color: colorByKind[kind],
    autoClose: 6_000,
  });
}
