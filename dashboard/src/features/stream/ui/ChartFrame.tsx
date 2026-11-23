import { useEffect, useRef, type ReactNode } from "react";

type ChartFrameProps = {
  children: ReactNode;
};

export function ChartFrame({ children }: ChartFrameProps) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    ref.current?.querySelectorAll("svg").forEach((node) => {
      node.setAttribute("aria-hidden", "true");
      node.setAttribute("focusable", "false");
    });
  });
  return <div ref={ref}>{children}</div>;
}
