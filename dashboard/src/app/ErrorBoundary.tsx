import { Component, type ErrorInfo, type ReactNode } from "react";

import { Button, Container, Text, Title } from "@mantine/core";

import { logger } from "@shared/lib/logger";

type Props = { children: ReactNode; title?: string };
type State = { hasError: boolean };

export class ErrorBoundary extends Component<Props, State> {
  override state: State = { hasError: false };

  static getDerivedStateFromError(): State {
    return { hasError: true };
  }

  override componentDidCatch(error: Error, info: ErrorInfo): void {
    logger.error("ui crashed", {
      name: error.name,
      message: error.message,
      info: info.componentStack,
    });
  }

  private handleReload = (): void => {
    this.setState({ hasError: false });
    window.location.assign("/");
  };

  override render(): ReactNode {
    if (this.state.hasError) {
      return (
        <Container py="xl">
          <Title order={2}>{this.props.title ?? "Something went wrong"}</Title>
          <Text c="dimmed" mt="sm">
            The screen failed to render. Reload to continue.
          </Text>
          <Button mt="md" onClick={this.handleReload}>
            Reload
          </Button>
        </Container>
      );
    }
    return this.props.children;
  }
}
