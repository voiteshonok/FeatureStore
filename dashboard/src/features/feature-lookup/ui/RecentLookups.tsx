import { Button, Stack, Text, Title } from "@mantine/core";

import type { LookupFormValues } from "../model/schema";

type RecentLookupsProps = {
  items: LookupFormValues[];
  onPick: (values: LookupFormValues) => void;
};

export function RecentLookups({ items, onPick }: RecentLookupsProps) {
  if (items.length === 0) {
    return null;
  }
  return (
    <Stack gap="xs">
      <Title order={3}>Recent lookups</Title>
      {items.map((item) => (
        <Button
          key={`${item.cardToken}:${item.merchantId}:${item.ts}`}
          variant="subtle"
          justify="flex-start"
          onClick={() => {
            onPick(item);
          }}
        >
          <Text span size="sm">
            {lookupLabel(item)}
          </Text>
        </Button>
      ))}
    </Stack>
  );
}

function lookupLabel(item: LookupFormValues): string {
  const token = item.cardToken ? previewToken(item.cardToken) : "";
  if (token && item.merchantId) {
    return `${token} · ${item.merchantId}`;
  }
  return token || item.merchantId;
}

function previewToken(token: string): string {
  if (token.length <= 6) {
    return token;
  }
  return `${token.slice(0, 4)}…${token.slice(-2)}`;
}
