import { zodResolver } from "@hookform/resolvers/zod";
import { Button, Group, Stack, TextInput } from "@mantine/core";
import { useForm } from "react-hook-form";

import type { ApiFailure } from "@shared/api/problem";

import { lookupSchema, type LookupFormValues } from "../model/schema";

type LookupFormProps = {
  initial: LookupFormValues;
  submitting: boolean;
  onSubmit: (values: LookupFormValues) => Promise<void>;
};

export function LookupForm({ initial, submitting, onSubmit }: LookupFormProps) {
  const {
    register,
    handleSubmit,
    setError,
    formState: { errors },
  } = useForm<LookupFormValues>({
    defaultValues: initial,
    resolver: (values, context, options) =>
      zodResolver(lookupSchema(Date.now()))(values, context, options),
  });

  return (
    <form
      onSubmit={(event) => {
        void handleSubmit(async (values) => {
          try {
            await onSubmit(values);
          } catch (err) {
            applyFieldErrors(err, setError);
          }
        })(event);
      }}
    >
      <Stack gap="sm">
        <TextInput
          label="Card token"
          description="64 hex characters, not the card number"
          error={errors.cardToken?.message}
          autoComplete="off"
          {...register("cardToken")}
        />
        <TextInput
          label="Merchant id"
          error={errors.merchantId?.message}
          autoComplete="off"
          {...register("merchantId")}
        />
        <Group grow align="flex-start">
          <TextInput label="Latitude" error={errors.lat?.message} {...register("lat")} />
          <TextInput label="Longitude" error={errors.lon?.message} {...register("lon")} />
        </Group>
        <TextInput
          label="Time"
          type="datetime-local"
          step={1}
          error={errors.ts?.message}
          {...register("ts")}
        />
        <Group>
          <Button type="submit" loading={submitting}>
            Look up
          </Button>
        </Group>
      </Stack>
    </form>
  );
}

const FIELDS = ["cardToken", "merchantId", "lat", "lon", "ts"] as const;

function applyFieldErrors(
  err: unknown,
  setError: (field: (typeof FIELDS)[number], error: { message: string }) => void,
): void {
  if (!isFailure(err) || !err.fieldErrors) {
    return;
  }
  for (const item of err.fieldErrors) {
    const field = formField(item.field);
    if (field) {
      setError(field, { message: item.message });
    }
  }
}

function isFailure(value: unknown): value is ApiFailure {
  return typeof value === "object" && value !== null && "status" in value;
}

function formField(name: string): (typeof FIELDS)[number] | null {
  if (name === "card_token") {
    return "cardToken";
  }
  if (name === "merchant_id") {
    return "merchantId";
  }
  if (name === "lat" || name === "lon" || name === "ts") {
    return name;
  }
  return null;
}
