import { zodResolver } from "@hookform/resolvers/zod";
import { Alert, Button, Group, Modal, Select, Stack, Text, Textarea } from "@mantine/core";
import { useEffect } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";

import {
  RESOLVE_OUTCOMES,
  commentLimit,
  noteError,
  resolutionNote,
  type ResolveOutcome,
} from "../model/note";

const schema = z
  .object({
    outcome: z.enum(["confirmed_fraud", "false_positive", "other"]),
    comment: z.string().trim().min(3, "At least 3 characters"),
  })
  .superRefine((value, ctx) => {
    const error = noteError(resolutionNote(value.outcome, value.comment));
    if (error && value.comment.trim().length >= 3) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["comment"], message: error });
    }
  });

type FormValues = z.infer<typeof schema>;

type ResolveDialogProps = {
  opened: boolean;
  liveStatus: string;
  baselineStatus: string | null;
  submitting: boolean;
  onClose: () => void;
  onResolve: (note: string) => void;
};

export function ResolveDialog({
  opened,
  liveStatus,
  baselineStatus,
  submitting,
  onClose,
  onResolve,
}: ResolveDialogProps) {
  const stale = baselineStatus != null && liveStatus !== baselineStatus;
  const {
    register,
    handleSubmit,
    watch,
    reset,
    setValue,
    formState: { errors },
  } = useForm<FormValues>({
    resolver: zodResolver(schema),
    defaultValues: { outcome: "false_positive", comment: "" },
  });
  const outcome = watch("outcome");
  const comment = watch("comment");
  const note = resolutionNote(outcome, comment);
  const limit = commentLimit(outcome);

  useEffect(() => {
    if (!opened) {
      reset({ outcome: "false_positive", comment: "" });
    }
  }, [opened, reset]);

  return (
    <Modal opened={opened} onClose={onClose} title="Resolve alert" fullScreen={false}>
      <form
        onSubmit={(event) => {
          void handleSubmit((values) => {
            if (stale) {
              return;
            }
            onResolve(resolutionNote(values.outcome, values.comment));
          })(event);
        }}
      >
        <Stack gap="sm">
          {stale ? (
            <Alert color="yellow" role="status">
              Status changed to {liveStatus}. Close this dialog before resolving.
            </Alert>
          ) : null}
          <Select
            label="Outcome"
            data={RESOLVE_OUTCOMES.map((item) => ({ value: item.id, label: item.label }))}
            value={outcome}
            onChange={(value) => {
              if (value === "confirmed_fraud" || value === "false_positive" || value === "other") {
                setValue("outcome", value);
              }
            }}
            allowDeselect={false}
          />
          <Textarea
            label="Comment"
            description={`${note.length} / 500`}
            error={errors.comment?.message}
            maxLength={limit}
            autosize
            minRows={3}
            {...register("comment")}
          />
          <Text size="xs" c="dimmed">
            Saved as “{note || `${labelOf(outcome)}: `}”
          </Text>
          <Group justify="flex-end">
            <Button variant="default" type="button" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit" loading={submitting} disabled={stale}>
              Resolve
            </Button>
          </Group>
        </Stack>
      </form>
    </Modal>
  );
}

function labelOf(outcome: ResolveOutcome): string {
  return RESOLVE_OUTCOMES.find((item) => item.id === outcome)?.label ?? "Other";
}
