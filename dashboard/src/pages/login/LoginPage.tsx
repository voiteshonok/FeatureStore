import { zodResolver } from "@hookform/resolvers/zod";
import { Alert, Button, PasswordInput, Stack, TextInput, Title } from "@mantine/core";
import { Controller, useForm } from "react-hook-form";
import { useNavigate, useSearchParams } from "react-router-dom";
import { z } from "zod";

import { useLoginMutation } from "@features/session";
import type { ApiFailure } from "@shared/api/problem";
import { safeNextPath } from "@shared/lib/safeNext";

const schema = z.object({
  email: z
    .string()
    .trim()
    .min(1, "Email is required")
    .email("Invalid email")
    .transform((value) => value.toLowerCase()),
  password: z.string().min(1, "Password is required"),
});

type FormValues = z.input<typeof schema>;

export default function LoginPage() {
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const [login, { isLoading }] = useLoginMutation();
  const {
    register,
    control,
    handleSubmit,
    setError,
    formState: { errors },
  } = useForm<FormValues>({
    resolver: zodResolver(schema),
    defaultValues: { email: "", password: "" },
  });

  const onSubmit = handleSubmit(async (values) => {
    const parsed = schema.parse(values);
    try {
      await login(parsed).unwrap();
      navigate(safeNextPath(params.get("next")), { replace: true });
    } catch (err) {
      const failure = err as ApiFailure;
      if (failure.type === "rate_limited" || failure.status === 429) {
        const wait = failure.retryAfterSec;
        setError("root", {
          message:
            wait && Number.isFinite(wait)
              ? `Too many attempts. Try again in ${wait}s`
              : "Too many attempts",
        });
        return;
      }
      if (failure.type === "unauthorized" || failure.status === 401) {
        setError("root", { message: "Invalid email or password" });
        return;
      }
      setError("root", { message: "Could not sign in. Please try again" });
    }
  });

  return (
    <Stack maw={400} mx="auto" mt="xl" gap="md">
      <Title order={2}>Sign in</Title>
      <form
        onSubmit={(event) => {
          void onSubmit(event);
        }}
      >
        <Stack gap="sm">
          <TextInput
            label="Email"
            autoComplete="username"
            error={errors.email?.message}
            {...register("email")}
          />
          <Controller
            name="password"
            control={control}
            render={({ field }) => (
              <PasswordInput
                label="Password"
                autoComplete="current-password"
                error={errors.password?.message}
                value={field.value}
                onChange={field.onChange}
                onBlur={field.onBlur}
                name={field.name}
                ref={field.ref}
              />
            )}
          />
          {errors.root?.message ? (
            <Alert color="red" title="Sign-in failed">
              {errors.root.message}
            </Alert>
          ) : null}
          <Button type="submit" loading={isLoading}>
            Sign in
          </Button>
        </Stack>
      </form>
    </Stack>
  );
}
