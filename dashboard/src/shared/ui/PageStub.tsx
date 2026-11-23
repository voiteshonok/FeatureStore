import { Text, Title } from "@mantine/core";

type PageStubProps = {
  title: string;
  description?: string;
};

/** Temporary screen body until the feature page is implemented. */
export function PageStub({ title, description }: PageStubProps) {
  return (
    <>
      <Title order={2}>{title}</Title>
      {description ? (
        <Text c="dimmed" mt="sm">
          {description}
        </Text>
      ) : null}
    </>
  );
}
