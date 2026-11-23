import {
  Button,
  Drawer,
  SimpleGrid,
  SegmentedControl,
  Select,
  MultiSelect,
  Stack,
  TextInput,
} from "@mantine/core";
import { useDisclosure, useMediaQuery } from "@mantine/hooks";

import type { components } from "@shared/api/schema";

import { type AlertsFilter, type EntityType, type PeriodPreset } from "../model/filters";

type RuleOut = components["schemas"]["RuleOut"];

type AlertFiltersProps = {
  filter: AlertsFilter;
  rules: RuleOut[];
  search: string;
  onSearch: (value: string) => void;
  onChange: (next: AlertsFilter) => void;
  onReset: () => void;
};

const PERIODS = [
  { label: "All", value: "all" },
  { label: "15m", value: "15m" },
  { label: "1h", value: "1h" },
  { label: "24h", value: "24h" },
  { label: "Custom", value: "custom" },
];

export function AlertFilters(props: AlertFiltersProps) {
  const [opened, { open, close }] = useDisclosure(false);
  const phone = useMediaQuery("(max-width: 47.99em)", false, {
    getInitialValueInEffect: false,
  });
  const fields = <FilterFields {...props} />;

  if (!phone) {
    return fields;
  }

  return (
    <>
      <Button variant="default" onClick={open}>
        Filters
      </Button>
      <Drawer opened={opened} onClose={close} title="Filters" position="right" size="100%">
        {fields}
      </Drawer>
    </>
  );
}

function FilterFields({ filter, rules, search, onSearch, onChange, onReset }: AlertFiltersProps) {
  return (
    <Stack gap="sm">
      <SimpleGrid cols={{ base: 1, sm: 2, lg: 3 }}>
        <MultiSelect
          label="Status"
          data={[
            { value: "open", label: "Open" },
            { value: "acknowledged", label: "Acknowledged" },
            { value: "resolved", label: "Resolved" },
          ]}
          value={filter.statuses}
          onChange={(statuses) => {
            onChange({ ...filter, statuses: statuses as AlertsFilter["statuses"] });
          }}
          placeholder="Any"
          clearable
        />
        <Select
          label="Rule"
          data={rules.map((rule) => ({ value: rule.rule_id, label: rule.name }))}
          value={filter.ruleId}
          onChange={(ruleId) => {
            onChange({ ...filter, ruleId });
          }}
          placeholder="Any"
          clearable
          searchable
        />
        <Select
          label="Entity"
          data={[
            { value: "card", label: "Card" },
            { value: "merchant", label: "Merchant" },
          ]}
          value={filter.entityType}
          onChange={(entityType) => {
            onChange({ ...filter, entityType: entityType as EntityType | null });
          }}
          placeholder="Any"
          clearable
        />
        <Select
          label="Sort"
          data={[
            { value: "-created_at", label: "Newest first" },
            { value: "created_at", label: "Oldest first" },
          ]}
          value={filter.sort}
          onChange={(sort) => {
            if (sort === "created_at" || sort === "-created_at") {
              onChange({ ...filter, sort });
            }
          }}
          allowDeselect={false}
        />
        <TextInput
          label="Search"
          description="Prefix of a loaded token or merchant"
          value={search}
          onChange={(event) => {
            onSearch(event.currentTarget.value);
          }}
          placeholder="ab12 or merchant"
        />
      </SimpleGrid>
      <SegmentedControl
        value={filter.preset ?? "all"}
        onChange={(value) => {
          const preset = value === "all" ? null : (value as PeriodPreset);
          onChange({
            ...filter,
            preset,
            from: preset === "custom" ? filter.from : null,
            to: preset === "custom" ? filter.to : null,
          });
        }}
        data={PERIODS}
      />
      {filter.preset === "custom" ? (
        <SimpleGrid cols={{ base: 1, sm: 2 }}>
          <TextInput
            label="From"
            type="datetime-local"
            value={toLocalInput(filter.from)}
            onChange={(event) => {
              onChange({ ...filter, from: fromLocalInput(event.currentTarget.value) });
            }}
          />
          <TextInput
            label="To"
            type="datetime-local"
            value={toLocalInput(filter.to)}
            onChange={(event) => {
              onChange({ ...filter, to: fromLocalInput(event.currentTarget.value) });
            }}
          />
        </SimpleGrid>
      ) : null}
      <Button variant="default" onClick={onReset}>
        Reset
      </Button>
    </Stack>
  );
}

function fromLocalInput(value: string): string | null {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString();
}

function toLocalInput(iso: string | null): string {
  if (!iso) {
    return "";
  }
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}
