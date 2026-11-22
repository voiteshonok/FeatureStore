import { useEffect, useRef, useState } from "react";
import { Button, Stack, Title } from "@mantine/core";
import { useDispatch, useSelector } from "react-redux";
import { useSearchParams } from "react-router-dom";

import {
  featureLookupPushed,
  selectRecentLookups,
  type FeatureLookupState,
} from "@features/feature-lookup/model/featureLookupSlice";
import {
  formFromSearch,
  lookupSchema,
  toFeatureRequest,
  writeLookupSearch,
  type LookupFormValues,
} from "@features/feature-lookup/model/schema";
import { FeatureTable } from "@features/feature-lookup/ui/FeatureTable";
import { LookupForm } from "@features/feature-lookup/ui/LookupForm";
import { RecentLookups } from "@features/feature-lookup/ui/RecentLookups";
import { useGetCatalogQuery, useLookupFeaturesMutation } from "@shared/api/baseApi";
import type { components } from "@shared/api/schema";

type FeatureResponse = components["schemas"]["FeatureResponse"];

export default function FeaturesPage() {
  const [params, setParams] = useSearchParams();
  const dispatch = useDispatch();
  const recent = useSelector((state: { featureLookup: FeatureLookupState }) =>
    selectRecentLookups(state),
  );
  const catalog = useGetCatalogQuery(undefined);
  const [lookup, { isLoading }] = useLookupFeaturesMutation();
  const [seed, setSeed] = useState<LookupFormValues>(() => formFromSearch(params));
  const [seedKey, setSeedKey] = useState(0);
  const [lastRequest, setLastRequest] = useState<LookupFormValues | null>(null);
  const [result, setResult] = useState<{ data: FeatureResponse; elapsedMs: number } | null>(null);
  const seedRef = useRef(seed);
  const runRef = useRef<(values: LookupFormValues) => Promise<void>>(() => Promise.resolve());
  seedRef.current = seed;

  useEffect(() => {
    const values = seedRef.current;
    if (!lookupSchema(Date.now()).safeParse(values).success) {
      return;
    }
    void runRef.current(values).catch(() => undefined);
  }, [seedKey]);

  async function run(values: LookupFormValues): Promise<void> {
    const started = performance.now();
    const data = await lookup(toFeatureRequest(values)).unwrap();
    setResult({ data, elapsedMs: Math.round(performance.now() - started) });
    setLastRequest(values);
    dispatch(featureLookupPushed(values));
    setParams(writeLookupSearch(values), { replace: true });
  }
  runRef.current = run;

  function show(values: LookupFormValues) {
    seedRef.current = values;
    setSeed(values);
    setSeedKey((key) => key + 1);
  }

  return (
    <Stack gap="lg">
      <Title order={2}>Features</Title>
      <LookupForm key={seedKey} initial={seed} submitting={isLoading} onSubmit={run} />
      <Button
        variant="default"
        disabled={lastRequest == null || isLoading}
        onClick={() => {
          if (lastRequest) {
            void run(lastRequest).catch(() => undefined);
          }
        }}
      >
        Request again
      </Button>
      <FeatureTable
        catalog={catalog.data ?? []}
        response={result?.data ?? null}
        elapsedMs={result?.elapsedMs ?? null}
      />
      <RecentLookups items={recent} onPick={show} />
    </Stack>
  );
}
