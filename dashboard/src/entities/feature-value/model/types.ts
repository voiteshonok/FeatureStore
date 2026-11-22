/** Online feature value returned by lookup (T8.5). */
export type FeatureValue = {
  name: string;
  value: number | string | boolean | null;
  asOf: string | null;
};
