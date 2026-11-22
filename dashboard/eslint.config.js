import js from "@eslint/js";
import eslintConfigPrettier from "eslint-config-prettier";
import jsxA11y from "eslint-plugin-jsx-a11y";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import globals from "globals";
import tseslint from "typescript-eslint";

/** Restrict imports that break feature-sliced top-down direction. */
function layerRestrictions(forbiddenGroups) {
  return {
    "no-restricted-imports": [
      "error",
      {
        patterns: forbiddenGroups.map((group) => ({
          group: [group],
          message: "Feature-sliced: import only downward (or within the same layer).",
        })),
      },
    ],
  };
}

export default tseslint.config(
  { ignores: ["dist", "node_modules", "coverage", "src/shared/api/schema.d.ts"] },
  js.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  {
    languageOptions: {
      ecmaVersion: 2022,
      globals: globals.browser,
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
  {
    files: ["**/*.{ts,tsx}"],
    plugins: {
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
      "jsx-a11y": jsxA11y,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      ...jsxA11y.flatConfigs.recommended.rules,
      "react-refresh/only-export-components": ["warn", { allowConstantExport: true }],
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/consistent-type-imports": [
        "error",
        { prefer: "type-imports", fixStyle: "inline-type-imports" },
      ],
      "@typescript-eslint/restrict-template-expressions": [
        "error",
        { allowNumber: true, allowBoolean: true },
      ],
    },
  },
  {
    files: ["src/pages/**/*.{ts,tsx}"],
    rules: layerRestrictions(["@app/*", "@pages/*"]),
  },
  {
    files: ["src/features/**/*.{ts,tsx}"],
    rules: layerRestrictions(["@app/*", "@pages/*", "@features/*/ui", "../../pages/**"]),
  },
  {
    files: ["src/entities/**/*.{ts,tsx}"],
    rules: layerRestrictions(["@app/*", "@pages/*", "@features/*"]),
  },
  {
    files: ["src/shared/**/*.{ts,tsx}"],
    rules: layerRestrictions(["@app/*", "@pages/*", "@features/*", "@entities/*"]),
  },
  {
    files: [
      "vite.config.ts",
      "eslint.config.js",
      "src/test/**/*.{ts,tsx}",
      "src/**/*.test.{ts,tsx}",
    ],
    ...tseslint.configs.disableTypeChecked,
  },
  eslintConfigPrettier,
);
