import { createTheme, type MantineColorsTuple } from "@mantine/core";

const brand: MantineColorsTuple = [
  "#e8f1ff",
  "#d0e0ff",
  "#a0c0ff",
  "#6e9dff",
  "#447eff",
  "#2869ff",
  "#185eff",
  "#064fe4",
  "#0046cc",
  "#003bb4",
];

export const appTheme = createTheme({
  primaryColor: "brand",
  colors: { brand },
  fontFamily: "IBM Plex Sans, Segoe UI, sans-serif",
  headings: {
    fontFamily: "IBM Plex Sans, Segoe UI, sans-serif",
    fontWeight: "600",
  },
  defaultRadius: "sm",
  spacing: {
    xs: "0.5rem",
    sm: "0.75rem",
    md: "1rem",
    lg: "1.5rem",
    xl: "2rem",
  },
  other: {
    layoutMaxWidth: 1280,
  },
});
