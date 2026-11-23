# WCAG 2.1 AA checks

Checked on the dashboard at 360 px and desktop, light and dark schemes. Ratios are for normal text against the page background, measured in the browser unless noted.

- [x] Light and dark theme follow `prefers-color-scheme` until the operator chooses. The choice is stored in `localStorage` as `mantine-color-scheme-value`.
- [x] Header control is an icon button, "Switch to dark theme" or "Switch to light theme".
- [x] Body text contrast: 21 : 1 light (`#000` on `#fff`), 9.37 : 1 dark (`#c9c9c9` on `#242424`).
- [x] Dimmed text contrast: 8.18 : 1 light (`#495057` on `#fff`), 11.92 : 1 dark (`#dee2e6` on `#242424`).
- [x] New-alert highlight contrast: 12.19 : 1 light (`#3d2b00` on `#fff3bf`), 12.19 : 1 dark (`#fff3bf` on `#3d2b00`). The pulse step stays above 4.5 : 1 (6.77 : 1).
- [x] Focus is visible. Bottom tabs use a 2 px outline on `:focus-visible`. Buttons and fields keep the Mantine ring.
- [x] First Tab stop is "Skip to content". It moves focus to `#main`.
- [x] On a phone the off-screen sidebar is not in the tab order. Order is skip link, header, bottom tabs, then the page.
- [x] Resolve opens as a full-screen dialog at 360 px and keeps focus inside the dialog.
- [x] Icon-only controls have an accessible name: theme, Copy, and collapsed sidebar items.
- [x] Charts: the visible sentence is the screen-reader summary ("Current events/s: …", "No alerts in the last minute" or "N alerts in the last minute: …"). The chart svg is `aria-hidden`.
- [x] New-alert highlight pulses, and `prefers-reduced-motion: reduce` removes the animation and keeps the solid background.
- [x] Icons are Tabler SVG, including the theme control.
