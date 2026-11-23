import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

import { signIn } from "./auth";

async function expectNoSeriousViolations(page: Page): Promise<void> {
  const results = await new AxeBuilder({ page }).analyze();
  const serious = results.violations.filter(
    (item) => item.impact === "serious" || item.impact === "critical",
  );
  expect(serious, JSON.stringify(serious, null, 2)).toEqual([]);
}

test("pages have no serious accessibility violations", async ({ page }) => {
  await page.goto("/login");
  await expectNoSeriousViolations(page);
  await signIn(page, "analyst@featurestore.local", "demo-analyst-pass");
  for (const [path, heading] of [
    ["/", "Overview"],
    ["/alerts", "Alerts"],
    ["/features", "Features"],
  ] as const) {
    await page.goto(path);
    await expect(page.getByRole("heading", { name: heading })).toBeVisible();
    await expectNoSeriousViolations(page);
  }
});
