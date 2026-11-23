import { expect, test } from "@playwright/test";

import { signIn } from "./auth";
import { compose } from "./compose";

test("viewer has no actions and sees the stream recover", async ({ page }) => {
  await signIn(page, "viewer@featurestore.local", "demo-viewer-pass");
  await page.goto("/alerts");
  const alertLink = page.locator("a[href^='/alerts/']").filter({ visible: true }).first();
  await expect(alertLink).toBeVisible();
  await alertLink.click();
  await expect(page.getByRole("button", { name: "Take" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Resolve" })).toHaveCount(0);

  const banner = page.getByRole("status").filter({ hasText: "Data is not updating" });
  await compose(["pause", "gateway"]);
  try {
    await expect(banner).toBeVisible();
  } finally {
    await compose(["unpause", "gateway"]);
  }
  await expect(banner).toBeHidden();
});
