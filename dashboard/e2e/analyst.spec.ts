import { expect, test, type Page } from "@playwright/test";

import { signIn } from "./auth";
import { burstCard } from "./compose";

async function closeOpenBurstAlerts(page: Page): Promise<void> {
  const rulesResponse = await page.request.get("/api/v1/rules");
  expect(rulesResponse.ok()).toBeTruthy();
  const rules = (await rulesResponse.json()) as { rule_id: string; name: string }[];
  const rule = rules.find((item) => item.name === "card_velocity_burst");
  expect(rule).toBeTruthy();
  const list = await page.request.get(
    `/api/v1/alerts?status=open&rule_id=${rule?.rule_id}&limit=100`,
  );
  expect(list.ok()).toBeTruthy();
  const body = (await list.json()) as { items: { alert_id: string }[] };
  for (const item of body.items) {
    const headers = { "X-Requested-With": "fetch" };
    const take = await page.request.patch(`/api/v1/alerts/${item.alert_id}`, {
      headers,
      data: { status: "acknowledged" },
    });
    expect(take.ok()).toBeTruthy();
    const resolve = await page.request.patch(`/api/v1/alerts/${item.alert_id}`, {
      headers,
      data: { status: "resolved", note: "Cleared before the burst check" },
    });
    expect(resolve.ok()).toBeTruthy();
  }
}

test("analyst takes and resolves a burst alert", async ({ page }) => {
  await signIn(page, "analyst@featurestore.local", "demo-analyst-pass");
  await closeOpenBurstAlerts(page);
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  const hrefs = () =>
    page
      .locator("a[href^='/alerts/']")
      .evaluateAll((nodes) => nodes.map((node) => node.getAttribute("href") ?? ""));
  await expect.poll(async () => (await hrefs()).length).toBeGreaterThan(0);
  const before = new Set(await hrefs());
  await burstCard();
  let fresh = "";
  await expect
    .poll(
      async () => {
        const now = await hrefs();
        fresh = now.find((href) => !before.has(href)) ?? "";
        return fresh;
      },
      { timeout: 60_000 },
    )
    .not.toBe("");
  await page.locator(`a[href="${fresh}"]`).filter({ visible: true }).first().click();
  await page.getByRole("button", { name: "Take" }).click();
  await expect(page.getByText(/acknowledged/i).first()).toBeVisible();
  await page.getByRole("button", { name: "Resolve" }).click();
  await page.getByLabel("Comment").fill("Seen in the lab");
  await page.getByRole("button", { name: "Resolve" }).last().click();
  await expect(page.getByText("→")).toHaveCount(2);
});
