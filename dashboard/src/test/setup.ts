import "@testing-library/jest-dom/vitest";

Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => undefined,
    removeListener: () => undefined,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    dispatchEvent: () => false,
  }),
});

// jsdom fetch rejects relative URLs; prefix origin like a browser would.
const originalFetch = globalThis.fetch.bind(globalThis);
globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
  if (typeof input === "string" && input.startsWith("/")) {
    return originalFetch(`http://localhost${input}`, init);
  }
  if (input instanceof Request && input.url.startsWith("/")) {
    return originalFetch(new Request(`http://localhost${input.url}`, input), init);
  }
  return originalFetch(input, init);
}) as typeof fetch;
