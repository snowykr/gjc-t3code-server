import { describe, expect, it } from "@effect/vitest";

import { GjcDriver } from "./GjcDriver.ts";

describe("GjcDriver", () => {
  it("advertises GJC ACP metadata and defaults", () => {
    expect(GjcDriver.driverKind).toBe("gjc");
    expect(GjcDriver.metadata).toEqual({
      displayName: "GJC",
      supportsMultipleInstances: false,
    });
    expect(GjcDriver.defaultConfig()).toMatchObject({
      enabled: true,
      binaryPath: "gjc",
      customModels: [],
    });
  });
});
