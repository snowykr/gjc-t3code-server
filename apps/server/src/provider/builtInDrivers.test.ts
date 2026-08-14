import {
  DEFAULT_SERVER_SETTINGS,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { deriveProviderInstanceConfigMap } from "./Layers/ProviderInstanceRegistryHydration.ts";
import { redactServerSettingsForClient } from "../serverSettings.ts";
import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";

describe("BUILT_IN_DRIVERS", () => {
  it("ships GJC as its only provider driver", () => {
    expect(BUILT_IN_DRIVERS.map((driver) => driver.driverKind)).toEqual(["gjc"]);
  });

  it("publishes a Gajae Code instance with the legacy binary path to client forms", () => {
    const settings = redactServerSettingsForClient(DEFAULT_SERVER_SETTINGS);

    expect(settings.providerInstances).toMatchObject({
      gjc: {
        driver: "gjc",
        displayName: "Gajae Code",
        enabled: true,
        config: { enabled: true, binaryPath: "gjc", customModels: [] },
      },
    });
    expect(settings.providers.gjc).toMatchObject({
      enabled: true,
      binaryPath: "gjc",
    });
  });

  it("bridges the legacy GJC configuration into the registry instance", () => {
    const gjcInstanceId = ProviderInstanceId.make("gjc");
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      providers: {
        ...DEFAULT_SERVER_SETTINGS.providers,
        gjc: { ...DEFAULT_SERVER_SETTINGS.providers.gjc, binaryPath: "/opt/gjc/bin/gjc" },
      },
    };

    expect(deriveProviderInstanceConfigMap(settings)[gjcInstanceId]?.config).toMatchObject({
      binaryPath: "/opt/gjc/bin/gjc",
    });
  });

  it("drops persisted instances for providers this distribution does not ship", () => {
    const grokInstanceId = ProviderInstanceId.make("grok");
    const gjcInstanceId = ProviderInstanceId.make("gjc");
    const configMap = deriveProviderInstanceConfigMap({
      ...DEFAULT_SERVER_SETTINGS,
      providerInstances: {
        [grokInstanceId]: {
          driver: ProviderDriverKind.make("grok"),
          config: {},
        },
      },
    });

    expect(Object.keys(configMap)).toEqual(["gjc"]);
    expect(configMap[gjcInstanceId]?.driver).toBe("gjc");
  });
});
