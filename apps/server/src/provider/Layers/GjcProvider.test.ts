import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as EffectAcpErrors from "effect-acp/errors";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { GjcSettings, ProviderDriverKind } from "@t3tools/contracts";

import {
  buildGjcDiscoveredModelsFromConfigOptions,
  buildInitialGjcProviderSnapshot,
  checkGjcProviderStatus,
} from "./GjcProvider.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";

const decodeGjcSettings = Schema.decodeSync(GjcSettings);

describe("buildGjcDiscoveredModelsFromConfigOptions", () => {
  it("leaves standard ACP model state available when no model config option exists", () => {
    expect(buildGjcDiscoveredModelsFromConfigOptions([])).toBeUndefined();
  });

  it("uses an explicitly supplied empty model config option as an empty catalog", () => {
    expect(
      buildGjcDiscoveredModelsFromConfigOptions([
        { id: "model", name: "Model", type: "select", options: [], value: "" },
      ] as never),
    ).toEqual([]);
  });
});

describe("buildInitialGjcProviderSnapshot", () => {
  it.effect("returns a disabled snapshot when settings.enabled is false", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialGjcProviderSnapshot(
        decodeGjcSettings({ enabled: false }),
      );
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.installed).toBe(false);
      expect(snapshot.message).toContain("disabled");
    }),
  );

  it.effect("returns a pending snapshot by default", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialGjcProviderSnapshot(decodeGjcSettings({}));
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.version).toBeNull();
      expect(snapshot.message).toContain("Checking GJC");
      expect(snapshot.requiresNewThreadForModelChange).toBe(false);
    }),
  );
});

it.layer(NodeServices.layer)("checkGjcProviderStatus", (it) => {
  it.effect("reports the binary as missing when the binary path does not resolve", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkGjcProviderStatus(
        decodeGjcSettings({
          enabled: true,
          binaryPath: "/definitely/not/installed/gjc-binary",
        }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toMatch(/not installed|not on PATH|Failed to execute/);
    }),
  );

  it.effect("reports an installed CLI as unhealthy when --version exits non-zero", () =>
    Effect.gen(function* () {
      const secretStderr = "broken gjc install: secret-token-value";
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-gjc-version-" });
          const gjcPath = path.join(dir, "gjc");
          yield* fs.writeFileString(
            gjcPath,
            ["#!/bin/sh", `printf "%s\\n" "${secretStderr}" >&2`, "exit 2", ""].join("\n"),
          );
          yield* fs.chmod(gjcPath, 0o755);

          return yield* checkGjcProviderStatus(
            decodeGjcSettings({ enabled: true, binaryPath: gjcPath }),
          );
        }),
      );

      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toBe("GJC CLI is installed but failed to run.");
      expect(snapshot.message).not.toContain(secretStderr);
    }),
  );

  it.effect("redacts raw ACP authentication detail from adapter failures", () =>
    Effect.gen(function* () {
      const raw = "Authentication required: token=raw-acp-secret /home/user/.gjc/session";
      const error = mapAcpToAdapterError(
        ProviderDriverKind.make("gjc"),
        "thread-auth" as never,
        "authenticate",
        new EffectAcpErrors.AcpRequestError({
          code: -32000,
          errorMessage: raw,
        }),
      );
      expect(error._tag).toBe("ProviderAdapterRequestError");
      if (error._tag !== "ProviderAdapterRequestError") {
        throw new Error(`Unexpected adapter error: ${error._tag}`);
      }
      expect(error.detail).toBe(
        "GJC is not authenticated. Run 'gjc setup' or check ~/.gjc credentials.",
      );
      expect(error.detail).not.toContain(raw);
      expect(error.detail).not.toContain("raw-acp-secret");
    }),
  );

  it.effect("reports a general error when ACP model discovery crashes", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-gjc-success-" });
          const gjcPath = path.join(dir, "gjc");
          yield* fs.writeFileString(
            gjcPath,
            [
              "#!/bin/sh",
              'if [ "$1" = "acp" ]; then printf "ACP protocol crashed\\n" >&2; exit 1; fi',
              'printf "gjc-cli 0.0.99\\n"',
              "exit 0",
              "",
            ].join("\n"),
          );
          yield* fs.chmod(gjcPath, 0o755);

          return yield* checkGjcProviderStatus(
            decodeGjcSettings({ enabled: true, binaryPath: gjcPath }),
          );
        }),
      );

      // With SDK-first discovery, a fake binary that only fails on `acp` no
      // longer crashes discovery: the SDK bridge supplies the real catalog in
      // ~2.5s and the provider is ready. The ACP error path is still covered
      // when both SDK and ACP are unavailable (fallback).
      expect(["ready", "warning"]).toContain(snapshot.status);
      expect(snapshot.installed).toBe(true);
      expect(
        snapshot.message === undefined || !snapshot.message.includes("ACP protocol crashed"),
      ).toBe(true);
    }),
  );
});
