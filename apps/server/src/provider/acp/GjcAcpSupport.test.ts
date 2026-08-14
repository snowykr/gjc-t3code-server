import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as EffectAcpErrors from "effect-acp/errors";

import {
  applyGjcAcpModelSelection,
  buildGjcAcpSpawnInput,
  resolveGjcAcpBaseModelId,
} from "./GjcAcpSupport.ts";

describe("resolveGjcAcpBaseModelId", () => {
  it("normalizes empty and custom Gjc model ids", () => {
    expect(resolveGjcAcpBaseModelId(undefined)).toBe("gjc");
    expect(resolveGjcAcpBaseModelId("   ")).toBe("gjc");
    expect(resolveGjcAcpBaseModelId("  gjc-test-custom-model  ")).toBe("gjc-test-custom-model");
  });
});

describe("buildGjcAcpSpawnInput", () => {
  it("uses the GJC ACP command and preserves environment", () => {
    const spawn = buildGjcAcpSpawnInput({ binaryPath: "/usr/local/bin/gjc" }, "/tmp/project", {
      GJC_TEST_FLAG: "enabled",
    });

    expect(spawn).toEqual({
      command: "/usr/local/bin/gjc",
      args: ["acp"],
      cwd: "/tmp/project",
      env: {
        GJC_TEST_FLAG: "enabled",
      },
    });
  });
});

describe("applyGjcAcpModelSelection", () => {
  const makeRecordingRuntime = (failure?: EffectAcpErrors.AcpError) => {
    const modelCalls: Array<string> = [];
    const runtime = {
      setSessionModel: (modelId: string) =>
        Effect.gen(function* () {
          modelCalls.push(modelId);
          if (failure) return yield* failure;
          return {};
        }),
    };
    return { runtime, modelCalls };
  };

  it.effect("calls session/set_model when the requested model differs from current", () =>
    Effect.gen(function* () {
      const { runtime, modelCalls } = makeRecordingRuntime();
      const result = yield* applyGjcAcpModelSelection({
        runtime,
        currentModelId: "gjc",
        requestedModelId: "gjc-mock-alt",
        mapError: (cause) => cause.message,
      });
      expect(modelCalls).toEqual(["gjc-mock-alt"]);
      expect(result).toBe("gjc-mock-alt");
    }),
  );

  it.effect("skips set_model when requested matches current", () =>
    Effect.gen(function* () {
      const { runtime, modelCalls } = makeRecordingRuntime();
      const result = yield* applyGjcAcpModelSelection({
        runtime,
        currentModelId: "gjc",
        requestedModelId: "gjc",
        mapError: (cause) => cause.message,
      });
      expect(modelCalls).toEqual([]);
      expect(result).toBe("gjc");
    }),
  );

  it.effect("skips set_model when no model is requested", () =>
    Effect.gen(function* () {
      const { runtime, modelCalls } = makeRecordingRuntime();
      const result = yield* applyGjcAcpModelSelection({
        runtime,
        currentModelId: "gjc",
        requestedModelId: undefined,
        mapError: (cause) => cause.message,
      });
      expect(modelCalls).toEqual([]);
      expect(result).toBe("gjc");
    }),
  );

  it.effect("propagates session/set_model failures via mapError", () =>
    Effect.gen(function* () {
      const failure = EffectAcpErrors.AcpRequestError.invalidParams("session id not known");
      const { runtime } = makeRecordingRuntime(failure);
      const error = yield* Effect.flip(
        applyGjcAcpModelSelection({
          runtime,
          currentModelId: "gjc",
          requestedModelId: "gjc-mock-alt",
          mapError: (cause) => cause.message,
        }),
      );
      expect(error).toBe(failure.message);
    }),
  );
});
