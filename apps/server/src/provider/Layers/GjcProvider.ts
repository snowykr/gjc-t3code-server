import {
  type GjcSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import type * as EffectAcpSchema from "effect-acp/schema";
import * as EffectAcpErrors from "effect-acp/errors";
import * as Cause from "effect/Cause";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import { makeGjcAcpRuntime, resolveGjcAcpBaseModelId } from "../acp/GjcAcpSupport.ts";
import { makeGjcBridgeAdapterRuntime } from "../bridge/GjcBridgeAdapterRuntime.ts";
import {
  GJC_AUTHENTICATION_FAILURE_MESSAGE,
  isAcpAuthenticationFailure,
} from "../acp/AcpAdapterSupport.ts";

const GJC_PRESENTATION = {
  displayName: "Gajae Code",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  // GJC exposes `session/set_model`, which the adapter applies before each
  // subsequent prompt on the existing ACP session.
  requiresNewThreadForModelChange: false,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
// SDK bridge is ~2.5s (ready), ACP is 28-40s. Try SDK first (8s
// timeout) and fall back to ACP (60s) only when the bridge is
// unavailable or returns an empty catalog.
const GJC_SDK_MODEL_DISCOVERY_TIMEOUT_MS = 8_000;
const GJC_ACP_MODEL_DISCOVERY_TIMEOUT_MS = 60_000;

// GJC is a provider, not a model: there is no built-in model named "gjc". When
// ACP model discovery fails the provider reports an empty model list rather
// than a fake single model, so the UI never shows "GJC" as a selectable model.
const GJC_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [];

export function buildInitialGjcProviderSnapshot(
  gjcSettings: GjcSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = gjcModelsFromSettings(gjcSettings.customModels);

    if (!gjcSettings.enabled) {
      return buildServerProvider({
        presentation: GJC_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "GJC is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: GJC_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking GJC CLI availability...",
      },
    });
  });
}

function gjcModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = GJC_BUILT_IN_MODELS,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);
}

function buildGjcDiscoveredModelsFromSessionModelState(
  modelState: EffectAcpSchema.SessionModelState | null | undefined,
): ReadonlyArray<ServerProviderModel> {
  if (!modelState || modelState.availableModels.length === 0) {
    return [];
  }
  const seen = new Set<string>();
  return modelState.availableModels
    .map((model): ServerProviderModel | undefined => {
      const slug = resolveGjcAcpBaseModelId(model.modelId);
      if (!slug || seen.has(slug)) {
        return undefined;
      }
      seen.add(slug);
      return {
        slug,
        name: model.name.trim() || slug,
        isCustom: false,
        capabilities: EMPTY_CAPABILITIES,
      };
    })
    .filter((model): model is ServerProviderModel => model !== undefined);
}

/**
 * GJC's ACP `session/new` response does not carry a standard `models` field;
 * the selectable model list arrives as the `model` config option (options may
 * be flat or grouped). Read that option so the provider snapshot lists the
 * real model catalog instead of a single fallback.
 */
export function buildGjcDiscoveredModelsFromConfigOptions(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption> | null | undefined,
): ReadonlyArray<ServerProviderModel> | undefined {
  if (!configOptions) {
    return undefined;
  }
  const modelOption = configOptions.find((option) => option.id === "model");
  if (!modelOption || modelOption.type !== "select") {
    return undefined;
  }
  const seen = new Set<string>();
  const collect = (candidate: {
    readonly value?: string;
    readonly name?: string;
  }): ServerProviderModel | undefined => {
    const rawValue = candidate.value;
    if (typeof rawValue !== "string" || rawValue.trim().length === 0) {
      return undefined;
    }
    const slug = resolveGjcAcpBaseModelId(rawValue);
    if (!slug || seen.has(slug)) {
      return undefined;
    }
    seen.add(slug);
    return {
      slug,
      name: candidate.name?.trim() || slug,
      isCustom: false,
      capabilities: EMPTY_CAPABILITIES,
    };
  };
  const models: ServerProviderModel[] = [];
  for (const option of modelOption.options) {
    if ("options" in option && Array.isArray(option.options)) {
      // Grouped options: each group carries nested select options.
      for (const nested of option.options) {
        const model = collect(nested);
        if (model) models.push(model);
      }
    } else {
      const model = collect(option);
      if (model) models.push(model);
    }
  }
  return models;
}

const discoverGjcModelsViaAcp = (
  gjcSettings: GjcSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const acp = yield* makeGjcAcpRuntime({
      gjcSettings,
      environment,
      childProcessSpawner,
      cwd: process.cwd(),
      clientInfo: { name: "t3-code-provider-probe", version: "0.0.0" },
    });
    const started = yield* acp.start();
    return (
      buildGjcDiscoveredModelsFromConfigOptions(started.sessionSetupResult.configOptions) ??
      buildGjcDiscoveredModelsFromSessionModelState(started.sessionSetupResult.models)
    );
  }).pipe(Effect.scoped);

const discoverGjcModelsViaSdkBridge = (
  gjcSettings: GjcSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const bridge = yield* makeGjcBridgeAdapterRuntime({
      gjcSettings,
      cwd: process.cwd(),
      ...(environment ? { environment } : {}),
    });
    const started = yield* bridge.start();
    return (
      buildGjcDiscoveredModelsFromConfigOptions(
        started.sessionSetupResult.configOptions as never,
      ) ?? buildGjcDiscoveredModelsFromSessionModelState(started.sessionSetupResult.models as never)
    );
  }).pipe(Effect.scoped);

const isGjcAuthenticationFailureCause = (cause: Cause.Cause<unknown>): boolean => {
  const failure = Cause.findErrorOption(cause);
  return (
    Option.isSome(failure) &&
    Schema.is(EffectAcpErrors.AcpRequestError)(failure.value) &&
    isAcpAuthenticationFailure(failure.value, failure.value.method)
  );
};

const runGjcVersionCommand = (
  gjcSettings: GjcSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const command = gjcSettings.binaryPath || "gjc";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export const checkGjcProviderStatus = Effect.fn("checkGjcProviderStatus")(function* (
  gjcSettings: GjcSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = gjcModelsFromSettings(gjcSettings.customModels);

  if (!gjcSettings.enabled) {
    return buildServerProvider({
      presentation: GJC_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "GJC is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runGjcVersionCommand(gjcSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("GJC CLI health check failed.", {
      errorTag: error._tag,
    });
    return buildServerProvider({
      presentation: GJC_PRESENTATION,
      enabled: gjcSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "GJC CLI (`gjc`) is not installed or not on PATH."
          : "Failed to execute GJC CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: GJC_PRESENTATION,
      enabled: gjcSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "GJC CLI is installed but timed out while running `gjc --version`.",
      },
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("GJC CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
      stdoutLength: versionOutput.stdout.length,
      stderrLength: versionOutput.stderr.length,
    });
    return buildServerProvider({
      presentation: GJC_PRESENTATION,
      enabled: gjcSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "GJC CLI is installed but failed to run.",
      },
    });
  }

  // SDK bridge is ~2.5s (ready). Try it first with a short timeout; fall
  // back to ACP (28-40s) only when the bridge is unavailable or returns an
  // empty catalog. Keep ACP as the durable fallback.
  const sdkExit = yield* discoverGjcModelsViaSdkBridge(gjcSettings, environment).pipe(
    Effect.timeoutOption(GJC_SDK_MODEL_DISCOVERY_TIMEOUT_MS),
    Effect.exit,
  );
  if (Exit.isSuccess(sdkExit) && Option.isSome(sdkExit.value)) {
    const sdkModels = sdkExit.value.value;
    if (sdkModels !== undefined && sdkModels.length > 0) {
      const models = gjcModelsFromSettings(gjcSettings.customModels, sdkModels);
      return buildServerProvider({
        presentation: GJC_PRESENTATION,
        enabled: gjcSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version,
          status: "ready",
          auth: { status: "unknown" },
        },
      });
    }
  }
  if (Exit.isFailure(sdkExit)) {
    yield* Effect.logWarning("GJC SDK model discovery failed, falling back to ACP", {
      errorTag: causeErrorTag(sdkExit.cause),
    });
  } else if (Exit.isSuccess(sdkExit) && Option.isNone(sdkExit.value)) {
    yield* Effect.logWarning(
      `GJC SDK model discovery timed out after ${GJC_SDK_MODEL_DISCOVERY_TIMEOUT_MS}ms, falling back to ACP.`,
    );
  }

  const discoveryExit = yield* discoverGjcModelsViaAcp(gjcSettings, environment).pipe(
    Effect.timeoutOption(GJC_ACP_MODEL_DISCOVERY_TIMEOUT_MS),
    Effect.exit,
  );
  if (Exit.isFailure(discoveryExit)) {
    const authenticationFailure = isGjcAuthenticationFailureCause(discoveryExit.cause);
    const pretty = Cause.pretty(discoveryExit.cause);
    const requestError = Option.getOrUndefined(
      Option.filter(
        Cause.findErrorOption(discoveryExit.cause),
        Schema.is(EffectAcpErrors.AcpRequestError),
      ),
    );
    const brokerDetail =
      (requestError?.data as { details?: unknown } | undefined)?.details != null
        ? String((requestError.data as { details: unknown }).details)
        : pretty.replace(/\s+/g, " ").trim().slice(0, 400);
    yield* Effect.logWarning("GJC ACP model discovery failed", {
      errorTag: causeErrorTag(discoveryExit.cause),
      authenticationFailure,
      causeDetail: pretty,
    });
    return buildServerProvider({
      presentation: GJC_PRESENTATION,
      enabled: gjcSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: authenticationFailure ? "error" : "warning",
        auth: authenticationFailure ? { status: "unauthenticated" } : { status: "unknown" },
        message: authenticationFailure
          ? GJC_AUTHENTICATION_FAILURE_MESSAGE
          : brokerDetail
            ? `GJC CLI started but ACP model discovery failed: ${brokerDetail}`
            : "GJC CLI started but ACP model discovery failed.",
      },
    });
  }
  if (Option.isNone(discoveryExit.value)) {
    yield* Effect.logWarning(
      `GJC ACP model discovery timed out after ${GJC_ACP_MODEL_DISCOVERY_TIMEOUT_MS}ms.`,
    );
    return buildServerProvider({
      presentation: GJC_PRESENTATION,
      enabled: gjcSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "warning",
        auth: { status: "unknown" },
        message: "GJC CLI timed out while discovering models.",
      },
    });
  }
  const discoveredModels = discoveryExit.value.value;
  const models =
    discoveredModels !== undefined && discoveredModels.length > 0
      ? gjcModelsFromSettings(gjcSettings.customModels, discoveredModels)
      : fallbackModels;

  return buildServerProvider({
    presentation: GJC_PRESENTATION,
    enabled: gjcSettings.enabled,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: discoveredModels !== undefined && discoveredModels.length > 0 ? "ready" : "warning",
      auth: { status: "unknown" },
      ...(discoveredModels === undefined || discoveredModels.length === 0
        ? { message: "GJC model catalog is empty." }
        : {}),
    },
  });
});

export const enrichGjcSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> => {
  const { snapshot, publishSnapshot } = input;

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("GJC version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
