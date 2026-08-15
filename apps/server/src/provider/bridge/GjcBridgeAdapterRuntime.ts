/**
 * Bridge adapter runtime — exposes the SDK bridge behind the same
 * `AcpSessionRuntime.Service` surface the GjcAdapter already drives.
 *
 * The GjcAdapter calls `makeGjcAcpRuntime` (a `gjc acp` subprocess) and then
 * uses: start(), getEvents(), drainEvents(), prompt(), steer(), cancel(),
 * setConfigOption(), handleExtRequest(), handleRequestPermission(). This
 * module implements that exact surface over the bun SDK bridge
 * (`GjcBridgeRuntime`), so swapping the constructor in the adapter is the
 * only change needed to cut the ~13s ACP cold start to the ~2.5s SDK path.
 *
 * @module GjcBridgeAdapterRuntime
 */
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as Deferred from "effect/Deferred";
import type * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";
import type { GjcSettings } from "@t3tools/contracts";

import * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import { type AcpParsedSessionEvent } from "../acp/AcpRuntimeModel.ts";
import type { AcpSessionRuntimeEvent } from "../acp/AcpSessionRuntime.ts";
import {
  makeGjcBridgeRuntime,
  type GjcBridgeRuntime,
  type GjcBridgeCreateOptions,
} from "./GjcBridgeRuntime.ts";

export interface GjcBridgeAdapterRuntimeInput {
  readonly gjcSettings: GjcSettings;
  readonly cwd: string;
  readonly model?: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly mcpConfigPath?: string;
}

export const makeGjcBridgeAdapterRuntime = (
  input: GjcBridgeAdapterRuntimeInput,
): Effect.Effect<AcpSessionRuntime.AcpSessionRuntime["Service"], EffectAcpErrors.AcpError> =>
  Effect.gen(function* () {
    const runtime: GjcBridgeRuntime = makeGjcBridgeRuntime();

    const eventQueue = yield* Queue.unbounded<AcpSessionRuntimeEvent>();
    const sessionIdRef = yield* Effect.sync(() => {
      let value = "";
      return {
        get: () => value,
        set: (next: string) => {
          value = next;
        },
      };
    });

    // Bridge event -> AcpParsedSessionEvent mapping.
    runtime.onEvent((event) => {
      let parsed: AcpParsedSessionEvent | undefined;
      switch (event.kind) {
        case "text":
          parsed = {
            _tag: "ContentDelta",
            text: event.delta,
            streamKind: "assistant_text",
            rawPayload: { text: event.delta },
          };
          break;
        case "thinking":
          parsed = {
            _tag: "ContentDelta",
            text: event.delta,
            streamKind: "reasoning_text",
            rawPayload: { text: event.delta },
          };
          break;
        case "tool":
          parsed = {
            _tag: "ToolCallUpdated",
            toolCall: {
              toolCallId: `bridge-${event.name}`,
              title: event.name,
              status: "inProgress",
              data: event.input as Record<string, unknown>,
            },
            rawPayload: { name: event.name, input: event.input },
          };
          break;
        case "tool_result":
          parsed = {
            _tag: "ToolCallUpdated",
            toolCall: {
              toolCallId: `bridge-${event.name}`,
              title: event.name,
              status: "completed",
              data: event.output as Record<string, unknown>,
            },
            rawPayload: { name: event.name, output: event.output },
          };
          break;
        case "task":
          // task states (model-set, interrupted, completed) carry no
          // adapter-visible content; skip them.
          break;
      }
      if (parsed) {
        void Queue.offer(eventQueue, parsed).pipe(Effect.runFork);
      }
    });

    const createOptions = (): GjcBridgeCreateOptions => ({
      cwd: input.cwd,
      ...(input.model ? { model: input.model } : {}),
      ...(input.mcpConfigPath ? { mcpConfigPath: input.mcpConfigPath } : {}),
      options: {
        enableLsp: false,
        skipPythonPreflight: true,
        disableExtensionDiscovery: true,
        thinkingLevel: "low",
      },
    });

    const start = Effect.gen(function* () {
      yield* runtime.createSession(createOptions()).pipe(Effect.orDie);
      const sessionId = sessionIdRef.get() || `bridge-${process.pid}`;
      return {
        sessionId,
        initializeResult: {
          protocolVersion: 1,
          agentInfo: {
            name: "gajae-code",
            title: "Gajae Code",
            version: "0.13.2",
          } as never,
          agentCapabilities: {},
        } as never,
        sessionSetupResult: {
          sessionId,
          cwd: input.cwd,
          configOptions: [],
          models: { currentModelId: input.model },
        } as never,
        modelConfigId: input.model,
      } satisfies AcpSessionRuntime.AcpSessionRuntimeStartResult;
    });

    return {
      start: () => start,
      getEvents: () => Stream.fromQueue(eventQueue),
      drainEvents: Effect.gen(function* () {
        const acknowledge = yield* Deferred.make<void>();
        yield* Queue.offer(eventQueue, {
          _tag: "EventStreamBarrier",
          acknowledge,
        });
        yield* Deferred.await(acknowledge);
      }),
      getModeState: Effect.succeed(undefined),
      getConfigOptions: Effect.succeed([]),
      prompt: (payload) => {
        const text = extractPromptText(payload);
        return runtime
          .prompt({ text })
          .pipe(Effect.orDie, Effect.as(makeBridgePromptResponse(text)));
      },
      steer: (payload) => {
        const text = extractPromptText(payload);
        return runtime.steer(text).pipe(Effect.orDie, Effect.as(makeBridgePromptResponse(text)));
      },
      cancel: runtime.interrupt().pipe(Effect.orDie, Effect.asVoid),
      setMode: (modeId) =>
        Effect.succeed({
          sessionId: sessionIdRef.get(),
          configOption: { id: "mode", value: modeId },
        } as unknown as EffectAcpSchema.SetSessionModeResponse),
      setConfigOption: (configId, value) =>
        Effect.succeed({
          sessionId: sessionIdRef.get(),
          configOption: { id: configId, value },
        } as unknown as EffectAcpSchema.SetSessionConfigOptionResponse),
      setModel: (model) => runtime.setModel(model).pipe(Effect.asVoid, Effect.orDie),
      setSessionModel: (modelId) =>
        runtime.setModel(modelId).pipe(
          Effect.orDie,
          Effect.as({
            sessionId: sessionIdRef.get(),
            modelId,
          } as unknown as EffectAcpSchema.SetSessionModelResponse),
        ),
      request: () => Effect.fail(new Error("bridge request unsupported") as never),
      notify: () => Effect.void,
      handleSessionUpdate: () => Effect.void,
      handleElicitation: () => Effect.void,
      handleElicitationComplete: () => Effect.void,
      handleReadTextFile: () => Effect.void,
      handleWriteTextFile: () => Effect.void,
      handleCreateTerminal: () => Effect.void,
      handleUnknownExtRequest: () => Effect.void,
      handleUnknownExtNotification: () => Effect.void,
      handleExtRequest: () => Effect.void,
      handleExtNotification: () => Effect.void,
      handleRequestPermission: () => Effect.void,
      handleTerminalOutput: () => Effect.void,
      handleTerminalWaitForExit: () => Effect.void,
      handleTerminalKill: () => Effect.void,
      handleTerminalRelease: () => Effect.void,
    } satisfies AcpSessionRuntime.AcpSessionRuntime["Service"];
  });

function extractPromptText(payload: Omit<EffectAcpSchema.PromptRequest, "sessionId">): string {
  const parts = payload.prompt;
  if (Array.isArray(parts)) {
    return parts
      .map((part) => {
        if (typeof part === "string") return part;
        if (part && typeof part === "object" && "text" in part) {
          return String((part as { text?: unknown }).text ?? "");
        }
        return "";
      })
      .join("");
  }
  return typeof parts === "string" ? parts : "";
}

function makeBridgePromptResponse(text: string): EffectAcpSchema.PromptResponse {
  return {
    stopReason: "end_turn",
    message: {
      role: "assistant",
      content: [{ type: "text", text }],
    },
  } as unknown as EffectAcpSchema.PromptResponse;
}
