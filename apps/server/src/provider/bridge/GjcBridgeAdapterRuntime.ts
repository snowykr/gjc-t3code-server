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
import { type AcpParsedSessionEvent, type AcpSessionModeState } from "../acp/AcpRuntimeModel.ts";
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

    const toolInputs = new Map<string, unknown>();

    // Bridge event -> AcpParsedSessionEvent mapping.
    const bridgeKindToStreamKind: Record<
      string,
      "assistant_text" | "reasoning_text" | "reasoning_summary_text"
    > = {
      text: "assistant_text",
      thinking: "reasoning_text",
      reasoning_summary: "reasoning_summary_text",
    };
    runtime.onEvent((event) => {
      let parsed: AcpParsedSessionEvent | undefined;
      switch (event.kind) {
        case "text":
        case "thinking":
        case "reasoning_summary": {
          const streamKind = bridgeKindToStreamKind[event.kind] ?? "assistant_text";
          parsed = {
            _tag: "ContentDelta",
            text: event.delta,
            streamKind,
            rawPayload: { text: event.delta, kind: event.kind },
          };
          break;
        }
        case "tool": {
          toolInputs.set(event.toolCallId, event.input);
          parsed = {
            _tag: "ToolCallUpdated",
            toolCall: {
              toolCallId: event.toolCallId,
              kind: bridgeToolKind(event.name),
              title: event.name,
              ...(event.intent ? { detail: event.intent } : {}),
              status: "inProgress",
              data: bridgeToolData(event.toolCallId, event.name, event.input),
            },
            rawPayload: {
              toolCallId: event.toolCallId,
              name: event.name,
              input: event.input,
              ...(event.intent ? { intent: event.intent } : {}),
            },
          };
          break;
        }
        case "tool_progress": {
          toolInputs.set(event.toolCallId, event.input);
          parsed = {
            _tag: "ToolCallUpdated",
            toolCall: {
              toolCallId: event.toolCallId,
              kind: bridgeToolKind(event.name),
              title: event.name,
              status: "inProgress",
              data: bridgeToolData(event.toolCallId, event.name, event.input, event.output),
            },
            rawPayload: {
              toolCallId: event.toolCallId,
              name: event.name,
              input: event.input,
              output: event.output,
            },
          };
          break;
        }
        case "tool_result": {
          const input = toolInputs.get(event.toolCallId) ?? {};
          toolInputs.delete(event.toolCallId);
          parsed = {
            _tag: "ToolCallUpdated",
            toolCall: {
              toolCallId: event.toolCallId,
              kind: bridgeToolKind(event.name),
              title: event.isError ? `Failed: ${event.name}` : event.name,
              status: event.isError ? "failed" : "completed",
              data: bridgeToolData(event.toolCallId, event.name, input, event.output),
            },
            rawPayload: {
              toolCallId: event.toolCallId,
              name: event.name,
              input,
              output: event.output,
              isError: event.isError,
            },
          };
          break;
        }
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
      const bridgeConfigOptions = (runtime as any).getReadyConfigOptions?.() ?? [];
      const configOptions = Array.isArray(bridgeConfigOptions)
        ? (bridgeConfigOptions as unknown as ReadonlyArray<EffectAcpSchema.SessionConfigOption>)
        : [];
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
          configOptions,
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
      getModeState: Effect.succeed(undefined as AcpSessionModeState | undefined),
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

function bridgeToolKind(name: string): string {
  switch (name) {
    case "bash":
      return "execute";
    case "find":
    case "search":
      return "search";
    case "fetch":
      return "fetch";
    case "edit":
    case "write":
    case "delete":
    case "move":
      return name;
    default:
      return "dynamic";
  }
}

function bridgeToolData(
  toolCallId: string,
  name: string,
  input: unknown,
  output?: unknown,
): Record<string, unknown> {
  const item: Record<string, unknown> = { input };
  if (output !== undefined) {
    item.result = output;
  }
  return {
    toolCallId,
    kind: name,
    rawInput: input,
    ...(output !== undefined ? { rawOutput: output } : {}),
    item,
  };
}

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
