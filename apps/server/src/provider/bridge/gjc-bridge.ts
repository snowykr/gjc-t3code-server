import { createAgentSession } from "@gajae-code/coding-agent/sdk/session";
import { isModelProfileProviderAvailable } from "@gajae-code/coding-agent/config/model-profile-contract";
import {
  formatModelProfileDisplayLabel,
  type ModelProfileDefinition,
} from "@gajae-code/coding-agent/config/model-profiles";
import type {
  GjcBridgeClientFrame,
  GjcBridgeServerFrame,
  GjcBridgeSessionEvent,
} from "./bridgeProtocol.ts";
import {
  findAvailableConcreteModel,
  hasSyntheticProfileNamespaceCollision,
  profileNameForSelection,
  type GjcSelectableModel,
} from "./gjcModelSelection.ts";

let seq = 0;
const nextSeq = () => ++seq;

function send(frame: GjcBridgeServerFrame): void {
  process.stdout.write(`${JSON.stringify(frame)}\n`);
}

function log(...args: unknown[]): void {
  // stderr only — never contaminate the protocol stream.
  // @effect-diagnostics-next-line globalConsole:off - bridge stderr diagnostics
  console.error("[gjc-bridge]", ...args);
}

async function main(): Promise<void> {
  let session: Awaited<ReturnType<typeof createAgentSession>>["session"] | null = null;
  let sessionId = "";
  const pendingRequests = new Map<string, (frame: GjcBridgeServerFrame) => void>();

  const readline = (await import("node:readline")).createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });

  const handleClientFrame = async (raw: string): Promise<void> => {
    let frame: GjcBridgeClientFrame;
    try {
      frame = JSON.parse(raw) as GjcBridgeClientFrame;
    } catch {
      send({ seq: nextSeq(), type: "error", message: "invalid JSON frame" });
      return;
    }

    switch (frame.type) {
      case "ping":
        send({ seq: nextSeq(), type: "error", message: "pong" });
        return;

      case "session/create": {
        try {
          const requestedProfile = profileNameForSelection({
            model: frame.model,
            modelProfile: frame.options?.modelProfile,
          });
          const created = await createAgentSession({
            cwd: frame.cwd,
            // The SDK's `model` option is a Model object, not a provider/id
            // selector string. Defer concrete selectors to the SDK resolver;
            // synthetic profile selectors activate after the session exists.
            ...(frame.model && !requestedProfile ? { modelPattern: frame.model } : {}),
            ...(frame.mcpConfigPath ? { mcpConfigPath: frame.mcpConfigPath } : {}),
            enableLsp: frame.options?.enableLsp ?? false,
            skipPythonPreflight: frame.options?.skipPythonPreflight ?? true,
            disableExtensionDiscovery: frame.options?.disableExtensionDiscovery ?? true,
            ...(frame.options?.agentDir ? { agentDir: frame.options.agentDir } : {}),
            // Safe default: the user's config defaultThinkingLevel is "max",
            // which several providers reject. A bounded level keeps the first
            // prompt from failing on unsupported effort.
            ...(frame.options?.thinkingLevel
              ? { thinkingLevel: frame.options.thinkingLevel }
              : { thinkingLevel: "low" as never }),
          });
          session = created.session;
          if (requestedProfile) {
            const availableModels =
              session.getAvailableModels() as ReadonlyArray<GjcSelectableModel>;
            if (
              hasSyntheticProfileNamespaceCollision(
                availableModels,
                session.modelRegistry.getConfiguredProviderIds(),
              )
            ) {
              throw new Error(
                "The gajae-code provider namespace is reserved; profile selection is unavailable while it is configured.",
              );
            }
            const activated = await session.activateModelProfileForControl(requestedProfile);
            if (!activated)
              throw new Error(`Model profile ${requestedProfile} could not be activated.`);
            log("activated profile:", requestedProfile);
          }
          let configOptions:
            | ReadonlyArray<{
                readonly id: string;
                readonly name?: string;
                readonly type: "select";
                readonly options: ReadonlyArray<{
                  readonly value: string;
                  readonly name?: string;
                }>;
              }>
            | undefined;
          try {
            const sdkModels = session.getAvailableModels() as ReadonlyArray<GjcSelectableModel>;
            const namespaceCollision = hasSyntheticProfileNamespaceCollision(
              sdkModels,
              session.modelRegistry.getConfiguredProviderIds(),
            );
            const concreteOptions = sdkModels
              .filter((model) => model.provider !== "gajae-code")
              .map((m) => ({
                value: `${m.provider}/${m.id}`,
                name: String(m.name ?? `${m.provider}/${m.id}`),
              }));
            const availableProviders = new Set(sdkModels.map((model) => model.provider));
            // The SDK session owns the merged registry: its profile map includes
            // both built-ins and ~/.gjc/agent/models.yml definitions.
            const profiles = session.modelRegistry.getModelProfiles() as ReadonlyMap<
              string,
              ModelProfileDefinition
            >;
            const profileOptions = Array.from(profiles.values())
              .filter(
                (profile) =>
                  !namespaceCollision &&
                  isModelProfileProviderAvailable(profile, availableProviders),
              )
              .map((profile) => ({
                value: `gajae-code/${profile.name}`,
                name: formatModelProfileDisplayLabel(profile),
              }));
            const allOptions = [...profileOptions, ...concreteOptions];
            if (allOptions.length > 0) {
              configOptions = [
                {
                  id: "model",
                  name: "Model",
                  type: "select" as const,
                  options: allOptions,
                },
              ];
            }
          } catch {
            // Model discovery is best-effort; a failed lookup must not block ready.
          }
          // session.id may not exist on the public surface; fall back to the
          // session state id when present, else a stable per-process marker.
          const rawId =
            (session as unknown as { id?: string }).id ??
            (session.state as unknown as { id?: string })?.id;
          sessionId = rawId || `gjc-${process.pid}`;

          session.subscribe((event: unknown) => {
            for (const bridgeEvent of mapSessionEvents(event)) {
              send({ seq: nextSeq(), type: "event", event: bridgeEvent });
            }
          });

          // Wire the SDK permission provider: surface permission requests to the
          // node side as reverse frames and resolve them from permission/respond.
          (session as any).setSdkPermissionProvider?.(async (toolCall: any, options: any[]) => {
            const requestId = `perm-${nextSeq()}`;
            send({
              seq: nextSeq(),
              type: "permission/request",
              requestId,
              toolCall: {
                name: String(toolCall?.toolName ?? toolCall?.name ?? ""),
                input: toolCall?.input ?? {},
              },
              options: (options ?? []).map((opt: any) => ({
                id: String(opt?.id ?? ""),
                label: String(opt?.label ?? opt?.id ?? ""),
              })),
            });
            const outcome = await new Promise<{
              outcome: "allow" | "deny" | "selected";
              optionId?: string;
            }>((resolve) => {
              pendingRequests.set(`permission:${requestId}`, (frame) => {
                if (frame.type === "event") {
                  resolve(
                    frame.event as unknown as {
                      outcome: "allow" | "deny" | "selected";
                      optionId?: string;
                    },
                  );
                }
              });
            });
            if (outcome.outcome === "selected") {
              return { outcome: "selected" as const, optionId: outcome.optionId! };
            }
            return { outcome: outcome.outcome as "allow" | "deny" };
          });

          const rawModel = (session?.model as unknown as { id?: string })?.id ?? frame.model;
          send({
            seq: nextSeq(),
            type: "ready",
            sessionId,
            model: rawModel ? String(rawModel) : "",
            cwd: frame.cwd,
            ...(configOptions ? { configOptions } : {}),
          });
        } catch (error) {
          send({
            seq: nextSeq(),
            type: "error",
            message: error instanceof Error ? error.message : String(error),
            code: "session-create",
          });
        }
        return;
      }

      case "session/model-set": {
        if (!session) {
          send({ seq: nextSeq(), type: "error", message: "no session", code: "no-session" });
          return;
        }
        try {
          const raw = frame.model;
          const profile = profileNameForSelection({ model: raw });
          if (profile) {
            if (
              hasSyntheticProfileNamespaceCollision(
                session.getAvailableModels() as ReadonlyArray<GjcSelectableModel>,
                session.modelRegistry.getConfiguredProviderIds(),
              )
            ) {
              throw new Error(
                "The gajae-code provider namespace is reserved; profile selection is unavailable while it is configured.",
              );
            }
            const activated = await session.activateModelProfileForControl(profile);
            if (!activated) throw new Error(`Model profile ${profile} could not be activated.`);
            log("model-set activated profile:", profile);
            send({
              seq: nextSeq(),
              type: "event",
              event: { kind: "task", state: "model-set", detail: raw },
            });
            return;
          }
          const model = findAvailableConcreteModel(
            session.getAvailableModels() as ReadonlyArray<GjcSelectableModel>,
            raw,
          );
          if (!model) throw new Error(`Model ${raw} is not currently available.`);
          await (session as any).setModel(model, "default", { cause: "user-selection" });
          send({
            seq: nextSeq(),
            type: "event",
            event: { kind: "task", state: "model-set", detail: raw },
          });
        } catch (error) {
          send({
            seq: nextSeq(),
            type: "error",
            message: error instanceof Error ? error.message : String(error),
            code: "model-set",
          });
        }
        return;
      }

      case "session/prompt":
      case "session/steer": {
        if (!session) {
          send({ seq: nextSeq(), type: "error", message: "no session", code: "no-session" });
          return;
        }
        try {
          await session.prompt(frame.text);
          // The SDK prompt resolves when the turn completes; signal the
          // terminal frame so the node side can settle the adapter turn.
          send({
            seq: nextSeq(),
            type: "turn/terminal",
            stopReason: "end_turn",
          });
        } catch (error) {
          send({
            seq: nextSeq(),
            type: "error",
            message: error instanceof Error ? error.message : String(error),
            code: "prompt",
          });
        }
        return;
      }

      case "session/interrupt": {
        if (!session) {
          send({ seq: nextSeq(), type: "error", message: "no session", code: "no-session" });
          return;
        }
        try {
          await session.abort({ cause: "user_interrupt" });
          send({ seq: nextSeq(), type: "event", event: { kind: "task", state: "interrupted" } });
        } catch (error) {
          send({
            seq: nextSeq(),
            type: "error",
            message: error instanceof Error ? error.message : String(error),
            code: "interrupt",
          });
        }
        return;
      }

      case "session/dispose": {
        if (session) {
          try {
            await (session as any).dispose?.();
          } catch {
            // best-effort
          }
        }
        send({ seq: nextSeq(), type: "error", message: "disposed" });
        process.exit(0);
        return;
      }

      case "permission/respond": {
        const handler = pendingRequests.get(`permission:${frame.requestId}`);
        if (handler) {
          pendingRequests.delete(`permission:${frame.requestId}`);
          handler({
            seq: nextSeq(),
            type: "event",
            event: frame.outcome as unknown as GjcBridgeSessionEvent,
          });
        }
        return;
      }

      case "user-input/respond": {
        const handler = pendingRequests.get(`input:${frame.requestId}`);
        if (handler) {
          pendingRequests.delete(`input:${frame.requestId}`);
          handler({
            seq: nextSeq(),
            type: "event",
            event: { kind: "task", state: "user-input", detail: frame.value },
          });
        }
        return;
      }
    }
  };

  // Read frames line by line.
  readline.on("line", (line) => {
    if (line.trim().length === 0) return;
    void handleClientFrame(line).catch((error) => {
      send({
        seq: nextSeq(),
        type: "error",
        message: error instanceof Error ? error.message : String(error),
      });
    });
  });

  readline.on("close", () => {
    process.exit(0);
  });
}

/** Project SDK AgentSessionEvents into adapter-visible bridge events. */
function mapSessionEvents(event: unknown): ReadonlyArray<GjcBridgeSessionEvent> {
  if (!isRecord(event)) return [];

  switch (event.type) {
    case "tool_execution_start": {
      const intent = nonEmptyStringField(event, "intent");
      return [
        {
          kind: "tool",
          toolCallId: stringField(event, "toolCallId"),
          name: stringField(event, "toolName"),
          input: event.args,
          ...(intent ? { intent } : {}),
        },
      ];
    }
    case "tool_execution_update":
      return [
        {
          kind: "tool_progress",
          toolCallId: stringField(event, "toolCallId"),
          name: stringField(event, "toolName"),
          input: event.args,
          output: event.partialResult,
        },
      ];
    case "tool_execution_end":
      return [
        {
          kind: "tool_result",
          toolCallId: stringField(event, "toolCallId"),
          name: stringField(event, "toolName"),
          output: event.result,
          isError: event.isError === true,
        },
      ];
    case "message_update":
      return mapAssistantMessageUpdate(event.assistantMessageEvent);
    case "message_end":
      return mapAssistantMessageEnd(event.message);
    case "turn_completed":
      return [{ kind: "task", state: "completed" }];
    default:
      return [];
  }
}

function mapAssistantMessageUpdate(event: unknown): ReadonlyArray<GjcBridgeSessionEvent> {
  if (!isRecord(event)) return [];
  switch (event.type) {
    case "text_delta":
      return nonEmptyStringField(event, "delta")
        ? [{ kind: "text", delta: stringField(event, "delta") }]
        : [];
    case "thinking_delta":
      return nonEmptyStringField(event, "delta")
        ? [{ kind: "thinking", delta: stringField(event, "delta") }]
        : [];
    case "reasoning_summary_delta":
      return nonEmptyStringField(event, "delta")
        ? [{ kind: "reasoning_summary", delta: stringField(event, "delta") }]
        : [];
    default:
      // Tool-call streaming events describe the model's proposed call. The
      // subsequent tool_execution lifecycle has the stable call id, intent,
      // actual arguments, and result; project only that canonical lifecycle.
      return [];
  }
}

function mapAssistantMessageEnd(message: unknown): ReadonlyArray<GjcBridgeSessionEvent> {
  if (!isRecord(message) || message.role !== "assistant" || !Array.isArray(message.content)) {
    return [];
  }
  return message.content.flatMap<GjcBridgeSessionEvent>((part) => {
    if (!isRecord(part) || part.type !== "thinking") return [];
    const summaryText = nonEmptyStringField(part, "summaryText");
    if (summaryText) return [{ kind: "reasoning_summary" as const, delta: summaryText }];
    const thinking = nonEmptyStringField(part, "thinking");
    return thinking ? [{ kind: "thinking" as const, delta: thinking }] : [];
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function stringField(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  return typeof value === "string" ? value : "";
}

function nonEmptyStringField(record: Record<string, unknown>, key: string): string | undefined {
  const value = stringField(record, key).trim();
  return value.length > 0 ? value : undefined;
}

main().catch((error) => {
  log("fatal:", error);
  process.exit(1);
});
