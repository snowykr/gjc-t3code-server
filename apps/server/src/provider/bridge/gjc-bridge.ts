import { createAgentSession } from "@gajae-code/coding-agent/sdk/session";
import type {
  GjcBridgeClientFrame,
  GjcBridgeServerFrame,
  GjcBridgeSessionEvent,
} from "./bridgeProtocol.ts";

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
          const created = await createAgentSession({
            cwd: frame.cwd,
            ...(frame.model ? { model: frame.model } : {}),
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
          // T3 exposes GJC presets as `gajae-code/<profile>` model ids. When the
          // requested model is such a preset, activate the matching GJC model
          // profile explicitly — SDK mode does not auto-apply the configured
          // default profile, so without this the session resolves the parent
          // agent's current model instead of the user's configured default.
          const profileMatch =
            frame.model?.match(/^gajae-code\/([a-z0-9-]+)$/i) ??
            frame.options?.modelProfile?.match(/^([a-z0-9-]+)$/i);
          if (profileMatch) {
            try {
              const activated = await (session as any).activateModelProfileForControl?.(
                profileMatch[1],
              );
              log("activated profile:", profileMatch[1], "->", activated);
            } catch (error) {
              log("profile activation failed:", error instanceof Error ? error.message : error);
            }
          }
          // session.id may not exist on the public surface; fall back to the
          // session state id when present, else a stable per-process marker.
          const rawId =
            (session as unknown as { id?: string }).id ??
            (session.state as unknown as { id?: string })?.id;
          sessionId = rawId || `gjc-${process.pid}`;

          session.subscribe((event: any) => {
            const bridgeEvent = mapSessionEvent(event);
            if (bridgeEvent) {
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
          // T3 model ids of the form gajae-code/<profile> map to GJC model
          // profiles (presets); switch the active profile instead of calling
          // setModel with a string the registry cannot resolve.
          const profileMatch = raw?.match(/^gajae-code\/([a-z0-9-]+)$/i);
          if (profileMatch) {
            const activated = await (session as any).activateModelProfileForControl?.(
              profileMatch[1],
            );
            log("model-set activated profile:", profileMatch[1], "->", activated);
            send({
              seq: nextSeq(),
              type: "event",
              event: { kind: "task", state: "model-set", detail: raw },
            });
            return;
          }
          await (session as any).setModel(raw, "default", { cause: "user-selection" });
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

/** Map an SDK AgentSessionEvent to the normalized bridge event (or null to skip). */
function mapSessionEvent(event: any): GjcBridgeSessionEvent | null {
  if (event?.type === "message_update") {
    const ev = event.assistantMessageEvent;
    if (!ev) return null;
    switch (ev.type) {
      case "text_delta":
        return { kind: "text", delta: String(ev.delta ?? "") };
      case "thinking_delta":
        return { kind: "thinking", delta: String(ev.delta ?? "") };
      case "toolcall_start":
        return { kind: "tool", name: String(ev.name ?? ""), input: ev.input ?? {} };
      case "toolcall_end":
        return { kind: "tool_result", name: String(ev.name ?? ""), output: ev.output ?? {} };
      default:
        return null;
    }
  }
  if (event?.type === "turn_completed") {
    return { kind: "task", state: "completed" };
  }
  return null;
}

main().catch((error) => {
  log("fatal:", error);
  process.exit(1);
});
