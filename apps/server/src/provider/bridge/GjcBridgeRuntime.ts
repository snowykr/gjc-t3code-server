/**
 * Node-side runtime for the GJC SDK bridge.
 *
 * Spawns a bun subprocess running `gjc-bridge.ts`, speaks the NDJSON protocol
 * in `bridgeProtocol.ts`, and exposes a promise-based facade the GjcAdapter
 * can drive. The bridge owns one SDK session; this runtime owns the child
 * process lifecycle (spawn, frame dispatch, crash recovery).
 */
// @effect-diagnostics-next-line nodeBuiltinImport:off - bridge child process spawn
import * as ChildProcess from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off - bridge entry path probe
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off - bridge entry path
import * as NodePath from "node:path";
// @effect-diagnostics-next-line nodeBuiltinImport:off - bridge stdout line reader
import * as Readline from "node:readline";
import * as Effect from "effect/Effect";

import type {
  GjcBridgeClientFrame,
  GjcBridgeReady,
  GjcBridgeServerFrame,
  GjcBridgeSessionEvent,
} from "./bridgeProtocol.ts";

export interface GjcBridgeCreateOptions {
  readonly cwd: string;
  readonly model?: string;
  readonly mcpConfigPath?: string;
  readonly options?: {
    readonly enableLsp?: boolean;
    readonly skipPythonPreflight?: boolean;
    readonly disableExtensionDiscovery?: boolean;
    readonly agentDir?: string;
    readonly thinkingLevel?: string;
  };
}

export interface GjcBridgePromptOptions {
  readonly text: string;
  readonly attachments?: ReadonlyArray<{ readonly path?: string; readonly text?: string }>;
}

export class GjcBridgeError extends Error {
  readonly code: string | undefined;
  constructor(message: string, code?: string) {
    super(message);
    this.name = "GjcBridgeError";
    this.code = code;
  }
}

export interface GjcBridgeRuntime {
  /** Config options reported by the most recent successful ready frame. */
  readonly getReadyConfigOptions: () => ReadonlyArray<unknown> | undefined;
  /** Create the SDK session in the bridge and await the ready frame. */
  readonly createSession: (options: GjcBridgeCreateOptions) => Effect.Effect<void, GjcBridgeError>;
  readonly setModel: (model: string) => Effect.Effect<void, GjcBridgeError>;
  readonly prompt: (options: GjcBridgePromptOptions) => Effect.Effect<void, GjcBridgeError>;
  readonly steer: (text: string) => Effect.Effect<void, GjcBridgeError>;
  readonly interrupt: () => Effect.Effect<void, GjcBridgeError>;
  readonly dispose: () => Effect.Effect<void, GjcBridgeError>;
  /** Register a listener for live session events. Returns an unsubscribe fn. */
  readonly onEvent: (listener: (event: GjcBridgeSessionEvent) => void) => () => void;
  /** Kill the child process (for teardown). */
  readonly kill: () => void;
}

// @effect-diagnostics-next-line nodeBuiltinImport:off - bridge child path
const BRIDGE_ENTRY = resolveBridgeEntry();

const resolveBun = (): string => {
  const candidates: ReadonlyArray<string> = [
    process.env.BUN_BINARY ?? "",
    "/home/snowy/.bun/bin/bun",
    "bun",
  ];
  return candidates.find((candidate) => candidate.length > 0) ?? "bun";
};

function resolveBridgeEntry(): string {
  // Bundled server runs from apps/server/dist (dirname = apps/server/dist);
  // source runs from apps/server/src/provider/bridge. The bridge script is
  // not bundled (it must run under bun with the SDK's .ts sources), so
  // resolve it from the repo layout in either case.
  const candidates = [
    // dist -> apps/server/src/provider/bridge/gjc-bridge.ts
    NodePath.join(import.meta.dirname, "..", "src", "provider", "bridge", "gjc-bridge.ts"),
    // source dir is already apps/server/src/provider/bridge
    NodePath.join(import.meta.dirname, "gjc-bridge.ts"),
  ];
  for (const candidate of candidates) {
    if (NodeFS.existsSync(candidate)) return candidate;
  }
  return candidates[0]!;
}

export const makeGjcBridgeRuntime = (): GjcBridgeRuntime => {
  let seq = 0;
  const nextSeq = () => ++seq;

  const childEnv = { ...process.env };
  // The t3code server may itself run inside a GJC agent session (as it does
  // here). Those session-scoped variables make `createAgentSession` restore
  // the *current agent session's* model instead of the user's configured
  // default profile, which is why the bridge resolved the wrong default
  // model. Keep the agent dir (credentials), drop the session identity.
  delete childEnv.GJC_SESSION_ID;
  delete childEnv.GJC_SESSION_CWD;
  delete childEnv.GJCCODE;

  const child = ChildProcess.spawn(resolveBun(), [BRIDGE_ENTRY], {
    stdio: ["pipe", "pipe", "pipe"],
    env: childEnv,
  });

  const eventListeners = new Set<(event: GjcBridgeSessionEvent) => void>();

  // Pending request resolvers keyed by requestId (permission/user-input).
  const pendingRequests = new Map<string, (frame: GjcBridgeServerFrame) => void>();
  // Prompt completion resolvers: one in-flight prompt at a time, keyed by seq.
  const pendingPrompts = new Map<number, () => void>();

  let readyResolve: (() => void) | null = null;
  let readyReject: ((error: GjcBridgeError) => void) | null = null;
  let readyConfigOptions: ReadonlyArray<unknown> | undefined;

  const rejectReady = (error: GjcBridgeError): void => {
    const reject = readyReject;
    readyReject = null;
    reject?.(error);
  };

  const readline = Readline.createInterface({
    input: child.stdout!,
    crlfDelay: Infinity,
  });

  readline.on("line", (line) => {
    if (line.trim().length === 0) return;
    let frame: GjcBridgeServerFrame;
    try {
      frame = JSON.parse(line) as GjcBridgeServerFrame;
    } catch {
      return;
    }
    switch (frame.type) {
      case "ready":
        readyConfigOptions = (frame as GjcBridgeReady).configOptions;
        readyResolve?.();
        readyResolve = null;
        break;
      case "error": {
        const message = frame.message;
        if (frame.message === "pong" || frame.message === "disposed") {
          // ping/dispose ack — not a real error
        } else {
          const error = new GjcBridgeError(message, frame.code);
          rejectReady(error);
        }
        break;
      }
      case "event":
        for (const listener of eventListeners) {
          try {
            listener(frame.event);
          } catch {
            // listener errors are contained
          }
        }
        break;
      case "permission/request": {
        const handler = pendingRequests.get(`permission:${frame.requestId}`);
        if (handler) {
          pendingRequests.delete(`permission:${frame.requestId}`);
          handler(frame);
        }
        break;
      }
      case "user-input/request": {
        const handler = pendingRequests.get(`input:${frame.requestId}`);
        if (handler) {
          pendingRequests.delete(`input:${frame.requestId}`);
          handler(frame);
        }
        break;
      }
      case "turn/terminal": {
        // Resolve the in-flight prompt (if any) first.
        if (pendingPrompts.size > 0) {
          const [seqKey, resolve] = pendingPrompts.entries().next().value as [number, () => void];
          pendingPrompts.delete(seqKey);
          resolve();
        }
        for (const listener of eventListeners) {
          try {
            listener({ kind: "task", state: "completed", detail: frame.stopReason });
          } catch {
            // contained
          }
        }
        break;
      }
      default:
        break;
    }
  });

  child.on("error", (error) => {
    const wrapped = new GjcBridgeError(`bridge process error: ${error.message}`);
    rejectReady(wrapped);
  });

  child.on("exit", (code) => {
    const wrapped = new GjcBridgeError(`bridge process exited (${code ?? "signal"})`);
    rejectReady(wrapped);
  });

  const send = (frame: GjcBridgeClientFrame): void => {
    if (child.stdin?.writable) {
      child.stdin.write(`${JSON.stringify(frame)}\n`);
    }
  };

  const awaitReady = (): Effect.Effect<void, GjcBridgeError> =>
    Effect.tryPromise({
      try: () =>
        new Promise<void>((resolve, reject) => {
          readyResolve = () => resolve();
          readyReject = (error) => reject(error);
        }),
      catch: (error) => {
        if (error instanceof GjcBridgeError) {
          return error;
        }
        const message = error instanceof Error ? error.message : String(error);
        const wrapped = new GjcBridgeError(message);
        // @effect-diagnostics-next-line globalErrorInEffectCatch:off - tagged wrapper
        return wrapped;
      },
    });

  return {
    getReadyConfigOptions: () => readyConfigOptions,
    createSession: (options) =>
      Effect.gen(function* () {
        send({
          seq: nextSeq(),
          type: "session/create",
          cwd: options.cwd,
          ...(options.model ? { model: options.model } : {}),
          ...(options.mcpConfigPath ? { mcpConfigPath: options.mcpConfigPath } : {}),
          ...(options.options ? { options: options.options } : {}),
        });
        yield* awaitReady();
      }),
    setModel: (model) =>
      Effect.sync(() => {
        send({ seq: nextSeq(), type: "session/model-set", model });
      }),
    prompt: (options) =>
      Effect.tryPromise({
        try: () =>
          new Promise<void>((resolve) => {
            const promptSeq = nextSeq();
            pendingPrompts.set(promptSeq, () => resolve());
            send({
              seq: promptSeq,
              type: "session/prompt",
              text: options.text,
              ...(options.attachments && options.attachments.length > 0
                ? { attachments: options.attachments }
                : {}),
            });
          }),
        catch: (error) => new GjcBridgeError(String(error)),
      }),
    steer: (text) =>
      Effect.tryPromise({
        try: () =>
          new Promise<void>((resolve) => {
            const steerSeq = nextSeq();
            pendingPrompts.set(steerSeq, () => resolve());
            send({ seq: steerSeq, type: "session/steer", text });
          }),
        catch: (error) => new GjcBridgeError(String(error)),
      }),
    interrupt: () =>
      Effect.sync(() => {
        send({ seq: nextSeq(), type: "session/interrupt" });
      }),
    dispose: () =>
      Effect.sync(() => {
        send({ seq: nextSeq(), type: "session/dispose" });
      }),
    onEvent: (listener) => {
      eventListeners.add(listener);
      return () => {
        eventListeners.delete(listener);
      };
    },
    kill: () => {
      child.kill();
    },
  };
};

export type GjcBridgeRuntimeService = GjcBridgeRuntime;
