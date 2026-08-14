// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { describe, expect } from "vite-plus/test";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");

const makeRuntimeLayer = (
  env: Record<string, string>,
  requestLogger?: AcpSessionRuntime.AcpSessionRuntimeOptions["requestLogger"],
) =>
  AcpSessionRuntime.layer({
    spawn: {
      command: process.execPath,
      args: [mockAgentPath],
      env,
    },
    cwd: process.cwd(),
    clientInfo: { name: "t3-test", version: "0.0.0" },
    authMethodId: "test",
    ...(requestLogger ? { requestLogger } : {}),
  });

const promptPayload = (text: string) => ({
  prompt: [{ type: "text" as const, text }],
});

describe("AcpSessionRuntime steer fence", () => {
  it.effect("G3 drops a steer after the host cancel fence is set", () => {
    let runtime: AcpSessionRuntime.AcpSessionRuntime["Service"] | undefined;
    let promptCount = 0;
    const requestLogger: AcpSessionRuntime.AcpSessionRuntimeOptions["requestLogger"] = (event) =>
      Effect.gen(function* () {
        if (event.method === "session/prompt" && event.status === "started") {
          promptCount += 1;
          if (promptCount === 2 && runtime) {
            yield* runtime.cancel.pipe(Effect.forkChild({ startImmediately: true }));
            yield* Effect.yieldNow;
            yield* Effect.yieldNow;
          }
        }
      });

    return Effect.gen(function* () {
      runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();
      const hostFiber = yield* runtime
        .prompt(promptPayload("hang forever"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      for (let attempt = 0; attempt < 8; attempt += 1) {
        yield* Effect.yieldNow;
      }
      const steerResult = yield* runtime.steer(promptPayload("must not dispatch"));
      expect(steerResult.stopReason).toBe("cancelled");
      expect((yield* Fiber.join(hostFiber)).stopReason).toBe("cancelled");
    }).pipe(
      Effect.provide(makeRuntimeLayer({ T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1" }, requestLogger)),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("G3b settles a steer that was dispatched before cancel", () => {
    let runtime: AcpSessionRuntime.AcpSessionRuntime["Service"] | undefined;
    let promptCount = 0;
    const requestLogger: AcpSessionRuntime.AcpSessionRuntimeOptions["requestLogger"] = (event) =>
      Effect.gen(function* () {
        if (event.method === "session/prompt" && event.status === "started") {
          promptCount += 1;
        }
      });

    return Effect.gen(function* () {
      runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();
      const hostFiber = yield* runtime
        .prompt(promptPayload("hang forever"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      for (let attempt = 0; attempt < 8; attempt += 1) {
        yield* Effect.yieldNow;
      }
      const steerFiber = yield* runtime
        .steer(promptPayload("dispatch before cancel"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      for (let attempt = 0; attempt < 8; attempt += 1) {
        yield* Effect.yieldNow;
      }
      yield* runtime.cancel;
      const steerResult = yield* Fiber.join(steerFiber).pipe(Effect.timeout("3 seconds"));
      expect(["cancelled", "end_turn"]).toContain(steerResult.stopReason);
      expect((yield* Fiber.join(hostFiber)).stopReason).toBe("cancelled");
      // Exactly two wire dispatches: the host prompt and the steer. The steer
      // was admitted and dispatched BEFORE the cancel fence was set, so it
      // must not be dropped (G3 covers the drop side).
      expect(promptCount).toBe(2);
    }).pipe(
      Effect.provide(makeRuntimeLayer({ T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1" }, requestLogger)),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    );
  });

  it.effect("G4 clears the fence for an ordinary next-turn prompt", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();
      const hostFiber = yield* runtime
        .prompt(promptPayload("hang forever"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      yield* runtime.cancel;
      expect((yield* Fiber.join(hostFiber)).stopReason).toBe("cancelled");
      expect((yield* runtime.prompt(promptPayload("next turn"))).stopReason).toBe("end_turn");
      expect((yield* runtime.steer(promptPayload("fresh after clear"))).stopReason).toBe(
        "end_turn",
      );
    }).pipe(
      Effect.provide(makeRuntimeLayer({ T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1" })),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("H4/G5 inerts a cancel fence during ticket registration", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();
      const promptFiber = yield* runtime
        .prompt(promptPayload("register me"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* runtime.cancel;
      yield* Fiber.join(promptFiber);
      expect((yield* runtime.prompt(promptPayload("next turn"))).stopReason).toBe("end_turn");
      expect((yield* runtime.steer(promptPayload("after registration"))).stopReason).toBe(
        "end_turn",
      );
    }).pipe(
      Effect.provide(makeRuntimeLayer({ T3_ACP_PROMPT_DELAY_MS: "20" })),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("G6 keeps overlapping admission tickets independent", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();
      const first = yield* runtime.prompt(promptPayload("A")).pipe(Effect.forkChild());
      const second = yield* runtime.prompt(promptPayload("B")).pipe(Effect.forkChild());
      expect((yield* Fiber.join(first)).stopReason).toBe("end_turn");
      expect((yield* Fiber.join(second)).stopReason).toBe("end_turn");
    }).pipe(
      Effect.provide(makeRuntimeLayer({ T3_ACP_PROMPT_DELAY_MS: "20" })),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );

  it.effect("G7 stamps cancel against the live host while admissions overlap", () =>
    Effect.gen(function* () {
      const runtime = yield* AcpSessionRuntime.AcpSessionRuntime;
      yield* runtime.start();
      const host = yield* runtime
        .prompt(promptPayload("hang forever"))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      const cancelFiber = yield* runtime.cancel.pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      const steerResult = yield* runtime.steer(promptPayload("fenced steer"));
      expect(["cancelled", "end_turn"]).toContain(steerResult.stopReason);
      yield* Fiber.join(cancelFiber).pipe(Effect.timeout("3 seconds"));
      expect((yield* Fiber.join(host)).stopReason).toBe("cancelled");
    }).pipe(
      Effect.provide(makeRuntimeLayer({ T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1" })),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
  );
});
