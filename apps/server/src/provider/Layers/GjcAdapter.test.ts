// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import {
  ApprovalRequestId,
  GjcSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { gjcPromptSettlementBelongsToContext, makeGjcAdapter } from "./GjcAdapter.ts";
const decodeGjcSettings = Schema.decodeSync(GjcSettings);

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");
const mockAgentCommand = process.execPath;

async function makeMockGjcWrapper(extraEnv?: Record<string, string>) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "gjc-acp-mock-"));
  const wrapperPath = NodePath.join(dir, "fake-gjc.sh");
  const envExports = Object.entries(extraEnv ?? {})
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const script = `#!/bin/sh
${envExports}
exec ${JSON.stringify(mockAgentCommand)} ${JSON.stringify(mockAgentPath)} "$@"
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

function waitForFileContent(
  filePath: string,
  attempts = 40,
  expectedContent?: string,
): Effect.Effect<string> {
  const readAttempt = (remainingAttempts: number): Effect.Effect<string> =>
    Effect.gen(function* () {
      if (remainingAttempts <= 0) {
        return yield* Effect.die(new Error(`Timed out waiting for file content at ${filePath}`));
      }
      const raw = yield* Effect.tryPromise(() => NodeFSP.readFile(filePath, "utf8")).pipe(
        Effect.orElseSucceed(() => ""),
      );
      if (
        raw.trim().length > 0 &&
        (expectedContent === undefined || raw.includes(expectedContent))
      ) {
        return raw;
      }
      yield* Effect.sleep("25 millis");
      return yield* readAttempt(remainingAttempts - 1);
    });
  return readAttempt(attempts);
}

async function readJsonLines(filePath: string) {
  const raw = await NodeFSP.readFile(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

const gjcAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-gjc-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeTestAdapter = (binaryPath: string, options?: Parameters<typeof makeGjcAdapter>[1]) =>
  makeGjcAdapter(decodeGjcSettings({ binaryPath }), { useBridge: false, ...options }).pipe(
    Effect.orDie,
  );

function collectGjcTaskLifecycleEvents(mode: string, reason?: string, hierarchy = false) {
  return Effect.gen(function* () {
    const threadId = ThreadId.make(`gjc-task-lifecycle-${mode}-${reason ?? "normal"}`);
    const wrapperPath = yield* Effect.promise(() =>
      makeMockGjcWrapper({
        T3_ACP_GJC_TASK_LIFECYCLE: mode,
        ...(reason ? { T3_ACP_GJC_TASK_REASON: reason } : {}),
        ...(hierarchy ? { T3_ACP_GJC_TASK_HIERARCHY: "1" } : {}),
      }),
    );
    const adapter = yield* makeTestAdapter(wrapperPath);
    const runtimeEvents: ProviderRuntimeEvent[] = [];
    const turnCompleted = yield* Deferred.make<void>();
    const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.sync(() => {
        runtimeEvents.push(event);
      }).pipe(
        Effect.andThen(
          event.type === "turn.completed" && event.threadId === threadId
            ? Deferred.succeed(turnCompleted, undefined)
            : Effect.void,
        ),
      ),
    ).pipe(Effect.forkChild);

    yield* adapter.startSession({
      threadId,
      provider: ProviderDriverKind.make("gjc"),
      cwd: process.cwd(),
      runtimeMode: "full-access",
      modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
    });
    yield* adapter.sendTurn({ threadId, input: "emit GJC task lifecycle", attachments: [] });
    yield* Deferred.await(turnCompleted);
    yield* Fiber.interrupt(runtimeEventsFiber);
    yield* adapter.stopSession(threadId);
    return runtimeEvents;
  });
}

it("requires a settlement to match the live Gjc turn", () => {
  const staleTurnId = TurnId.make("stale-turn");
  const replacementTurnId = TurnId.make("replacement-turn");

  assert.isFalse(
    gjcPromptSettlementBelongsToContext({
      liveAcpSessionId: "session-1",
      expectedAcpSessionId: "session-1",
      liveActiveTurnId: replacementTurnId,
      liveSessionActiveTurnId: replacementTurnId,
      turnId: staleTurnId,
    }),
  );
  assert.isFalse(
    gjcPromptSettlementBelongsToContext({
      liveAcpSessionId: "replacement-session",
      expectedAcpSessionId: "stale-session",
      liveActiveTurnId: staleTurnId,
      liveSessionActiveTurnId: staleTurnId,
      turnId: staleTurnId,
    }),
  );
  assert.isTrue(
    gjcPromptSettlementBelongsToContext({
      liveAcpSessionId: "session-1",
      expectedAcpSessionId: "session-1",
      liveActiveTurnId: staleTurnId,
      liveSessionActiveTurnId: staleTurnId,
      turnId: staleTurnId,
    }),
  );
});

it.layer(gjcAdapterTestLayer)("GjcAdapterLive", (it) => {
  it.effect("starts a session and maps mock ACP prompt flow to runtime events", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-mock-thread");
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed"
              ? Deferred.succeed(turnCompleted, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-mock-alt" },
      });

      assert.equal(session.provider, "gjc");
      assert.equal(session.model, "grok-mock-alt");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-1",
      });

      yield* adapter.sendTurn({
        threadId,
        input: "hello gjc",
        attachments: [],
      });

      yield* Deferred.await(turnCompleted);
      yield* Fiber.interrupt(runtimeEventsFiber);
      const types = runtimeEvents.map((e) => e.type);

      assert.includeMembers(types, [
        "session.started",
        "session.state.changed",
        "thread.started",
        "turn.started",
        "item.started",
        "content.delta",
        "turn.completed",
      ] as const);

      const delta = runtimeEvents.find((e) => e.type === "content.delta");
      assert.isDefined(delta);
      if (delta?.type === "content.delta") {
        assert.equal(delta.payload.delta, "hello from mock");
      }

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("sets steering mode before selecting the requested model", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-steering-mode-order");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "gjc-steering-order-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({ T3_ACP_REQUEST_LOG_PATH: requestLogPath }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("gjc"),
          model: "grok-mock-alt",
        },
      });
      yield* waitForFileContent(requestLogPath);
      const methods = (yield* Effect.promise(() => readJsonLines(requestLogPath))).map((request) =>
        String(request.method),
      );
      const modelIndex = methods.indexOf("session/set_model");
      const steeringIndex = methods.indexOf("session/set_config_option");
      assert.isAtLeast(modelIndex, 0);
      assert.isAtLeast(steeringIndex, 0);
      assert.isBelow(steeringIndex, modelIndex);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("emits one GJC task start and completion across progress updates", () =>
    Effect.gen(function* () {
      const events = yield* collectGjcTaskLifecycleEvents("completed");
      const started = events.filter((event) => event.type === "task.started");
      const completed = events.filter((event) => event.type === "task.completed");

      assert.lengthOf(started, 1);
      assert.lengthOf(completed, 1);
      const startedEvent = started[0];
      const completedEvent = completed[0];
      assert.isDefined(startedEvent);
      assert.isDefined(completedEvent);
      if (startedEvent?.type === "task.started") {
        assert.equal(String(startedEvent.payload.taskId), "gjc-task-batch-1");
        assert.equal(startedEvent.payload.description, "batch-agent");
        assert.equal(startedEvent.payload.agentName, "batch-agent");
        assert.equal(startedEvent.payload.taskType, "subagent");
        assert.deepEqual(startedEvent.payload.requestedTaskIds, ["requested-a", "requested-b"]);
        assert.equal(startedEvent.payload.subagentCount, 2);
      }
      if (completedEvent?.type === "task.completed") {
        assert.equal(completedEvent.payload.status, "completed");
        assert.deepEqual(completedEvent.payload.agentIds, ["allocated-a", "allocated-b"]);
        assert.equal(completedEvent.payload.subagentCount, 2);
        assert.deepEqual(completedEvent.payload.requestedTaskIds, ["requested-a", "requested-b"]);
      }
    }),
  );

  it.effect("propagates GJC task hierarchy on start and completion", () =>
    Effect.gen(function* () {
      const events = yield* collectGjcTaskLifecycleEvents("completed", undefined, true);
      const started = events.find((event) => event.type === "task.started");
      const completed = events.find((event) => event.type === "task.completed");

      assert.isDefined(started);
      assert.isDefined(completed);
      if (started?.type === "task.started") {
        assert.equal(started.payload.agentId, "agent-child-1");
        assert.equal(started.payload.parentToolUseId, "tool-parent-1");
        assert.equal(started.payload.parentAgentId, "agent-parent-1");
        assert.equal(started.payload.agentIndex, 2);
        assert.equal(started.payload.title, "Nested child");
        assert.equal(started.payload.role, "reviewer");
      }
      if (completed?.type === "task.completed") {
        assert.equal(completed.payload.agentId, "agent-child-1");
        assert.equal(completed.payload.parentToolUseId, "tool-parent-1");
        assert.equal(completed.payload.parentAgentId, "agent-parent-1");
        assert.equal(completed.payload.agentIndex, 2);
        assert.equal(completed.payload.title, "Nested child");
        assert.equal(completed.payload.role, "reviewer");
      }
    }),
  );

  it.effect("emits one fallback completion for each GJC scheduling failure reason", () =>
    Effect.gen(function* () {
      for (const reason of [
        "scheduling_failed",
        "invalid_request",
        "no_allocated_agent_ids",
      ] as const) {
        const events = yield* collectGjcTaskLifecycleEvents("fallback", reason);
        const started = events.filter((event) => event.type === "task.started");
        const completed = events.filter((event) => event.type === "task.completed");

        assert.lengthOf(started, 1);
        assert.lengthOf(completed, 1);
        const completedEvent = completed[0];
        assert.isDefined(completedEvent);
        if (completedEvent?.type === "task.completed") {
          assert.equal(
            completedEvent.payload.status,
            reason === "no_allocated_agent_ids" ? "stopped" : "failed",
          );
          assert.deepEqual(completedEvent.payload.requestedTaskIds, ["requested-a", "requested-b"]);
          assert.equal(completedEvent.payload.reason, reason);
          assert.isTrue(completedEvent.payload.allocatedIdsUnavailable);
          assert.equal(completedEvent.payload.subagentCount, 2);
        }
      }
    }),
  );

  it.effect("emits one partial-failure completion when allocated ids and a reason coexist", () =>
    Effect.gen(function* () {
      const events = yield* collectGjcTaskLifecycleEvents("partial", "scheduling_failed");
      const started = events.filter((event) => event.type === "task.started");
      const completed = events.filter((event) => event.type === "task.completed");

      assert.lengthOf(started, 1);
      assert.lengthOf(completed, 1);
      const completedEvent = completed[0];
      assert.isDefined(completedEvent);
      if (completedEvent?.type === "task.completed") {
        assert.equal(completedEvent.payload.status, "failed");
        assert.deepEqual(completedEvent.payload.agentIds, ["allocated-a"]);
        assert.deepEqual(completedEvent.payload.requestedTaskIds, ["requested-a", "requested-b"]);
        assert.equal(completedEvent.payload.reason, "scheduling_failed");
        assert.equal(completedEvent.payload.subagentCount, 2);
      }
    }),
  );

  it.effect("does not emit GJC task events for malformed metadata", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-task-lifecycle-malformed");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({ T3_ACP_GJC_TASK_LIFECYCLE: "malformed" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed" && event.threadId === threadId
              ? Deferred.succeed(turnCompleted, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });
      yield* adapter.sendTurn({ threadId, input: "emit malformed GJC metadata", attachments: [] });
      yield* Deferred.await(turnCompleted);
      yield* Fiber.interrupt(runtimeEventsFiber);

      assert.isFalse(
        runtimeEvents.some(
          (event) => event.type === "task.started" || event.type === "task.completed",
        ),
      );
      assert.isTrue(runtimeEvents.some((event) => event.type === "item.completed"));
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("closes the ACP child process when a session stops", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-stop-session-close");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "gjc-adapter-exit-log-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      yield* adapter.stopSession(threadId);

      const exitLog = yield* waitForFileContent(exitLogPath);
      assert.include(exitLog, "SIGTERM");
    }),
  );

  it.effect("reports a Gjc session running only while the prompt is in flight", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-session-ready-after-prompt");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_EMIT_TOOL_CALLS: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const requestOpened =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "request.opened" }>>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "request.opened"
          ? Deferred.succeed(requestOpened, event).pipe(Effect.ignore)
          : Effect.void,
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "check lifecycle", attachments: [] })
        .pipe(Effect.forkChild);
      const requestOpenedEvent = yield* Deferred.await(requestOpened);

      const runningSessions = yield* adapter.listSessions();
      const runningSession = runningSessions.find((session) => session.threadId === threadId);
      assert.equal(runningSession?.status, "running");
      assert.isDefined(runningSession?.activeTurnId);

      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(String(requestOpenedEvent.requestId)),
        "accept",
      );
      yield* Fiber.join(sendTurnFiber);

      const readySessions = yield* adapter.listSessions();
      const readySession = readySessions.find((session) => session.threadId === threadId);
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("restores ready without completing an unstarted turn when preparation fails", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-preparation-failure-while-connecting");
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const error = yield* Effect.flip(
        adapter.sendTurn({
          threadId,
          input: "prepare invalid attachment",
          attachments: [
            {
              type: "image",
              id: "missing-image",
              name: "missing.png",
              mimeType: "image/png",
              sizeBytes: 1,
            },
          ],
        }),
      );
      for (let yieldAttempt = 0; yieldAttempt < 4; yieldAttempt += 1) {
        yield* Effect.yieldNow;
      }

      const turnCompletedEvent = runtimeEvents.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed",
      );
      const readySessions = yield* adapter.listSessions();
      const readySession = readySessions.find((session) => session.threadId === threadId);

      assert.equal(error._tag, "ProviderAdapterRequestError");
      assert.isUndefined(turnCompletedEvent);
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("completes a Gjc ACP turn from the prompt response", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-xai-prompt-complete-fallback");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_EMIT_FOREIGN_SESSION_UPDATES: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed"
              ? Deferred.succeed(turnCompleted, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const sendTurnResult = yield* adapter.sendTurn({
        threadId,
        input: "exercise fallback",
        attachments: [],
      });

      yield* Deferred.await(turnCompleted);
      for (let yieldAttempt = 0; yieldAttempt < 8; yieldAttempt += 1) {
        yield* Effect.yieldNow;
      }
      const readySessions = yield* adapter.listSessions();
      const readySession = readySessions.find((session) => session.threadId === threadId);
      const turnCompletedEvent = runtimeEvents.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed",
      );
      const eventTypes = runtimeEvents.map((event) => event.type);
      const content = runtimeEvents
        .filter(
          (event): event is Extract<ProviderRuntimeEvent, { type: "content.delta" }> =>
            event.type === "content.delta" && String(event.threadId) === String(threadId),
        )
        .map((event) => event.payload.delta)
        .join("");
      const terminalIndex = runtimeEvents.findIndex(
        (event) => event.type === "turn.completed" && String(event.threadId) === String(threadId),
      );
      const turnOutputTypes = new Set([
        "content.delta",
        "item.started",
        "item.updated",
        "item.completed",
        "turn.plan.updated",
      ]);
      const outputAfterTerminal = runtimeEvents
        .slice(terminalIndex + 1)
        .filter(
          (event) => String(event.threadId) === String(threadId) && turnOutputTypes.has(event.type),
        );
      const toolTitles = runtimeEvents.flatMap((event) =>
        event.type === "item.updated" && event.payload.title ? [event.payload.title] : [],
      );

      assert.equal(sendTurnResult.threadId, threadId);
      assert.include(eventTypes, "turn.completed");
      assert.equal(content, "root before child root after child");
      assert.isAtLeast(terminalIndex, 0);
      assert.deepEqual(outputAfterTerminal, []);
      assert.notInclude(toolTitles, "Child-only tool");
      assert.equal(turnCompletedEvent?.payload.stopReason, "end_turn");
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("retains turn transcript when sendTurn is interrupted after prompt success", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-send-turn-interrupt-after-prompt");
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper({}));
      const adapter = yield* makeTestAdapter(wrapperPath);
      const contentDelta = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "content.delta" ? Deferred.succeed(contentDelta, undefined) : Effect.void,
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "interrupt after prompt",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      yield* Deferred.await(contentDelta);
      for (let yieldAttempt = 0; yieldAttempt < 6; yieldAttempt += 1) {
        yield* Effect.yieldNow;
      }
      yield* Fiber.interrupt(sendTurnFiber);
      for (let yieldAttempt = 0; yieldAttempt < 4; yieldAttempt += 1) {
        yield* Effect.yieldNow;
      }

      const snapshot = yield* adapter.readThread(threadId);
      assert.equal(snapshot.turns.length, 1);
      assert.equal(snapshot.turns[0]?.items.length, 1);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("reports the ACP stop reason from a standard prompt response", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-xai-prompt-complete-missing-stop-reason");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_PROMPT_RESPONSE_STOP_REASON: "end_turn",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed"
              ? Deferred.succeed(turnCompleted, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "exercise missing stop reason",
        attachments: [],
      });

      yield* Deferred.await(turnCompleted);
      const turnCompletedEvent = runtimeEvents.find(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed",
      );

      assert.equal(turnCompletedEvent?.payload.state, "completed");
      assert.equal(turnCompletedEvent?.payload.stopReason, "end_turn");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("lets Stop unblock a fully silent Gjc prompt and accept a follow-up turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-stop-after-full-silence");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      yield* Effect.gen(function* () {
        yield* Effect.sleep("500 millis");
        yield* adapter.interruptTurn(threadId);
      }).pipe(Effect.forkChild({ startImmediately: true }));

      yield* adapter.sendTurn({
        threadId,
        input: "hang forever",
        attachments: [],
      });
      for (let yieldAttempt = 0; yieldAttempt < 8; yieldAttempt += 1) {
        yield* Effect.yieldNow;
      }

      const cancelledEvents = runtimeEvents.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed" && String(event.threadId) === String(threadId),
      );
      const readySessions = yield* adapter.listSessions();
      const readySession = readySessions.find((session) => session.threadId === threadId);

      assert.lengthOf(cancelledEvents, 1);
      assert.equal(cancelledEvents[0]?.payload.state, "cancelled");
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);

      const followUpEventsBefore = runtimeEvents.length;
      yield* adapter.sendTurn({
        threadId,
        input: "continue after stop",
        attachments: [],
      });
      for (let yieldAttempt = 0; yieldAttempt < 8; yieldAttempt += 1) {
        yield* Effect.yieldNow;
      }

      const followUpCompletedEvents = runtimeEvents
        .slice(followUpEventsBefore)
        .filter(
          (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
            event.type === "turn.completed" && String(event.threadId) === String(threadId),
        );
      assert.lengthOf(followUpCompletedEvents, 1);
      assert.equal(followUpCompletedEvents[0]?.payload.state, "completed");

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("does not let a cancelled prompt settlement consume the follow-up prompt slot", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-cancelled-settlement-before-follow-up");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "gjc-acp-cancel-race-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const firstTurnStarted = yield* Deferred.make<TurnId>();
      const twoTurnsCompleted = yield* Deferred.make<void>();
      const completedCountRef = yield* Ref.make(0);
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          runtimeEvents.push(event);
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "turn.started" && event.turnId !== undefined) {
            yield* Deferred.succeed(firstTurnStarted, event.turnId).pipe(Effect.ignore);
            return;
          }
          if (event.type !== "turn.completed") {
            return;
          }
          const completedCount = yield* Ref.updateAndGet(completedCountRef, (count) => count + 1);
          if (completedCount === 2) {
            yield* Deferred.succeed(twoTurnsCompleted, undefined);
          }
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const firstSendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel this prompt", attachments: [] })
        .pipe(Effect.forkChild);
      const firstTurnId = yield* Deferred.await(firstTurnStarted).pipe(Effect.timeout("2 seconds"));
      yield* waitForFileContent(requestLogPath, 80, '"method":"session/prompt"');

      yield* adapter.interruptTurn(threadId, firstTurnId).pipe(Effect.timeout("2 seconds"));
      const followUp = yield* adapter
        .sendTurn({ threadId, input: "complete the follow-up", attachments: [] })
        .pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(firstSendTurnFiber).pipe(Effect.timeout("2 seconds"));
      yield* Deferred.await(twoTurnsCompleted).pipe(Effect.timeout("2 seconds"));

      const turnCompletedEvents = runtimeEvents.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed" && String(event.threadId) === String(threadId),
      );
      const readySessions = yield* adapter.listSessions();
      const readySession = readySessions.find((session) => session.threadId === threadId);

      assert.notEqual(String(followUp.turnId), String(firstTurnId));
      assert.deepEqual(
        turnCompletedEvents.map((event) => [String(event.turnId), event.payload.state]),
        [
          [String(firstTurnId), "cancelled"],
          [String(followUp.turnId), "completed"],
        ],
      );
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("drops late ACP notifications after a turn is cancelled", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-drop-late-cancelled-notifications");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_HANG_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_LATE_UPDATE_AFTER_CANCEL: "1",
        }),
      );
      const lateNativeUpdate = yield* Deferred.make<void>();
      const adapter = yield* makeTestAdapter(wrapperPath, {
        nativeEventLogger: {
          filePath: "memory://gjc-cancelled-native-events",
          write: (record: unknown) =>
            JSON.stringify(record).includes("late after cancel")
              ? Deferred.succeed(lateNativeUpdate, undefined).pipe(Effect.asVoid)
              : Effect.void,
          close: () => Effect.void,
        },
      });

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnStarted = yield* Deferred.make<TurnId>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.started" &&
              event.turnId !== undefined &&
              String(event.threadId) === String(threadId)
              ? Deferred.succeed(turnStarted, event.turnId).pipe(Effect.asVoid)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "cancel before the late update", attachments: [] })
        .pipe(Effect.forkChild);
      const turnId = yield* Deferred.await(turnStarted).pipe(Effect.timeout("2 seconds"));
      yield* adapter.interruptTurn(threadId, turnId).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));
      yield* Deferred.await(lateNativeUpdate).pipe(Effect.timeout("2 seconds"));
      for (let yieldAttempt = 0; yieldAttempt < 8; yieldAttempt += 1) {
        yield* Effect.yieldNow;
      }

      const cancelledIndex = runtimeEvents.findIndex(
        (event) =>
          event.type === "turn.completed" &&
          String(event.threadId) === String(threadId) &&
          String(event.turnId) === String(turnId) &&
          event.payload.state === "cancelled",
      );
      const turnOutputTypes = new Set([
        "content.delta",
        "item.started",
        "item.updated",
        "item.completed",
        "turn.plan.updated",
      ]);
      const outputAfterCancellation = runtimeEvents
        .slice(cancelledIndex + 1)
        .filter(
          (event) => String(event.threadId) === String(threadId) && turnOutputTypes.has(event.type),
        );

      assert.isAtLeast(cancelledIndex, 0);
      assert.deepEqual(outputAfterCancellation, []);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }).pipe(TestClock.withLive),
  );

  it.effect("lets Stop cancel during the xAI completion drain window", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-stop-during-completion-drain");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_EMIT_XAI_PROMPT_COMPLETE_THEN_HANG: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const activeTurnIdRef = yield* Ref.make<TurnId | undefined>(undefined);
      const trailingChunkTurnId = yield* Deferred.make<TurnId>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          runtimeEvents.push(event);
          if (String(event.threadId) !== String(threadId)) {
            return;
          }
          if (event.type === "turn.started") {
            yield* Ref.set(activeTurnIdRef, event.turnId);
          }
          if (event.type !== "content.delta" || event.payload.delta !== "mock") {
            return;
          }
          const turnId = event.turnId ?? (yield* Ref.get(activeTurnIdRef));
          if (turnId === undefined) {
            return;
          }
          yield* Deferred.succeed(trailingChunkTurnId, turnId).pipe(Effect.ignore);
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({
          threadId,
          input: "cancel during completion drain",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      const turnId = yield* Deferred.await(trailingChunkTurnId).pipe(Effect.timeout("2 seconds"));
      yield* adapter.interruptTurn(threadId, turnId).pipe(Effect.timeout("2 seconds"));
      yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));

      const turnCompletedEvents = runtimeEvents.filter(
        (event): event is Extract<ProviderRuntimeEvent, { type: "turn.completed" }> =>
          event.type === "turn.completed" && String(event.threadId) === String(threadId),
      );
      const readySessions = yield* adapter.listSessions();
      const readySession = readySessions.find((session) => session.threadId === threadId);

      assert.lengthOf(turnCompletedEvents, 1);
      assert.equal(turnCompletedEvents[0]?.payload.state, "cancelled");
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("settles the in-flight prompt before emitting completion", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-completion-before-next-turn");
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const completedCountRef = yield* Ref.make(0);
      const secondTurnCompleted = yield* Deferred.make<void>();

      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) => {
        if (event.type !== "turn.completed" || String(event.threadId) !== String(threadId)) {
          return Effect.void;
        }

        return Ref.modify(completedCountRef, (count) => {
          const nextCount = count + 1;
          return [nextCount, nextCount] as const;
        }).pipe(
          Effect.flatMap((count) => {
            if (count === 1) {
              return adapter
                .sendTurn({
                  threadId,
                  input: "second turn after completion",
                  attachments: [],
                })
                .pipe(Effect.forkChild, Effect.asVoid);
            }
            if (count === 2) {
              return Deferred.succeed(secondTurnCompleted, undefined).pipe(Effect.asVoid);
            }
            return Effect.void;
          }),
        );
      }).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "first turn",
        attachments: [],
      });
      yield* Deferred.await(secondTurnCompleted);

      const completedCount = yield* Ref.get(completedCountRef);
      const readySessions = yield* adapter.listSessions();
      const readySession = readySessions.find((session) => session.threadId === threadId);

      assert.equal(completedCount, 2);
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("restores a Gjc session to ready when the prompt RPC fails", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-prompt-failure-ready");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_FAIL_PROMPT: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const error = yield* Effect.flip(
        adapter.sendTurn({
          threadId,
          input: "fail prompt",
          attachments: [],
        }),
      );
      const readySessions = yield* adapter.listSessions();
      const readySession = readySessions.find((session) => session.threadId === threadId);
      const failedTurnCompleted = runtimeEvents.find(
        (event) => event.type === "turn.completed" && event.threadId === threadId,
      );

      assert.equal(error._tag, "ProviderAdapterRequestError");
      assert.equal(readySession?.status, "ready");
      assert.isUndefined(readySession?.activeTurnId);
      assert.equal(failedTurnCompleted?.type, "turn.completed");
      if (failedTurnCompleted?.type === "turn.completed") {
        assert.equal(failedTurnCompleted.payload.state, "failed");
        assert.isString(failedTurnCompleted.payload.errorMessage);
      }

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("ignores replayed session/load updates when resuming a Gjc session", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-load-replay-filter");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_EMIT_LOAD_REPLAY: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }),
      ).pipe(Effect.forkChild);

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
        resumeCursor: { schemaVersion: 1, sessionId: "mock-session-1" },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "after resume",
        attachments: [],
      });

      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-1",
      });
      assert.isFalse(
        runtimeEvents.some(
          (event) => event.type === "item.completed" && event.payload.title === "Replay tool",
        ),
      );
      assert.isFalse(
        runtimeEvents.some(
          (event) =>
            event.type === "content.delta" && event.payload.delta === "replayed assistant text",
        ),
      );

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects startSession when provider mismatches", () =>
    Effect.gen(function* () {
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const threadId = ThreadId.make("gjc-provider-mismatch");

      const error = yield* Effect.flip(
        adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("cursor"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
          modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
        }),
      );

      assert.equal(error._tag, "ProviderAdapterValidationError");
    }),
  );

  it.effect("rejects sendTurn with empty input and no attachments", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-empty-turn");

      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const error = yield* Effect.flip(
        adapter.sendTurn({
          threadId,
          input: "   ",
          attachments: [],
        }),
      );

      assert.equal(error._tag, "ProviderAdapterValidationError");

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects an empty steer message", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-empty-steer");
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const error = yield* Effect.flip(
        adapter.steerTurn!({
          threadId,
          turnId: TurnId.make("turn-missing"),
          message: { text: "   " },
        }),
      );
      assert.equal(error._tag, "ProviderAdapterValidationError");
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects a stale steer turn with a typed request error", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-stale-steer");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({ T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const firstTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "first", attachments: [] })
        .pipe(Effect.forkChild);
      let activeTurnId: TurnId | undefined;
      for (let attempt = 0; attempt < 40 && activeTurnId === undefined; attempt += 1) {
        activeTurnId = (yield* adapter.listSessions()).find(
          (entry) => entry.threadId === threadId,
        )?.activeTurnId;
        if (activeTurnId === undefined) {
          yield* Effect.yieldNow;
        }
      }
      assert.isDefined(activeTurnId);
      const error = yield* Effect.flip(
        adapter.steerTurn!({
          threadId,
          turnId: TurnId.make("stale-turn"),
          message: { text: "continue" },
        }),
      );
      assert.equal(error._tag, "ProviderAdapterRequestError");
      if (error._tag === "ProviderAdapterRequestError") {
        assert.include(error.detail, "stale turn");
      }
      yield* adapter.stopSession(threadId);
      yield* Fiber.interrupt(firstTurnFiber);
    }),
  );

  it.effect("dispatches steer text to ACP", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-wire-steer");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "gjc-steer-wire-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_PROMPT_DELAY_MS: "3000",
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });
      const firstTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "first", attachments: [] })
        .pipe(Effect.forkChild);
      let liveTurn: TurnId | undefined;
      for (let attempt = 0; attempt < 40 && liveTurn === undefined; attempt += 1) {
        const session = (yield* adapter.listSessions()).find(
          (entry) => entry.threadId === threadId,
        );
        liveTurn = session?.activeTurnId;
        if (liveTurn === undefined) {
          yield* Effect.yieldNow;
        }
      }
      assert.isDefined(liveTurn);
      yield* adapter.steerTurn!({
        threadId,
        turnId: liveTurn,
        message: {
          text: "continue with text",
          attachments: [],
        },
      }).pipe(Effect.timeout("20 seconds"));
      yield* waitForFileContent(requestLogPath);
      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const promptRequests = requests.filter((request) => request.method === "session/prompt");
      const steerRequest = promptRequests.find((request) => {
        const prompt = (request.params as { prompt?: ReadonlyArray<Record<string, unknown>> })
          ?.prompt;
        return (
          prompt?.some((block) => block.type === "text" && block.text === "continue with text") ??
          false
        );
      });
      assert.isDefined(steerRequest);
      const prompt = steerRequest?.params as { prompt?: ReadonlyArray<Record<string, unknown>> };
      assert.deepEqual(prompt.prompt?.[0], { type: "text", text: "continue with text" });
      assert.lengthOf(prompt.prompt ?? [], 1);
      // Settlement accounting (B2): the steer is a second in-flight prompt on
      // the same turn. Both prompts complete (the mock delays the first by
      // 3s); the turn must settle exactly once with no double-settle crash,
      // and the host sendTurn must join cleanly.
      yield* Fiber.join(firstTurnFiber).pipe(Effect.timeout("10 seconds"));
      const settled = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);
      assert.equal(settled?.status, "ready");
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects steer attachments because GJC ACP steer is text-only", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-steer-attachments");
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });

      const error = yield* Effect.flip(
        adapter.steerTurn!({
          threadId,
          turnId: TurnId.make("turn-with-attachment"),
          message: {
            text: "continue",
            attachments: [
              {
                id: "unused-image",
                type: "image",
                name: "image.png",
                mimeType: "image/png",
                sizeBytes: 1,
              },
            ],
          },
        }),
      );
      assert.equal(error._tag, "ProviderAdapterValidationError");
      if (error._tag === "ProviderAdapterValidationError") {
        assert.equal(error.issue, "steer supports text only");
      }
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("returns a typed request error for unsupported rollback", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-rollback");
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("gjc"), model: "grok-build" },
      });
      const error = yield* Effect.flip(adapter.rollbackThread!(threadId, 1));
      assert.equal(error._tag, "ProviderAdapterRequestError");
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("responds to ACP approvals using provider-supplied option ids", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-custom-approval-option-id");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "gjc-acp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_EMIT_TOOL_CALLS: "1",
          T3_ACP_ALLOW_ONCE_OPTION_ID: "agent-defined-approval-id",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "request.opened"
          ? adapter.respondToRequest(
              threadId,
              ApprovalRequestId.make(String(event.requestId)),
              "accept",
            )
          : Effect.void,
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      yield* adapter.sendTurn({ threadId, input: "approve this", attachments: [] });

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isTrue(
        requests.some(
          (entry) =>
            !("method" in entry) &&
            typeof entry.result === "object" &&
            entry.result !== null &&
            "outcome" in entry.result &&
            typeof entry.result.outcome === "object" &&
            entry.result.outcome !== null &&
            "optionId" in entry.result.outcome &&
            entry.result.outcome.optionId === "agent-defined-approval-id",
        ),
      );

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("handles xAI ask_user_question extension requests", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-xai-ask-user-question");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockGjcWrapper({ T3_ACP_EMIT_XAI_ASK_USER_QUESTION: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const requested =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "user-input.requested" }>>();
      const resolved =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "user-input.resolved" }>>();

      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) => {
        if (String(event.threadId) !== String(threadId)) {
          return Effect.void;
        }
        if (event.type === "user-input.requested") {
          return Deferred.succeed(requested, event).pipe(Effect.ignore);
        }
        if (event.type === "user-input.resolved") {
          return Deferred.succeed(resolved, event).pipe(Effect.ignore);
        }
        return Effect.void;
      }).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "ask before continuing", attachments: [] })
        .pipe(Effect.forkChild);

      const requestedEvent = yield* Deferred.await(requested);
      assert.equal(requestedEvent.payload.questions.length, 1);
      assert.equal(requestedEvent.payload.questions[0]?.id, "Which scope should Grok use?");
      assert.equal(requestedEvent.payload.questions[0]?.question, "Which scope should Grok use?");
      assert.equal(requestedEvent.raw?.method, "_x.ai/ask_user_question");

      yield* adapter.respondToUserInput(
        threadId,
        ApprovalRequestId.make(String(requestedEvent.requestId)),
        {
          "Which scope should Grok use?": "Workspace",
        },
      );

      const resolvedEvent = yield* Deferred.await(resolved);
      assert.deepEqual(resolvedEvent.payload.answers, {
        "Which scope should Grok use?": "Workspace",
      });
      assert.equal(String(resolvedEvent.turnId), String(requestedEvent.turnId));
      yield* Fiber.join(sendTurnFiber);

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("continues streaming events when native notification logging fails", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("gjc-native-log-failure");
      const wrapperPath = yield* Effect.promise(() => makeMockGjcWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath, {
        nativeEventLogger: {
          filePath: "memory://gjc-native-events",
          write: (record: unknown) =>
            typeof record === "object" &&
            record !== null &&
            "event" in record &&
            typeof record.event === "object" &&
            record.event !== null &&
            "kind" in record.event &&
            record.event.kind === "notification"
              ? Effect.die(new Error("native log write failed"))
              : Effect.void,
          close: () => Effect.void,
        },
      });
      const contentDelta = yield* Deferred.make<void>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "content.delta" ? Deferred.succeed(contentDelta, undefined) : Effect.void,
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("gjc"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "keep streaming", attachments: [] });
      yield* Deferred.await(contentDelta);

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );
});
