import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ChatAttachment,
  type OrchestrationCommand,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as NodeServices from "@effect/platform-node/NodeServices";

import { decideOrchestrationCommand } from "./decider.ts";

const NOW = "2026-01-01T00:00:00.000Z";
const THREAD_ID = ThreadId.make("thread-steer");
const ACTIVE_TURN_ID = TurnId.make("turn-active");

function makeReadModel(
  input: {
    readonly status?: "running" | "ready";
    readonly activeTurnId?: TurnId | null;
    readonly latestTurn?: "matching" | "missing" | "not-running" | "mismatched";
    readonly steer?: "supported" | "unsupported";
    readonly settledOverride?: "settled" | "active" | null;
    readonly snoozed?: boolean;
  } = {},
): OrchestrationReadModel {
  return {
    snapshotSequence: 0,
    projects: [],
    threads: [
      {
        id: THREAD_ID,
        projectId: ProjectId.make("project-1"),
        title: "Thread",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        latestTurn:
          input.latestTurn === "missing"
            ? null
            : {
                turnId:
                  input.latestTurn === "mismatched" ? TurnId.make("turn-other") : ACTIVE_TURN_ID,
                state: input.latestTurn === "not-running" ? "completed" : "running",
                requestedAt: NOW,
                startedAt: NOW,
                completedAt: null,
                assistantMessageId: null,
              },
        createdAt: NOW,
        updatedAt: NOW,
        archivedAt: null,
        settledOverride: input.settledOverride ?? null,
        settledAt: input.settledOverride === "settled" ? NOW : null,
        snoozedUntil: input.snoozed ? NOW : null,
        snoozedAt: input.snoozed ? NOW : null,
        pinnedAt: null,
        pinOrderKey: null,
        deletedAt: null,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: {
          threadId: THREAD_ID,
          status: input.status ?? "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          runtimeMode: "full-access",
          activeTurnId: input.activeTurnId === undefined ? ACTIVE_TURN_ID : input.activeTurnId,
          ...(input.steer !== undefined ? { adapterCapabilities: { steer: input.steer } } : {}),
          lastError: null,
          updatedAt: NOW,
        },
      },
    ],
    updatedAt: NOW,
  };
}

function makeCommand(
  attachments: ReadonlyArray<ChatAttachment> = [],
): Extract<OrchestrationCommand, { type: "thread.turn.steer" }> {
  return {
    type: "thread.turn.steer" as const,
    commandId: CommandId.make("cmd-steer"),
    threadId: THREAD_ID,
    message: {
      messageId: MessageId.make("message-steer"),
      role: "user" as const,
      text: "Use the smaller implementation.",
      attachments,
    },
    createdAt: NOW,
  };
}

it.layer(NodeServices.layer)("steer decider", (it) => {
  it.effect("accepts only a supported active running turn atomically", () =>
    Effect.gen(function* () {
      const result = yield* decideOrchestrationCommand({
        command: makeCommand(),
        readModel: makeReadModel({ steer: "supported" }),
      });
      const events = Array.isArray(result) ? result : [result];
      expect(events.map((event) => event.type)).toEqual([
        "thread.message-sent",
        "thread.turn-steer-requested",
      ]);
      const message = events[0];
      const request = events[1];
      expect(message?.type === "thread.message-sent" ? message.payload.turnId : undefined).toBe(
        ACTIVE_TURN_ID,
      );
      expect(
        request?.type === "thread.turn-steer-requested" ? request.payload.turnId : undefined,
      ).toBe(ACTIVE_TURN_ID);
      expect(events.some((event) => event.type === "thread.unsettled")).toBe(false);
      expect(events.some((event) => event.type === "thread.unsnoozed")).toBe(false);
      expect(events.some((event) => event.type === "thread.turn-diff-completed")).toBe(false);
    }),
  );

  it.effect("rejects a command targeting a stale turn", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: { ...makeCommand(), turnId: TurnId.make("stale-turn") },
          readModel: makeReadModel({ steer: "supported" }),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("rejects when latest turn is missing", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: makeCommand(),
          readModel: makeReadModel({ latestTurn: "missing", steer: "supported" }),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("rejects when latest turn is not running", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: makeCommand(),
          readModel: makeReadModel({ latestTurn: "not-running", steer: "supported" }),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("rejects when latest turn does not match the active turn", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: makeCommand(),
          readModel: makeReadModel({ latestTurn: "mismatched", steer: "supported" }),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("rejects when no active turn is present", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: makeCommand(),
          readModel: makeReadModel({ activeTurnId: null, steer: "supported" }),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("rejects legacy sessions with an absent capability stamp", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: makeCommand(),
          readModel: makeReadModel(),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("rejects steer on a ready thread without emitting events", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: makeCommand(),
          readModel: makeReadModel({ status: "ready", steer: "supported" }),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("rejects steer on a settled thread without emitting events", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: makeCommand(),
          readModel: makeReadModel({ steer: "supported", settledOverride: "settled" }),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("rejects steer on a snoozed thread without emitting events", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        decideOrchestrationCommand({
          command: makeCommand(),
          readModel: makeReadModel({ steer: "supported", snoozed: true }),
        }),
      );
      expect(exit._tag).toBe("Failure");
    }),
  );

  it.effect("preserves steer attachments on the provider request", () =>
    Effect.gen(function* () {
      const command = makeCommand([
        {
          id: "attachment-1",
          type: "image",
          name: "shot.png",
          mimeType: "image/png",
          sizeBytes: 3,
        },
      ]);
      const result = yield* decideOrchestrationCommand({
        command,
        readModel: makeReadModel({ steer: "supported" }),
      });
      const events = Array.isArray(result) ? result : [result];
      const request = events.find((event) => event.type === "thread.turn-steer-requested");
      expect(
        request?.type === "thread.turn-steer-requested" ? request.payload.attachments : null,
      ).toEqual(command.message.attachments);
    }),
  );
});
