import { TurnId, type OrchestrationThread } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { EMPTY_ENVIRONMENT_THREAD_STATE, threadCanSteer } from "./threadState.ts";

const TURN_ID = TurnId.make("turn-1");

function threadLike(
  overrides: Partial<Pick<OrchestrationThread, "session" | "latestTurn">> = {},
): Pick<OrchestrationThread, "session" | "latestTurn"> {
  return {
    latestTurn: {
      turnId: TURN_ID,
      state: "running",
      requestedAt: "2026-06-06T00:00:00.000Z",
      startedAt: "2026-06-06T00:00:00.000Z",
      completedAt: null,
      assistantMessageId: null,
    },
    session: {
      threadId: "thread-1",
      status: "running",
      providerName: "gjc",
      runtimeMode: "full-access",
      activeTurnId: TURN_ID,
      adapterCapabilities: { steer: "supported" },
      lastError: null,
      updatedAt: "2026-06-06T00:00:00.000Z",
    },
    ...overrides,
  } as Pick<OrchestrationThread, "session" | "latestTurn">;
}

describe("threadCanSteer", () => {
  it("requires a supported running active turn", () => {
    expect(threadCanSteer(threadLike())).toBe(true);
    expect(threadCanSteer(threadLike({ latestTurn: null }))).toBe(false);
    expect(
      threadCanSteer(
        threadLike({
          session: {
            ...threadLike().session!,
            adapterCapabilities: { steer: "unsupported" },
          },
        }),
      ),
    ).toBe(false);
    expect(
      threadCanSteer(
        threadLike({ latestTurn: { ...threadLike().latestTurn!, state: "completed" } }),
      ),
    ).toBe(false);
    expect(threadCanSteer(EMPTY_ENVIRONMENT_THREAD_STATE)).toBe(false);
  });

  it("rejects a capability advertised for a different active turn", () => {
    expect(
      threadCanSteer(
        threadLike({
          session: { ...threadLike().session!, activeTurnId: TurnId.make("turn-2") },
        }),
      ),
    ).toBe(false);
  });
});
