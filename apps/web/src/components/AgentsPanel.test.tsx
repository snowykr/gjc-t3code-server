import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import {
  deriveAgentPanelModel,
  type RuntimeSubagent,
} from "@t3tools/client-runtime/state/subagentRuntime";

import { AgentsPanel } from "./AgentsPanel";

function batchAgent(overrides: Partial<RuntimeSubagent> = {}): RuntimeSubagent {
  return {
    id: "batch-tool-1",
    kind: "subagent",
    title: "reviewer",
    agentName: "reviewer",
    requestedTaskIds: ["requested-a", "requested-b"],
    agentIds: ["allocated-a", "allocated-b"],
    subagentCount: 2,
    role: null,
    model: null,
    effort: null,
    status: "completed",
    activationCount: 1,
    usage: null,
    progress: null,
    lastToolName: null,
    result: null,
    error: null,
    outputFile: null,
    parentAgentId: null,
    agentIndex: null,
    phaseIndex: null,
    phaseTitle: null,
    attempt: null,
    workflowName: null,
    phases: [],
    runHandles: null,
    recentActivity: [],
    firstSeenAt: "2026-08-01T10:00:00.000Z",
    startedAt: "2026-08-01T10:00:00.000Z",
    completedAt: "2026-08-01T10:00:01.000Z",
    updatedAt: "2026-08-01T10:00:01.000Z",
    ...overrides,
  };
}

describe("AgentsPanel GJC batch rows", () => {
  it("renders batch identity, count, and fallback terminal details", () => {
    const model = deriveAgentPanelModel({
      agents: [
        batchAgent({
          status: "failed",
          agentIds: [],
          allocatedIdsUnavailable: true,
          reason: "scheduling_failed",
        }),
      ],
    });

    const markup = renderToStaticMarkup(<AgentsPanel model={model} />);

    expect(markup).toContain("reviewer");
    expect(markup).toContain("2 subagents");
    expect(markup).toContain("requested ids: requested-a, requested-b");
    expect(markup).toContain("allocated ids unavailable");
    expect(markup).toContain("reason: scheduling_failed");
  });
});
