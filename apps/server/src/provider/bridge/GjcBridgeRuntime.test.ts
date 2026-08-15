import { describe, expect, it } from "vite-plus/test";

import { type GjcBridgeClientFrame, type GjcBridgeServerFrame } from "./bridgeProtocol.ts";

describe("GjcBridgeProtocol", () => {
  it("types session/create with model profile and thinking options", () => {
    const frame: GjcBridgeClientFrame = {
      seq: 1,
      type: "session/create",
      cwd: "/tmp",
      model: "gajae-code/mixed-high",
      options: { enableLsp: false, skipPythonPreflight: true, thinkingLevel: "high" },
    };
    expect(frame.type).toBe("session/create");
    expect(frame.model).toBe("gajae-code/mixed-high");
    expect(frame.options?.thinkingLevel).toBe("high");
  });

  it("types ready and event server frames", () => {
    const ready: GjcBridgeServerFrame = {
      seq: 2,
      type: "ready",
      sessionId: "s1",
      model: "deepseek/deepseek-v4-flash",
      cwd: "/tmp",
    };
    expect(ready.type).toBe("ready");
    const event: GjcBridgeServerFrame = {
      seq: 3,
      type: "event",
      event: { kind: "text", delta: "hi" },
    };
    expect(event.type).toBe("event");
  });

  it("preserves SDK tool execution identity, arguments, output, and failure", () => {
    const started: GjcBridgeServerFrame = {
      seq: 4,
      type: "event",
      event: {
        kind: "tool",
        toolCallId: "call-find-1",
        name: "find",
        input: { paths: ["src/**/*.ts"] },
        intent: "Finding TypeScript files",
      },
    };
    const updated: GjcBridgeServerFrame = {
      seq: 5,
      type: "event",
      event: {
        kind: "tool_progress",
        toolCallId: "call-find-1",
        name: "find",
        input: { paths: ["src/**/*.ts"] },
        output: { matches: ["src/example.ts"] },
      },
    };
    const completed: GjcBridgeServerFrame = {
      seq: 6,
      type: "event",
      event: {
        kind: "tool_result",
        toolCallId: "call-find-1",
        name: "find",
        output: { matches: ["src/example.ts"] },
        isError: false,
      },
    };

    expect(started.event).toMatchObject({ kind: "tool", toolCallId: "call-find-1" });
    expect(updated.event).toMatchObject({ kind: "tool_progress", name: "find" });
    expect(completed.event).toMatchObject({ kind: "tool_result", isError: false });
  });
});
