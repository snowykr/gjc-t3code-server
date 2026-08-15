import { describe, expect, it } from "vite-plus/test";

import { type GjcBridgeClientFrame, type GjcBridgeServerFrame } from "./bridgeProtocol.ts";
import {
  findAvailableConcreteModel,
  hasSyntheticProfileNamespaceCollision,
  profileNameForSelection,
  selectionInputError,
} from "./gjcModelSelection.ts";

describe("GjcBridgeProtocol", () => {
  it("preserves configured profile names outside the legacy slug syntax", () => {
    expect(profileNameForSelection({ model: "gajae-code/mixed profile/v2" })).toBe(
      "mixed profile/v2",
    );
    expect(profileNameForSelection({ modelProfile: " custom_profile " })).toBe("custom_profile");
  });

  it("rejects empty and whitespace-only model selectors", () => {
    expect(selectionInputError("")).toBe("Model selection must not be empty.");
    expect(selectionInputError("  ")).toBe("Model selection must not be empty.");
    expect(selectionInputError("gajae-code/")).toBe(
      "Model profile selection must include a profile name.",
    );
    expect(selectionInputError("gajae-code/  ")).toBe(
      "Model profile selection must include a profile name.",
    );
  });

  it("resolves concrete choices to available model objects", () => {
    const available = [
      { provider: "cliproxy", id: "gpt-5.6-luna" },
      { provider: "openai-codex", id: "gpt-5" },
      { provider: "provider/with-slash", id: "model/with-slash" },
    ];

    expect(findAvailableConcreteModel(available, "cliproxy/gpt-5.6-luna")).toEqual(available[0]);
    expect(findAvailableConcreteModel(available, "provider/with-slash/model/with-slash")).toEqual(
      available[2],
    );
    expect(findAvailableConcreteModel(available, "gajae-code/mixed-high")).toBeUndefined();
    expect(findAvailableConcreteModel(available, "cliproxy/missing")).toBeUndefined();
  });

  it("fails closed when a provider claims the synthetic profile namespace", () => {
    expect(hasSyntheticProfileNamespaceCollision([{ provider: "gajae-code" }], [])).toBe(true);
    expect(hasSyntheticProfileNamespaceCollision([], ["gajae-code"])).toBe(true);
    expect(hasSyntheticProfileNamespaceCollision([{ provider: "cliproxy" }], [])).toBe(false);
  });

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
      configOptions: [
        {
          id: "model",
          type: "select",
          options: [
            { value: "gajae-code/mixed-high", name: "mixed-high" },
            { value: "cliproxy/gpt-5.6-luna", name: "GPT-5.6 Luna" },
          ],
        },
      ],
    };
    expect(ready.type).toBe("ready");
    if (ready.type === "ready") {
      expect(ready.configOptions?.[0]?.options.map((option) => option.value)).toEqual([
        "gajae-code/mixed-high",
        "cliproxy/gpt-5.6-luna",
      ]);
    }
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
