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
});
