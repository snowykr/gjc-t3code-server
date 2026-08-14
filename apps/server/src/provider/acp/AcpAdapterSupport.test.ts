import { describe, expect, it } from "vite-plus/test";
import * as EffectAcpErrors from "effect-acp/errors";
import { ProviderDriverKind } from "@t3tools/contracts";

import { acpPermissionOutcome, mapAcpToAdapterError } from "./AcpAdapterSupport.ts";

describe("AcpAdapterSupport", () => {
  it("maps ACP approval decisions to permission outcomes", () => {
    expect(acpPermissionOutcome("accept")).toBe("allow-once");
    expect(acpPermissionOutcome("acceptForSession")).toBe("allow-always");
    expect(acpPermissionOutcome("decline")).toBe("reject-once");
  });

  it("maps an ACP authentication failure to the GJC credential diagnostic", () => {
    const error = mapAcpToAdapterError(
      ProviderDriverKind.make("gjc"),
      "thread-auth" as never,
      "authenticate",
      new EffectAcpErrors.AcpRequestError({
        code: -32000,
        errorMessage: "Authentication required",
      }),
    );

    expect(error._tag).toBe("ProviderAdapterRequestError");
    expect(error.message).toContain(
      "GJC is not authenticated. Run 'gjc setup' or check ~/.gjc credentials.",
    );
  });

  it("preserves generic -32000 ACP failure details", () => {
    const error = mapAcpToAdapterError(
      ProviderDriverKind.make("gjc"),
      "thread-generic" as never,
      "session/new",
      new EffectAcpErrors.AcpRequestError({
        code: -32000,
        errorMessage: "Internal protocol failure",
      }),
    );

    expect(error._tag).toBe("ProviderAdapterRequestError");
    expect(error.message).toContain("Internal protocol failure");
    expect(error.message).not.toContain("GJC is not authenticated");
  });

  it("preserves non-GJC error details that contain authentication words", () => {
    const error = mapAcpToAdapterError(
      ProviderDriverKind.make("cursor"),
      "thread-auth-word" as never,
      "session/prompt",
      new EffectAcpErrors.AcpRequestError({
        code: -32603,
        errorMessage: "Authentication state could not be parsed.",
      }),
    );

    expect(error._tag).toBe("ProviderAdapterRequestError");
    expect(error.message).toContain("Authentication state could not be parsed.");
  });

  it("maps ACP request errors to provider adapter request errors", () => {
    const error = mapAcpToAdapterError(
      ProviderDriverKind.make("cursor"),
      "thread-1" as never,
      "session/prompt",
      new EffectAcpErrors.AcpRequestError({
        code: -32602,
        errorMessage: "Invalid params",
      }),
    );

    expect(error._tag).toBe("ProviderAdapterRequestError");
    expect(error.message).toContain("Invalid params");
  });
});
