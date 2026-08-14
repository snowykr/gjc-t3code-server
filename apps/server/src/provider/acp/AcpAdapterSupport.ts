import {
  type ProviderApprovalDecision,
  type ProviderDriverKind,
  type ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as EffectAcpErrors from "effect-acp/errors";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  type ProviderAdapterError,
} from "../Errors.ts";
const isAcpProcessExitedError = Schema.is(EffectAcpErrors.AcpProcessExitedError);
const isAcpRequestError = Schema.is(EffectAcpErrors.AcpRequestError);

export const GJC_AUTHENTICATION_FAILURE_MESSAGE =
  "GJC is not authenticated. Run 'gjc setup' or check ~/.gjc credentials.";

const ACP_AUTHENTICATION_MESSAGE_PATTERN =
  /\bauth\b|authenticat(?:e|ed|ion|ing)|credential|unauthoriz|invalid token|login required|not logged in/i;

/**
 * ACP reserves -32000 for authentication-required responses, but agents also
 * (incorrectly) reuse that code for generic startup/protocol failures. Treat
 * the code as an auth signal only when the request itself is the authenticate
 * phase; otherwise require an explicit authentication signal in the message.
 */
export const isAcpAuthenticationFailure = (
  error: EffectAcpErrors.AcpError,
  method?: string,
): boolean => {
  if (!isAcpRequestError(error)) {
    return false;
  }
  const requestMethod = error.method ?? method;
  const isAuthenticateRequest = requestMethod === "authenticate";
  return (
    (error.code === -32000 && isAuthenticateRequest) ||
    ACP_AUTHENTICATION_MESSAGE_PATTERN.test(error.message)
  );
};

const safeAcpFailureDetail = (
  provider: ProviderDriverKind,
  method: string,
  error: EffectAcpErrors.AcpError,
) =>
  String(provider) === "gjc" && isAcpAuthenticationFailure(error, method)
    ? GJC_AUTHENTICATION_FAILURE_MESSAGE
    : error.message;

export function mapAcpToAdapterError(
  provider: ProviderDriverKind,
  threadId: ThreadId,
  method: string,
  error: EffectAcpErrors.AcpError,
): ProviderAdapterError {
  if (isAcpProcessExitedError(error)) {
    return new ProviderAdapterSessionClosedError({
      provider,
      threadId,
      cause: error,
    });
  }
  if (isAcpRequestError(error)) {
    return new ProviderAdapterRequestError({
      provider,
      method,
      detail: safeAcpFailureDetail(provider, method, error),
      cause: error,
    });
  }
  return new ProviderAdapterRequestError({
    provider,
    method,
    detail: safeAcpFailureDetail(provider, method, error),
    cause: error,
  });
}

export function acpPermissionOutcome(decision: ProviderApprovalDecision): string {
  switch (decision) {
    case "acceptForSession":
      return "allow-always";
    case "accept":
      return "allow-once";
    case "decline":
    default:
      return "reject-once";
  }
}
