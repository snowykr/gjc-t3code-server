/**
 * GJC SDK bridge protocol — newline-delimited JSON frames over stdio.
 *
 * The node server spawns a bun subprocess running `gjc-bridge.ts`. The bridge
 * loads `@gajae-code/coding-agent` in-process (bun-only) and exposes one ACP
 * session per bridge process over this IPC. Frames are single-line JSON
 * (`JSON.stringify` without embedded newlines) on stdout; diagnostics go to
 * stderr only so the protocol stream stays parseable.
 *
 * Frame shapes (all frames carry a monotonic `seq` for ordering assertions):
 *
 *   C->B  {seq, type:"session/create", cwd, model?, mcpConfigPath?, options?}
 *   C->B  {seq, type:"session/model-set", model}
 *   C->B  {seq, type:"session/prompt", text, attachments?: [{path?, text?}]}
 *   C->B  {seq, type:"session/steer", text}
 *   C->B  {seq, type:"session/interrupt"}
 *   C->B  {seq, type:"session/dispose"}
 *   C->B  {seq, type:"ping"}
 *   C->B  {seq, type:"permission/respond", requestId, outcome: {outcome:"allow"|"deny"|"selected", optionId?}}
 *   C->B  {seq, type:"user-input/respond", requestId, value, cancelled?}
 *
 *   B->C  {seq, type:"ready", sessionId, model, cwd}
 *   B->C  {seq, type:"event", event: {...}}            // normalized session event
 *   B->C  {seq, type:"permission/request", requestId, toolCall, options}
 *   B->C  {seq, type:"user-input/request", requestId, prompt}
 *   B->C  {seq, type:"turn/terminal", stopReason}
 *   B->C  {seq, type:"error", message, code?}
 *
 * @module GjcBridgeProtocol
 */

/** Monotonic sequence number shared by both directions. */
export interface BridgeFrameBase {
  readonly seq: number;
}

/* ------------------------------------------------------------------ */
/* Client -> Bridge                                                     */
/* ------------------------------------------------------------------ */

export type GjcBridgeClientFrame =
  | GjcBridgeSessionCreate
  | GjcBridgeSessionModelSet
  | GjcBridgeSessionPrompt
  | GjcBridgeSessionSteer
  | GjcBridgeSessionInterrupt
  | GjcBridgeSessionDispose
  | GjcBridgePing
  | GjcBridgePermissionRespond
  | GjcBridgeUserInputRespond;

export interface GjcBridgeSessionCreate extends BridgeFrameBase {
  readonly type: "session/create";
  readonly cwd: string;
  readonly model?: string;
  readonly mcpConfigPath?: string;
  readonly options?: {
    readonly enableLsp?: boolean;
    readonly skipPythonPreflight?: boolean;
    readonly disableExtensionDiscovery?: boolean;
    readonly agentDir?: string;
    readonly thinkingLevel?: string;
    readonly modelProfile?: string;
  };
}

export interface GjcBridgeSessionModelSet extends BridgeFrameBase {
  readonly type: "session/model-set";
  readonly model: string;
}

export interface GjcBridgeSessionPrompt extends BridgeFrameBase {
  readonly type: "session/prompt";
  readonly text: string;
  readonly attachments?: ReadonlyArray<{ readonly path?: string; readonly text?: string }>;
}

export interface GjcBridgeSessionSteer extends BridgeFrameBase {
  readonly type: "session/steer";
  readonly text: string;
}

export interface GjcBridgeSessionInterrupt extends BridgeFrameBase {
  readonly type: "session/interrupt";
}

export interface GjcBridgeSessionDispose extends BridgeFrameBase {
  readonly type: "session/dispose";
}

export interface GjcBridgePing extends BridgeFrameBase {
  readonly type: "ping";
}

export interface GjcBridgePermissionRespond extends BridgeFrameBase {
  readonly type: "permission/respond";
  readonly requestId: string;
  readonly outcome: { readonly outcome: "allow" | "deny" | "selected"; readonly optionId?: string };
}

export interface GjcBridgeUserInputRespond extends BridgeFrameBase {
  readonly type: "user-input/respond";
  readonly requestId: string;
  readonly value: string;
  readonly cancelled?: boolean;
}

/* ------------------------------------------------------------------ */
/* Bridge -> Client                                                     */
/* ------------------------------------------------------------------ */

export type GjcBridgeServerFrame =
  | GjcBridgeReady
  | GjcBridgeEvent
  | GjcBridgePermissionRequest
  | GjcBridgeUserInputRequest
  | GjcBridgeTurnTerminal
  | GjcBridgeError;

export interface GjcBridgeReady extends BridgeFrameBase {
  readonly type: "ready";
  readonly sessionId: string;
  readonly model: string;
  readonly cwd: string;
  /** SDK ready includes the available model catalog when one is available. */
  readonly configOptions?: ReadonlyArray<{
    readonly id: string;
    readonly name?: string;
    readonly type: "select";
    readonly options: ReadonlyArray<{ readonly value: string; readonly name?: string }>;
  }>;
}

export interface GjcBridgeEvent extends BridgeFrameBase {
  readonly type: "event";
  readonly event: GjcBridgeSessionEvent;
}

/** Normalized session event surfaced to the adapter. */
export type GjcBridgeSessionEvent =
  | { readonly kind: "text"; readonly delta: string }
  | { readonly kind: "thinking"; readonly delta: string }
  | { readonly kind: "reasoning_summary"; readonly delta: string }
  | {
      readonly kind: "tool";
      readonly toolCallId: string;
      readonly name: string;
      readonly input: unknown;
      readonly intent?: string;
    }
  | {
      readonly kind: "tool_progress";
      readonly toolCallId: string;
      readonly name: string;
      readonly input: unknown;
      readonly output: unknown;
    }
  | {
      readonly kind: "tool_result";
      readonly toolCallId: string;
      readonly name: string;
      readonly output: unknown;
      readonly isError: boolean;
    }
  | { readonly kind: "task"; readonly state: string; readonly detail?: string };

export interface GjcBridgePermissionRequest extends BridgeFrameBase {
  readonly type: "permission/request";
  readonly requestId: string;
  readonly toolCall: { readonly name: string; readonly input: unknown };
  readonly options: ReadonlyArray<{ readonly id: string; readonly label: string }>;
}

export interface GjcBridgeUserInputRequest extends BridgeFrameBase {
  readonly type: "user-input/request";
  readonly requestId: string;
  readonly prompt: string;
}

export interface GjcBridgeTurnTerminal extends BridgeFrameBase {
  readonly type: "turn/terminal";
  readonly stopReason: string;
}

export interface GjcBridgeError extends BridgeFrameBase {
  readonly type: "error";
  readonly message: string;
  readonly code?: string;
}
