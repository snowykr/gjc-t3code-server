# GJC Provider Integration: Gap Analysis

Status: SDK-bridge primary (2026-08-15), discovery is SDK 2.5s → ACP 60s fallback; ACP verified 2026-08-13 against `gjc` 0.13.2. `gjc acp` remains the durable fallback, not the primary path.

## Context

The goal is to run [Gajae Code](https://github.com/Yeachan-Heo/gajae-code) (GJC) as a first-class T3 Code
provider, covering session operation, thinking flow, tool calls, subagents, cancel, steer, session
linking, and mobile support. T3 Code ships a generic ACP runtime
(`apps/server/src/provider/acp/`, used by the Cursor and Grok adapters), and the GJC adapter now uses the SDK bridge (`apps/server/src/provider/bridge/GjcBridgeRuntime.ts`, `apps/server/src/provider/bridge/gjc-bridge.ts`) as the primary path with ACP (`apps/server/src/provider/acp/GjcAcpSupport.ts`) as fallback. GJC ships a conformant
ACP v1 endpoint (`gjc acp`) plus an SDK v3 control bus. This document records the four
identified gaps and the current status after the SDK-primary switch.

### Verification evidence

### Verification evidence

The following was verified with a live run, not inferred from documentation:

- `printf '{"jsonrpc":"2.0","id":1,"method":"initialize",...}' | gjc acp` handshake succeeds:
  `protocolVersion: 1`, agent title "Gajae Code", `authMethods: [{ id: "agent" }]` (reuses
  credentials already configured under `~/.gjc`), `agentCapabilities.loadSession: true`, and
  `sessionCapabilities: { list, fork, resume, close, delete }`.
- A scripted end-to-end turn (`initialize → authenticate → session/new → session/prompt →
session/cancel`) completed with `stopReason: "end_turn"`. The session update tally for a trivial
  one-line prompt was:

  ```
  user_message_chunk: 1, session_info_update: 2, available_commands_update: 1,
  agent_thought_chunk: 14, agent_message_chunk: 2
  ```

- `session/new` advertises config options `mode` (category `mode`), `model` (category `model`),
  `thinking` (category `thought_level`), `steeringMode`, `followUpMode`, and `interruptMode`.

Reference harness: `/tmp/gjc-acp-verify.ts` (reusable; not part of the repo).

## Gap 1: Thinking is dropped in the shared ACP runtime

### Current behavior

GJC streams model thinking through standard ACP `agent_thought_chunk` session updates (see
`packages/coding-agent/src/modes/acp/acp-event-mapper.ts` in the GJC repo). The T3 ACP pipeline
discards them entirely:

- `parseSessionUpdateEvent` in `apps/server/src/provider/acp/AcpRuntimeModel.ts` only has a case for
  `"agent_message_chunk"` (which becomes a `ContentDelta` event). `agent_thought_chunk` falls into
  the `default: break` branch and is dropped.
- Even if it were parsed, `makeAcpContentDeltaEvent` in
  `apps/server/src/provider/acp/AcpCoreRuntimeEvents.ts` hardcodes `streamKind: "assistant_text"`.
  The contracts already support `reasoning_text` and `reasoning_summary_text` stream kinds
  (`RuntimeContentStreamKind` in `packages/contracts/src/providerRuntime.ts`), and the Claude,
  Codex, and OpenCode adapters use them (`ClaudeAdapter.ts` `deltaType.includes("thinking")`,
  `CodexAdapter.ts:1263`, `OpenCodeAdapter.ts:403`). The ACP path is the only one without reasoning
  support.
- The replay path compounds the loss: on `session/load`, T3 filters replayed updates via
  `sessionUpdateIsReplay` (`AcpRuntimeModel.ts`), so historical thinking is also invisible.

### Why it matters

In the live verification run, 14 of 16 content chunks were `agent_thought_chunk`. For GJC, thinking
is the dominant share of streamed content; dropping it hides most of the agent's work and makes the
provider look silent or broken during long tool-free reasoning stretches.

### Options

1. **Fix the shared ACP layer (recommended).** Add an `agent_thought_chunk` case in
   `parseSessionUpdateEvent` producing a thought-tagged event, thread a `streamKind` parameter
   through `AcpSessionRuntime` → `makeAcpContentDeltaEvent`, and emit `content.delta` with
   `streamKind: "reasoning_text"`. Design decisions to settle:
   - Whether reasoning deltas ride the assistant item segment (current
     `ensureActiveAssistantSegment` logic) or get their own reasoning item id; Claude/Codex attach
     reasoning to the turn while the UI folds it into reasoning items, so the ingestion layer
     (`ProviderRuntimeIngestion.ts`) already knows how to render `reasoning_text` deltas.
   - Whether the change is unconditional for all ACP providers or gated by a provider capability.
     Cursor and Grok currently surface no thinking; turning it on for them changes existing UX and
     needs probe verification (`CursorAcpCliProbe`, `GrokAcpCliProbe`).
2. **GJC-adapter-only override.** Keep the shared layer as is and map thought chunks in the new
   `GjcAdapter` by inspecting `rawPayload.sessionUpdate === "agent_thought_chunk"` before they are
   dropped. This requires the shared parser to stop dropping them first, so it does not actually
   avoid touching `AcpRuntimeModel.ts`; it only avoids changing behavior for Cursor/Grok.

### Acceptance criteria

- A GJC turn with thinking emits `content.delta` events with `streamKind: "reasoning_text"`, visible
  in the web reasoning UI, without regression in `AcpCoreRuntimeEvents.test.ts`,
  `AcpSessionRuntime` tests, and the Cursor/Grok probe tests.

## Gap 2: There is no steer verb in T3 orchestration

### Current behavior

T3's command surface has exactly two turn verbs: `thread.turn.start`
(`ThreadTurnStartCommand`) and `thread.turn.interrupt` (`ThreadTurnInterruptCommand`), both in
`packages/contracts/src/orchestration.ts`. The decider persists
`thread.turn-start-requested` / `thread.turn-interrupt-requested` intent events, and
`ProviderCommandReactor` routes them to `ProviderAdapterShape.sendTurn` / `interruptTurn`
(`apps/server/src/provider/Services/ProviderAdapter.ts`). There is one active turn per thread; the
client composer locks to stop/interrupt while a turn is running. Sending additional content mid-turn
is not representable today.

### GJC side is ready

- A `session/prompt` sent while the session is busy is acknowledged as a **steer**, distinguished
  from a fresh turn at prompt ingress (`acp-agent.ts` in GJC: session record `busy` flag, "a prompt
  acknowledgement can distinguish a steer from a fresh turn").
- Queue semantics are client-controllable through standard ACP config options surfaced in
  `session/new` (`session/set_config_option`):
  - `steeringMode` — "Steering queue": `all` | `one-at-a-time` (`queue.steering_mode.set`)
  - `followUpMode` — "Follow-up queue": `all` | `one-at-a-time` (`queue.follow_up_mode.set`)
  - `interruptMode` — "Interrupt mode": `immediate` | `wait` (`queue.interrupt_mode.set`)
- The SDK v3 bus has explicit verbs `turn.prompt`, `turn.steer`, `turn.follow_up`, `turn.abort`
  (GJC `docs/sdk.md`), so anything ACP cannot express can fall back to the SDK.

### What is missing on the T3 side

- **Contracts**: a `thread.turn.steer` command (mirroring `ThreadTurnStartCommand` but targeting
  the active turn), a corresponding intent event, and a receipt.
- **Decider/reactor**: accept steer while a turn is active, route to the provider without
  disturbing checkpoint/revert semantics (steer does not start a new turn or a new checkpoint).
- **Adapter surface**: `ProviderAdapterShape.steerTurn` plus a capability flag
  (`ProviderAdapterCapabilities.steer: "supported" | "unsupported"`) so non-steering providers keep
  current behavior. Internally, `AcpSessionRuntime` serializes prompts with
  `promptSerializationSemaphore` and tracks `activePromptFiberRef`; steer needs a defined
  interaction with that (a steer is a second prompt while the first is in flight — GJC accepts it,
  but the T3 runtime must allow it through deliberately).
- **Clients**: web composer UX for sending a message during a running turn, plus mobile parity.
  `thread.turn.interrupt` already has a stop control on both surfaces, so the UI shell exists.
- **Docs**: `docs/user/` note for the new interaction, and `docs/internals/providers.md` adapter
  notes.

### Options

1. **Full steer (recommended, Phase 2).** Implement the contract + reactor + adapter + client work
   above, wired to GJC's `steeringMode` config option. GJC is the only provider that supports it
   today, so the capability flag keeps the blast radius small.
2. **Defer steer, ship follow-ups.** GJC's `followUpMode` lets a client send the next turn while the
   current one settles. This needs no new verb, but it is not the same feature and does not satisfy
   the mid-turn steering requirement.

### Acceptance criteria

- With a GJC turn running, `thread.turn.steer` routes to `session/prompt` on the live session and
  the injected text appears as a steered turn fragment without ending or checkpointing the turn.
- `interruptTurn` behavior is unchanged (`session/cancel` notification; GJC settles the prompt as
  `cancelled` with bounded settlement, per `acp-agent.ts` `#scheduleCancelSettlement`).

## Gap 3: Subagent identity metadata is absent on GJC ACP tool calls

### Current behavior

T3 has a full subagent/task event surface in `packages/contracts/src/providerRuntime.ts`:
`task.started` / `task.progress` / `task.completed` events with `taskId` (`RuntimeTaskId`),
`taskType` ("subagent", "shell", "monitor", "local_workflow", ...), `agentId`, and
`parentToolUseId` linkage. The Claude adapter maps `task_started` system messages to `task.started`
(`ClaudeAdapter.ts:3192`), and the Codex adapter maps agent-thread events similarly
(`CodexAdapter.ts:545`). Client-side, `packages/client-runtime/src/state/subagentRuntime.ts` builds
the Agents-surface roster from these events.

GJC executes subagents through its `task` tool. Over ACP this appears as an ordinary tool call:
`acp-event-mapper.ts` emits `tool_call` / `tool_call_update` with `kind: mapToolKind(toolName)`
(`"task"` falls through to `"other"`), a generated `title`, `rawInput` (the tool args), `content`,
and `locations`. No `_meta` is attached, and subagent lifecycle events
(`subagent_steer_message`) are explicitly dropped (`return []`). T3's ACP pipeline maps `kind:
"other"` to `dynamic_tool_call` (`canonicalItemTypeFromAcpToolKind` in `AcpCoreRuntimeEvents.ts`),
so the task renders as a plain tool item with no agent linkage.

### Impact

Subagents run and their tool calls are visible, but: no Agents-surface entry per subagent, no
folding of a subagent's tool activity under it, no parent linkage for nested agents, and no
background-liveness signal from subagent work.

### Options

1. **GJC emits `_meta` on task tool calls (recommended).** Attach
   `_meta.gjc = { agentId, subagentType, parentToolUseId }` to `tool_call` /
   `tool_call_update` for the task tool (the ACP `ToolCall` schema reserves `_meta` for exactly
   this). GJC already uses `_meta.gjc*` on `session_info_update` for thinking/goal state, so the
   pattern is established. The T3 `GjcAdapter` then maps these to `task.started` /
   `task.completed` with the standard fields. This is a small GJC-side change with full fidelity.
2. **T3-side inference only.** In the adapter, treat `rawInput.toolName === "task"` as a subagent
   and synthesize `taskId` from `toolCallId` with `taskType: "subagent"`. Works without GJC
   changes, but cannot reconstruct `agentId`/parent linkage, so roster folding and nested-agent
   attribution degrade.

### Acceptance criteria

- A GJC task-tool invocation produces `task.started` / `task.completed` events with a stable
  `taskId` and `taskType: "subagent"`, its tool activity carries `taskId`, and the Agents surface
  shows one row per subagent.

## Gap 4: Provider-side rollback is unsupported for ACP providers

### Current behavior

`ProviderAdapterShape.rollbackThread` (roll back N turns, return a `ProviderThreadSnapshot`) is
implemented by the Claude and OpenCode adapters, but both ACP adapters reject it:

- `GrokAdapter.ts:1400` returns `ProviderAdapterRequestError` with "Grok ACP sessions do not support
  provider-side rollback yet."
- `CursorAdapter.ts:1117` behaves the same way.

`ProviderService.rollbackConversation` (`ProviderService.ts:1052`) is called from
`CheckpointReactor` (`CheckpointReactor.ts:779`), i.e. provider-side rollback is part of T3's
checkpoint-revert path. With an unsupported provider, revert relies on the git-based workspace
restore only.

### GJC options

- GJC advertises `sessionCapabilities.fork` in `initialize`. ACP `session/fork` creates a **new**
  session from an existing one rather than rolling one back in place, so mapping
  `rollbackThread` to fork requires session-id remapping in the adapter — possible but more than a
  thin wrap.
- The SDK v3 bus exposes `retry.*` and `transcript.*` operations (GJC `docs/sdk.md`), which could
  drive a rollback-and-replay outside ACP.

### Options

1. **Parity with Grok/Cursor (recommended for Phase 1).** Return `ProviderAdapterRequestError`
   like the other ACP adapters. Checkpoint revert still works at the workspace level; only
   provider-side conversation truncation is unavailable. GJC's own session replay
   (`session/load`) keeps full history visible on reconnect.
2. **Fork-based rollback (optional, later).** Implement `rollbackThread` via `session/fork` +
   `session/close` of the old session, with adapter-internal session-id remapping. Requires care
   around `threadId ↔ sessionId` bookkeeping and checkpoint correlation.

### Acceptance criteria (Phase 1)

- `rollbackThread` fails fast with a typed adapter error; checkpoint revert of the workspace still
  succeeds end to end, and the revert does not corrupt the live GJC session mapping.

## Adjacent observations (non-blocking)

- **Permission mode.** GJC honors `_meta.gjc.permissionHandling` from the client's
  `clientCapabilities._meta` (fallback: `GJC_ACP_PERMISSION_MODE`, default `prompt`). The GJC
  adapter should advertise `prompt` so gated shell/monitor/eval/delete/move operations surface as
  T3 approval requests (`request.opened` / `request.resolved` already exist in the ACP pipeline).
- **User-input forms.** GJC sends `AskUserQuestion` through ACP form elicitation when the client
  advertises the `elicitation` capability. `AcpSessionRuntime` already has registration points for
  `session/elicitation` / `session/elicitation/complete`; the GJC adapter should wire them to
  `user-input.requested` / `user-input.resolved` (`respondToUserInput` exists on the adapter
  shape).
- **Session-link fidelity.** GJC replays text, thought, tool-call, and tool-result history on
  `session/load`, but not historical binary image bytes. Reconnects must source images from T3's
  own transcript store.
- **Model catalog.** GJC filters its advertised model catalog to providers with usable stored
  credentials (`providers.list/active`), falling back to the full catalog; the T3 model picker for
  GJC will therefore only show usable models.
- **Broker lifecycle.** `gjc acp` attaches to a long-lived SDK broker per agent directory, which
  spawns one session host per session. T3 spawns one `gjc acp` process per session, matching this
  model, but a GJC binary upgrade does not affect the broker of an already-running agent
  directory. Restarting the T3 provider session picks up the new build.
- **Subagent steer messages.** GJC currently drops `subagent_steer_message` events in the ACP
  mapping. If steering subagents from the T3 UI ever becomes a requirement, this needs a GJC-side
  decision on how to surface them (e.g. via `session_info_update` `_meta`).

## Suggested sequencing

1. **Phase 1 (core provider).** GjcAcpSupport + GjcAdapter + GjcDriver modeled on Grok/Cursor;
   Gap 1 shared-layer thinking fix; Gap 4 parity (fail fast); permissions and elicitation wiring;
   web + mobile provider registration. Gaps closed: thinking, cancel, session linking, permissions.
2. **Phase 2 (agent fidelity).** Gap 3 `_meta` contract with GJC (and the GJC-side change);
   subagent roster end to end.
3. **Phase 3 (interaction).** Gap 2 steer verb across contracts, server, web, and mobile.
