# GJC SDK Bridge

The t3code server (node) drives the GJC SDK v3 (`@gajae-code/coding-agent`) through a
bun subprocess bridge speaking newline-delimited JSON over stdio.

## Why

The previous path spawned `gjc acp` per session. First-turn cold start measured:
initialize 11.3s + session/new 8.5s + set_model 3.6s = ~23.5s. The in-process SDK
`createAgentSession` takes ~1.1s for the same cwd; with bun spawn + model profile
activation the bridge is ready in ~2.5s (p50), p95 ~3.9s cold (10 trials).

## Files

- `bridgeProtocol.ts` — NDJSON frame types (session/create, session/model-set,
  session/prompt, session/steer, session/interrupt, session/dispose, ping,
  permission/respond, user-input/respond; ready/event/permission-request/
  user-input-request/turn-terminal/error).
- `gjc-bridge.ts` — bun subprocess entry. Loads the SDK in-process, maps IPC to
  the public SDK API, forwards session events, permission requests and user
  input. stderr is diagnostics only; stdout is protocol only.
- `GjcBridgeRuntime.ts` — node-side spawn/supervisor, frame dispatch, crash
  recovery (exit/error handlers), `onEvent` listener registry, ready await.

## Runtime

The bridge must run under bun: the SDK package ships `.ts` sources with
`bun:sqlite`/`bun:ffi` imports that node cannot load
(`ERR_UNSUPPORTED_NODE_MODULES_TYPE_STRIPPING`). Run it with
`bun src/provider/bridge/gjc-bridge.ts`.

The SDK is a dependency of `apps/server` (`@gajae-code/coding-agent@^0.13.1`),
so `bun src/provider/bridge/gjc-bridge.ts` resolves the package from the repo
`node_modules`. A standalone bundled artifact is not produced; the package
layout requires the node_modules install.

## Model resolution

- T3 model ids of the form `gajae-code/<profile>` (e.g. `gajae-code/mixed-high`)
  map to GJC model profiles. On session/create and session/model-set the bridge
  calls `activateModelProfileForControl(<profile>)` — SDK mode does not
  auto-apply the configured default profile, and without this the session
  resolves the parent agent's current model.
- `thinkingLevel` defaults to `"low"` when not supplied: the user's
  `defaultThinkingLevel: max` is rejected by several providers
  (e.g. google-antigravity claude-opus only supports minimal..high).
- The bridge drops `GJC_SESSION_ID`/`GJC_SESSION_CWD`/`GJCCODE` from the child
  env so the SDK does not restore the current GJC agent session's model.

## Reverse control

- Permission requests: the bridge wires `session.setSdkPermissionProvider`
  (public API) and forwards `permission/request` frames to node; the node side
  answers with `permission/respond` (allow/deny/selected).
- User input / ask: the public SDK path is `sdk.host.SessionSdkHost` +
  `sdk.bus.createNotificationsExtension` (`ui.elicit`). The x.ai
  `ask_user_question` ACP extension is ACP-only and has no in-process SDK
  equivalent; it is out of scope for the bridge unless a product/architect
  carve-out approves a shim.

## Verification

- Protocol unit test: `apps/server/src/provider/bridge/GjcBridgeRuntime.test.ts`
- E2E (node): create -> ready ~2.5s, prompt streams text/thinking/tool events,
  model-set switches profile, interrupt returns task:interrupted, dispose
  exits 0, malformed frame returns typed error, SIGKILL/SIGTERM exit observed.
