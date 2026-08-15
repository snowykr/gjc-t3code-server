# GJC (Gajae Code)

GJC (Gajae Code) runs through its SDK bridge, with ACP available as a fallback. T3 Code supports
GJC sessions, streaming text and reasoning, tool calls, approvals, cancellation, and session
resume.

## First-time setup

1. Install GJC and make sure its providers are authenticated:

   ```sh
   npm i -g gajae-code
   gjc setup
   ```

   GJC keeps credentials in `~/.gjc`; the provider reuses whatever is already configured
   there. There is no separate T3 Code login step.

2. In T3 Code, open Settings → Providers and add a **GJC** instance. The default binary
   path is `gjc`; set a custom path if it is not on `PATH` for the T3 Code server.

## Notes

- **Models and profiles** share the model picker. It shows the concrete models currently usable
  with your configured GJC providers and the profiles whose required providers are available.
  Profiles appear as their profile names, such as `codex-eco` or a custom profile from
  `~/.gjc/agent/models.yml` such as `mixed-high`; selecting one applies its GJC role mapping.
- **Thinking** streams as reasoning while GJC works, and is kept in your thread transcript.
- **Subagents** launched by GJC appear in the Agents surface as one row per batch, with the
  requested and allocated agent ids and counts.
- **Approvals** for gated tools (shell, delete, move, and friends) surface as T3 Code
  approval requests. Set `GJC_ACP_PERMISSION_MODE=always-allow` in the provider's environment
  variables only if you want GJC to skip prompting.
- **Cancelling** a turn sends ACP `session/cancel`; GJC settles the turn as cancelled.
- **Rollback** (checkpoint revert) restores the workspace only; provider-side conversation
  truncation is not supported for ACP providers.

## Steering

While a GJC turn is running, keep typing in the composer and send a message to steer
the active turn. The message appears immediately in the thread; Stop remains available to
interrupt the turn.

## Requirements

- `gjc` on the machine running the T3 Code server (local or remote host).
- Node.js is not required by the provider itself beyond what GJC needs.
