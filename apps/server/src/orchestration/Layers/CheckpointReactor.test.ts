// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";

import {
  ProviderDriverKind,
  ProviderRuntimeEvent,
  ProviderSession,
  ProviderInstanceId,
} from "@t3tools/contracts";
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { CheckpointReactorLive } from "./CheckpointReactor.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { RuntimeReceiptBusTest } from "./RuntimeReceiptBus.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { ProjectionTurnRepositoryLive } from "../../persistence/Layers/ProjectionTurns.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ProjectionTurnRepository } from "../../persistence/Services/ProjectionTurns.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
import { CheckpointReactor } from "../Services/CheckpointReactor.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  RuntimeReceiptBus,
  type OrchestrationRuntimeReceipt,
} from "../Services/RuntimeReceiptBus.ts";
import {
  ProviderService,
  type ProviderServiceShape,
} from "../../provider/Services/ProviderService.ts";
import { checkpointRefForThreadTurn } from "../../checkpointing/Utils.ts";
import { ServerConfig } from "../../config.ts";
import * as WorkspaceEntries from "../../workspace/WorkspaceEntries.ts";
import * as WorkspacePaths from "../../workspace/WorkspacePaths.ts";

const asProjectId = (value: string): ProjectId => ProjectId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);

type LegacyProviderRuntimeEvent = {
  readonly type: string;
  readonly eventId: EventId;
  readonly provider: ProviderDriverKind;
  readonly createdAt: string;
  readonly threadId: ThreadId;
  readonly turnId?: string | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly payload?: unknown | undefined;
  readonly [key: string]: unknown;
};

function createProviderServiceHarness(
  cwd: string,
  hasSession = true,
  sessionCwd = cwd,
  providerName: ProviderSession["provider"] = ProviderDriverKind.make("codex"),
) {
  const now = "2026-01-01T00:00:00.000Z";
  const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
  const rollbackConversation = vi.fn(
    (_input: { readonly threadId: ThreadId; readonly numTurns: number }) => Effect.void,
  );

  const unsupported = <A>() =>
    Effect.die(new Error("Unsupported provider call in test")) as Effect.Effect<A, never>;
  const listSessions = () =>
    hasSession
      ? Effect.succeed([
          {
            provider: providerName,
            status: "ready",
            runtimeMode: "full-access",
            threadId: ThreadId.make("thread-1"),
            cwd: sessionCwd,
            createdAt: now,
            updatedAt: now,
          },
        ] satisfies ReadonlyArray<ProviderSession>)
      : Effect.succeed([] as ReadonlyArray<ProviderSession>);
  const service: ProviderServiceShape = {
    startSession: () => unsupported(),
    sendTurn: () => unsupported(),
    interruptTurn: () => unsupported(),
    respondToRequest: () => unsupported(),
    respondToUserInput: () => unsupported(),
    stopSession: () => unsupported(),
    listSessions,
    getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
    getInstanceInfo: (instanceId) =>
      Effect.succeed({
        instanceId,
        driverKind: ProviderDriverKind.make(providerName),
        displayName: undefined,
        enabled: true,
        continuationIdentity: {
          driverKind: ProviderDriverKind.make(providerName),
          continuationKey: `${providerName}:instance:${instanceId}`,
        },
      }),
    rollbackConversation,
    get streamEvents() {
      return Stream.fromPubSub(runtimeEventPubSub);
    },
  };

  const emit = (event: LegacyProviderRuntimeEvent): void => {
    Effect.runSync(PubSub.publish(runtimeEventPubSub, event as unknown as ProviderRuntimeEvent));
  };

  return {
    service,
    rollbackConversation,
    emit,
  };
}

async function waitForThread(
  readModel: () => Promise<{
    readonly threads: ReadonlyArray<{
      readonly id: ThreadId;
      readonly latestTurn: { readonly turnId: string } | null;
      readonly checkpoints: ReadonlyArray<{ readonly checkpointTurnCount: number }>;
      readonly activities: ReadonlyArray<{ readonly kind: string }>;
    }>;
  }>,
  predicate: (thread: {
    latestTurn: { turnId: string } | null;
    checkpoints: ReadonlyArray<{ checkpointTurnCount: number }>;
    activities: ReadonlyArray<{ kind: string }>;
  }) => boolean,
  timeoutMs = 15_000,
) {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<{
    latestTurn: { turnId: string } | null;
    checkpoints: ReadonlyArray<{ checkpointTurnCount: number }>;
    activities: ReadonlyArray<{ kind: string }>;
  }> => {
    const snapshot = await readModel();
    const thread = snapshot.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    if (thread && predicate(thread)) {
      return thread;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for thread state.");
    }
    await Effect.runPromise(Effect.sleep("10 millis"));
    return poll();
  };
  return poll();
}

async function waitForEvent(
  engine: OrchestrationEngineShape,
  predicate: (event: { type: string }) => boolean,
  timeoutMs = 15_000,
) {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async () => {
    const events = await Effect.runPromise(
      Stream.runCollect(engine.readEvents(0)).pipe(Effect.map((chunk) => Array.from(chunk))),
    );
    if (events.some(predicate)) {
      return events;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for orchestration event.");
    }
    await Effect.runPromise(Effect.sleep("10 millis"));
    return poll();
  };
  return poll();
}

async function waitForReceipt(
  receipts: ReadonlyArray<OrchestrationRuntimeReceipt>,
  predicate: (receipt: OrchestrationRuntimeReceipt) => boolean,
  timeoutMs = 15_000,
) {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<OrchestrationRuntimeReceipt> => {
    const receipt = receipts.find(predicate);
    if (receipt) return receipt;
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error("Timed out waiting for checkpoint receipt.");
    }
    await Effect.runPromise(Effect.sleep("10 millis"));
    return poll();
  };
  return poll();
}

function runGit(cwd: string, args: ReadonlyArray<string>) {
  return NodeChildProcess.execFileSync("git", args, {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
    encoding: "utf8",
  });
}

function createGitRepository() {
  const cwd = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-handler-"));
  runGit(cwd, ["init", "--initial-branch=main"]);
  runGit(cwd, ["config", "user.email", "test@example.com"]);
  runGit(cwd, ["config", "user.name", "Test User"]);
  NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v1\n", "utf8");
  runGit(cwd, ["add", "."]);
  runGit(cwd, ["commit", "-m", "Initial"]);
  return cwd;
}

function gitRefExists(cwd: string, ref: string): boolean {
  try {
    runGit(cwd, ["show-ref", "--verify", "--quiet", ref]);
    return true;
  } catch {
    return false;
  }
}

function gitShowFileAtRef(cwd: string, ref: string, filePath: string): string {
  return runGit(cwd, ["show", `${ref}:${filePath}`]);
}

async function waitForGitRefExists(cwd: string, ref: string, timeoutMs = 15_000) {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + timeoutMs;
  const poll = async (): Promise<void> => {
    if (gitRefExists(cwd, ref)) {
      return;
    }
    if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) {
      throw new Error(`Timed out waiting for git ref '${ref}'.`);
    }
    await Effect.runPromise(Effect.sleep("10 millis"));
    return poll();
  };
  return poll();
}

describe("CheckpointReactor", () => {
  let runtime: ManagedRuntime.ManagedRuntime<
    | OrchestrationEngineService
    | CheckpointReactor
    | CheckpointStore.CheckpointStore
    | ProjectionSnapshotQuery
    | ProjectionTurnRepository
    | RuntimeReceiptBus,
    unknown
  > | null = null;
  let scope: Scope.Closeable | null = null;
  const tempDirs: string[] = [];

  afterEach(async () => {
    if (scope) {
      await Effect.runPromise(Scope.close(scope, Exit.void));
    }
    scope = null;
    if (runtime) {
      await runtime.dispose();
    }
    runtime = null;
    while (tempDirs.length > 0) {
      const dir = tempDirs.pop();
      if (dir) {
        NodeFS.rmSync(dir, { recursive: true, force: true });
      }
    }
  });

  async function createHarness(options?: {
    readonly hasSession?: boolean;
    readonly seedFilesystemCheckpoints?: boolean;
    readonly projectWorkspaceRoot?: string;
    readonly threadWorktreePath?: string | null;
    readonly threadBranch?: string | null;
    readonly secondThreadSharingWorktree?: boolean;
    readonly localStatusRefName?: string | null;
    readonly providerSessionCwd?: string;
    readonly providerName?: ProviderDriverKind;
    readonly gitStatusRefreshCalls?: Array<string>;
  }) {
    const cwd = createGitRepository();
    tempDirs.push(cwd);
    const provider = createProviderServiceHarness(
      cwd,
      options?.hasSession ?? true,
      options?.providerSessionCwd ?? cwd,
      options?.providerName ?? ProviderDriverKind.make("codex"),
    );
    const orchestrationLayer = OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(OrchestrationProjectionPipelineLive),
      Layer.provide(OrchestrationEventStoreLive),
      Layer.provide(OrchestrationCommandReceiptRepositoryLive),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );
    const projectionSnapshotLayer = OrchestrationProjectionSnapshotQueryLive.pipe(
      Layer.provide(ThreadBackgroundLiveness.layer),
      Layer.provide(ThreadPlanProgress.layer),
      Layer.provide(RepositoryIdentityResolver.layer),
      Layer.provide(SqlitePersistenceMemory),
    );

    const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
      prefix: "t3-checkpoint-reactor-test-",
    });
    const vcsStatusBroadcasterLayer = Layer.succeed(VcsStatusBroadcaster, {
      getStatus: () => Effect.die("getStatus should not be called in this test"),
      refreshLocalStatus: (cwd: string) =>
        Effect.sync(() => {
          options?.gitStatusRefreshCalls?.push(cwd);
        }).pipe(
          Effect.as({
            isRepo: true,
            hasPrimaryRemote: false,
            isDefaultRef: true,
            refName:
              options?.localStatusRefName !== undefined ? options.localStatusRefName : "main",
            hasWorkingTreeChanges: false,
            workingTree: { files: [], insertions: 0, deletions: 0 },
          }),
        ),
      refreshStatus: () => Effect.die("refreshStatus should not be called in this test"),
      streamStatus: () => Stream.empty,
    });

    const layer = CheckpointReactorLive.pipe(
      Layer.provideMerge(ProjectionTurnRepositoryLive),
      Layer.provideMerge(orchestrationLayer),
      Layer.provideMerge(projectionSnapshotLayer),
      Layer.provideMerge(RuntimeReceiptBusTest),
      Layer.provideMerge(Layer.succeed(ProviderService, provider.service)),
      Layer.provideMerge(vcsStatusBroadcasterLayer),
      Layer.provideMerge(CheckpointStore.layer.pipe(Layer.provide(VcsDriverRegistry.layer))),
      Layer.provideMerge(
        WorkspaceEntries.layer.pipe(
          Layer.provide(WorkspacePaths.layer),
          Layer.provideMerge(VcsDriverRegistry.layer),
        ),
      ),
      Layer.provideMerge(WorkspacePaths.layer),
      Layer.provideMerge(VcsProcess.layer),
      Layer.provideMerge(ServerConfigLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    runtime = ManagedRuntime.make(layer);
    const engine = await runtime.runPromise(Effect.service(OrchestrationEngineService));
    const snapshotQuery = await runtime.runPromise(Effect.service(ProjectionSnapshotQuery));
    const reactor = await runtime.runPromise(Effect.service(CheckpointReactor));
    const receiptBus = await runtime.runPromise(Effect.service(RuntimeReceiptBus));
    const checkpointStore = await runtime.runPromise(
      Effect.service(CheckpointStore.CheckpointStore),
    );
    const projectionTurnRepository = await runtime.runPromise(
      Effect.service(ProjectionTurnRepository),
    );
    scope = await Effect.runPromise(Scope.make("sequential"));
    const receipts: OrchestrationRuntimeReceipt[] = [];
    await runtime.runPromise(
      Stream.runForEach(receiptBus.streamEventsForTest, (receipt) =>
        Effect.sync(() => {
          receipts.push(receipt);
        }),
      ).pipe(Effect.forkScoped, Scope.provide(scope)),
    );
    await Effect.runPromise(reactor.start().pipe(Scope.provide(scope)));
    const drain = () => Effect.runPromise(reactor.drain);

    const createdAt = "2026-01-01T00:00:00.000Z";
    await Effect.runPromise(
      engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cmd-project-create"),
        projectId: asProjectId("project-1"),
        title: "Test Project",
        workspaceRoot: options?.projectWorkspaceRoot ?? cwd,
        defaultModelSelection: {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5-codex",
        },
        createdAt,
      }),
    );
    await Effect.runPromise(
      engine
        .dispatch({
          type: "thread.create",
          commandId: CommandId.make("cmd-thread-create"),
          threadId: ThreadId.make("thread-1"),
          projectId: asProjectId("project-1"),
          title: "Thread",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex"),
            model: "gpt-5-codex",
          },
          interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
          runtimeMode: "approval-required",
          branch: options?.threadBranch ?? null,
          worktreePath: options?.threadWorktreePath ?? cwd,
          createdAt,
        })
        .pipe(
          options?.secondThreadSharingWorktree
            ? Effect.andThen(
                engine.dispatch({
                  type: "thread.create",
                  commandId: CommandId.make("cmd-thread-create-2"),
                  threadId: ThreadId.make("thread-2"),
                  projectId: asProjectId("project-1"),
                  title: "Thread 2",
                  modelSelection: {
                    instanceId: ProviderInstanceId.make("codex"),
                    model: "gpt-5-codex",
                  },
                  interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
                  runtimeMode: "approval-required",
                  branch: null,
                  worktreePath: options?.threadWorktreePath ?? cwd,
                  createdAt,
                }),
              )
            : Effect.asVoid,
        ),
    );

    if (options?.seedFilesystemCheckpoints ?? true) {
      await runtime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
        }),
      );
      NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v2\n", "utf8");
      await runtime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        }),
      );
      NodeFS.writeFileSync(NodePath.join(cwd, "README.md"), "v3\n", "utf8");
      await runtime.runPromise(
        checkpointStore.captureCheckpoint({
          cwd,
          checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
        }),
      );
    }

    return {
      engine,
      readModel: () => Effect.runPromise(snapshotQuery.getSnapshot()),
      provider,
      cwd,
      checkpointStore,
      projectionTurnRepository,
      drain,
      receipts,
      registerAbortCheckpoint: (input: { readonly threadId: ThreadId; readonly turnId: TurnId }) =>
        runtime!.runPromise(reactor.registerAbortCheckpoint(input)),
      awaitAbortCheckpoint: (input: { readonly threadId: ThreadId; readonly turnId: TurnId }) =>
        runtime!.runPromise(reactor.awaitAbortCheckpoint(input)),
      awaitAbortCheckpointEffect: (input: {
        readonly threadId: ThreadId;
        readonly turnId: TurnId;
      }) => reactor.awaitAbortCheckpoint(input),
      reconcileInterruptedTurn: (input: { readonly threadId: ThreadId; readonly turnId: TurnId }) =>
        runtime!.runPromise(reactor.reconcileInterruptedTurn(input)),
    };
  }

  it("captures pre-turn baseline on turn.started and post-turn checkpoint on turn.completed", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-capture"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-1"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
    });
    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-1"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-1"),
      payload: { state: "completed" },
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-1" && entry.checkpoints.length === 1,
    );
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0)),
    ).toBe(true);
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1)),
    ).toBe(true);
    expect(
      gitShowFileAtRef(
        harness.cwd,
        checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
        "README.md",
      ),
    ).toBe("v1\n");
    expect(
      gitShowFileAtRef(
        harness.cwd,
        checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        "README.md",
      ),
    ).toBe("v2\n");
  });

  it("captures a ready checkpoint when a turn aborts without a completion event", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-aborted-checkpoint");

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-aborted-checkpoint"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: "2026-01-01T00:00:00.000Z",
        },
        createdAt: "2026-01-01T00:00:00.000Z",
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-aborted-checkpoint"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId,
      turnId,
    });
    await waitForGitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 0));

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "aborted work\n", "utf8");
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-interrupted-aborted-checkpoint"),
        threadId,
        session: {
          threadId,
          status: "interrupted",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-01-01T00:00:01.000Z",
        },
        createdAt: "2026-01-01T00:00:01.000Z",
      }),
    );
    await harness.registerAbortCheckpoint({ threadId, turnId });
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-checkpoint"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:01.000Z",
      threadId,
      turnId,
      payload: { reason: "user requested interruption" },
    });

    await waitForReceipt(
      harness.receipts,
      (receipt) =>
        receipt.type === "checkpoint.diff.finalized" &&
        receipt.turnId === turnId &&
        receipt.status === "ready",
    );
    await waitForReceipt(
      harness.receipts,
      (receipt) => receipt.type === "turn.processing.quiesced" && receipt.turnId === turnId,
    );
    await harness.drain();
    await Effect.runPromise(Effect.yieldNow);
    await harness.drain();
    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.checkpoints.some(
        (checkpoint) =>
          checkpoint.turnId === turnId &&
          checkpoint.status === "ready" &&
          checkpoint.checkpointTurnCount === 1,
      ),
    );
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 1))).toBe(true);
    expect(
      gitShowFileAtRef(harness.cwd, checkpointRefForThreadTurn(threadId, 1), "README.md"),
    ).toBe("aborted work\n");
    expect(thread.checkpoints).toHaveLength(1);
    expect(thread.latestTurn?.state).toBe("interrupted");
    expect(
      harness.receipts.filter(
        (receipt) => receipt.type === "checkpoint.diff.finalized" && receipt.turnId === turnId,
      ),
    ).toHaveLength(1);
    expect(
      harness.receipts.filter(
        (receipt) => receipt.type === "turn.processing.quiesced" && receipt.turnId === turnId,
      ),
    ).toHaveLength(1);
  });

  it("settles an accepted abort gate when no checkpoint CWD is available", async () => {
    const unavailableCwd = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-unavailable-cwd-"),
    );
    tempDirs.push(unavailableCwd);
    const harness = await createHarness({
      hasSession: false,
      seedFilesystemCheckpoints: false,
      threadWorktreePath: unavailableCwd,
      projectWorkspaceRoot: unavailableCwd,
    });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-aborted-without-cwd");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-abort-without-cwd"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );
    await harness.registerAbortCheckpoint({ threadId, turnId });

    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-without-cwd"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
      payload: { reason: "user requested interruption" },
    });

    await harness.drain();
    expect(await harness.awaitAbortCheckpoint({ threadId, turnId })).toBe(true);
    const thread = (await harness.readModel()).threads.find((entry) => entry.id === threadId);
    expect(thread?.checkpoints).toEqual([]);
    expect(
      harness.receipts.some(
        (receipt) => receipt.type === "checkpoint.diff.finalized" && receipt.turnId === turnId,
      ),
    ).toBe(false);
  });

  it("settles an accepted abort gate when the session CWD is not a Git repository", async () => {
    const nonRepositorySessionCwd = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-abort-non-repo-"),
    );
    tempDirs.push(nonRepositorySessionCwd);
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      providerSessionCwd: nonRepositorySessionCwd,
    });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-aborted-in-non-repo");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-abort-non-repo"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );
    await harness.registerAbortCheckpoint({ threadId, turnId });

    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-non-repo"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
      payload: { reason: "user requested interruption" },
    });

    await harness.drain();
    expect(await harness.awaitAbortCheckpoint({ threadId, turnId })).toBe(true);
    const thread = (await harness.readModel()).threads.find((entry) => entry.id === threadId);
    expect(thread?.checkpoints).toEqual([]);
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 1))).toBe(false);
  });

  it("finalizes a materialized terminal-abort ref without recapture", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-aborted-materialized-reservation");
    const createdAt = "2026-01-01T00:00:00.000Z";
    const checkpointRef = checkpointRefForThreadTurn(threadId, 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-interrupted-materialized-reservation"),
        threadId,
        session: {
          threadId,
          status: "interrupted",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-materialized-terminal-abort-reservation"),
        threadId,
        turnId,
        completedAt: createdAt,
        checkpointRef,
        status: "missing",
        files: [],
        isTerminalAbort: true,
        checkpointTurnCount: 1,
        createdAt,
      }),
    );
    await waitForThread(harness.readModel, (entry) =>
      entry.checkpoints.some(
        (checkpoint) =>
          checkpoint.turnId === turnId &&
          checkpoint.status === "missing" &&
          checkpoint.checkpointTurnCount === 1,
      ),
    );

    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef: checkpointRefForThreadTurn(threadId, 0),
      }),
    );
    NodeFS.writeFileSync(
      NodePath.join(harness.cwd, "README.md"),
      "reserved terminal state\n",
      "utf8",
    );
    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef,
      }),
    );

    // A post-crash edit must not replace the already-materialized reservation.
    NodeFS.writeFileSync(
      NodePath.join(harness.cwd, "README.md"),
      "edited after recovery\n",
      "utf8",
    );
    await harness.reconcileInterruptedTurn({ threadId, turnId });
    await harness.drain();

    expect(gitShowFileAtRef(harness.cwd, checkpointRef, "README.md")).toBe(
      "reserved terminal state\n",
    );
    expect(
      harness.receipts.filter(
        (receipt) => receipt.type === "checkpoint.diff.finalized" && receipt.turnId === turnId,
      ),
    ).toHaveLength(1);
    const terminalTurn = await runtime!.runPromise(
      harness.projectionTurnRepository.getByTurnId({ threadId, turnId }),
    );
    expect(terminalTurn._tag).toBe("Some");
    if (terminalTurn._tag === "Some") {
      expect(terminalTurn.value.checkpointStatus).toBe("ready");
      expect(terminalTurn.value.isTerminalAbortCheckpoint).toBe(true);
    }
  });

  it("releases an abort gate when normal completion reuses a durable checkpoint", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-completed-with-reusable-checkpoint");
    const createdAt = "2026-01-01T00:00:00.000Z";
    const checkpointRef = checkpointRefForThreadTurn(threadId, 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-reusable-checkpoint"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );
    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef: checkpointRefForThreadTurn(threadId, 0),
      }),
    );
    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "durable completion\n", "utf8");
    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-reusable-checkpoint-ready"),
        threadId,
        turnId,
        completedAt: createdAt,
        checkpointRef,
        status: "ready",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );
    await waitForThread(harness.readModel, (entry) => entry.checkpoints.length === 1);
    await harness.registerAbortCheckpoint({ threadId, turnId });

    NodeFS.writeFileSync(
      NodePath.join(harness.cwd, "README.md"),
      "later completion edit\n",
      "utf8",
    );
    const waiting = runtime!.runPromise(
      harness.awaitAbortCheckpointEffect({ threadId, turnId }).pipe(Effect.timeout("100 millis")),
    );
    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-reusable-checkpoint"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
      payload: { state: "completed" },
    });

    expect(await waiting).toBe(true);
    expect(gitShowFileAtRef(harness.cwd, checkpointRef, "README.md")).toBe("durable completion\n");
  });

  it("retains a consumable completion sentinel when abort capture precedes gate registration", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-abort-before-gate-registration");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-abort-before-gate-registration"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-abort-before-gate-registration"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
    });
    await waitForGitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 0));

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "aborted work\n", "utf8");
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-before-gate-registration"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
      payload: { reason: "user requested interruption" },
    });

    await waitForReceipt(
      harness.receipts,
      (receipt) =>
        receipt.type === "checkpoint.diff.finalized" &&
        receipt.turnId === turnId &&
        receipt.status === "ready",
    );

    // Ingestion registers its gate after the reactor has already captured the
    // accepted provider abort. The completion sentinel must satisfy the late
    // waiter rather than leave it blocked forever.
    await harness.registerAbortCheckpoint({ threadId, turnId });
    const observed = await runtime!.runPromise(
      harness
        .awaitAbortCheckpointEffect({ threadId, turnId })
        .pipe(Effect.timeoutOption("50 millis")),
    );
    expect(observed._tag).toBe("Some");
    if (observed._tag === "Some") {
      expect(observed.value).toBe(true);
    }
    expect(await harness.awaitAbortCheckpoint({ threadId, turnId })).toBe(false);
    expect(
      harness.receipts.filter(
        (receipt) => receipt.type === "checkpoint.diff.finalized" && receipt.turnId === turnId,
      ),
    ).toHaveLength(1);
  });

  it("refreshes a ready mid-turn checkpoint when an accepted abort arrives", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-aborted-after-mid-turn-checkpoint");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-mid-turn-checkpoint"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-mid-turn-checkpoint"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
    });
    await waitForGitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 0));

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "mid-turn work\n", "utf8");
    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef: checkpointRefForThreadTurn(threadId, 1),
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-mid-turn-checkpoint-ready"),
        threadId,
        turnId,
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(threadId, 1),
        status: "ready",
        files: [],
        assistantMessageId: MessageId.make("assistant-mid-turn-checkpoint"),
        checkpointTurnCount: 1,
        createdAt,
      }),
    );
    await waitForThread(harness.readModel, (entry) => entry.checkpoints.length === 1);

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "later abort edit\n", "utf8");
    await harness.registerAbortCheckpoint({ threadId, turnId });
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-interrupted-mid-turn-checkpoint"),
        threadId,
        session: {
          threadId,
          status: "interrupted",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-01-01T00:00:01.000Z",
        },
        createdAt: "2026-01-01T00:00:01.000Z",
      }),
    );
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-after-mid-turn-checkpoint"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:01.000Z",
      threadId,
      turnId,
      payload: { reason: "user requested interruption" },
    });

    await waitForReceipt(
      harness.receipts,
      (receipt) =>
        receipt.type === "checkpoint.diff.finalized" &&
        receipt.turnId === turnId &&
        receipt.status === "ready",
    );
    await waitForThread(harness.readModel, (entry) => entry.checkpoints.length === 1);
    await harness.drain();
    expect(
      gitShowFileAtRef(harness.cwd, checkpointRefForThreadTurn(threadId, 1), "README.md"),
    ).toBe("later abort edit\n");

    const terminalTurn = await runtime!.runPromise(
      harness.projectionTurnRepository.getByTurnId({
        threadId,
        turnId,
      }),
    );
    expect(terminalTurn._tag).toBe("Some");
    if (terminalTurn._tag === "Some") {
      expect(terminalTurn.value.isTerminalAbortCheckpoint).toBe(true);
    }
  });

  it("does not checkpoint a stale abort without a matching active or interrupted turn", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");

    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-stale-checkpoint"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId,
      turnId: asTurnId("turn-stale-checkpoint"),
      payload: { reason: "late provider event" },
    });

    await harness.drain();
    const thread = (await harness.readModel()).threads.find((entry) => entry.id === threadId);
    expect(thread?.checkpoints).toEqual([]);
    expect(gitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 1))).toBe(false);
  });

  it("does not recapture a finalized terminal abort after its in-memory gate is lost", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-aborted-checkpoint-reconcile");

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-aborted-checkpoint-reconcile"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: "2026-01-01T00:00:00.000Z",
        },
        createdAt: "2026-01-01T00:00:00.000Z",
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-aborted-checkpoint-reconcile"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId,
      turnId,
    });
    await waitForGitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 0));

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "aborted work\n", "utf8");
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-interrupted-aborted-checkpoint-reconcile"),
        threadId,
        session: {
          threadId,
          status: "interrupted",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-01-01T00:00:01.000Z",
        },
        createdAt: "2026-01-01T00:00:01.000Z",
      }),
    );
    await harness.registerAbortCheckpoint({ threadId, turnId });
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-checkpoint-reconcile"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:01.000Z",
      threadId,
      turnId,
      payload: { reason: "user requested interruption" },
    });

    await waitForReceipt(
      harness.receipts,
      (receipt) =>
        receipt.type === "checkpoint.diff.finalized" &&
        receipt.turnId === turnId &&
        receipt.status === "ready",
    );
    await waitForThread(harness.readModel, (entry) =>
      entry.checkpoints.some(
        (checkpoint) =>
          checkpoint.turnId === turnId &&
          checkpoint.status === "ready" &&
          checkpoint.checkpointTurnCount === 1,
      ),
    );
    const terminalTurn = await runtime!.runPromise(
      harness.projectionTurnRepository.getByTurnId({
        threadId,
        turnId,
      }),
    );
    expect(terminalTurn._tag).toBe("Some");
    if (terminalTurn._tag === "Some") {
      expect(terminalTurn.value.isTerminalAbortCheckpoint).toBe(true);
    }
    await harness.drain();
    expect(await harness.awaitAbortCheckpoint({ threadId, turnId })).toBe(true);
    expect(await harness.awaitAbortCheckpoint({ threadId, turnId })).toBe(false);

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "post-abort edit\n", "utf8");
    await harness.reconcileInterruptedTurn({ threadId, turnId });

    expect(
      gitShowFileAtRef(harness.cwd, checkpointRefForThreadTurn(threadId, 1), "README.md"),
    ).toBe("aborted work\n");
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe(
      "post-abort edit\n",
    );
    expect(
      harness.receipts.filter(
        (receipt) => receipt.type === "checkpoint.diff.finalized" && receipt.turnId === turnId,
      ),
    ).toHaveLength(1);
  });

  it("does not treat an existing hidden checkpoint ref as terminal completion during reconciliation", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-aborted-crash-window");
    const createdAt = "2026-01-01T00:00:00.000Z";
    const checkpointRef = checkpointRefForThreadTurn(threadId, 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-interrupted-crash-window"),
        threadId,
        session: {
          threadId,
          status: "interrupted",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );
    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef: checkpointRefForThreadTurn(threadId, 0),
      }),
    );
    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "aborted before crash\n", "utf8");
    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef,
      }),
    );

    // The terminal projection dispatch was lost, but the durable checkpoint ref
    // survived the crash. Keep a missing projection entry so reconciliation has
    // the interrupted turn context without triggering placeholder recapture.
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-crash-window-placeholder"),
        threadId,
        turnId,
        completedAt: createdAt,
        checkpointRef,
        status: "missing",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );
    await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === turnId && entry.checkpoints.length === 1,
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "edited after restart\n", "utf8");
    await harness.reconcileInterruptedTurn({ threadId, turnId });
    await harness.drain();

    expect(gitShowFileAtRef(harness.cwd, checkpointRef, "README.md")).toBe(
      "edited after restart\n",
    );
    expect(
      harness.receipts.filter(
        (receipt) => receipt.type === "checkpoint.diff.finalized" && receipt.turnId === turnId,
      ),
    ).toHaveLength(1);
    const terminalTurn = await runtime!.runPromise(
      harness.projectionTurnRepository.getByTurnId({ threadId, turnId }),
    );
    expect(terminalTurn._tag).toBe("Some");
    if (terminalTurn._tag === "Some") {
      expect(terminalTurn.value.isTerminalAbortCheckpoint).toBe(true);
    }
  });

  it("reuses a persisted terminal-abort reservation after a crash", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-aborted-reserved-crash");
    const createdAt = "2026-01-01T00:00:00.000Z";
    const checkpointRef = checkpointRefForThreadTurn(threadId, 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-interrupted-reserved-crash"),
        threadId,
        session: {
          threadId,
          status: "interrupted",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-terminal-abort-reservation"),
        threadId,
        turnId,
        completedAt: createdAt,
        checkpointRef,
        status: "missing",
        files: [],
        isTerminalAbort: true,
        checkpointTurnCount: 1,
        createdAt,
      }),
    );

    await waitForThread(harness.readModel, (entry) =>
      entry.checkpoints.some(
        (checkpoint) =>
          checkpoint.turnId === turnId &&
          checkpoint.status === "missing" &&
          checkpoint.checkpointTurnCount === 1,
      ),
    );
    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef: checkpointRefForThreadTurn(threadId, 0),
      }),
    );
    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "reserved abort work\n", "utf8");

    await harness.reconcileInterruptedTurn({ threadId, turnId });
    await harness.drain();

    expect(gitShowFileAtRef(harness.cwd, checkpointRef, "README.md")).toBe("reserved abort work\n");
    expect(
      harness.receipts.filter(
        (receipt) => receipt.type === "checkpoint.diff.finalized" && receipt.turnId === turnId,
      ),
    ).toHaveLength(1);
    const terminalTurn = await runtime!.runPromise(
      harness.projectionTurnRepository.getByTurnId({ threadId, turnId }),
    );
    expect(terminalTurn._tag).toBe("Some");
    if (terminalTurn._tag === "Some") {
      expect(terminalTurn.value.checkpointTurnCount).toBe(1);
      expect(terminalTurn.value.checkpointRef).toBe(checkpointRef);
      expect(terminalTurn.value.isTerminalAbortCheckpoint).toBe(true);
    }
  });

  it("evicts a completed abort gate after concurrent deferred waiters wake", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-abort-gate-eviction");
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-gate-eviction"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );
    await runtime!.runPromise(
      harness.checkpointStore.captureCheckpoint({
        cwd: harness.cwd,
        checkpointRef: checkpointRefForThreadTurn(threadId, 0),
      }),
    );
    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "first abort\n", "utf8");

    await harness.registerAbortCheckpoint({ threadId, turnId });
    const firstWaiter = harness.awaitAbortCheckpoint({ threadId, turnId });
    const secondWaiter = harness.awaitAbortCheckpoint({ threadId, turnId });
    await Effect.runPromise(Effect.yieldNow);
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-gate-eviction-first"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
      payload: { reason: "user requested interruption" },
    });
    expect(await firstWaiter).toBe(true);
    expect(await secondWaiter).toBe(true);
    await harness.drain();

    await harness.registerAbortCheckpoint({ threadId, turnId });
    let thirdWaiterSettled = false;
    const thirdWaiter = harness.awaitAbortCheckpoint({ threadId, turnId }).then((observed) => {
      thirdWaiterSettled = true;
      return observed;
    });
    await Effect.runPromise(Effect.sleep("10 millis"));
    expect(thirdWaiterSettled).toBe(false);

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "second abort\n", "utf8");
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-gate-eviction-second"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
      payload: { reason: "user requested interruption again" },
    });
    expect(await thirdWaiter).toBe(true);
  });

  it("keeps an abort gate pending when terminal checkpoint capture fails", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const turnId = asTurnId("turn-abort-capture-failure");
    const createdAt = "2026-01-01T00:00:00.000Z";
    const checkpointRef = checkpointRefForThreadTurn(threadId, 1);

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-abort-capture-failure"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: turnId,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-abort-capture-failure"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
    });
    await waitForGitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 0));
    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "aborted work\n", "utf8");

    // Block update-ref for the terminal checkpoint while leaving the worktree
    // detectable as a Git repository. The capture must fail after the turn's
    // gate has been registered, with no durable terminal ref to release it.
    const checkpointPath = NodePath.join(harness.cwd, ".git", checkpointRef);
    NodeFS.mkdirSync(checkpointPath, {
      recursive: true,
    });
    NodeFS.writeFileSync(NodePath.join(checkpointPath, "block"), "not a ref", "utf8");
    await harness.registerAbortCheckpoint({ threadId, turnId });
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-capture-failure"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
      payload: { reason: "user requested interruption" },
    });

    const failedThread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some((activity) => activity.kind === "checkpoint.capture.failed"),
    );
    await harness.drain();

    const blocked = await runtime!.runPromise(
      harness
        .awaitAbortCheckpointEffect({ threadId, turnId })
        .pipe(Effect.timeoutOption("20 millis")),
    );
    expect(blocked._tag).toBe("None");
    expect(failedThread.checkpoints).toHaveLength(1);
    expect(failedThread.checkpoints[0]?.status).toBe("missing");
    expect(gitRefExists(harness.cwd, checkpointRef)).toBe(false);
    expect(
      harness.receipts.filter(
        (receipt) => receipt.type === "checkpoint.diff.finalized" && receipt.turnId === turnId,
      ),
    ).toHaveLength(0);

    // A later terminal event may retry the pending gate. It is released only
    // after the Git ref becomes durable, never by the failed attempt itself.
    NodeFS.rmSync(checkpointPath, { recursive: true, force: true });
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-capture-retry"),
      provider: ProviderDriverKind.make("codex"),
      createdAt,
      threadId,
      turnId,
      payload: { reason: "retry terminal capture" },
    });

    await waitForReceipt(
      harness.receipts,
      (receipt) =>
        receipt.type === "checkpoint.diff.finalized" &&
        receipt.turnId === turnId &&
        receipt.status === "ready",
    );
    expect(await harness.awaitAbortCheckpoint({ threadId, turnId })).toBe(true);
    expect(gitRefExists(harness.cwd, checkpointRef)).toBe(true);
  });

  it("checkpoints an interrupted turn after a newer turn has started", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const threadId = ThreadId.make("thread-1");
    const abortedTurnId = asTurnId("turn-aborted-before-next-turn");
    const nextTurnId = asTurnId("turn-after-abort");

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-aborted-before-next-turn"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: abortedTurnId,
          lastError: null,
          updatedAt: "2026-01-01T00:00:00.000Z",
        },
        createdAt: "2026-01-01T00:00:00.000Z",
      }),
    );
    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-aborted-before-next-turn"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId,
      turnId: abortedTurnId,
    });
    await waitForGitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 0));

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "aborted work\n", "utf8");
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-interrupted-before-next-turn"),
        threadId,
        session: {
          threadId,
          status: "interrupted",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-01-01T00:00:01.000Z",
        },
        createdAt: "2026-01-01T00:00:01.000Z",
      }),
    );
    await harness.registerAbortCheckpoint({ threadId, turnId: abortedTurnId });
    harness.provider.emit({
      type: "turn.aborted",
      eventId: EventId.make("evt-turn-aborted-before-next-turn"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:01.000Z",
      threadId,
      turnId: abortedTurnId,
      payload: { reason: "user requested interruption" },
    });
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-running-after-abort"),
        threadId,
        session: {
          threadId,
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: nextTurnId,
          lastError: null,
          updatedAt: "2026-01-01T00:00:02.000Z",
        },
        createdAt: "2026-01-01T00:00:02.000Z",
      }),
    );
    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-after-abort"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:02.000Z",
      threadId,
      turnId: nextTurnId,
    });

    await waitForGitRefExists(harness.cwd, checkpointRefForThreadTurn(threadId, 1));
    expect(
      gitShowFileAtRef(harness.cwd, checkpointRefForThreadTurn(threadId, 1), "README.md"),
    ).toBe("aborted work\n");
  });

  it("refreshes local git status state on turn completion using the session cwd", async () => {
    const gitStatusRefreshCalls: string[] = [];
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      gitStatusRefreshCalls,
    });

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-refresh-local-status"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-refresh-local-status"),
      payload: { state: "completed" },
    });

    await harness.drain();

    expect(gitStatusRefreshCalls).toEqual([harness.cwd]);
  });

  it("adopts a drifted checkout as the thread branch on a dedicated worktree", async () => {
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      threadBranch: "t3code/original-branch",
      localStatusRefName: "t3code/renamed-by-agent",
    });

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-branch-drift"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-branch-drift"),
      payload: { state: "completed" },
    });

    await harness.drain();
    await waitForEvent(
      harness.engine,
      (event) =>
        event.type === "thread.meta-updated" &&
        (event as unknown as { payload: { branch?: string } }).payload.branch ===
          "t3code/renamed-by-agent",
    );

    const snapshot = await harness.readModel();
    const thread = snapshot.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.branch).toBe("t3code/renamed-by-agent");
  });

  it("does not adopt a drifted checkout when the worktree is shared by another thread", async () => {
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      threadBranch: "t3code/original-branch",
      localStatusRefName: "t3code/renamed-by-agent",
      secondThreadSharingWorktree: true,
    });

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-branch-drift-shared"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-branch-drift-shared"),
      payload: { state: "completed" },
    });

    await harness.drain();

    const snapshot = await harness.readModel();
    const thread = snapshot.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.branch).toBe("t3code/original-branch");
  });

  it("does not adopt a temporary placeholder checkout as the thread branch", async () => {
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      threadBranch: "t3code/original-branch",
      localStatusRefName: "t3code/0a1b2c3d",
    });

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-branch-drift-temp"),
      provider: ProviderDriverKind.make("codex"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-branch-drift-temp"),
      payload: { state: "completed" },
    });

    await harness.drain();

    const snapshot = await harness.readModel();
    const thread = snapshot.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.branch).toBe("t3code/original-branch");
  });

  it("ignores auxiliary thread turn completion while primary turn is active", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-primary-running"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-main"),
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-main"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-main"),
    });
    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-aux"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-aux"),
      payload: { state: "completed" },
    });

    await harness.drain();
    const midReadModel = await harness.readModel();
    const midThread = midReadModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(midThread?.checkpoints).toHaveLength(0);

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-main"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-main"),
      payload: { state: "completed" },
    });

    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-main" && entry.checkpoints.length === 1,
    );
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
  });

  it("captures pre-turn and completion checkpoints for claude runtime events", async () => {
    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      providerName: ProviderDriverKind.make("claudeAgent"),
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-capture-claude"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "claudeAgent",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-claude-1"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-claude-1"),
    });
    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-claude-1"),
      provider: ProviderDriverKind.make("claudeAgent"),
      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-claude-1"),
      payload: { state: "completed" },
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.latestTurn?.turnId === "turn-claude-1" && entry.checkpoints.length === 1,
    );

    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1)),
    ).toBe(true);
  });

  it("appends capture failure activity when turn diff summary cannot be derived", async () => {
    const harness = await createHarness({ seedFilesystemCheckpoints: false });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-missing-baseline-diff"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-missing-baseline"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-missing-baseline"),
      payload: { state: "completed" },
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    const thread = await waitForThread(
      harness.readModel,
      (entry) =>
        entry.checkpoints.length === 1 &&
        entry.activities.some((activity) => activity.kind === "checkpoint.capture.failed"),
    );

    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(
      thread.activities.some((activity) => activity.kind === "checkpoint.capture.failed"),
    ).toBe(true);
  });

  it("captures pre-turn baseline from project workspace root when thread worktree is unset", async () => {
    const harness = await createHarness({
      hasSession: false,
      seedFilesystemCheckpoints: false,
      threadWorktreePath: null,
    });

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.start",
        commandId: CommandId.make("cmd-turn-start-for-baseline"),
        threadId: ThreadId.make("thread-1"),
        message: {
          messageId: MessageId.make("message-user-1"),
          role: "user",
          text: "start turn",
          attachments: [],
        },
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        runtimeMode: "approval-required",
        createdAt: "2026-01-01T00:00:00.000Z",
      }),
    );

    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );
    expect(
      gitShowFileAtRef(
        harness.cwd,
        checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
        "README.md",
      ),
    ).toBe("v1\n");
  });

  it("captures turn completion checkpoint from project workspace root when provider session cwd is unavailable", async () => {
    const harness = await createHarness({
      hasSession: false,
      seedFilesystemCheckpoints: false,
      threadWorktreePath: null,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-missing-provider-cwd"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "running",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: asTurnId("turn-missing-cwd"),
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    NodeFS.writeFileSync(NodePath.join(harness.cwd, "README.md"), "v2\n", "utf8");
    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-turn-completed-missing-provider-cwd"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-missing-cwd"),
      payload: { state: "completed" },
    });

    await waitForEvent(harness.engine, (event) => event.type === "thread.turn-diff-completed");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1)),
    ).toBe(true);
    expect(
      gitShowFileAtRef(
        harness.cwd,
        checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        "README.md",
      ),
    ).toBe("v2\n");
  });

  it("ignores non-v2 checkpoint.captured runtime events", async () => {
    const harness = await createHarness();
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-checkpoint-captured"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "checkpoint.captured",
      eventId: EventId.make("evt-checkpoint-captured-3"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-3"),
      turnCount: 3,
      status: "completed",
    });

    await harness.drain();
    const readModel = await harness.readModel();
    const thread = readModel.threads.find((entry) => entry.id === ThreadId.make("thread-1"));
    expect(thread?.checkpoints.some((checkpoint) => checkpoint.checkpointTurnCount === 3)).toBe(
      false,
    );
  });

  it("continues processing runtime events after a single checkpoint runtime failure", async () => {
    const nonRepositorySessionCwd = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "t3-checkpoint-runtime-non-repo-"),
    );
    tempDirs.push(nonRepositorySessionCwd);

    const harness = await createHarness({
      seedFilesystemCheckpoints: false,
      providerSessionCwd: nonRepositorySessionCwd,
    });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-non-repo-runtime"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    harness.provider.emit({
      type: "turn.completed",
      eventId: EventId.make("evt-runtime-capture-failure"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-runtime-failure"),
      payload: { state: "completed" },
    });

    harness.provider.emit({
      type: "turn.started",
      eventId: EventId.make("evt-turn-started-after-runtime-failure"),
      provider: ProviderDriverKind.make("codex"),

      createdAt: "2026-01-01T00:00:00.000Z",
      threadId: ThreadId.make("thread-1"),
      turnId: asTurnId("turn-after-runtime-failure"),
    });

    await waitForGitRefExists(
      harness.cwd,
      checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0),
    );
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 0)),
    ).toBe(true);
  });

  it("executes provider revert and emits thread.reverted for checkpoint revert requests", async () => {
    const harness = await createHarness();
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-diff-1"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-1"),
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        status: "ready",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-diff-2"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-2"),
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
        status: "ready",
        files: [],
        checkpointTurnCount: 2,
        createdAt,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.checkpoint.revert",
        commandId: CommandId.make("cmd-revert-request"),
        threadId: ThreadId.make("thread-1"),
        turnCount: 1,
        createdAt,
      }),
    );

    await waitForEvent(harness.engine, (event) => event.type === "thread.reverted");
    const thread = await waitForThread(
      harness.readModel,
      (entry) => entry.checkpoints.length === 1,
    );

    expect(thread.latestTurn?.turnId).toBe("turn-1");
    expect(thread.checkpoints).toHaveLength(1);
    expect(thread.checkpoints[0]?.checkpointTurnCount).toBe(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: ThreadId.make("thread-1"),
      numTurns: 1,
    });
    expect(NodeFS.readFileSync(NodePath.join(harness.cwd, "README.md"), "utf8")).toBe("v2\n");
    expect(
      gitRefExists(harness.cwd, checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2)),
    ).toBe(false);
  });

  it("executes provider revert and emits thread.reverted for claude sessions", async () => {
    const harness = await createHarness({ providerName: ProviderDriverKind.make("claudeAgent") });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-claude"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "claudeAgent",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-diff-claude-1"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-claude-1"),
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        status: "ready",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-diff-claude-2"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-claude-2"),
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
        status: "ready",
        files: [],
        checkpointTurnCount: 2,
        createdAt,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.checkpoint.revert",
        commandId: CommandId.make("cmd-revert-request-claude"),
        threadId: ThreadId.make("thread-1"),
        turnCount: 1,
        createdAt,
      }),
    );

    await waitForEvent(harness.engine, (event) => event.type === "thread.reverted");
    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(1);
    expect(harness.provider.rollbackConversation).toHaveBeenCalledWith({
      threadId: ThreadId.make("thread-1"),
      numTurns: 1,
    });
  });

  it("processes consecutive revert requests with deterministic rollback sequencing", async () => {
    const harness = await createHarness();
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("cmd-session-set-inline-revert"),
        threadId: ThreadId.make("thread-1"),
        session: {
          threadId: ThreadId.make("thread-1"),
          status: "ready",
          providerName: "codex",
          runtimeMode: "approval-required",
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-inline-revert-diff-1"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-1"),
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 1),
        status: "ready",
        files: [],
        checkpointTurnCount: 1,
        createdAt,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.turn.diff.complete",
        commandId: CommandId.make("cmd-inline-revert-diff-2"),
        threadId: ThreadId.make("thread-1"),
        turnId: asTurnId("turn-2"),
        completedAt: createdAt,
        checkpointRef: checkpointRefForThreadTurn(ThreadId.make("thread-1"), 2),
        status: "ready",
        files: [],
        checkpointTurnCount: 2,
        createdAt,
      }),
    );

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.checkpoint.revert",
        commandId: CommandId.make("cmd-sequenced-revert-request-1"),
        threadId: ThreadId.make("thread-1"),
        turnCount: 1,
        createdAt,
      }),
    );
    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.checkpoint.revert",
        commandId: CommandId.make("cmd-sequenced-revert-request-0"),
        threadId: ThreadId.make("thread-1"),
        turnCount: 0,
        createdAt,
      }),
    );

    await harness.drain();

    expect(harness.provider.rollbackConversation).toHaveBeenCalledTimes(2);
    expect(harness.provider.rollbackConversation.mock.calls[0]?.[0]).toEqual({
      threadId: ThreadId.make("thread-1"),
      numTurns: 1,
    });
    expect(harness.provider.rollbackConversation.mock.calls[1]?.[0]).toEqual({
      threadId: ThreadId.make("thread-1"),
      numTurns: 1,
    });
  });

  it("appends an error activity when revert is requested without an active session", async () => {
    const harness = await createHarness({ hasSession: false });
    const createdAt = "2026-01-01T00:00:00.000Z";

    await Effect.runPromise(
      harness.engine.dispatch({
        type: "thread.checkpoint.revert",
        commandId: CommandId.make("cmd-revert-no-session"),
        threadId: ThreadId.make("thread-1"),
        turnCount: 1,
        createdAt,
      }),
    );

    const thread = await waitForThread(harness.readModel, (entry) =>
      entry.activities.some((activity) => activity.kind === "checkpoint.revert.failed"),
    );

    expect(thread.activities.some((activity) => activity.kind === "checkpoint.revert.failed")).toBe(
      true,
    );
    expect(harness.provider.rollbackConversation).not.toHaveBeenCalled();
  });
});
