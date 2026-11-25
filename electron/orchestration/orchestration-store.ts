import { randomUUID } from 'node:crypto';
import { existsSync, readFileSync, renameSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { AtomicSnapshotWriter } from '../persistence/atomic-snapshot-writer.js';
import { cloneTaskContext, listWorkerBriefFiles, mergeTaskContext, normalizeTaskContext, removeWorkerBriefFile } from './worker-brief-context.js';
import {
  ACTIVE_DISPATCH_STATUSES,
  DEPENDENCY_FAILED_REASON,
  assertDispatchTransition,
  assertTaskTransition,
  isBlockedByFailedDependency,
  isTaskSettled,
  reportCorrectsUnobservedPrompt,
  reportCorrectsUnresponsiveWorker,
} from './orchestration-lifecycle-graph.js';
import { buildOrchestrationTaskDisplayMetadata } from './orchestration-task-display.js';
import { mapTimelineStrings, trimTimelineSnapshot, TIMELINE_LIMITS, type WorkerTimelineSnapshot } from './worker-timeline.js';
import {
  AGENT_PROMPT_STALLED_ERROR,
  DEFAULT_NESTED_WORKER_MAX_DEPTH,
  DISPATCH_CIRCUIT_BREAK_FAILURES,
  ORCHESTRATION_DELIVERY_BATCH_LIMIT,
  OrchestrationError,
  UNBOUND_RUN_ID,
  type OrchestrationBoardPostKind,
  type OrchestrationCoordinatorRunRecord,
  type OrchestrationDeliveryRecord,
  type OrchestrationDispatchRecord,
  type OrchestrationDispatchStage,
  type OrchestrationGateRecord,
  type OrchestrationGateStatus,
  type OrchestrationIntegrationStatus,
  type OrchestrationMemberCursor,
  type OrchestrationMessagePriority,
  type OrchestrationMessageRecord,
  type OrchestrationMessageType,
  type OrchestrationPersistedState,
  type OrchestrationQuestionRecord,
  type OrchestrationReportArtifacts,
  type OrchestrationReviewFinding,
  type OrchestrationReviewStatus,
  type OrchestrationRunIntegrationStatus,
  type OrchestrationRunRecord,
  type OrchestrationSnapshot,
  type OrchestrationStartObservation,
  type OrchestrationTaskContext,
  type OrchestrationTaskRecord,
  type OrchestrationTaskStatus,
  type OrchestrationTaskWithDispatch,
  type OrchestrationWorkerReportOutcome,
  type WorkerTurnStopReason,
} from './orchestration-types.js';

export const ORCHESTRATION_STATE_FILENAME = 'orchestration-state.json';

export function isBroadcastMailbox(mailboxHandle: string): boolean {
  return (
    mailboxHandle === '@workers' ||
    mailboxHandle === '@all' ||
    mailboxHandle.startsWith('run:')
  );
}

function emptyPersistedState(): OrchestrationPersistedState {
  return {
    version: 1,
    nextSequence: 1,
    currentRunByConsumer: {},
    runs: [],
    tasks: [],
    dispatches: [],
    gates: [],
    messages: [],
    deliveries: [],
    consumerReadByMessageId: {},
    consumerDeliveredByMessageId: {},
    questions: [],
    coordinatorRuns: [],
    capabilities: {},
    nextBoardSeqByRun: {},
    memberCursorsByRun: {},
  };
}

function cloneMessageRecord(msg: OrchestrationMessageRecord): OrchestrationMessageRecord {
  return {
    ...msg,
    ...(msg.mentions ? { mentions: [...msg.mentions] } : {}),
    ...(msg.refs ? { refs: [...msg.refs] } : {}),
    ...(msg.artifacts
      ? {
          artifacts: {
            ...msg.artifacts,
            ...(msg.artifacts.changedFiles ? { changedFiles: [...msg.artifacts.changedFiles] } : {}),
          },
        }
      : {}),
  };
}

function cloneRunRecord(run: OrchestrationRunRecord): OrchestrationRunRecord {
  return {
    ...run,
    ...(run.runReviewFindings
      ? { runReviewFindings: run.runReviewFindings.map((f) => ({ ...f })) }
      : {}),
  };
}

function cloneTaskRecord(task: OrchestrationTaskRecord): OrchestrationTaskRecord {
  const clonedContext = cloneTaskContext(task.taskContext);
  return {
    ...task,
    deps: [...task.deps],
    ...(clonedContext ? { taskContext: clonedContext } : {}),
  };
}

function generatePrefixedId(prefix: string): string {
  return `${prefix}_${randomUUID().replace(/-/g, '').slice(0, 12)}`;
}

export interface OrchestrationStoreOptions {
  resolveFile?: () => string;
  briefsDir?: string;
  now?: () => string;
  maxMessagesPerRun?: number;
  maxMessages?: number;
  maxDeliveries?: number;
  maxRuns?: number;
}

const DEFAULT_MAX_MESSAGES_PER_RUN = 200;
const DEFAULT_MAX_MESSAGES = 1000;
const DEFAULT_MAX_DELIVERIES = 500;
const DEFAULT_MAX_RUNS = 50;
const DISPATCH_CREDENTIAL_PREFIX = 'aha_dispatch_';
const DEFERRED_CREDENTIAL_SLOT = '<aha-dispatch-credential>';
const PREFIXED_CREDENTIAL_RE = /aha_dispatch_[0-9a-f-]{36}\b/gi;
const LABELLED_LEGACY_CREDENTIAL_RE = /(\bDispatch Capability:\s*|--capability(?:=|\s+)["']?)([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\b/gi;

export class OrchestrationStore {
  private readonly customClock: boolean;
  private now: () => string;
  private readonly resolveFile: (() => string) | undefined;
  private briefsDirOverride: string | null = null;
  private readonly writer: AtomicSnapshotWriter | null;
  private readonly maxMessagesPerRun: number;
  private readonly maxMessages: number;
  private readonly maxDeliveries: number;
  private readonly maxRuns: number;
  private maxActiveDispatches = Number.POSITIVE_INFINITY;

  private nextSequence = 1;
  private currentRunByConsumer = new Map<string, string>();
  private runs = new Map<string, OrchestrationRunRecord>();
  private tasks = new Map<string, OrchestrationTaskRecord>();
  private dispatches = new Map<string, OrchestrationDispatchRecord>();
  private gates = new Map<string, OrchestrationGateRecord>();
  private messages = new Map<string, OrchestrationMessageRecord>();
  private messageOrder: string[] = [];
  private deliveries = new Map<string, OrchestrationDeliveryRecord>();
  private consumerReadByMessageId = new Map<string, Set<string>>();
  private consumerDeliveredByMessageId = new Map<string, Set<string>>();
  private questions = new Map<string, OrchestrationQuestionRecord>();
  private coordinatorRuns = new Map<string, OrchestrationCoordinatorRunRecord>();
  private dispatchCapabilities = new Map<string, string>();
  private readonly deferredPrompts = new Map<
    string, { ptyId: string; prompt: string; submit: boolean }
  >();
  private nextBoardSeqByRun = new Map<string, number>();
  private memberCursorsByRun = new Map<string, Map<string, OrchestrationMemberCursor>>();

  constructor(options: OrchestrationStoreOptions = {}) {
    this.customClock = typeof options.now === 'function';
    this.now = options.now ?? (() => new Date().toISOString());
    this.resolveFile = options.resolveFile;
    this.briefsDirOverride = options.briefsDir?.trim() || null;
    this.maxMessagesPerRun = Math.max(0, Math.floor(options.maxMessagesPerRun ?? DEFAULT_MAX_MESSAGES_PER_RUN));
    this.maxMessages = Math.max(0, Math.floor(options.maxMessages ?? DEFAULT_MAX_MESSAGES));
    this.maxDeliveries = Math.max(0, Math.floor(options.maxDeliveries ?? DEFAULT_MAX_DELIVERIES));
    this.maxRuns = Math.max(0, Math.floor(options.maxRuns ?? DEFAULT_MAX_RUNS));
    this.writer = options.resolveFile
      ? new AtomicSnapshotWriter(options.resolveFile, {
          skipUnchanged: true,
          backupSuffix: '.bak',
          fileMode: 0o600,
          directoryMode: 0o700,
        })
      : null;
  }

  hasCustomClock(): boolean {
    return this.customClock;
  }

  getBriefsDir(): string | null {
    const filePath = this.resolveFile?.();
    return this.briefsDirOverride ?? (filePath ? join(dirname(filePath), 'orchestration-briefs') : null);
  }

  configureBriefsDir(directory: string): void {
    this.briefsDirOverride = directory;
    this.reconcileBriefFiles();
  }

  /** Upper bound on simultaneously active dispatches across every run. */
  configureMaxActiveDispatches(limit: number): void {
    this.maxActiveDispatches = Number.isFinite(limit) ? Math.max(1, Math.floor(limit)) : Number.POSITIVE_INFINITY;
  }

  getRemainingDispatchCapacity(): number {
    let active = 0;
    for (const d of this.dispatches.values()) {
      if (ACTIVE_DISPATCH_STATUSES.has(d.status)) active += 1;
    }
    return Math.max(0, this.maxActiveDispatches - active);
  }

  assertDispatchCapacity(): void {
    if (this.getRemainingDispatchCapacity() <= 0) {
      throw new OrchestrationError(
        'worker_limit_reached',
        `At most ${this.maxActiveDispatches} workers may be active at once. Wait for one to finish or stop one first.`,
        { maxActiveDispatches: this.maxActiveDispatches },
      );
    }
  }

  reconcileBriefFiles(): void {
    const directory = this.getBriefsDir();
    const files = listWorkerBriefFiles(directory);
    if (!files) return;
    const existing = new Map<string, string>();
    const unsafe = new Set<string>();
    const temporary = new Set<string>();
    for (const file of files) {
      if (file.temporary) temporary.add(file.dispatchId);
      else if (file.regularFile) existing.set(file.dispatchId, file.path);
      else unsafe.add(file.dispatchId);
    }
    const failedRemovals = new Set<string>();
    const fileOwners = new Set([...existing.keys(), ...temporary, ...unsafe]);
    for (const id of fileOwners) {
      const dispatch = this.dispatches.get(id);
      const keep = dispatch?.briefFilePath && !dispatch.releasedAt && !unsafe.has(id) &&
        dispatch.status !== 'failed' && dispatch.status !== 'circuit_broken';
      if (keep) {
        if (temporary.has(id)) removeWorkerBriefFile(directory, id, true);
      } else {
        if (!removeWorkerBriefFile(directory, id)) failedRemovals.add(id);
        existing.delete(id);
      }
    }
    let changed = false;
    for (const dispatch of this.dispatches.values()) {
      if (!dispatch.briefFilePath || failedRemovals.has(dispatch.id)) continue;
      const path = existing.get(dispatch.id) ?? null;
      if (path === dispatch.briefFilePath) continue;
      dispatch.briefFilePath = path;
      changed = true;
    }
    if (changed) this.persist();
  }

  cleanupDispatchBrief(dispatchId: string): void {
    const dispatch = this.dispatches.get(dispatchId);
    if (dispatch?.briefFilePath && removeWorkerBriefFile(this.getBriefsDir(), dispatchId)) {
      dispatch.briefFilePath = null;
      this.persist();
    }
  }

  getClockMs(): number {
    const parsed = Date.parse(this.now());
    return Number.isFinite(parsed) ? parsed : Date.now();
  }

  bindDefaultClock(nowIso: () => string): void {
    if (!this.customClock) {
      this.now = nowIso;
    }
  }

  static forUserDataDir(getUserDataDir: () => string): OrchestrationStore {
    const store = new OrchestrationStore({
      resolveFile: () => join(getUserDataDir(), ORCHESTRATION_STATE_FILENAME),
    });
    store.load();
    return store;
  }

  /** Set when load() had to fall back to the backup or quarantine an unreadable snapshot. */
  lastLoadRecovery: string | null = null;

  load(): void {
    const filePath = this.resolveFile?.();
    if (!filePath) {
      return;
    }

    for (const candidate of [filePath, `${filePath}.bak`]) {
      if (!existsSync(candidate)) {
        continue;
      }
      let raw: string;
      try {
        raw = readFileSync(candidate, 'utf8');
      } catch (error) {
        console.warn(`[orchestration] failed to read ${candidate}:`, error);
        continue;
      }
      if (!raw.trim()) {
        continue;
      }
      try {
        const parsed = JSON.parse(raw) as Partial<OrchestrationPersistedState>;
        if (!parsed || parsed.version !== 1) {
          throw new Error('Unsupported orchestration snapshot version');
        }
        this.hydrate(parsed);
        this.writer?.primeCommittedContent(raw);
        if (candidate !== filePath) {
          this.lastLoadRecovery = `Primary snapshot unreadable; recovered from ${candidate}`;
          console.warn(`[orchestration] ${this.lastLoadRecovery}`);
          // Republish immediately so the next launch reads a healthy primary snapshot.
          this.quarantineUnreadablePrimary(filePath);
          // The primary no longer matches the backup, so the unchanged-write skip must not apply.
          this.writer?.primeCommittedContent(null);
          this.flushSync();
        }
        this.reconcileBriefFiles();
        return;
      } catch (error) {
        console.warn(`[orchestration] failed to parse ${candidate}:`, error);
      }
    }

    if (existsSync(filePath)) {
      this.lastLoadRecovery = 'Orchestration snapshot and backup were both unreadable';
      console.warn(`[orchestration] ${this.lastLoadRecovery}; state was not loaded`);
      this.quarantineUnreadablePrimary(filePath);
    }
    this.reconcileBriefFiles();
  }

  private quarantineUnreadablePrimary(filePath: string): void {
    if (!existsSync(filePath)) {
      return;
    }
    try {
      renameSync(filePath, `${filePath}.corrupt-${Date.now()}`);
    } catch (error) {
      console.warn('[orchestration] failed to quarantine unreadable snapshot:', error);
    }
  }

  private hydrate(parsed: Partial<OrchestrationPersistedState>): void {
    this.nextSequence = typeof parsed.nextSequence === 'number' && parsed.nextSequence > 0
      ? parsed.nextSequence
      : 1;
    this.currentRunByConsumer.clear();
    if (parsed.currentRunByConsumer && typeof parsed.currentRunByConsumer === 'object') {
      for (const [k, v] of Object.entries(parsed.currentRunByConsumer)) {
        if (typeof v === 'string') {
          this.currentRunByConsumer.set(k, v);
        }
      }
    }

    this.runs.clear();
    for (const run of parsed.runs ?? []) {
      if (run && typeof run.id === 'string') {
        this.runs.set(run.id, cloneRunRecord(run));
      }
    }

    this.tasks.clear();
    for (const task of parsed.tasks ?? []) {
      if (task && typeof task.id === 'string') {
        const normalizedContext = cloneTaskContext(task.taskContext);
        this.tasks.set(task.id, {
          ...task,
          deps: [...(task.deps ?? [])],
          ...(normalizedContext ? { taskContext: normalizedContext } : {}),
        });
      }
    }

    this.dispatches.clear();
    for (const dispatch of parsed.dispatches ?? []) {
      if (dispatch && typeof dispatch.id === 'string') {
        const defaultStage: OrchestrationDispatchStage = dispatch.releasedAt
          ? 'released'
          : dispatch.status === 'completed'
            ? 'done'
            : dispatch.status === 'failed' || dispatch.status === 'circuit_broken'
              ? dispatch.lastFailureReason === 'worker_stopped'
                ? 'stopped'
                : 'failed'
              : dispatch.startObservation === 'permission'
                ? 'waiting_permission'
                : dispatch.startObservation === 'observed'
                  ? 'working'
                  : 'starting';
        this.dispatches.set(dispatch.id, {
          ...dispatch,
          creatorDispatchId: dispatch.creatorDispatchId ?? null,
          parentPaneId: dispatch.parentPaneId ?? null,
          worktreePath: dispatch.worktreePath ?? null,
          branch: dispatch.branch ?? null,
          ownsWorktree: dispatch.ownsWorktree ?? false,
          ownsPane: dispatch.ownsPane ?? false,
          cliId: dispatch.cliId ?? null,
          model: dispatch.model ?? null,
          effort: dispatch.effort ?? null,
          processGeneration: dispatch.processGeneration ?? 1,
          stage: dispatch.stage ?? defaultStage,
          turnStopReportedGeneration: dispatch.turnStopReportedGeneration ?? null,
          followUpOfDispatchId: dispatch.followUpOfDispatchId ?? null,
          retained: dispatch.retained ?? false,
          releasedAt: dispatch.releasedAt ?? null,
          archivedOutput: dispatch.archivedOutput ?? null,
          startObservation: dispatch.startObservation ?? 'pending',
          startObservationReason: dispatch.startObservationReason ?? null,
          turnStartedAt: dispatch.turnStartedAt ?? null,
          promptSentAt: dispatch.promptSentAt ?? null,
          queuedWorkingBaseline: dispatch.queuedWorkingBaseline ?? null,
          staleWarningAt: dispatch.staleWarningAt ?? null,
        });
      }
    }

    this.gates.clear();
    for (const gate of parsed.gates ?? []) {
      if (gate && typeof gate.id === 'string') {
        this.gates.set(gate.id, { ...gate, options: [...(gate.options ?? [])] });
      }
    }

    this.nextBoardSeqByRun.clear();
    if (parsed.nextBoardSeqByRun && typeof parsed.nextBoardSeqByRun === 'object') {
      for (const [runId, nextSeq] of Object.entries(parsed.nextBoardSeqByRun)) {
        if (typeof nextSeq === 'number' && Number.isInteger(nextSeq) && nextSeq > 0) {
          this.nextBoardSeqByRun.set(runId, nextSeq);
        }
      }
    }

    this.memberCursorsByRun.clear();
    if (parsed.memberCursorsByRun && typeof parsed.memberCursorsByRun === 'object') {
      for (const [runId, byMember] of Object.entries(parsed.memberCursorsByRun)) {
        if (!byMember || typeof byMember !== 'object') continue;
        const memberMap = new Map<string, OrchestrationMemberCursor>();
        for (const [memberKey, rawCursor] of Object.entries(byMember)) {
          if (!rawCursor || typeof rawCursor !== 'object') continue;
          memberMap.set(memberKey, {
            lastReadSeq:
              typeof rawCursor.lastReadSeq === 'number' && Number.isFinite(rawCursor.lastReadSeq)
                ? Math.max(0, Math.floor(rawCursor.lastReadSeq))
                : 0,
            lastPostSeq:
              typeof rawCursor.lastPostSeq === 'number' && Number.isFinite(rawCursor.lastPostSeq)
                ? Math.max(0, Math.floor(rawCursor.lastPostSeq))
                : 0,
            lastDeliveredSeq:
              typeof rawCursor.lastDeliveredSeq === 'number' &&
              Number.isFinite(rawCursor.lastDeliveredSeq)
                ? Math.max(0, Math.floor(rawCursor.lastDeliveredSeq))
                : 0,
          });
        }
        if (memberMap.size > 0) {
          this.memberCursorsByRun.set(runId, memberMap);
        }
      }
    }

    this.messages.clear();
    this.messageOrder = [];
    const sortedMsgs = [...(parsed.messages ?? [])].sort((a, b) => a.seq - b.seq);
    for (const msg of sortedMsgs) {
      if (msg && typeof msg.id === 'string') {
        const cloned = cloneMessageRecord(msg);
        if (cloned.type !== 'heartbeat') {
          if (
            typeof cloned.boardSeq === 'number' &&
            Number.isInteger(cloned.boardSeq) &&
            cloned.boardSeq > 0
          ) {
            const curNext = this.nextBoardSeqByRun.get(cloned.runId) ?? 1;
            if (cloned.boardSeq >= curNext) {
              this.nextBoardSeqByRun.set(cloned.runId, cloned.boardSeq + 1);
            }
          } else {
            const assigned = this.nextBoardSeqByRun.get(cloned.runId) ?? 1;
            cloned.boardSeq = assigned;
            this.nextBoardSeqByRun.set(cloned.runId, assigned + 1);
          }
        }
        this.messages.set(cloned.id, cloned);
        this.messageOrder.push(cloned.id);
        if (cloned.seq >= this.nextSequence) {
          this.nextSequence = cloned.seq + 1;
        }
      }
    }

    this.deliveries.clear();
    for (const delivery of parsed.deliveries ?? []) {
      if (delivery && typeof delivery.id === 'string') {
        this.deliveries.set(delivery.id, { ...delivery, messageIds: [...(delivery.messageIds ?? [])] });
      }
    }

    this.consumerReadByMessageId.clear();
    for (const [msgId, consumers] of Object.entries(parsed.consumerReadByMessageId ?? {})) {
      if (Array.isArray(consumers) && consumers.length > 0) {
        this.consumerReadByMessageId.set(
          msgId,
          new Set(consumers.filter((c): c is string => typeof c === 'string' && c.length > 0))
        );
      }
    }

    this.consumerDeliveredByMessageId.clear();
    for (const [msgId, consumers] of Object.entries(parsed.consumerDeliveredByMessageId ?? {})) {
      if (Array.isArray(consumers) && consumers.length > 0) {
        this.consumerDeliveredByMessageId.set(
          msgId,
          new Set(consumers.filter((c): c is string => typeof c === 'string' && c.length > 0))
        );
      }
    }

    this.questions.clear();
    for (const q of parsed.questions ?? []) {
      if (q && typeof q.messageId === 'string') {
        this.questions.set(q.messageId, { ...q, options: [...(q.options ?? [])] });
      }
    }

    this.coordinatorRuns.clear();
    const now = this.now();
    for (const cr of parsed.coordinatorRuns ?? []) {
      if (cr && typeof cr.id === 'string') {
        // After process restart, previously running coordinator loops are no longer executing.
        const recovered = cr.status === 'running' || cr.status === 'paused'
          ? { ...cr, status: 'stopped' as const, endedAt: cr.endedAt ?? now }
          : { ...cr };
        this.coordinatorRuns.set(cr.id, recovered);
      }
    }

    this.dispatchCapabilities.clear();
    for (const [dispatchId, capability] of Object.entries(parsed.capabilities ?? {})) {
      if (typeof capability === 'string' && capability) {
        this.dispatchCapabilities.set(dispatchId, capability);
      }
    }
    for (const dispatch of this.dispatches.values()) {
      if (dispatch.archivedOutput) dispatch.archivedOutput = this.redactDispatchCredentials(dispatch.archivedOutput);
      if (dispatch.archivedTimeline) {
        dispatch.archivedTimeline = mapTimelineStrings(dispatch.archivedTimeline, (text) => this.redactDispatchCredentials(text));
      }
      delete dispatch.hasArchivedTimeline;
    }
    this.deferredPrompts.clear();
    for (const [id, prompt] of Object.entries(parsed.deferredDispatchPrompts ?? {})) {
      const dispatch = this.dispatches.get(id);
      if (dispatch && ACTIVE_DISPATCH_STATUSES.has(dispatch.status) &&
          prompt && typeof prompt.prompt === 'string' && typeof prompt.submit === 'boolean' &&
          (dispatch.assigneePaneId ?? dispatch.assigneeHandle) === prompt.ptyId) {
        this.deferredPrompts.set(id, { ...prompt, prompt: this.storePromptTemplate(id, prompt.prompt) });
      }
    }
    for (const dispatch of this.dispatches.values()) {
      if (ACTIVE_DISPATCH_STATUSES.has(dispatch.status) &&
          dispatch.startObservation === 'permission' && !dispatch.promptSentAt &&
          !this.deferredPrompts.has(dispatch.id)) {
        this.failDispatch(dispatch.id, 'Deferred prompt delivery could not be recovered after restart', {
          forceFailTask: true,
          summary: 'Check the worker terminal before retrying delivery.',
        });
      }
    }

    for (const run of this.runs.values()) {
      if (run.id === UNBOUND_RUN_ID || run.status !== 'active') continue;
      // Workers live in the terminal daemon or their own processes and can outlast the app; their
      // runs stay open so late reports land, and exit reconciliation settles them otherwise.
      const hasLiveWork = [...this.dispatches.values()].some(
        (d) => d.runId === run.id && ACTIVE_DISPATCH_STATUSES.has(d.status),
      );
      if (hasLiveWork) continue;
      const runTasks = [...this.tasks.values()].filter((t) => t.runId === run.id);
      if (runTasks.length > 0 && runTasks.every(isTaskSettled)) {
        run.status = runTasks.some((t) => t.status === 'failed' || isBlockedByFailedDependency(t)) ? 'failed' : 'completed';
      } else {
        run.status = 'stopped';
      }
      run.endedAt = run.endedAt ?? now;
    }

    this.prune();
  }

  private exportPersistedState(): OrchestrationPersistedState {
    return {
      version: 1,
      nextSequence: this.nextSequence,
      currentRunByConsumer: Object.fromEntries(this.currentRunByConsumer.entries()),
      runs: [...this.runs.values()],
      tasks: [...this.tasks.values()],
      dispatches: [...this.dispatches.values()],
      gates: [...this.gates.values()],
      messages: this.messageOrder
        .map((id) => this.messages.get(id))
        .filter((m): m is OrchestrationMessageRecord => m !== undefined),
      deliveries: [...this.deliveries.values()],
      consumerReadByMessageId: Object.fromEntries(
        [...this.consumerReadByMessageId.entries()].map(([k, v]) => [k, [...v]])
      ),
      consumerDeliveredByMessageId: Object.fromEntries(
        [...this.consumerDeliveredByMessageId.entries()].map(([k, v]) => [k, [...v]])
      ),
      questions: [...this.questions.values()],
      coordinatorRuns: [...this.coordinatorRuns.values()],
      capabilities: Object.fromEntries(this.dispatchCapabilities),
      deferredDispatchPrompts: Object.fromEntries(this.listDeferredDispatchPrompts()),
      nextBoardSeqByRun: Object.fromEntries(this.nextBoardSeqByRun.entries()),
      memberCursorsByRun: this.exportMemberCursorsByRun(),
    };
  }

  private exportMemberCursorsByRun(
    filterRunId?: string
  ): Record<string, Record<string, OrchestrationMemberCursor>> {
    const out: Record<string, Record<string, OrchestrationMemberCursor>> = {};
    for (const [runId, memberMap] of this.memberCursorsByRun.entries()) {
      if (filterRunId && runId !== filterRunId) continue;
      const entries: Record<string, OrchestrationMemberCursor> = {};
      for (const [memberKey, cursor] of memberMap.entries()) {
        entries[memberKey] = { ...cursor };
      }
      out[runId] = entries;
    }
    return out;
  }

  private allocateBoardSeq(runId: string): number {
    const next = this.nextBoardSeqByRun.get(runId) ?? 1;
    this.nextBoardSeqByRun.set(runId, next + 1);
    return next;
  }

  getHeadBoardSeq(runId: string): number {
    const next = this.nextBoardSeqByRun.get(runId);
    return next && next > 1 ? next - 1 : 0;
  }

  getMemberCursor(runId: string, memberKey: string): OrchestrationMemberCursor {
    const existing = this.memberCursorsByRun.get(runId)?.get(memberKey);
    return existing
      ? { ...existing }
      : { lastReadSeq: 0, lastPostSeq: 0, lastDeliveredSeq: 0 };
  }

  advanceMemberCursor(
    runId: string,
    memberKey: string,
    patch: Partial<OrchestrationMemberCursor>
  ): OrchestrationMemberCursor {
    const cleanRunId = runId.trim();
    const cleanKey = memberKey.trim();
    if (!cleanRunId || !cleanKey) {
      return { lastReadSeq: 0, lastPostSeq: 0, lastDeliveredSeq: 0 };
    }
    let memberMap = this.memberCursorsByRun.get(cleanRunId);
    if (!memberMap) {
      memberMap = new Map();
      this.memberCursorsByRun.set(cleanRunId, memberMap);
    }
    const existing = memberMap.get(cleanKey) ?? {
      lastReadSeq: 0,
      lastPostSeq: 0,
      lastDeliveredSeq: 0,
    };
    const nextRead =
      typeof patch.lastReadSeq === 'number' && Number.isFinite(patch.lastReadSeq)
        ? Math.max(existing.lastReadSeq, Math.floor(patch.lastReadSeq))
        : existing.lastReadSeq;
    const nextPost =
      typeof patch.lastPostSeq === 'number' && Number.isFinite(patch.lastPostSeq)
        ? Math.max(existing.lastPostSeq, Math.floor(patch.lastPostSeq))
        : existing.lastPostSeq;
    const nextDelivered =
      typeof patch.lastDeliveredSeq === 'number' && Number.isFinite(patch.lastDeliveredSeq)
        ? Math.max(existing.lastDeliveredSeq, Math.floor(patch.lastDeliveredSeq))
        : existing.lastDeliveredSeq;

    const changed =
      !memberMap.has(cleanKey) ||
      nextRead !== existing.lastReadSeq ||
      nextPost !== existing.lastPostSeq ||
      nextDelivered !== existing.lastDeliveredSeq;

    const updated: OrchestrationMemberCursor = {
      lastReadSeq: nextRead,
      lastPostSeq: nextPost,
      lastDeliveredSeq: nextDelivered,
    };
    if (changed) {
      memberMap.set(cleanKey, updated);
      this.persist();
    }
    return { ...updated };
  }

  listBoardPosts(
    runId: string,
    opts?: { since?: number; seqs?: readonly number[]; limit?: number }
  ): OrchestrationMessageRecord[] {
    const allForRun: OrchestrationMessageRecord[] = [];
    for (const id of this.messageOrder) {
      const msg = this.messages.get(id);
      if (!msg || msg.runId !== runId || msg.type === 'heartbeat') continue;
      if (msg.lifecycleRejectedReason) continue;
      if (typeof msg.boardSeq !== 'number') continue;
      allForRun.push(msg);
    }
    allForRun.sort((a, b) => (a.boardSeq ?? 0) - (b.boardSeq ?? 0));

    if (opts?.seqs && opts.seqs.length > 0) {
      const seqSet = new Set(opts.seqs);
      return allForRun
        .filter((msg) => msg.boardSeq !== undefined && seqSet.has(msg.boardSeq))
        .map(cloneMessageRecord);
    }

    const limit = Math.max(1, Math.min(500, Math.floor(opts?.limit ?? 50)));
    if (typeof opts?.since === 'number' && Number.isFinite(opts.since)) {
      const since = Math.max(0, Math.floor(opts.since));
      return allForRun
        .filter((msg) => (msg.boardSeq ?? 0) > since)
        .slice(0, limit)
        .map(cloneMessageRecord);
    }

    return allForRun.slice(-limit).map(cloneMessageRecord);
  }

  private persist(): void {
    this.writer?.write(() => JSON.stringify(this.exportPersistedState()));
  }

  listDeferredDispatchPrompts() {
    return [...this.deferredPrompts.entries()].filter(([id]) => {
      const dispatch = this.dispatches.get(id);
      return dispatch && ACTIVE_DISPATCH_STATUSES.has(dispatch.status);
    });
  }

  private storePromptTemplate(id: string, prompt: string): string {
    const capability = this.dispatchCapabilities.get(id);
    const template = capability ? prompt.split(capability).join(DEFERRED_CREDENTIAL_SLOT) : prompt;
    return this.redactDispatchCredentials(template);
  }

  restoreDeferredDispatchPrompt(id: string, template: string): string {
    return template.replaceAll(DEFERRED_CREDENTIAL_SLOT, this.dispatchCapabilities.get(id) ?? '<redacted>');
  }

  saveDeferredDispatchPrompt(id: string, prompt: { ptyId: string; prompt: string; submit: boolean }): void {
    const dispatch = this.dispatches.get(id);
    if (!dispatch || !ACTIVE_DISPATCH_STATUSES.has(dispatch.status)) return;
    this.deferredPrompts.set(id, { ...prompt, prompt: this.storePromptTemplate(id, prompt.prompt) });
    this.updateDispatchObservation(id, 'permission', { refreshHeartbeat: true });
    this.persist();
  }

  deleteDeferredDispatchPrompt(id: string): void {
    if (this.deferredPrompts.delete(id)) this.persist();
  }

  flushSync(): void {
    this.writer?.writeSync(() => JSON.stringify(this.exportPersistedState()));
  }

  async waitForPendingWrite(): Promise<void> {
    await this.writer?.waitForPendingWrite();
  }

  close(): void {
    this.flushSync();
    this.writer?.close();
  }

  // ==================== Runs ====================

  private ensureUnboundRun(): OrchestrationRunRecord {
    const existing = this.runs.get(UNBOUND_RUN_ID);
    if (existing) {
      return existing;
    }
    const now = this.now();
    const run: OrchestrationRunRecord = {
      id: UNBOUND_RUN_ID,
      objective: 'Unbound direct orchestration mail',
      status: 'active',
      coordinatorHandle: 'coordinator',
      coordinatorPaneId: null,
      consumerGeneration: 1,
      createdAt: now,
      updatedAt: now,
      endedAt: null,
    };
    this.runs.set(run.id, run);
    return run;
  }

  createRun(input: {
    id?: string;
    objective: string;
    coordinatorHandle: string;
    coordinatorPaneId?: string | null;
  }): OrchestrationRunRecord {
    const id = input.id?.trim() || generatePrefixedId('run');
    if (this.runs.has(id)) {
      throw new OrchestrationError('duplicate_run', `Run already exists: ${id}`);
    }
    const now = this.now();
    const run: OrchestrationRunRecord = {
      id,
      objective: input.objective,
      status: 'active',
      coordinatorHandle: input.coordinatorHandle,
      coordinatorPaneId: input.coordinatorPaneId ?? null,
      consumerGeneration: 1,
      createdAt: now,
      updatedAt: now,
      endedAt: null,
    };
    this.runs.set(run.id, run);
    this.bindConsumerToRun(run.id, input.coordinatorHandle, input.coordinatorPaneId ?? null);
    this.prune();
    this.persist();
    return { ...run };
  }

  private bindConsumerToRun(runId: string, handle: string, paneId: string | null): void {
    if (paneId) {
      this.currentRunByConsumer.set(paneId, runId);
    }
    if (handle) {
      this.currentRunByConsumer.set(handle, runId);
    }
  }

  bindRun(input: {
    runId: string;
    coordinatorHandle: string;
    coordinatorPaneId?: string | null;
  }): OrchestrationRunRecord | null {
    const run = this.runs.get(input.runId);
    if (!run || run.id === UNBOUND_RUN_ID) {
      return null;
    }
    const now = this.now();
    const prevHandle = run.coordinatorHandle;
    const prevPaneId = run.coordinatorPaneId;
    if (prevHandle && this.currentRunByConsumer.get(prevHandle) === run.id) {
      this.currentRunByConsumer.delete(prevHandle);
    }
    if (prevPaneId && this.currentRunByConsumer.get(prevPaneId) === run.id) {
      this.currentRunByConsumer.delete(prevPaneId);
    }

    run.coordinatorHandle = input.coordinatorHandle;
    run.coordinatorPaneId = input.coordinatorPaneId ?? null;
    run.consumerGeneration += 1;
    run.updatedAt = now;
    this.bindConsumerToRun(run.id, input.coordinatorHandle, input.coordinatorPaneId ?? null);

    const runMailbox = `run:${run.id}`;
    for (const d of this.deliveries.values()) {
      if (
        d.mailboxHandle === runMailbox &&
        d.status === 'pending' &&
        d.consumerGeneration < run.consumerGeneration
      ) {
        d.status = 'fenced';
        for (const msgId of d.messageIds) {
          const msg = this.messages.get(msgId);
          if (msg && !msg.read) {
            if (d.consumerHandle) {
              this.consumerDeliveredByMessageId.get(msgId)?.delete(d.consumerHandle);
            }
            if (msg.deliveredAt !== null) {
              msg.deliveredAt = null;
            }
          }
        }
      }
    }

    this.persist();
    return { ...run };
  }

  getConsumersForRun(runId: string): string[] {
    const consumers = new Set<string>();
    const run = this.runs.get(runId);
    if (run) {
      if (run.coordinatorPaneId) consumers.add(run.coordinatorPaneId);
      if (run.coordinatorHandle) consumers.add(run.coordinatorHandle);
    }
    for (const [consumer, boundRunId] of this.currentRunByConsumer.entries()) {
      if (boundRunId === runId) {
        consumers.add(consumer);
      }
    }
    return [...consumers];
  }

  getRun(runId: string): OrchestrationRunRecord | undefined {
    const run = this.runs.get(runId);
    return run ? cloneRunRecord(run) : undefined;
  }

  requireRun(runId: string): OrchestrationRunRecord {
    const run = this.runs.get(runId);
    if (!run) {
      throw new OrchestrationError('run_not_found', `Run ${runId} was not found.`);
    }
    return cloneRunRecord(run);
  }

  getCurrentRunForConsumer(handleOrPane: string): OrchestrationRunRecord | undefined {
    const direct = this.currentRunByConsumer.get(handleOrPane);
    if (direct) {
      const run = this.runs.get(direct);
      if (run && run.id !== UNBOUND_RUN_ID) {
        return cloneRunRecord(run);
      }
    }
    return undefined;
  }

  listRuns(filter?: { status?: string }): OrchestrationRunRecord[] {
    return [...this.runs.values()]
      .filter((r) => r.id !== UNBOUND_RUN_ID && (!filter?.status || r.status === filter.status))
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map(cloneRunRecord);
  }

  updateRunStatus(
    runId: string,
    status: OrchestrationRunRecord['status']
  ): OrchestrationRunRecord {
    const run = this.runs.get(runId);
    if (!run) {
      throw new OrchestrationError('run_not_found', `Run ${runId} was not found.`);
    }
    const now = this.now();
    run.status = status;
    run.updatedAt = now;
    if (status === 'completed' || status === 'failed' || status === 'stopped') {
      run.endedAt = run.endedAt ?? now;
    } else if (status === 'active') {
      run.endedAt = null;
    }
    this.prune();
    this.persist();
    return cloneRunRecord(run);
  }

  updateRunIntegrationState(
    runId: string,
    patch: {
      integrationBranch?: string | null;
      integrationWorktreePath?: string | null;
      integrationBaseCommit?: string | null;
      integrationBaseBranch?: string | null;
      integrationRepoRootPath?: string | null;
      integrationStatus?: OrchestrationRunIntegrationStatus;
      integrationHeadCommit?: string | null;
      finalizedCommit?: string | null;
    },
  ): OrchestrationRunRecord | null {
    const run = this.runs.get(runId);
    if (!run) return null;
    if (patch.integrationBranch !== undefined) run.integrationBranch = patch.integrationBranch;
    if (patch.integrationWorktreePath !== undefined) {
      run.integrationWorktreePath = patch.integrationWorktreePath;
    }
    if (patch.integrationBaseCommit !== undefined) {
      run.integrationBaseCommit = patch.integrationBaseCommit;
    }
    if (patch.integrationBaseBranch !== undefined) {
      run.integrationBaseBranch = patch.integrationBaseBranch;
    }
    if (patch.integrationRepoRootPath !== undefined) {
      run.integrationRepoRootPath = patch.integrationRepoRootPath;
    }
    if (patch.integrationStatus !== undefined) run.integrationStatus = patch.integrationStatus;
    if (patch.integrationHeadCommit !== undefined) {
      run.integrationHeadCommit = patch.integrationHeadCommit;
    }
    if (patch.finalizedCommit !== undefined) run.finalizedCommit = patch.finalizedCommit;
    run.updatedAt = this.now();
    this.persist();
    return cloneRunRecord(run);
  }

  updateRunReviewState(
    runId: string,
    patch: {
      runReviewBatchId?: string | null;
      runReviewSnapshotOid?: string | null;
      runReviewBaseCommit?: string | null;
      runReviewStatus?: OrchestrationReviewStatus;
      runReviewRound?: number;
      runReviewFindings?: OrchestrationReviewFinding[];
    },
  ): OrchestrationRunRecord | null {
    const run = this.runs.get(runId);
    if (!run) return null;
    if (patch.runReviewBatchId !== undefined) run.runReviewBatchId = patch.runReviewBatchId;
    if (patch.runReviewSnapshotOid !== undefined) {
      run.runReviewSnapshotOid = patch.runReviewSnapshotOid;
    }
    if (patch.runReviewBaseCommit !== undefined) {
      run.runReviewBaseCommit = patch.runReviewBaseCommit;
    }
    if (patch.runReviewStatus !== undefined) run.runReviewStatus = patch.runReviewStatus;
    if (patch.runReviewRound !== undefined) run.runReviewRound = patch.runReviewRound;
    if (patch.runReviewFindings !== undefined) {
      run.runReviewFindings = patch.runReviewFindings.map((f) => ({ ...f }));
    }
    run.updatedAt = this.now();
    this.persist();
    return cloneRunRecord(run);
  }

  setRunAutoReviewAuthorized(
    runId: string,
    authorized: boolean,
  ): OrchestrationRunRecord | null {
    const run = this.runs.get(runId);
    if (!run) return null;
    run.reviewAutoAuthorized = Boolean(authorized);
    run.updatedAt = this.now();
    this.persist();
    return cloneRunRecord(run);
  }

  // ==================== Tasks ====================

  createTask(input: {
    id?: string;
    runId: string;
    spec: string;
    taskContext?: OrchestrationTaskContext | null;
    taskTitle?: string | null;
    displayName?: string | null;
    deps?: string[];
    priority?: OrchestrationMessagePriority;
    worktreePath?: string | null;
    singleShot?: boolean;
  }): OrchestrationTaskRecord {
    const run = this.requireRun(input.runId);
    if (run.id === UNBOUND_RUN_ID) {
      throw new OrchestrationError('invalid_argument', 'Cannot create tasks in the unbound run.');
    }
    const id = input.id?.trim() || generatePrefixedId('task');
    if (this.tasks.has(id)) {
      throw new OrchestrationError('duplicate_task', `Task already exists: ${id}`);
    }
    const dedupedDeps = [...new Set((input.deps ?? []).map((d) => d.trim()).filter(Boolean))];
    if (dedupedDeps.includes(id)) {
      throw new OrchestrationError('task_cycle', `Task ${id} cannot depend on itself.`);
    }
    for (const depId of dedupedDeps) {
      const depTask = this.tasks.get(depId);
      if (!depTask) {
        throw new OrchestrationError('task_not_found', `Dependency task not found: ${depId}`);
      }
      if (depTask.runId !== input.runId) {
        throw new OrchestrationError(
          'invalid_argument',
          `Dependency ${depId} belongs to run ${depTask.runId}, not ${input.runId}.`
        );
      }
    }
    if (this.wouldIntroduceCycle(id, dedupedDeps)) {
      throw new OrchestrationError(
        'cyclic_dependency',
        `Creating task ${id} would introduce a cyclic dependency.`
      );
    }
    const allDepsCompleted = dedupedDeps.every(
      (depId) => this.tasks.get(depId)?.status === 'completed'
    );
    const display = buildOrchestrationTaskDisplayMetadata({
      spec: input.spec,
      taskTitle: input.taskTitle,
      displayName: input.displayName,
    });
    const normalizedContext = normalizeTaskContext({ taskContext: input.taskContext });
    const now = this.now();
    const task: OrchestrationTaskRecord = {
      id,
      runId: input.runId,
      spec: this.redactDispatchCredentials(input.spec),
      ...(normalizedContext ? { taskContext: normalizedContext } : {}),
      taskTitle: display.taskTitle,
      displayName: display.displayName,
      deps: dedupedDeps,
      priority: input.priority ?? 'normal',
      status: allDepsCompleted ? 'ready' : 'pending',
      ...(input.singleShot ? { singleShot: true } : {}),
      worktreePath: input.worktreePath ?? null,
      createdAt: now,
      updatedAt: now,
    };
    this.tasks.set(task.id, task);
    this.persist();
    return cloneTaskRecord(task);
  }

  private wouldIntroduceCycle(newTaskId: string, newDeps: readonly string[]): boolean {
    const visiting = new Set<string>([newTaskId]);
    const visited = new Set<string>();
    const dfs = (node: string, depth: number): boolean => {
      const deps = node === newTaskId ? newDeps : (this.tasks.get(node)?.deps ?? []);
      for (const dep of deps) {
        if (dep === newTaskId && depth >= 1) {
          return true;
        }
        if (visiting.has(dep)) {
          return true;
        }
        if (visited.has(dep)) {
          continue;
        }
        visiting.add(dep);
        if (dfs(dep, depth + 1)) {
          return true;
        }
        visiting.delete(dep);
        visited.add(dep);
      }
      return false;
    };
    return dfs(newTaskId, 0);
  }

  getTask(taskId: string): OrchestrationTaskRecord | undefined {
    const task = this.tasks.get(taskId);
    return task ? cloneTaskRecord(task) : undefined;
  }

  requireTask(taskId: string): OrchestrationTaskRecord {
    const task = this.getTask(taskId);
    if (!task) {
      throw new OrchestrationError('task_not_found', `Task not found: ${taskId}`);
    }
    return task;
  }

  listTasks(filter?: {
    runId?: string;
    status?: OrchestrationTaskStatus;
    priority?: OrchestrationMessagePriority;
  }): OrchestrationTaskRecord[] {
    return [...this.tasks.values()]
      .filter((t) => {
        if (filter?.runId && t.runId !== filter.runId) return false;
        if (filter?.status && t.status !== filter.status) return false;
        if (filter?.priority && t.priority !== filter.priority) return false;
        return true;
      })
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((t) => cloneTaskRecord(t));
  }

  listTasksWithDispatch(filter?: {
    runId?: string;
    status?: OrchestrationTaskStatus;
  }): OrchestrationTaskWithDispatch[] {
    return this.listTasks(filter).map((task) => ({
      ...task,
      latestDispatch: this.getLatestDispatchForTask(task.id) ?? null,
    }));
  }

  /**
   * Moves the DAG forward after any task change: blocks dependents of failed tasks (and unblocks
   * them once the dependency is retried) and promotes pending tasks whose deps completed.
   */
  promoteReadyTasks(runId?: string): OrchestrationTaskRecord[] {
    const promoted: OrchestrationTaskRecord[] = [];
    const now = this.now();
    const changed = this.reconcileDependencyBlocks(runId, now);
    for (const task of this.tasks.values()) {
      if (runId && task.runId !== runId) continue;
      if (task.status !== 'pending') continue;
      if (this.hasPendingGates(task.id)) continue;
      const depsDone = task.deps.every((depId) => this.tasks.get(depId)?.status === 'completed');
      if (!depsDone) continue;
      assertTaskTransition(task.id, task.status, 'ready');
      task.status = 'ready';
      task.updatedAt = now;
      promoted.push({ ...task, deps: [...task.deps] });
    }
    if (promoted.length > 0 || changed) {
      this.persist();
    }
    return promoted;
  }

  private reconcileDependencyBlocks(runId: string | undefined, now: string): boolean {
    let changed = false;
    for (let pass = 0, moved = true; moved && pass <= this.tasks.size; pass += 1) {
      moved = false;
      for (const task of this.tasks.values()) {
        if (runId && task.runId !== runId) continue;
        if (task.status !== 'blocked' && task.blockedReason) {
          task.blockedReason = null;
          changed = true;
        }
        const failedDep = task.deps.find((depId) => {
          const dep = this.tasks.get(depId);
          return Boolean(dep && (dep.status === 'failed' || isBlockedByFailedDependency(dep)));
        });
        if (task.status === 'pending' && failedDep) {
          task.status = 'blocked';
          task.blockedReason = `${DEPENDENCY_FAILED_REASON}:${failedDep}`;
          task.updatedAt = now;
          moved = true;
        } else if (isBlockedByFailedDependency(task) && !failedDep && !this.hasPendingGates(task.id)) {
          task.status = 'pending';
          task.blockedReason = null;
          task.updatedAt = now;
          moved = true;
        }
      }
      changed ||= moved;
    }
    return changed;
  }

  mergeTaskContext(taskId: string, override: OrchestrationTaskContext): OrchestrationTaskRecord {
    const task = this.tasks.get(taskId);
    if (!task) throw new OrchestrationError('task_not_found', 'Task not found: ' + taskId);
    task.taskContext = mergeTaskContext(task.taskContext, override);
    task.updatedAt = this.now();
    this.persist();
    return cloneTaskRecord(task);
  }

  updateTaskStatus(taskId: string, status: OrchestrationTaskStatus): OrchestrationTaskRecord {
    const task = this.tasks.get(taskId);
    if (!task) {
      throw new OrchestrationError('task_not_found', `Task not found: ${taskId}`);
    }
    if (task.status === status) {
      if (status === 'completed') {
        this.promoteReadyTasks(task.runId);
      }
      return { ...task, deps: [...task.deps] };
    }
    if (status === 'ready' && this.hasPendingGates(taskId)) {
      throw new OrchestrationError(
        'invalid_transition',
        `Task ${taskId} has pending decision gates and cannot become ready.`
      );
    }
    assertTaskTransition(task.id, task.status, status);
    const now = this.now();
    task.status = status;
    task.blockedReason = null;
    task.updatedAt = now;

    if (status === 'completed' || status === 'failed') {
      this.settleActiveDispatchesForTask(
        taskId,
        status === 'completed' ? 'completed' : 'failed',
        `Task marked ${status}`
      );
    }
    this.promoteReadyTasks(task.runId);
    this.persist();
    return { ...task, deps: [...task.deps] };
  }

  // ==================== Dispatches & Circuit Breaker ====================

  getDispatchContext(dispatchId: string): OrchestrationDispatchRecord | undefined {
    const ctx = this.dispatches.get(dispatchId);
    return ctx ? { ...ctx } : undefined;
  }

  getLatestDispatchForTask(taskId: string): OrchestrationDispatchRecord | undefined {
    const matches = [...this.dispatches.values()]
      .filter((d) => d.taskId === taskId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt));
    const last = matches[matches.length - 1];
    return last ? { ...last } : undefined;
  }

  getActiveDispatchesForTask(taskId: string): OrchestrationDispatchRecord[] {
    return [...this.dispatches.values()]
      .filter((d) => d.taskId === taskId && ACTIVE_DISPATCH_STATUSES.has(d.status))
      .map((d) => ({ ...d }));
  }

  getActiveDispatchForIdentity(
    handle: string,
    paneId?: string | null
  ): OrchestrationDispatchRecord | undefined {
    for (const d of this.dispatches.values()) {
      if (!ACTIVE_DISPATCH_STATUSES.has(d.status)) continue;
      if (paneId && d.assigneePaneId === paneId) {
        return { ...d };
      }
      if (d.assigneeHandle === handle || (d.assigneePaneId && d.assigneePaneId === handle)) {
        return { ...d };
      }
    }
    return undefined;
  }

  listDispatches(filter?: {
    runId?: string;
    taskId?: string;
    status?: OrchestrationDispatchRecord['status'];
  }): OrchestrationDispatchRecord[] {
    return [...this.dispatches.values()]
      .filter((d) => {
        if (filter?.runId && d.runId !== filter.runId) return false;
        if (filter?.taskId && d.taskId !== filter.taskId) return false;
        if (filter?.status && d.status !== filter.status) return false;
        return true;
      })
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((d) => ({ ...d }));
  }

  resolveChildDispatchDepth(creatorDispatchId?: string | null): number {
    if (!creatorDispatchId) {
      return 1;
    }
    const clean = creatorDispatchId.startsWith('dispatch:')
      ? creatorDispatchId.slice('dispatch:'.length)
      : creatorDispatchId;
    const parent = this.dispatches.get(clean);
    return parent ? parent.depth + 1 : 1;
  }

  createDispatchContext(input: {
    taskId: string;
    providerSessionId?: string;
    assigneeHandle: string;
    assigneePaneId?: string | null;
    processIncarnation?: string | null;
    creator?: string;
    creatorRunId?: string | null;
    creatorDispatchId?: string | null;
    parentPaneId?: string | null;
    worktreePath?: string | null;
    branch?: string | null;
    baseCommit?: string | null;
    repoRootPath?: string | null;
    ownsWorktree?: boolean;
    ownsBranch?: boolean;
    isDetachedReviewWorktree?: boolean;
    reviewOfDispatchId?: string | null;
    reviewOfRunId?: string | null;
    reviewFocus?: string | null;
    reviewBatchId?: string | null;
    reviewSnapshotOid?: string | null;
    reviewBaseCommit?: string | null;
    reviewRound?: number;
    ownsPane?: boolean;
    cliId?: string | null;
    model?: string | null;
    effort?: string | null;
    processGeneration?: number;
    followUpOfDispatchId?: string | null;
    retained?: boolean;
    maxDepth?: number;
    /** Depth of the caller's own lineage when it is no longer an active dispatch. */
    parentDepth?: number;
    startObservation?: OrchestrationStartObservation;
    startObservationReason?: string | null;
    promptSentAt?: string | null;
    queuedWorkingBaseline?: number | null;
  }): OrchestrationDispatchRecord {
    const task = this.tasks.get(input.taskId);
    if (!task) {
      throw new OrchestrationError('task_not_found', `Task not found: ${input.taskId}`);
    }
    if (task.status !== 'ready') {
      throw new OrchestrationError(
        'task_not_ready',
        `Task ${task.id} is ${task.status}; only ready tasks can be dispatched.`,
        { taskId: task.id, status: task.status }
      );
    }
    if (this.hasPendingGates(task.id)) {
      throw new OrchestrationError(
        'task_not_ready',
        `Task ${task.id} has pending decision gates.`
      );
    }

    const maxDepth = input.maxDepth ?? DEFAULT_NESTED_WORKER_MAX_DEPTH;
    const cleanCreatorDispatchId = input.creatorDispatchId
      ? input.creatorDispatchId.startsWith('dispatch:')
        ? input.creatorDispatchId.slice('dispatch:'.length)
        : input.creatorDispatchId
      : null;
    const depth = Math.max(this.resolveChildDispatchDepth(cleanCreatorDispatchId), (input.parentDepth ?? 0) + 1);
    if (depth > maxDepth) {
      throw new OrchestrationError(
        'operation_rejected',
        `Nested worker dispatch depth ${depth} exceeds max depth ${maxDepth}.`,
        { depth, maxDepth }
      );
    }

    const priorMaxFailureCount = [...this.dispatches.values()]
      .filter((d) => d.taskId === task.id)
      .reduce((max, d) => Math.max(max, d.failureCount), 0);

    if (priorMaxFailureCount >= DISPATCH_CIRCUIT_BREAK_FAILURES) {
      assertTaskTransition(task.id, task.status, 'failed');
      task.status = 'failed';
      task.updatedAt = this.now();
      this.promoteReadyTasks(task.runId);
      this.persist();
      throw new OrchestrationError(
        'circuit_broken',
        `Task ${task.id} exceeded ${DISPATCH_CIRCUIT_BREAK_FAILURES} failed dispatches and is circuit-broken.`,
        { taskId: task.id, failureCount: priorMaxFailureCount }
      );
    }

    const activeForAssignee = this.getActiveDispatchForIdentity(
      input.assigneeHandle,
      input.assigneePaneId
    );
    if (activeForAssignee) {
      throw new OrchestrationError(
        'worker_busy',
        `Worker ${input.assigneeHandle} already holds active dispatch ${activeForAssignee.id}.`,
        { activeDispatchId: activeForAssignee.id, assigneeHandle: input.assigneeHandle }
      );
    }
    this.assertDispatchCapacity();

    const now = this.now();
    // New work reopens a run that had already settled.
    const run = this.runs.get(task.runId);
    if (run && run.id !== UNBOUND_RUN_ID && run.status !== 'active') {
      run.status = 'active';
      run.endedAt = null;
      run.updatedAt = now;
    }
    const resolvedWorktreePath = input.worktreePath ?? task.worktreePath ?? null;
    if (resolvedWorktreePath && !task.worktreePath) {
      task.worktreePath = resolvedWorktreePath;
    }
    const initialObservation = input.startObservation ?? 'pending';
    const initialStage: OrchestrationDispatchStage =
      initialObservation === 'permission'
        ? 'waiting_permission'
        : initialObservation === 'observed'
          ? 'working'
          : 'starting';

    const ctx: OrchestrationDispatchRecord = {
      id: generatePrefixedId('ctx'),
      taskId: task.id,
      runId: task.runId,
      creator: input.creator ?? 'coordinator',
      creatorRunId: input.creatorRunId ?? task.runId,
      creatorDispatchId: cleanCreatorDispatchId,
      parentPaneId: input.parentPaneId ?? null,
      depth,
      assigneeHandle: input.assigneeHandle,
      ...(input.providerSessionId ? {
        executionMode: 'codex-app-server' as const,
        providerSessionId: input.providerSessionId,
      } : {}),
      assigneePaneId: input.assigneePaneId ?? null,
      processIncarnation: input.processIncarnation ?? null,
      worktreePath: resolvedWorktreePath,
      branch: input.branch ?? null,
      ...(input.baseCommit ? { baseCommit: input.baseCommit } : {}),
      ...(input.repoRootPath ? { repoRootPath: input.repoRootPath } : {}),
      ownsWorktree: input.ownsWorktree ?? false,
      ...(input.ownsBranch ? { ownsBranch: true } : {}),
      ...(input.isDetachedReviewWorktree ? { isDetachedReviewWorktree: true } : {}),
      ...(input.reviewOfDispatchId ? { reviewOfDispatchId: input.reviewOfDispatchId } : {}),
      ...(input.reviewOfRunId ? { reviewOfRunId: input.reviewOfRunId } : {}),
      ...(input.reviewFocus ? { reviewFocus: input.reviewFocus } : {}),
      ...(input.reviewBatchId ? { reviewBatchId: input.reviewBatchId } : {}),
      ...(input.reviewSnapshotOid ? { reviewSnapshotOid: input.reviewSnapshotOid } : {}),
      ...(input.reviewBaseCommit ? { reviewBaseCommit: input.reviewBaseCommit } : {}),
      ...(typeof input.reviewRound === 'number' && input.reviewRound > 0 ? { reviewRound: input.reviewRound } : {}),
      ownsPane: input.ownsPane ?? false,
      cliId: input.cliId ?? null,
      model: input.model ?? null,
      effort: input.effort ?? null,
      processGeneration: input.processGeneration ?? 1,
      stage: initialStage,
      turnStopReportedGeneration: null,
      followUpOfDispatchId: input.followUpOfDispatchId ?? null,
      retained: input.retained ?? false,
      releasedAt: null,
      archivedOutput: null,
      status: 'dispatched',
      failureCount: priorMaxFailureCount,
      lastFailureReason: null,
      failureSummary: null,
      lastHeartbeatAt: now,
      consumerGeneration: 1,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      startObservation: initialObservation,
      startObservationReason: input.startObservationReason ?? null,
      turnStartedAt: initialObservation === 'observed' ? now : null,
      promptSentAt: input.promptSentAt ?? null,
      queuedWorkingBaseline: input.queuedWorkingBaseline ?? null,
      staleWarningAt: null,
    };

    assertTaskTransition(task.id, task.status, 'dispatched');
    task.status = 'dispatched';
    task.updatedAt = now;
    this.dispatches.set(ctx.id, ctx);
    this.dispatchCapabilities.set(ctx.id, DISPATCH_CREDENTIAL_PREFIX + randomUUID());
    this.persist();
    return { ...ctx };
  }

  /** Token the assignee must present for worker_done/heartbeat/ask; undefined for legacy dispatches. */
  getDispatchCapability(dispatchId: string): string | undefined {
    return this.dispatchCapabilities.get(dispatchId);
  }

  redactDispatchCredentials(text: string): string {
    let redacted = text;
    for (const capability of this.dispatchCapabilities.values()) {
      redacted = redacted.replaceAll(capability, '<redacted>');
    }
    return redacted.replace(PREFIXED_CREDENTIAL_RE, '<redacted>').replace(LABELLED_LEGACY_CREDENTIAL_RE, '$1<redacted>');
  }

  assertDispatchCapability(dispatchId: string, capability: string | undefined): void {
    const expected = this.dispatchCapabilities.get(dispatchId);
    if (expected === undefined) {
      // Dispatches created before capabilities existed keep the legacy handle-based authority.
      return;
    }
    if (!capability || capability !== expected) {
      throw new OrchestrationError(
        'operation_rejected',
        `A valid dispatch capability is required for dispatch ${dispatchId}.`,
        { dispatchId }
      );
    }
  }

  private promoteDispatchObserved(
    dispatch: OrchestrationDispatchRecord,
    now: string,
    refreshHeartbeat = false
  ): void {
    dispatch.startObservation = 'observed';
    dispatch.startObservationReason = null;
    dispatch.staleWarningAt = null;
    dispatch.turnStartedAt = dispatch.turnStartedAt ?? now;
    if (ACTIVE_DISPATCH_STATUSES.has(dispatch.status) && !dispatch.releasedAt) {
      dispatch.stage = 'working';
    }
    if (refreshHeartbeat) {
      dispatch.lastHeartbeatAt = now;
    }
    dispatch.updatedAt = now;
  }

  updateDispatchStage(
    dispatchId: string,
    stage: OrchestrationDispatchStage
  ): OrchestrationDispatchRecord | null {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx) return null;
    if (ctx.stage === stage) return { ...ctx };
    ctx.stage = stage;
    ctx.updatedAt = this.now();
    this.persist();
    return { ...ctx };
  }

  markTurnStopReported(
    dispatchId: string,
    generation: number,
    stopReason: WorkerTurnStopReason
  ): OrchestrationDispatchRecord | null {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || !ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      return null;
    }
    ctx.turnStopReportedGeneration = generation;
    ctx.stage =
      stopReason === 'needs-input'
        ? 'waiting_input'
        : stopReason === 'errored' || stopReason === 'exited'
          ? 'failed'
          : 'awaiting_review';
    ctx.updatedAt = this.now();
    this.persist();
    return { ...ctx };
  }

  bumpDispatchGenerationForSteering(dispatchId: string): OrchestrationDispatchRecord | null {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || !ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      return null;
    }
    const currentGen = ctx.processGeneration ?? 1;
    if (ctx.turnStopReportedGeneration === currentGen) {
      ctx.processGeneration = currentGen + 1;
      ctx.stage = 'working';
      ctx.updatedAt = this.now();
      this.persist();
    }
    return { ...ctx };
  }

  clearWaitingInputTurnStop(dispatchId: string): OrchestrationDispatchRecord | null {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || !ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      return null;
    }
    const currentGen = ctx.processGeneration ?? 1;
    if (ctx.stage === 'waiting_input' && ctx.turnStopReportedGeneration === currentGen) {
      ctx.processGeneration = currentGen + 1;
      ctx.stage = 'working';
      ctx.updatedAt = this.now();
      this.persist();
    }
    return { ...ctx };
  }

  hasActiveChildDispatches(parentDispatchId: string): boolean {
    const lineageIds = new Set<string>([parentDispatchId]);
    let prevId = this.dispatches.get(parentDispatchId)?.followUpOfDispatchId ?? null;
    while (prevId && !lineageIds.has(prevId)) {
      lineageIds.add(prevId);
      prevId = this.dispatches.get(prevId)?.followUpOfDispatchId ?? null;
    }
    for (const d of this.dispatches.values()) {
      if (d.creatorDispatchId && lineageIds.has(d.creatorDispatchId) && ACTIVE_DISPATCH_STATUSES.has(d.status)) {
        return true;
      }
    }
    return false;
  }

  getFollowUpChildDispatch(dispatchId: string): OrchestrationDispatchRecord | undefined {
    const matches = [...this.dispatches.values()]
      .filter((d) => d.followUpOfDispatchId === dispatchId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt));
    const latest = matches[matches.length - 1];
    return latest ? { ...latest } : undefined;
  }

  resolveDispatchLineage(dispatchId: string): OrchestrationDispatchRecord | undefined {
    let current = this.dispatches.get(dispatchId);
    if (!current) return undefined;
    const visited = new Set<string>([current.id]);
    while (true) {
      const next = this.getFollowUpChildDispatch(current.id);
      if (!next || visited.has(next.id)) break;
      visited.add(next.id);
      current = this.dispatches.get(next.id) ?? current;
    }
    return { ...current };
  }

  listDispatchAncestorIds(dispatchId: string): string[] {
    const ids = new Set<string>();
    let cursor: string | null = dispatchId;
    while (cursor && !ids.has(cursor)) {
      ids.add(cursor);
      cursor = this.dispatches.get(cursor)?.followUpOfDispatchId ?? null;
    }
    return [...ids];
  }

  transferCustodyToFollowUp(prevDispatchId: string, nextDispatchId: string): void {
    const prev = this.dispatches.get(prevDispatchId);
    const next = this.dispatches.get(nextDispatchId);
    if (!prev || !next) return;
    const now = this.now();
    if (prev.ownsWorktree) {
      next.ownsWorktree = true;
      prev.ownsWorktree = false;
    }
    if (prev.ownsBranch) {
      next.ownsBranch = true;
      prev.ownsBranch = false;
    }
    if (prev.baseCommit && !next.baseCommit) {
      next.baseCommit = prev.baseCommit;
    }
    if (prev.repoRootPath && !next.repoRootPath) {
      next.repoRootPath = prev.repoRootPath;
    }
    if (prev.ownsPane) {
      if (prev.assigneePaneId && prev.assigneePaneId === next.assigneePaneId) {
        next.ownsPane = true;
      }
      prev.ownsPane = false;
    }
    prev.updatedAt = now;
    next.updatedAt = now;
    this.persist();
  }

  setDispatchRetained(dispatchId: string, retained: boolean): OrchestrationDispatchRecord {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx) {
      throw new OrchestrationError('dispatch_not_found', `Dispatch not found: ${dispatchId}`);
    }
    ctx.retained = retained;
    ctx.updatedAt = this.now();
    this.persist();
    return { ...ctx };
  }

  switchUnacceptedCodexDispatchToPane(dispatchId: string, ptyId: string): OrchestrationDispatchRecord {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || ctx.executionMode !== 'codex-app-server' || ctx.startObservation !== 'pending' ||
        !ACTIVE_DISPATCH_STATUSES.has(ctx.status) || this.getActiveDispatchForIdentity(ptyId, ptyId)) {
      throw new OrchestrationError('operation_rejected', 'Cannot rebind this Codex dispatch to a terminal.');
    }
    ctx.assigneeHandle = ptyId;
    ctx.assigneePaneId = ptyId;
    ctx.ownsPane = true;
    delete ctx.executionMode;
    delete ctx.providerSessionId;
    ctx.updatedAt = this.now();
    this.persist();
    return { ...ctx };
  }

  recordCodexTurn(
    dispatchId: string,
    result: NonNullable<OrchestrationDispatchRecord['codexTurn']>,
    awaitingReview = true,
  ): void {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || ctx.executionMode !== 'codex-app-server') return;
    ctx.codexTurn = {
      ...result,
      finalText: this.redactDispatchCredentials(result.finalText),
      diff: result.diff === null ? null : this.redactDispatchCredentials(result.diff),
      error: result.error === null ? null : this.redactDispatchCredentials(result.error),
    };
    if (awaitingReview && ACTIVE_DISPATCH_STATUSES.has(ctx.status)) ctx.stage = 'awaiting_review';
    ctx.updatedAt = this.now();
    this.persist();
  }

  recordCodexDeliveryFailure(dispatchId: string, failure: NonNullable<OrchestrationDispatchRecord['codexDeliveryFailure']>): void {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || !ACTIVE_DISPATCH_STATUSES.has(ctx.status)) return;
    ctx.codexDeliveryFailure = failure;
    ctx.updatedAt = this.now();
    this.persist();
  }

  archiveDispatchOutput(dispatchId: string, output: string): void {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || !output) return;
    ctx.archivedOutput = this.redactDispatchCredentials(output);
    ctx.updatedAt = this.now();
    this.persist();
  }

  archiveDispatchTimeline(dispatchId: string, timeline: WorkerTimelineSnapshot): void {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || ctx.executionMode !== 'codex-app-server') return;
    const redacted = mapTimelineStrings(timeline, (text) => this.redactDispatchCredentials(text));
    ctx.archivedTimeline = trimTimelineSnapshot(redacted, TIMELINE_LIMITS.archiveBytes);
    ctx.updatedAt = this.now();
    this.persist();
  }

  getArchivedTimeline(dispatchId: string): WorkerTimelineSnapshot | null {
    const timeline = this.dispatches.get(dispatchId)?.archivedTimeline;
    return timeline ? structuredClone(timeline) : null;
  }

  markDispatchPaneClosed(dispatchId: string): OrchestrationDispatchRecord | null {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx) return null;
    if (!ctx.ownsPane) return { ...ctx };
    ctx.ownsPane = false;
    ctx.updatedAt = this.now();
    this.persist();
    return { ...ctx };
  }

  findLatestWorkerDoneMessage(dispatchId: string): OrchestrationMessageRecord | undefined {
    for (let i = this.messageOrder.length - 1; i >= 0; i -= 1) {
      const msg = this.messages.get(this.messageOrder[i]!);
      if (!msg || msg.type !== 'worker_done' || msg.lifecycleRejectedReason) continue;
      if (msg.fromHandle === `dispatch:${dispatchId}`) return cloneMessageRecord(msg);
      if (msg.payload) {
        try {
          const parsed = JSON.parse(msg.payload) as { dispatchId?: unknown };
          if (parsed?.dispatchId === dispatchId) return cloneMessageRecord(msg);
        } catch {
          /* ignore */
        }
      }
    }
    return undefined;
  }

  markDispatchReleased(
    dispatchId: string,
    opts?: { keepPane?: boolean; keepWorktree?: boolean }
  ): OrchestrationDispatchRecord {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx) {
      throw new OrchestrationError('dispatch_not_found', `Dispatch not found: ${dispatchId}`);
    }
    const now = this.now();
    ctx.releasedAt = ctx.releasedAt ?? now;
    ctx.stage = 'released';
    if (!opts?.keepPane) {
      ctx.ownsPane = false;
    }
    if (!opts?.keepWorktree) {
      ctx.ownsWorktree = false;
    }
    ctx.retained = false;
    ctx.updatedAt = now;
    this.persist();
    return { ...ctx };
  }

  hasUnreadReportFromDispatch(dispatchId: string): boolean {
    const lineageIds = new Set<string>();
    let curId: string | null = dispatchId;
    while (curId && !lineageIds.has(curId)) {
      lineageIds.add(curId);
      curId = this.dispatches.get(curId)?.followUpOfDispatchId ?? null;
    }
    const fromHandles = new Set<string>([...lineageIds].map((id) => `dispatch:${id}`));
    const ctx = this.dispatches.get(dispatchId);
    for (const id of this.messageOrder) {
      const msg = this.messages.get(id);
      if (!msg || msg.read) continue;
      if (isBroadcastMailbox(msg.toHandle) && (this.consumerReadByMessageId.get(msg.id)?.size ?? 0) > 0) {
        continue;
      }
      if (
        fromHandles.has(msg.fromHandle) &&
        (msg.type === 'worker_done' || msg.type === 'escalation' || msg.type === 'question')
      ) {
        return true;
      }
      if (msg.type === 'worker_done' && msg.payload) {
        try {
          const parsed = JSON.parse(msg.payload) as { dispatchId?: unknown };
          if (typeof parsed?.dispatchId === 'string' && lineageIds.has(parsed.dispatchId)) {
            return true;
          }
        } catch {
          /* ignore */
        }
      }
      if (
        ctx &&
        msg.runId === ctx.runId &&
        (msg.fromHandle === ctx.assigneeHandle ||
          (ctx.assigneePaneId !== null && msg.fromHandle === ctx.assigneePaneId)) &&
        (msg.type === 'worker_done' || msg.type === 'escalation' || msg.type === 'question')
      ) {
        return true;
      }
    }
    return false;
  }

  requeueTaskForRetry(taskId: string): OrchestrationTaskRecord {
    const task = this.tasks.get(taskId);
    if (!task) {
      throw new OrchestrationError('task_not_found', `Task not found: ${taskId}`);
    }
    if (task.status === 'ready') {
      return { ...task, deps: [...task.deps] };
    }
    if (task.status === 'dispatched') {
      throw new OrchestrationError(
        'worker_busy',
        `Task ${task.id} already has an active dispatch.`
      );
    }
    const now = this.now();
    task.status = 'ready';
    task.blockedReason = null;
    task.updatedAt = now;
    this.promoteReadyTasks(task.runId);
    this.persist();
    return { ...task, deps: [...task.deps] };
  }

  listReviewDispatchesForTarget(targetDispatchId: string): OrchestrationDispatchRecord[] {
    return [...this.dispatches.values()]
      .filter((d) => d.reviewOfDispatchId === targetDispatchId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((d) => ({ ...d }));
  }

  listReviewDispatchesForBatch(
    targetDispatchId: string,
    reviewBatchId: string,
  ): OrchestrationDispatchRecord[] {
    return [...this.dispatches.values()]
      .filter(
        (d) =>
          d.reviewOfDispatchId === targetDispatchId &&
          d.reviewBatchId === reviewBatchId,
      )
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((d) => ({ ...d }));
  }

  listReviewDispatchesForRun(runId: string): OrchestrationDispatchRecord[] {
    return [...this.dispatches.values()]
      .filter((d) => d.reviewOfRunId === runId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((d) => ({ ...d }));
  }

  listReviewDispatchesForRunBatch(
    runId: string,
    reviewBatchId: string,
  ): OrchestrationDispatchRecord[] {
    return [...this.dispatches.values()]
      .filter(
        (d) =>
          d.reviewOfRunId === runId &&
          d.reviewBatchId === reviewBatchId,
      )
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((d) => ({ ...d }));
  }

  listIntegratedDispatchesForRun(runId: string): OrchestrationDispatchRecord[] {
    return [...this.dispatches.values()]
      .filter(
        (d) =>
          d.runId === runId &&
          !d.reviewOfDispatchId &&
          !d.reviewOfRunId &&
          d.integrationStatus === 'merged',
      )
      .sort((a, b) => a.updatedAt.localeCompare(b.updatedAt))
      .map((d) => ({ ...d }));
  }

  updateDispatchReviewState(
    dispatchId: string,
    patch: {
      reviewStatus?: OrchestrationReviewStatus;
      reviewRound?: number;
      reviewBatchId?: string | null;
      reviewSnapshotOid?: string | null;
      reviewBaseCommit?: string | null;
      reviewFindings?: OrchestrationReviewFinding[];
      reviewMutatedWorkspace?: boolean;
    },
  ): OrchestrationDispatchRecord | null {
    const dispatch = this.dispatches.get(dispatchId);
    if (!dispatch) return null;
    if (patch.reviewStatus !== undefined) dispatch.reviewStatus = patch.reviewStatus;
    if (patch.reviewRound !== undefined) dispatch.reviewRound = patch.reviewRound;
    if (patch.reviewBatchId !== undefined) dispatch.reviewBatchId = patch.reviewBatchId;
    if (patch.reviewSnapshotOid !== undefined) dispatch.reviewSnapshotOid = patch.reviewSnapshotOid;
    if (patch.reviewBaseCommit !== undefined) dispatch.reviewBaseCommit = patch.reviewBaseCommit;
    if (patch.reviewFindings !== undefined) {
      dispatch.reviewFindings = patch.reviewFindings.map((f) => ({ ...f }));
    }
    if (patch.reviewMutatedWorkspace !== undefined) {
      dispatch.reviewMutatedWorkspace = patch.reviewMutatedWorkspace;
    }
    dispatch.updatedAt = this.now();
    this.persist();
    return { ...dispatch };
  }

  updateDispatchIntegrationState(
    dispatchId: string,
    patch: {
      integrationStatus?: OrchestrationIntegrationStatus;
      integratedCommit?: string | null;
      integratedTargetBranch?: string | null;
    },
  ): OrchestrationDispatchRecord | null {
    const dispatch = this.dispatches.get(dispatchId);
    if (!dispatch) return null;
    if (patch.integrationStatus !== undefined) dispatch.integrationStatus = patch.integrationStatus;
    if (patch.integratedCommit !== undefined) dispatch.integratedCommit = patch.integratedCommit;
    if (patch.integratedTargetBranch !== undefined) {
      dispatch.integratedTargetBranch = patch.integratedTargetBranch;
    }
    dispatch.updatedAt = this.now();
    this.persist();
    return { ...dispatch };
  }

  setDispatchBriefFilePath(dispatchId: string, briefFilePath: string | null): OrchestrationDispatchRecord | null {
    const dispatch = this.dispatches.get(dispatchId);
    if (!dispatch) return null;
    dispatch.briefFilePath = briefFilePath;
    dispatch.updatedAt = this.now();
    this.persist();
    return { ...dispatch };
  }

  updateDispatchObservation(
    dispatchId: string,
    observation: OrchestrationStartObservation,
    options?: {
      reason?: string | null;
      refreshHeartbeat?: boolean;
      minHeartbeatIntervalMs?: number;
      promptSent?: boolean;
      queuedWorkingBaseline?: number | null;
    }
  ): OrchestrationDispatchRecord | null {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || !ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      return null;
    }

    const currentObservation: OrchestrationStartObservation = ctx.startObservation ?? 'pending';
    const now = this.now();
    let changed = false;

    if (options?.promptSent === true) {
      ctx.promptSentAt = now;
      changed = true;
    }
    if (options?.queuedWorkingBaseline !== undefined) {
      ctx.queuedWorkingBaseline = options.queuedWorkingBaseline;
      changed = true;
    }

    const isBlockedDowngrade =
      (currentObservation === 'observed' || currentObservation === 'permission') &&
      observation !== 'observed' &&
      observation !== 'permission';
    if (isBlockedDowngrade) {
      if (changed) {
        ctx.updatedAt = now;
        this.persist();
      }
      return { ...ctx };
    }

    if (ctx.startObservation !== observation) {
      ctx.startObservation = observation;
      ctx.staleWarningAt = null;
      if (observation === 'permission') {
        ctx.stage = 'waiting_permission';
      } else if (observation === 'observed') {
        ctx.stage = 'working';
      }
      changed = true;
    }

    if (options?.reason !== undefined) {
      if ((ctx.startObservationReason ?? null) !== options.reason) {
        ctx.startObservationReason = options.reason;
        changed = true;
      }
    } else if (observation === 'observed' && ctx.startObservationReason !== null) {
      ctx.startObservationReason = null;
      changed = true;
    }

    if (observation === 'observed' && ctx.turnStartedAt == null) {
      ctx.turnStartedAt = now;
      changed = true;
    }

    if (
      options?.refreshHeartbeat !== false &&
      (observation === 'observed' || observation === 'permission' || observation === 'queued')
    ) {
      const nowMs = Date.parse(now);
      const lastHbMs = ctx.lastHeartbeatAt ? Date.parse(ctx.lastHeartbeatAt) : Number.NaN;
      const throttled =
        options?.minHeartbeatIntervalMs !== undefined &&
        currentObservation === observation &&
        Number.isFinite(nowMs) &&
        Number.isFinite(lastHbMs) &&
        nowMs - lastHbMs < options.minHeartbeatIntervalMs;
      if (!throttled && ctx.lastHeartbeatAt !== now) {
        ctx.lastHeartbeatAt = now;
        changed = true;
      }
    }

    if (changed) {
      ctx.updatedAt = now;
      this.persist();
    }
    return { ...ctx };
  }

  recordHeartbeat(
    dispatchId: string,
    options?: { minIntervalMs?: number }
  ): OrchestrationDispatchRecord | null {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || !ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      return null;
    }
    const now = this.now();
    if (
      options?.minIntervalMs !== undefined &&
      ctx.startObservation === 'observed' &&
      ctx.lastHeartbeatAt
    ) {
      const elapsedMs = Date.parse(now) - Date.parse(ctx.lastHeartbeatAt);
      if (Number.isFinite(elapsedMs) && elapsedMs < options.minIntervalMs) {
        return { ...ctx };
      }
    }
    this.promoteDispatchObserved(ctx, now, true);
    this.persist();
    return { ...ctx };
  }

  private refreshParentDispatchHeartbeat(child: OrchestrationDispatchRecord, now: string): void {
    if (!child.creatorDispatchId) return;
    const resolved = this.resolveDispatchLineage(child.creatorDispatchId);
    const parent = resolved
      ? this.dispatches.get(resolved.id)
      : this.dispatches.get(child.creatorDispatchId);
    if (parent && ACTIVE_DISPATCH_STATUSES.has(parent.status)) {
      parent.lastHeartbeatAt = now;
      parent.updatedAt = now;
    }
  }

  completeDispatch(dispatchId: string): OrchestrationDispatchRecord {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx) {
      throw new OrchestrationError('dispatch_not_found', `Dispatch not found: ${dispatchId}`);
    }
    if (ctx.status === 'completed') {
      return { ...ctx };
    }
    assertDispatchTransition(ctx.id, ctx.status, 'completed');
    const now = this.now();
    ctx.status = 'completed';
    ctx.stage = 'done';
    ctx.completedAt = now;
    ctx.updatedAt = now;
    this.refreshParentDispatchHeartbeat(ctx, now);
    this.closeQuestionsForDispatch(ctx.id);
    this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);
    this.persist();
    return { ...ctx };
  }

  private settleActiveDispatchesForTask(
    taskId: string,
    targetStatus: 'completed' | 'failed',
    reason: string
  ): void {
    const now = this.now();
    for (const d of this.dispatches.values()) {
      if (d.taskId !== taskId || !ACTIVE_DISPATCH_STATUSES.has(d.status)) continue;
      assertDispatchTransition(d.id, d.status, targetStatus);
      d.status = targetStatus;
      d.stage = targetStatus === 'completed' ? 'done' : 'failed';
      d.completedAt = now;
      d.updatedAt = now;
      if (targetStatus === 'failed') {
        this.cleanupDispatchBrief(d.id);
        d.lastFailureReason = reason;
        d.failureSummary = reason;
      }
      this.refreshParentDispatchHeartbeat(d, now);
      this.closeQuestionsForDispatch(d.id);
      this.routeUnreadDispatchMailboxToRun(d.id, d.runId);
    }
  }

  failDispatch(
    dispatchId: string,
    reason: string,
    options?: { forceFailTask?: boolean; summary?: string }
  ): OrchestrationDispatchRecord {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx) {
      throw new OrchestrationError('dispatch_not_found', `Dispatch not found: ${dispatchId}`);
    }
    if (!ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      return { ...ctx };
    }

    const now = this.now();
    const nextFailureCount = ctx.failureCount + 1;
    const circuitBroken = nextFailureCount >= DISPATCH_CIRCUIT_BREAK_FAILURES;
    const targetDispatchStatus = circuitBroken ? 'circuit_broken' : 'failed';
    assertDispatchTransition(ctx.id, ctx.status, targetDispatchStatus);

    ctx.failureCount = nextFailureCount;
    ctx.lastFailureReason = reason;
    ctx.failureSummary = options?.summary ?? reason;
    ctx.status = targetDispatchStatus;
    ctx.stage = reason === 'worker_stopped' ? 'stopped' : 'failed';
    this.cleanupDispatchBrief(ctx.id);
    ctx.completedAt = now;
    ctx.updatedAt = now;
    this.refreshParentDispatchHeartbeat(ctx, now);

    this.closeQuestionsForDispatch(ctx.id);
    this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);

    const task = this.tasks.get(ctx.taskId);
    if (task && task.status !== 'completed' && task.status !== 'failed') {
      // `ready` invites another dispatch; a task made for one specific worker never gets one.
      const nextTaskStatus: OrchestrationTaskStatus = this.hasPendingGates(task.id)
        ? 'blocked'
        : circuitBroken || options?.forceFailTask || task.singleShot
          ? 'failed'
          : 'ready';
      if (task.status !== nextTaskStatus) {
        assertTaskTransition(task.id, task.status, nextTaskStatus);
        task.status = nextTaskStatus;
        task.blockedReason = null;
        task.updatedAt = now;
      }
    }

    this.promoteReadyTasks(ctx.runId);
    this.persist();
    return { ...ctx };
  }

  /**
   * Park a dispatch whose worker stopped showing signs of life. Unlike failDispatch this never
   * re-queues the task: the assignee may still be executing it, so re-dispatching would run the
   * task twice. The task is blocked for a human and a late worker_done can still settle it.
   */
  blockUnresponsiveDispatch(dispatchId: string, reason: string): OrchestrationDispatchRecord {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx) {
      throw new OrchestrationError('dispatch_not_found', `Dispatch not found: ${dispatchId}`);
    }
    if (!ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      return { ...ctx };
    }

    const now = this.now();
    assertDispatchTransition(ctx.id, ctx.status, 'failed');
    ctx.status = 'failed';
    ctx.stage = 'failed';
    ctx.lastFailureReason = reason;
    ctx.failureSummary = reason;
    ctx.completedAt = now;
    ctx.updatedAt = now;
    this.refreshParentDispatchHeartbeat(ctx, now);

    this.closeQuestionsForDispatch(ctx.id);
    this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);

    const task = this.tasks.get(ctx.taskId);
    if (task && task.status !== 'completed' && task.status !== 'failed' && task.status !== 'blocked') {
      assertTaskTransition(task.id, task.status, 'blocked');
      task.status = 'blocked';
      task.updatedAt = now;
    }

    this.insertMessage({
      runId: ctx.runId,
      from: `dispatch:${ctx.id}`,
      to: `run:${ctx.runId}`,
      type: 'escalation',
      priority: 'high',
      subject: `Worker unresponsive for task ${ctx.taskId}`,
      body: reason,
      payload: JSON.stringify({ dispatchId: ctx.id, taskId: ctx.taskId }),
    });

    this.persist();
    return { ...ctx };
  }

  /**
   * Emit a one-shot advisory escalation for a dispatch that shows no progress. Unlike
   * blockUnresponsiveDispatch this changes no dispatch/task status; `advisory: true` keeps the
   * coordinator's escalation handler from settling anything.
   */
  markStaleDispatchWarned(
    dispatchId: string,
    subject: string,
    body: string
  ): OrchestrationDispatchRecord | null {
    const ctx = this.dispatches.get(dispatchId);
    if (!ctx || !ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      return null;
    }
    if (ctx.staleWarningAt) {
      return { ...ctx };
    }

    const now = this.now();
    ctx.staleWarningAt = now;
    ctx.updatedAt = now;
    this.insertMessage({
      runId: ctx.runId,
      from: `dispatch:${ctx.id}`,
      to: `run:${ctx.runId}`,
      type: 'escalation',
      priority: 'normal',
      subject,
      body,
      payload: JSON.stringify({ dispatchId: ctx.id, taskId: ctx.taskId, advisory: true }),
    });
    this.persist();
    return { ...ctx };
  }

  /** Live dispatches assigned to a pane, without cloning or sorting (hot PTY activity path). */
  getActiveDispatchesForPane(ptyId: string): OrchestrationDispatchRecord[] {
    const matches: OrchestrationDispatchRecord[] = [];
    for (const d of this.dispatches.values()) {
      if (!ACTIVE_DISPATCH_STATUSES.has(d.status)) continue;
      if (d.assigneePaneId === ptyId || d.assigneeHandle === ptyId) matches.push(d);
    }
    return matches;
  }

  private recoverUnobservedPromptReport(
    ctx: OrchestrationDispatchRecord,
    task: OrchestrationTaskRecord,
    outcome: OrchestrationWorkerReportOutcome,
    summary?: string
  ): {
    action: 'settled';
    dispatch: OrchestrationDispatchRecord;
    task: OrchestrationTaskRecord;
  } {
    const now = this.now();
    this.promoteDispatchObserved(ctx, now);

    if (outcome === 'completed') {
      ctx.status = 'completed';
      ctx.stage = 'done';
      ctx.lastFailureReason = null;
      ctx.failureSummary = null;
      ctx.completedAt = now;
      ctx.updatedAt = now;
      task.status = 'completed';
      task.updatedAt = now;
      this.closeQuestionsForDispatch(ctx.id);
      this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);
      this.promoteReadyTasks(ctx.runId);
    } else if (outcome === 'blocked') {
      ctx.status = 'completed';
      ctx.stage = 'done';
      ctx.lastFailureReason = null;
      ctx.failureSummary = null;
      ctx.completedAt = now;
      ctx.updatedAt = now;
      task.status = 'blocked';
      task.updatedAt = now;
      this.closeQuestionsForDispatch(ctx.id);
      this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);
    } else if (outcome === 'escalated') {
      ctx.status = 'failed';
      ctx.stage = 'failed';
      ctx.lastFailureReason = 'escalated';
      ctx.failureSummary = summary ?? 'Worker escalated task';
      ctx.completedAt = now;
      ctx.updatedAt = now;
      task.status = 'blocked';
      task.updatedAt = now;
      this.closeQuestionsForDispatch(ctx.id);
      this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);
    } else {
      const rawReason = summary || `Worker reported ${outcome}`;
      const failureText = rawReason.startsWith(AGENT_PROMPT_STALLED_ERROR)
        ? `Worker reported ${outcome}: ${rawReason}`
        : rawReason;
      ctx.status = 'failed';
      ctx.stage = 'failed';
      ctx.lastFailureReason = failureText;
      ctx.failureSummary = failureText;
      ctx.completedAt = now;
      ctx.updatedAt = now;
      task.status = 'failed';
      task.updatedAt = now;
      this.closeQuestionsForDispatch(ctx.id);
      this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);
    }

    this.promoteReadyTasks(ctx.runId);
    this.persist();
    return {
      action: 'settled',
      dispatch: { ...ctx },
      task: { ...task, deps: [...task.deps] },
    };
  }

  settleWorkerReport(input: {
    dispatchId: string;
    taskId: string;
    outcome: OrchestrationWorkerReportOutcome;
    summary?: string;
    reportedByHandle?: string;
    reportedByPaneId?: string | null;
  }): {
    action: 'settled' | 'duplicate' | 'rejected';
    reason?: string;
    dispatch?: OrchestrationDispatchRecord;
    task?: OrchestrationTaskRecord;
  } {
    const ctx = this.dispatches.get(input.dispatchId);
    if (!ctx) {
      return { action: 'rejected', reason: `Unknown dispatch ${input.dispatchId}` };
    }
    const task = this.tasks.get(input.taskId);
    if (!task || ctx.taskId !== input.taskId) {
      return {
        action: 'rejected',
        reason: `Dispatch ${input.dispatchId} does not match task ${input.taskId}`,
      };
    }

    // Verify lifecycle authority against assignee if caller identity provided.
    if (
      input.reportedByHandle &&
      input.reportedByHandle !== ctx.assigneeHandle &&
      input.reportedByHandle !== `dispatch:${ctx.id}` &&
      (!input.reportedByPaneId || input.reportedByPaneId !== ctx.assigneePaneId)
    ) {
      return {
        action: 'rejected',
        reason: `Sender ${input.reportedByHandle} is not authorized to settle dispatch ${ctx.id}`,
      };
    }

    // Idempotent duplicate handling and unobserved-prompt late report recovery.
    if (!ACTIVE_DISPATCH_STATUSES.has(ctx.status)) {
      if (
        (reportCorrectsUnobservedPrompt(ctx, task) || reportCorrectsUnresponsiveWorker(ctx, task)) &&
        this.getLatestDispatchForTask(task.id)?.id === ctx.id &&
        this.getActiveDispatchesForTask(task.id).length === 0
      ) {
        return this.recoverUnobservedPromptReport(ctx, task, input.outcome, input.summary);
      }

      const expectedDone =
        (input.outcome === 'completed' &&
          ctx.status === 'completed' &&
          task.status === 'completed') ||
        (input.outcome === 'blocked' && task.status === 'blocked') ||
        ((input.outcome === 'failed' ||
          input.outcome === 'abandoned' ||
          input.outcome === 'escalated') &&
          (ctx.status === 'failed' || ctx.status === 'circuit_broken'));
      if (expectedDone) {
        return {
          action: 'duplicate',
          dispatch: { ...ctx },
          task: { ...task, deps: [...task.deps] },
        };
      }
      return {
        action: 'rejected',
        reason: `Dispatch ${ctx.id} is already ${ctx.status}`,
      };
    }

    const now = this.now();
    if (input.outcome === 'completed') {
      assertDispatchTransition(ctx.id, ctx.status, 'completed');
      assertTaskTransition(task.id, task.status, 'completed');
      this.promoteDispatchObserved(ctx, now);
      ctx.status = 'completed';
      ctx.stage = 'done';
      ctx.completedAt = now;
      ctx.updatedAt = now;
      this.refreshParentDispatchHeartbeat(ctx, now);
      this.closeQuestionsForDispatch(ctx.id);
      this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);

      task.status = 'completed';
      task.updatedAt = now;

      this.settleActiveDispatchesForTask(task.id, 'completed', 'Sibling dispatch superseded');
      this.promoteReadyTasks(task.runId);
      this.persist();
      return {
        action: 'settled',
        dispatch: { ...ctx },
        task: { ...task, deps: [...task.deps] },
      };
    }

    if (input.outcome === 'blocked') {
      assertDispatchTransition(ctx.id, ctx.status, 'completed');
      if (task.status !== 'blocked') {
        assertTaskTransition(task.id, task.status, 'blocked');
      }
      this.promoteDispatchObserved(ctx, now);
      ctx.status = 'completed';
      ctx.stage = 'done';
      ctx.completedAt = now;
      ctx.updatedAt = now;
      this.refreshParentDispatchHeartbeat(ctx, now);
      this.closeQuestionsForDispatch(ctx.id);
      this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);

      if (task.status !== 'blocked') {
        task.status = 'blocked';
        task.updatedAt = now;
      }
      this.persist();
      return {
        action: 'settled',
        dispatch: { ...ctx },
        task: { ...task, deps: [...task.deps] },
      };
    }

    if (input.outcome === 'escalated') {
      assertDispatchTransition(ctx.id, ctx.status, 'failed');
      if (task.status !== 'blocked') {
        assertTaskTransition(task.id, task.status, 'blocked');
      }
      this.promoteDispatchObserved(ctx, now);
      ctx.status = 'failed';
      ctx.stage = 'failed';
      ctx.lastFailureReason = 'escalated';
      ctx.failureSummary = input.summary ?? 'Worker escalated task';
      ctx.completedAt = now;
      ctx.updatedAt = now;
      this.refreshParentDispatchHeartbeat(ctx, now);
      this.closeQuestionsForDispatch(ctx.id);
      this.routeUnreadDispatchMailboxToRun(ctx.id, ctx.runId);

      if (task.status !== 'blocked') {
        task.status = 'blocked';
        task.updatedAt = now;
      }
      this.persist();
      return {
        action: 'settled',
        dispatch: { ...ctx },
        task: { ...task, deps: [...task.deps] },
      };
    }

    // 'failed' or 'abandoned' -> failDispatch (retries up to circuit break).
    this.promoteDispatchObserved(ctx, now);
    const failedDispatch = this.failDispatch(
      ctx.id,
      input.summary || `Worker reported ${input.outcome}`,
      { summary: input.summary }
    );
    const updatedTask = this.requireTask(task.id);
    return {
      action: 'settled',
      dispatch: failedDispatch,
      task: updatedTask,
    };
  }

  // ==================== Decision Gates ====================

  hasPendingGates(taskId: string): boolean {
    for (const gate of this.gates.values()) {
      if (gate.taskId === taskId && gate.status === 'pending') {
        return true;
      }
    }
    return false;
  }

  createGate(input: {
    taskId: string;
    question: string;
    options?: string[];
    requesterDispatchId?: string | null;
    requesterHandle?: string | null;
  }): OrchestrationGateRecord {
    const task = this.tasks.get(input.taskId);
    if (!task) {
      throw new OrchestrationError('task_not_found', `Task not found: ${input.taskId}`);
    }
    if (task.status === 'completed' || task.status === 'failed') {
      throw new OrchestrationError(
        'invalid_transition',
        `Cannot open decision gate on ${task.status} task ${task.id}.`
      );
    }

    const requesterDispatch = input.requesterDispatchId
      ? this.dispatches.get(input.requesterDispatchId)
      : undefined;
    if (input.requesterDispatchId) {
      if (
        !requesterDispatch ||
        requesterDispatch.taskId !== task.id ||
        !ACTIVE_DISPATCH_STATUSES.has(requesterDispatch.status)
      ) {
        throw new OrchestrationError(
          'dispatch_inactive',
          `Dispatch ${input.requesterDispatchId} is not active for task ${task.id}.`
        );
      }
    }

    const now = this.now();
    if (requesterDispatch && ACTIVE_DISPATCH_STATUSES.has(requesterDispatch.status)) {
      this.promoteDispatchObserved(requesterDispatch, now, true);
    }
    const gate: OrchestrationGateRecord = {
      id: generatePrefixedId('gate'),
      taskId: task.id,
      runId: task.runId,
      requesterDispatchId: input.requesterDispatchId ?? null,
      requesterHandle: input.requesterHandle ?? null,
      question: input.question,
      options: [...(input.options ?? [])],
      status: 'pending',
      resolution: null,
      createdAt: now,
      resolvedAt: null,
    };
    this.gates.set(gate.id, gate);

    this.settleActiveDispatchesForTask(task.id, 'completed', 'Paused by decision gate');
    if (task.status !== 'blocked') {
      assertTaskTransition(task.id, task.status, 'blocked');
      task.status = 'blocked';
      task.updatedAt = now;
    }

    this.persist();
    return { ...gate, options: [...gate.options] };
  }

  resolveGate(gateId: string, resolution: string): OrchestrationGateRecord {
    const gate = this.gates.get(gateId);
    if (!gate) {
      throw new OrchestrationError('gate_not_found', `Decision gate not found: ${gateId}`);
    }
    if (gate.status !== 'pending') {
      throw new OrchestrationError(
        'gate_already_resolved',
        `Decision gate ${gateId} is already ${gate.status}.`,
        { gateId, status: gate.status }
      );
    }
    const now = this.now();
    gate.status = 'resolved';
    gate.resolution = resolution;
    gate.resolvedAt = now;

    const task = this.tasks.get(gate.taskId);
    if (task && task.status === 'blocked' && !this.hasPendingGates(task.id)) {
      const depsDone = task.deps.every((depId) => this.tasks.get(depId)?.status === 'completed');
      const nextStatus: OrchestrationTaskStatus = depsDone ? 'ready' : 'pending';
      assertTaskTransition(task.id, task.status, nextStatus);
      task.status = nextStatus;
      task.updatedAt = now;
    }

    this.persist();
    return { ...gate, options: [...gate.options] };
  }

  timeoutGate(gateId: string): OrchestrationGateRecord {
    const gate = this.gates.get(gateId);
    if (!gate) {
      throw new OrchestrationError('gate_not_found', `Decision gate not found: ${gateId}`);
    }
    if (gate.status !== 'pending') {
      return { ...gate, options: [...gate.options] };
    }
    gate.status = 'timeout';
    gate.resolvedAt = this.now();
    this.persist();
    return { ...gate, options: [...gate.options] };
  }

  getGate(gateId: string): OrchestrationGateRecord | undefined {
    const gate = this.gates.get(gateId);
    return gate ? { ...gate, options: [...gate.options] } : undefined;
  }

  listGates(filter?: {
    runId?: string;
    taskId?: string;
    status?: OrchestrationGateStatus;
  }): OrchestrationGateRecord[] {
    return [...this.gates.values()]
      .filter((g) => {
        if (filter?.runId && g.runId !== filter.runId) return false;
        if (filter?.taskId && g.taskId !== filter.taskId) return false;
        if (filter?.status && g.status !== filter.status) return false;
        return true;
      })
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((g) => ({ ...g, options: [...g.options] }));
  }

  // ==================== Messages & Mailbox ====================

  insertMessage(input: {
    id?: string;
    runId?: string | null;
    from: string;
    to: string;
    type?: OrchestrationMessageType;
    priority?: OrchestrationMessagePriority;
    subject: string;
    body?: string;
    payload?: string | null;
    threadId?: string | null;
    fromRole?: string;
    mentions?: readonly string[];
    refs?: readonly number[];
    kind?: OrchestrationBoardPostKind;
    artifacts?: OrchestrationReportArtifacts | null;
  }): OrchestrationMessageRecord {
    let runId = input.runId?.trim();
    if (!runId) {
      if (input.to.startsWith('run:')) {
        runId = input.to.slice('run:'.length);
      } else if (input.to.startsWith('dispatch:')) {
        const d = this.dispatches.get(input.to.slice('dispatch:'.length));
        runId = d?.runId;
      } else if (input.from.startsWith('dispatch:')) {
        const d = this.dispatches.get(input.from.slice('dispatch:'.length));
        runId = d?.runId;
      }
    }
    if (!runId || runId === UNBOUND_RUN_ID) {
      runId = this.ensureUnboundRun().id;
    } else {
      this.requireRun(runId);
    }

    const now = this.now();
    const type: OrchestrationMessageType = input.type ?? 'status';
    const isHeartbeat = type === 'heartbeat';
    if (isHeartbeat) {
      const staleHeartbeats = new Set<string>();
      for (const existing of this.messages.values()) {
        if (existing.type === 'heartbeat' && existing.runId === runId && existing.fromHandle === input.from) {
          staleHeartbeats.add(existing.id);
        }
      }
      if (staleHeartbeats.size > 0) {
        this.deleteMessageIds(staleHeartbeats);
      }
    }
    const cleanFromRole = input.fromRole?.trim().replace(/^@+/, '') || undefined;
    const cleanMentions = input.mentions
      ? [...new Set(input.mentions.map((m) => m.trim().replace(/^@+/, '').toLowerCase()).filter(Boolean))]
      : undefined;
    const cleanRefs = input.refs
      ? [...new Set(input.refs.filter((r) => Number.isInteger(r) && r > 0))]
      : undefined;
    const msg: OrchestrationMessageRecord = {
      id: input.id?.trim() || generatePrefixedId('msg'),
      runId,
      seq: this.nextSequence++,
      ...(!isHeartbeat ? { boardSeq: this.allocateBoardSeq(runId) } : {}),
      type,
      priority: input.priority ?? 'normal',
      fromHandle: input.from,
      toHandle: input.to,
      ...(cleanFromRole ? { fromRole: cleanFromRole } : {}),
      ...(cleanMentions && cleanMentions.length > 0 ? { mentions: cleanMentions } : {}),
      ...(cleanRefs && cleanRefs.length > 0 ? { refs: cleanRefs } : {}),
      ...(input.kind ? { kind: input.kind } : {}),
      ...(input.artifacts !== undefined ? { artifacts: input.artifacts } : {}),
      subject: this.redactDispatchCredentials(input.subject),
      body: this.redactDispatchCredentials(input.body ?? ''),
      payload: input.payload ?? null,
      threadId: input.threadId ?? null,
      deliveredAt: isHeartbeat ? now : null,
      read: isHeartbeat,
      createdAt: now,
      lifecycleRejectedReason: null,
    };

    this.messages.set(msg.id, msg);
    this.messageOrder.push(msg.id);
    this.prune();
    this.persist();
    return cloneMessageRecord(msg);
  }

  getMessageById(id: string): OrchestrationMessageRecord | undefined {
    const msg = this.messages.get(id);
    return msg ? { ...msg } : undefined;
  }

  markLifecycleRejected(messageId: string, reason: string): OrchestrationMessageRecord | undefined {
    const msg = this.messages.get(messageId);
    if (!msg) {
      return undefined;
    }
    msg.lifecycleRejectedReason = reason;
    msg.read = true;
    msg.deliveredAt = msg.deliveredAt ?? this.now();
    this.persist();
    return { ...msg };
  }

  /** Moves unread direct mail sent to a pane/handle into its role mailbox (`run:` or `dispatch:`). */
  routeUnreadDirectMessages(
    directHandle: string,
    targetMailboxHandle: string,
    runId?: string
  ): OrchestrationMessageRecord[] {
    if (!directHandle || directHandle === targetMailboxHandle) {
      return [];
    }
    const routed: OrchestrationMessageRecord[] = [];
    for (const id of this.messageOrder) {
      const msg = this.messages.get(id);
      if (!msg || msg.read) continue;
      if (msg.toHandle !== directHandle) continue;
      if (runId && msg.runId !== runId && msg.runId !== UNBOUND_RUN_ID) continue;
      msg.toHandle = targetMailboxHandle;
      if (runId && msg.runId === UNBOUND_RUN_ID) {
        msg.runId = runId;
      }
      routed.push({ ...msg });
    }
    if (routed.length > 0) {
      this.persist();
    }
    return routed;
  }

  /** Forward leftover unread mail from a closed dispatch mailbox up to the owning run mailbox. */
  routeUnreadDispatchMailboxToRun(dispatchId: string, runId: string): OrchestrationMessageRecord[] {
    const fromMailbox = `dispatch:${dispatchId}`;
    const toMailbox = `run:${runId}`;
    const routed: OrchestrationMessageRecord[] = [];
    for (const id of this.messageOrder) {
      const msg = this.messages.get(id);
      if (!msg || msg.read) continue;
      if (msg.toHandle !== fromMailbox) continue;
      msg.toHandle = toMailbox;
      msg.deliveredAt = null;
      routed.push({ ...msg });
    }
    if (routed.length > 0) {
      this.persist();
    }
    return routed;
  }

  private isMessageReadByConsumer(msg: OrchestrationMessageRecord, consumerHandle?: string): boolean {
    if (msg.read) return true;
    if (consumerHandle && isBroadcastMailbox(msg.toHandle)) {
      return this.consumerReadByMessageId.get(msg.id)?.has(consumerHandle) ?? false;
    }
    return false;
  }

  private isMessageDeliveredToConsumer(
    msg: OrchestrationMessageRecord,
    consumerHandle?: string
  ): boolean {
    if (consumerHandle && isBroadcastMailbox(msg.toHandle)) {
      return (
        (this.consumerDeliveredByMessageId.get(msg.id)?.has(consumerHandle) ?? false) ||
        (this.consumerReadByMessageId.get(msg.id)?.has(consumerHandle) ?? false)
      );
    }
    return msg.deliveredAt !== null;
  }

  getUnreadMessages(
    mailboxHandle: string,
    options?: {
      types?: readonly OrchestrationMessageType[];
      excludeTypes?: readonly OrchestrationMessageType[];
      excludeIds?: readonly string[];
      consumerHandle?: string;
      limit?: number;
    }
  ): OrchestrationMessageRecord[] {
    const typeSet = options?.types?.length ? new Set(options.types) : null;
    const excludeTypeSet = options?.excludeTypes?.length ? new Set(options.excludeTypes) : null;
    const excludeIdSet = options?.excludeIds?.length ? new Set(options.excludeIds) : null;
    const limit = options?.limit ?? ORCHESTRATION_DELIVERY_BATCH_LIMIT;

    const results: OrchestrationMessageRecord[] = [];
    for (const id of this.messageOrder) {
      const msg = this.messages.get(id);
      if (!msg || this.isMessageReadByConsumer(msg, options?.consumerHandle)) continue;
      if (msg.toHandle !== mailboxHandle) continue;
      if (typeSet && !typeSet.has(msg.type)) continue;
      if (excludeTypeSet && excludeTypeSet.has(msg.type)) continue;
      if (excludeIdSet && excludeIdSet.has(msg.id)) continue;
      results.push({ ...msg });
      if (results.length >= limit) break;
    }
    return results;
  }

  getUndeliveredUnreadMessages(
    mailboxHandle: string,
    options?: {
      types?: readonly OrchestrationMessageType[];
      excludeTypes?: readonly OrchestrationMessageType[];
      excludeIds?: readonly string[];
      consumerHandle?: string;
      limit?: number;
    }
  ): OrchestrationMessageRecord[] {
    return this.getUnreadMessages(mailboxHandle, options).filter(
      (m) => !this.isMessageDeliveredToConsumer(m, options?.consumerHandle)
    );
  }

  getAllMessagesForHandle(
    mailboxHandle: string,
    options?: {
      types?: readonly OrchestrationMessageType[];
      limit?: number;
    }
  ): OrchestrationMessageRecord[] {
    const typeSet = options?.types?.length ? new Set(options.types) : null;
    const limit = options?.limit ?? 100;
    const matches: OrchestrationMessageRecord[] = [];
    for (let i = this.messageOrder.length - 1; i >= 0; i--) {
      const msg = this.messages.get(this.messageOrder[i]!);
      if (!msg || msg.toHandle !== mailboxHandle) continue;
      if (typeSet && !typeSet.has(msg.type)) continue;
      matches.push({ ...msg });
      if (matches.length >= limit) break;
    }
    return matches.reverse();
  }

  listMessages(filter?: {
    runId?: string;
    toHandle?: string;
    fromHandle?: string;
    type?: OrchestrationMessageType;
    unreadOnly?: boolean;
    limit?: number;
  }): OrchestrationMessageRecord[] {
    const limit = filter?.limit ?? 200;
    const list: OrchestrationMessageRecord[] = [];
    for (const id of this.messageOrder) {
      const msg = this.messages.get(id);
      if (!msg) continue;
      if (filter?.runId && msg.runId !== filter.runId) continue;
      if (filter?.toHandle && msg.toHandle !== filter.toHandle) continue;
      if (filter?.fromHandle && msg.fromHandle !== filter.fromHandle) continue;
      if (filter?.type && msg.type !== filter.type) continue;
      if (filter?.unreadOnly && msg.read) continue;
      list.push({ ...msg });
    }
    return list.length > limit ? list.slice(list.length - limit) : list;
  }

  markAsRead(messageIds: readonly string[]): void {
    const now = this.now();
    let changed = false;
    for (const id of messageIds) {
      const msg = this.messages.get(id);
      if (msg && !msg.read) {
        msg.read = true;
        msg.deliveredAt = msg.deliveredAt ?? now;
        changed = true;
      }
    }
    if (changed) {
      this.prune();
      this.persist();
    }
  }

  markAsDelivered(messageIds: readonly string[]): void {
    const now = this.now();
    let changed = false;
    for (const id of messageIds) {
      const msg = this.messages.get(id);
      if (msg && msg.deliveredAt === null) {
        msg.deliveredAt = now;
        changed = true;
      }
    }
    if (changed) {
      this.persist();
    }
  }

  markAsUndelivered(messageIds: readonly string[]): void {
    let changed = false;
    for (const id of messageIds) {
      const msg = this.messages.get(id);
      if (msg && !msg.read && msg.deliveredAt !== null) {
        msg.deliveredAt = null;
        changed = true;
      }
    }
    if (changed) {
      this.persist();
    }
  }

  markAsReadAndDelivered(messageIds: readonly string[]): void {
    const now = this.now();
    let changed = false;
    for (const id of messageIds) {
      const msg = this.messages.get(id);
      if (msg && (!msg.read || msg.deliveredAt === null)) {
        msg.read = true;
        msg.deliveredAt = msg.deliveredAt ?? now;
        changed = true;
      }
    }
    if (changed) {
      this.prune();
      this.persist();
    }
  }

  markAsDeliveredForConsumer(messageIds: readonly string[], consumerHandle: string): void {
    const now = this.now();
    let changed = false;
    for (const id of messageIds) {
      const msg = this.messages.get(id);
      if (!msg) continue;
      if (isBroadcastMailbox(msg.toHandle)) {
        let set = this.consumerDeliveredByMessageId.get(id);
        if (!set) {
          set = new Set();
          this.consumerDeliveredByMessageId.set(id, set);
        }
        if (!set.has(consumerHandle)) {
          set.add(consumerHandle);
          changed = true;
        }
      } else if (msg.deliveredAt === null) {
        msg.deliveredAt = now;
        changed = true;
      }
    }
    if (changed) {
      this.persist();
    }
  }

  markAsUndeliveredForConsumer(messageIds: readonly string[], consumerHandle: string): void {
    let changed = false;
    for (const id of messageIds) {
      const msg = this.messages.get(id);
      if (!msg) continue;
      if (isBroadcastMailbox(msg.toHandle)) {
        const set = this.consumerDeliveredByMessageId.get(id);
        if (set?.delete(consumerHandle)) {
          if (set.size === 0) {
            this.consumerDeliveredByMessageId.delete(id);
          }
          changed = true;
        }
      } else if (!msg.read && msg.deliveredAt !== null) {
        msg.deliveredAt = null;
        changed = true;
      }
    }
    if (changed) {
      this.persist();
    }
  }

  markAsReadAndDeliveredForConsumer(messageIds: readonly string[], consumerHandle: string): void {
    const now = this.now();
    let changed = false;
    for (const id of messageIds) {
      const msg = this.messages.get(id);
      if (!msg) continue;
      if (isBroadcastMailbox(msg.toHandle)) {
        let readSet = this.consumerReadByMessageId.get(id);
        if (!readSet) {
          readSet = new Set();
          this.consumerReadByMessageId.set(id, readSet);
        }
        if (!readSet.has(consumerHandle)) {
          readSet.add(consumerHandle);
          changed = true;
        }
        let dlvSet = this.consumerDeliveredByMessageId.get(id);
        if (!dlvSet) {
          dlvSet = new Set();
          this.consumerDeliveredByMessageId.set(id, dlvSet);
        }
        if (!dlvSet.has(consumerHandle)) {
          dlvSet.add(consumerHandle);
          changed = true;
        }
      } else if (!msg.read || msg.deliveredAt === null) {
        msg.read = true;
        msg.deliveredAt = msg.deliveredAt ?? now;
        changed = true;
      }
    }
    if (changed) {
      this.prune();
      this.persist();
    }
  }

  areUnreadMessages(mailboxHandle: string, messageIds: readonly string[]): boolean {
    return messageIds.every((id) => {
      const m = this.messages.get(id);
      return m !== undefined && m.toHandle === mailboxHandle && !m.read;
    });
  }

  getOutstandingDelivery(
    mailboxHandle: string,
    consumerHandle?: string
  ): OrchestrationDeliveryRecord | undefined {
    for (const d of this.deliveries.values()) {
      if (d.mailboxHandle !== mailboxHandle || d.status !== 'pending') continue;
      if (consumerHandle && d.consumerHandle && d.consumerHandle !== consumerHandle) {
        continue;
      }
      return { ...d, messageIds: [...d.messageIds] };
    }
    return undefined;
  }

  getDelivery(deliveryId: string): OrchestrationDeliveryRecord | undefined {
    const d = this.deliveries.get(deliveryId);
    return d ? { ...d, messageIds: [...d.messageIds] } : undefined;
  }

  takeMailboxDelivery(input: {
    runId: string;
    mailboxHandle: string;
    consumerHandle?: string;
    consumerGeneration: number;
    types?: readonly OrchestrationMessageType[];
    holdAck?: boolean;
    limit?: number;
  }): {
    delivery: OrchestrationDeliveryRecord;
    messages: OrchestrationMessageRecord[];
    replayed: boolean;
  } | undefined {
    if (input.mailboxHandle.startsWith('run:')) {
      const run = this.runs.get(input.runId);
      if (run && run.consumerGeneration !== input.consumerGeneration) {
        throw new OrchestrationError(
          'consumer_fenced',
          `Consumer generation ${input.consumerGeneration} for ${input.mailboxHandle} is fenced (current: ${run.consumerGeneration}).`
        );
      }
    }

    const perConsumerBroadcast =
      isBroadcastMailbox(input.mailboxHandle) && Boolean(input.consumerHandle);
    const outstanding = this.getOutstandingDelivery(input.mailboxHandle, input.consumerHandle);
    if (outstanding) {
      if (outstanding.consumerGeneration !== input.consumerGeneration) {
        throw new OrchestrationError(
          'consumer_fenced',
          `Outstanding delivery ${outstanding.id} belongs to consumer generation ${outstanding.consumerGeneration}, not ${input.consumerGeneration}.`
        );
      }
      const msgs = outstanding.messageIds
        .map((id) => this.messages.get(id))
        .filter((m): m is OrchestrationMessageRecord => m !== undefined)
        .map((m) => ({ ...m }));
      if (!input.holdAck) {
        const stored = this.deliveries.get(outstanding.id)!;
        stored.status = 'acknowledged';
        stored.acknowledgedAt = this.now();
        if (perConsumerBroadcast) {
          this.markAsReadAndDeliveredForConsumer(outstanding.messageIds, input.consumerHandle!);
        } else {
          this.markAsReadAndDelivered(outstanding.messageIds);
        }
        this.persist();
        return { delivery: { ...stored, messageIds: [...stored.messageIds] }, messages: msgs, replayed: true };
      }
      return { delivery: outstanding, messages: msgs, replayed: true };
    }

    const unread = this.getUnreadMessages(input.mailboxHandle, {
      types: input.types,
      consumerHandle: input.consumerHandle,
      limit: input.limit ?? ORCHESTRATION_DELIVERY_BATCH_LIMIT,
    });
    if (unread.length === 0) {
      return undefined;
    }

    const now = this.now();
    const messageIds = unread.map((m) => m.id);
    const delivery: OrchestrationDeliveryRecord = {
      id: generatePrefixedId('dlv'),
      runId: input.runId,
      mailboxHandle: input.mailboxHandle,
      ...(input.consumerHandle ? { consumerHandle: input.consumerHandle } : {}),
      messageIds,
      status: input.holdAck ? 'pending' : 'acknowledged',
      consumerGeneration: input.consumerGeneration,
      createdAt: now,
      acknowledgedAt: input.holdAck ? null : now,
    };
    this.deliveries.set(delivery.id, delivery);
    if (perConsumerBroadcast) {
      if (input.holdAck) {
        this.markAsDeliveredForConsumer(messageIds, input.consumerHandle!);
      } else {
        this.markAsReadAndDeliveredForConsumer(messageIds, input.consumerHandle!);
      }
    } else if (input.holdAck) {
      this.markAsDelivered(messageIds);
    } else {
      this.markAsReadAndDelivered(messageIds);
    }
    this.persist();
    return {
      delivery: { ...delivery, messageIds: [...delivery.messageIds] },
      messages: unread,
      replayed: false,
    };
  }

  acknowledgeDelivery(
    deliveryId: string,
    expectedMailboxHandle?: string,
    expectedConsumerGeneration?: number,
    expectedConsumerHandle?: string
  ): OrchestrationDeliveryRecord {
    const d = this.deliveries.get(deliveryId);
    if (!d) {
      throw new OrchestrationError('delivery_not_found', `Delivery not found: ${deliveryId}`);
    }
    const isSharedBroadcastTarget =
      (d.mailboxHandle === '@workers' || d.mailboxHandle === '@all') && Boolean(d.consumerHandle);
    if (isSharedBroadcastTarget) {
      if (expectedConsumerHandle && d.consumerHandle !== expectedConsumerHandle) {
        throw new OrchestrationError(
          'delivery_not_found',
          `Delivery ${deliveryId} does not belong to consumer ${expectedConsumerHandle}.`
        );
      }
    } else {
      if (expectedMailboxHandle && d.mailboxHandle !== expectedMailboxHandle) {
        throw new OrchestrationError(
          'stale_delivery',
          `Delivery ${deliveryId} belongs to ${d.mailboxHandle}, not ${expectedMailboxHandle}.`
        );
      }
      if (expectedConsumerHandle && d.consumerHandle && d.consumerHandle !== expectedConsumerHandle) {
        throw new OrchestrationError(
          'delivery_not_found',
          `Delivery ${deliveryId} does not belong to consumer ${expectedConsumerHandle}.`
        );
      }
    }
    const activeRun = d.mailboxHandle.startsWith('run:') ? this.runs.get(d.runId) : undefined;
    if (
      d.status === 'fenced' ||
      (expectedConsumerGeneration !== undefined &&
        d.consumerGeneration !== expectedConsumerGeneration) ||
      (activeRun && d.consumerGeneration !== activeRun.consumerGeneration)
    ) {
      throw new OrchestrationError(
        'consumer_fenced',
        `Delivery ${deliveryId} belongs to a fenced consumer generation.`
      );
    }
    if (d.status !== 'acknowledged') {
      d.status = 'acknowledged';
      d.acknowledgedAt = this.now();
      if (isBroadcastMailbox(d.mailboxHandle) && d.consumerHandle) {
        this.markAsReadAndDeliveredForConsumer(d.messageIds, d.consumerHandle);
      } else {
        this.markAsReadAndDelivered(d.messageIds);
      }
      this.prune();
      this.persist();
    }
    return { ...d, messageIds: [...d.messageIds] };
  }

  acknowledgeDeliveriesForMessages(
    messageIds: readonly string[],
    consumerHandle?: string
  ): number {
    if (messageIds.length === 0) return 0;
    const idSet = new Set(messageIds);
    const now = this.now();
    let count = 0;
    for (const d of this.deliveries.values()) {
      if (d.status !== 'pending') continue;
      if (consumerHandle && d.consumerHandle && d.consumerHandle !== consumerHandle) continue;
      if (!d.messageIds.some((id) => idSet.has(id))) continue;
      const allRead = d.messageIds.every((id) => {
        const msg = this.messages.get(id);
        return !msg || this.isMessageReadByConsumer(msg, d.consumerHandle);
      });
      if (!allRead) continue;
      d.status = 'acknowledged';
      d.acknowledgedAt = now;
      count += 1;
    }
    if (count > 0) {
      this.prune();
      this.persist();
    }
    return count;
  }

  getThreadMessages(threadId: string): OrchestrationMessageRecord[] {
    const result: OrchestrationMessageRecord[] = [];
    for (const id of this.messageOrder) {
      const m = this.messages.get(id);
      if (m && (m.id === threadId || m.threadId === threadId)) {
        result.push({ ...m });
      }
    }
    return result;
  }

  // ==================== Questions ====================

  createQuestion(input: {
    messageId: string;
    runId: string;
    dispatchId: string;
    fromHandle: string;
    toHandle: string;
    question: string;
    options?: string[];
  }): OrchestrationQuestionRecord {
    const now = this.now();
    const dispatch = this.dispatches.get(input.dispatchId);
    if (dispatch && ACTIVE_DISPATCH_STATUSES.has(dispatch.status)) {
      this.promoteDispatchObserved(dispatch, now, true);
    }
    const q: OrchestrationQuestionRecord = {
      messageId: input.messageId,
      runId: input.runId,
      dispatchId: input.dispatchId,
      fromHandle: input.fromHandle,
      toHandle: input.toHandle,
      question: input.question,
      options: [...(input.options ?? [])],
      status: 'pending',
      answerBody: null,
      answerMessageId: null,
      answeredBy: null,
      createdAt: now,
      answeredAt: null,
    };
    this.questions.set(q.messageId, q);
    this.persist();
    return { ...q, options: [...q.options] };
  }

  getQuestion(messageId: string): OrchestrationQuestionRecord | undefined {
    const q = this.questions.get(messageId);
    return q ? { ...q, options: [...q.options] } : undefined;
  }

  listQuestions(filter?: {
    runId?: string;
    dispatchId?: string;
    status?: OrchestrationQuestionRecord['status'];
  }): OrchestrationQuestionRecord[] {
    return [...this.questions.values()]
      .filter((q) => {
        if (filter?.runId && q.runId !== filter.runId) return false;
        if (filter?.dispatchId && q.dispatchId !== filter.dispatchId) return false;
        if (filter?.status && q.status !== filter.status) return false;
        return true;
      })
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((q) => ({ ...q, options: [...q.options] }));
  }

  answerQuestion(input: {
    messageId: string;
    answeredBy: string;
    body: string;
    fromRole?: string;
    mentions?: readonly string[];
    refs?: readonly number[];
    existingAnswerMessageId?: string;
  }): {
    question: OrchestrationQuestionRecord;
    message: OrchestrationMessageRecord;
    duplicate: boolean;
  } {
    const q = this.questions.get(input.messageId);
    if (!q) {
      throw new OrchestrationError('not_found', `Question not found: ${input.messageId}`);
    }
    if (q.status === 'closed') {
      throw new OrchestrationError(
        'dispatch_inactive',
        `Question ${q.messageId} is closed because dispatch ${q.dispatchId} ended.`
      );
    }
    if (q.status === 'answered') {
      if ((q.answerBody ?? '').trim() === input.body.trim()) {
        const existingMsg = q.answerMessageId
          ? this.getMessageById(q.answerMessageId)
          : undefined;
        return {
          question: { ...q, options: [...q.options] },
          message:
            existingMsg ??
            this.insertMessage({
              runId: q.runId,
              from: input.answeredBy,
              to: `dispatch:${q.dispatchId}`,
              type: 'status',
              subject: `Re: ${q.question.slice(0, 60)}`,
              body: input.body,
              threadId: q.messageId,
              fromRole: input.fromRole,
              mentions: input.mentions,
              refs: input.refs,
              kind: 'answer',
            }),
          duplicate: true,
        };
      }
      throw new OrchestrationError(
        'answer_conflict',
        `Question ${q.messageId} was already answered differently by ${q.answeredBy}.`,
        { messageId: q.messageId, existingAnswer: q.answerBody }
      );
    }

    const reusedMsg = input.existingAnswerMessageId
      ? this.messages.get(input.existingAnswerMessageId)
      : undefined;
    const answerMsg =
      reusedMsg ??
      this.insertMessage({
        runId: q.runId,
        from: input.answeredBy,
        to: `dispatch:${q.dispatchId}`,
        type: 'status',
        priority: 'high',
        subject: `Answer to question ${q.messageId}`,
        body: input.body,
        threadId: q.messageId,
        fromRole: input.fromRole,
        mentions: input.mentions,
        refs: input.refs,
        kind: 'answer',
      });
    this.markAsRead([q.messageId]);
    this.acknowledgeDeliveriesForMessages([q.messageId]);

    q.status = 'answered';
    q.answerBody = input.body;
    q.answerMessageId = answerMsg.id;
    q.answeredBy = input.answeredBy;
    q.answeredAt = this.now();
    this.persist();
    return {
      question: { ...q, options: [...q.options] },
      message: { ...answerMsg },
      duplicate: false,
    };
  }

  closeQuestionsForDispatch(dispatchId: string): void {
    let changed = false;
    for (const q of this.questions.values()) {
      if (q.dispatchId === dispatchId && q.status === 'pending') {
        q.status = 'closed';
        changed = true;
      }
    }
    if (changed) {
      this.persist();
    }
  }

  // ==================== Coordinator Runs ====================

  createCoordinatorRun(input: {
    runId: string;
    spec: string;
    coordinatorHandle: string;
    pollIntervalMs: number;
    maxConcurrent: number;
  }): OrchestrationCoordinatorRunRecord {
    const existing = this.getActiveCoordinatorRun();
    if (existing) {
      throw new OrchestrationError(
        'coordinator_already_running',
        `Coordinator already running: ${existing.id}`
      );
    }
    this.requireRun(input.runId);
    const cr: OrchestrationCoordinatorRunRecord = {
      id: generatePrefixedId('coord'),
      runId: input.runId,
      spec: input.spec,
      status: 'running',
      coordinatorHandle: input.coordinatorHandle,
      pollIntervalMs: input.pollIntervalMs,
      maxConcurrent: input.maxConcurrent,
      startedAt: this.now(),
      endedAt: null,
      error: null,
    };
    this.coordinatorRuns.set(cr.id, cr);
    this.persist();
    return { ...cr };
  }

  getCoordinatorRun(id: string): OrchestrationCoordinatorRunRecord | undefined {
    const cr = this.coordinatorRuns.get(id);
    return cr ? { ...cr } : undefined;
  }

  getActiveCoordinatorRun(): OrchestrationCoordinatorRunRecord | undefined {
    for (const cr of this.coordinatorRuns.values()) {
      if (cr.status === 'running' || cr.status === 'paused') {
        return { ...cr };
      }
    }
    return undefined;
  }

  listCoordinatorRuns(runId?: string): OrchestrationCoordinatorRunRecord[] {
    return [...this.coordinatorRuns.values()]
      .filter((c) => !runId || c.runId === runId)
      .sort((a, b) => a.startedAt.localeCompare(b.startedAt))
      .map((c) => ({ ...c }));
  }

  updateCoordinatorRun(
    id: string,
    patch: Partial<Pick<OrchestrationCoordinatorRunRecord, 'status' | 'endedAt' | 'error'>>
  ): OrchestrationCoordinatorRunRecord {
    const cr = this.coordinatorRuns.get(id);
    if (!cr) {
      throw new OrchestrationError('not_found', `Coordinator run not found: ${id}`);
    }
    if (patch.status !== undefined) cr.status = patch.status;
    if (patch.endedAt !== undefined) cr.endedAt = patch.endedAt;
    if (patch.error !== undefined) cr.error = patch.error;
    this.persist();
    return { ...cr };
  }

  // ==================== Snapshot ====================

  getSnapshot(runId?: string): OrchestrationSnapshot {
    const tasks = this.listTasksWithDispatch(runId ? { runId } : undefined);
    const runs = this.listRuns();
    const dispatches = this.listDispatches(runId ? { runId } : undefined).map((dispatch) => {
      if (!dispatch.archivedTimeline) return dispatch;
      const { archivedTimeline: _timeline, ...rest } = dispatch;
      return { ...rest, hasArchivedTimeline: true };
    });
    const gates = this.listGates(runId ? { runId } : undefined);
    const messages = this.listMessages(runId ? { runId, limit: 200 } : { limit: 200 });
    const questions = this.listQuestions(runId ? { runId } : undefined);
    const coordinatorRuns = [...this.coordinatorRuns.values()]
      .filter((c) => !runId || c.runId === runId)
      .sort((a, b) => a.startedAt.localeCompare(b.startedAt))
      .map((c) => ({ ...c }));
    const activeCoordinatorRun = this.getActiveCoordinatorRun() ?? null;
    return {
      runs,
      tasks,
      dispatches,
      gates,
      messages,
      questions,
      memberCursorsByRun: this.exportMemberCursorsByRun(runId),
      coordinatorRuns,
      activeCoordinatorRun,
      currentRunByConsumer: Object.fromEntries(this.currentRunByConsumer.entries()),
    };
  }

  private deleteMessageIds(ids: ReadonlySet<string>): void {
    if (ids.size === 0) return;
    for (const id of ids) {
      this.messages.delete(id);
      this.consumerReadByMessageId.delete(id);
      this.consumerDeliveredByMessageId.delete(id);
    }
    this.messageOrder = this.messageOrder.filter((id) => !ids.has(id));
  }

  prune(): void {
    const protectedMsgIds = new Set<string>();
    for (const d of this.deliveries.values()) {
      if (d.status === 'pending') {
        for (const msgId of d.messageIds) {
          protectedMsgIds.add(msgId);
        }
      }
    }
    for (const q of this.questions.values()) {
      if (q.status === 'pending') {
        protectedMsgIds.add(q.messageId);
      }
    }
    for (const msg of this.messages.values()) {
      if (!msg.read) {
        protectedMsgIds.add(msg.id);
      }
    }

    const toDeleteMsgIds = new Set<string>();
    const runMsgCounts = new Map<string, number>();
    for (const id of this.messageOrder) {
      const msg = this.messages.get(id);
      if (!msg) continue;
      runMsgCounts.set(msg.runId, (runMsgCounts.get(msg.runId) ?? 0) + 1);
    }

    for (const id of this.messageOrder) {
      const msg = this.messages.get(id);
      if (!msg || protectedMsgIds.has(id)) continue;
      const count = runMsgCounts.get(msg.runId) ?? 0;
      if (count > this.maxMessagesPerRun) {
        toDeleteMsgIds.add(id);
        runMsgCounts.set(msg.runId, count - 1);
      }
    }

    let remainingCount = this.messageOrder.length - toDeleteMsgIds.size;
    if (remainingCount > this.maxMessages) {
      for (const id of this.messageOrder) {
        if (remainingCount <= this.maxMessages) break;
        if (toDeleteMsgIds.has(id)) continue;
        const msg = this.messages.get(id);
        if (!msg || protectedMsgIds.has(id)) continue;
        toDeleteMsgIds.add(id);
        remainingCount -= 1;
      }
    }

    if (toDeleteMsgIds.size > 0) {
      this.deleteMessageIds(toDeleteMsgIds);
    }

    for (const [id, d] of this.deliveries.entries()) {
      if (d.status !== 'pending' && d.messageIds.every((msgId) => !this.messages.has(msgId))) {
        this.deliveries.delete(id);
      }
    }

    if (this.deliveries.size > this.maxDeliveries) {
      const settledDeliveries = [...this.deliveries.values()]
        .filter((d) => d.status !== 'pending')
        .sort((a, b) => a.createdAt.localeCompare(b.createdAt));
      let overflow = this.deliveries.size - this.maxDeliveries;
      for (const d of settledDeliveries) {
        if (overflow <= 0) break;
        this.deliveries.delete(d.id);
        overflow -= 1;
      }
    }

    const nonUnboundRuns = [...this.runs.values()].filter((r) => r.id !== UNBOUND_RUN_ID);
    if (nonUnboundRuns.length > this.maxRuns) {
      const boundRunIds = new Set(this.currentRunByConsumer.values());
      const activeCoordRunIds = new Set(
        [...this.coordinatorRuns.values()]
          .filter((cr) => cr.status === 'running' || cr.status === 'paused')
          .map((cr) => cr.runId)
      );
      const activeDispatchRunIds = new Set(
        [...this.dispatches.values()]
          .filter((d) => ACTIVE_DISPATCH_STATUSES.has(d.status))
          .map((d) => d.runId)
      );

      const candidates = nonUnboundRuns
        .filter(
          (r) =>
            (r.status === 'completed' || r.status === 'failed' || r.status === 'stopped') &&
            !boundRunIds.has(r.id) &&
            !activeCoordRunIds.has(r.id) &&
            !activeDispatchRunIds.has(r.id)
        )
        .sort((a, b) => a.updatedAt.localeCompare(b.updatedAt));

      const deleteCount = Math.min(candidates.length, nonUnboundRuns.length - this.maxRuns);
      if (deleteCount > 0) {
        const deletedRunIds = new Set(candidates.slice(0, deleteCount).map((r) => r.id));
        for (const runId of deletedRunIds) {
          this.runs.delete(runId);
          this.nextBoardSeqByRun.delete(runId);
          this.memberCursorsByRun.delete(runId);
        }
        for (const [id, t] of this.tasks.entries()) {
          if (deletedRunIds.has(t.runId)) this.tasks.delete(id);
        }
        for (const [id, d] of this.dispatches.entries()) {
          if (deletedRunIds.has(d.runId)) {
            this.cleanupDispatchBrief(id);
            this.dispatches.delete(id);
            this.dispatchCapabilities.delete(id);
          }
        }
        for (const [id, g] of this.gates.entries()) {
          if (deletedRunIds.has(g.runId)) this.gates.delete(id);
        }
        for (const [id, q] of this.questions.entries()) {
          if (deletedRunIds.has(q.runId)) this.questions.delete(id);
        }
        for (const [id, d] of this.deliveries.entries()) {
          if (deletedRunIds.has(d.runId)) this.deliveries.delete(id);
        }
        for (const [id, cr] of this.coordinatorRuns.entries()) {
          if (deletedRunIds.has(cr.runId)) this.coordinatorRuns.delete(id);
        }
        const runMsgIds = new Set<string>();
        for (const [id, m] of this.messages.entries()) {
          if (deletedRunIds.has(m.runId)) runMsgIds.add(id);
        }
        if (runMsgIds.size > 0) {
          this.deleteMessageIds(runMsgIds);
        }
      }
    }
  }

  reset(scope: 'all' | 'tasks' | 'messages'): {
    reset: 'all' | 'tasks' | 'messages';
    removed: {
      runs: number;
      tasks: number;
      dispatches: number;
      gates: number;
      messages: number;
    };
  } {
    const removed = {
      runs: 0,
      tasks: 0,
      dispatches: 0,
      gates: 0,
      messages: 0,
    };

    if (scope !== 'messages') {
      for (const id of this.dispatches.keys()) this.cleanupDispatchBrief(id);
    }
    if (scope === 'all') {
      removed.runs = [...this.runs.keys()].filter((id) => id !== UNBOUND_RUN_ID).length;
      removed.tasks = this.tasks.size;
      removed.dispatches = this.dispatches.size;
      removed.gates = this.gates.size;
      removed.messages = this.messages.size;
      this.currentRunByConsumer.clear();
      this.runs.clear();
      this.tasks.clear();
      this.dispatches.clear();
      this.dispatchCapabilities.clear();
      this.deferredPrompts.clear();
      this.gates.clear();
      this.messages.clear();
      this.messageOrder = [];
      this.deliveries.clear();
      this.consumerReadByMessageId.clear();
      this.consumerDeliveredByMessageId.clear();
      this.questions.clear();
      this.coordinatorRuns.clear();
      this.nextSequence = 1;
      this.nextBoardSeqByRun.clear();
      this.memberCursorsByRun.clear();
    } else if (scope === 'tasks') {
      removed.tasks = this.tasks.size;
      removed.dispatches = this.dispatches.size;
      removed.gates = this.gates.size;
      this.tasks.clear();
      this.dispatches.clear();
      this.dispatchCapabilities.clear();
      this.deferredPrompts.clear();
      this.gates.clear();
      this.questions.clear();
      this.coordinatorRuns.clear();
    } else {
      removed.messages = this.messages.size;
      this.messages.clear();
      this.messageOrder = [];
      this.deliveries.clear();
      this.consumerReadByMessageId.clear();
      this.consumerDeliveredByMessageId.clear();
      this.questions.clear();
      this.nextBoardSeqByRun.clear();
      this.memberCursorsByRun.clear();
    }

    if (scope !== 'messages') this.reconcileBriefFiles();
    this.persist();
    return { reset: scope, removed };
  }
}

export { emptyPersistedState };
