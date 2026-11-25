import { randomUUID } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { basename } from 'node:path';
import { AtomicSnapshotWriter } from '../persistence/atomic-snapshot-writer.js';
import type {
  AgentGoalAction, AgentProvider, AgentQuestion, AgentRuntimeRequest, AgentRuntimeResponse, AgentSessionDriver,
  AgentSessionEvents, AgentSessionGoal, AgentSessionPolicy, AgentTurnAttachment, TurnDelivery,
} from './agent-session-driver.js';
import { WorkerTimeline, type WorkerTimelineChange, type WorkerTimelineSnapshot } from './worker-timeline.js';

export const AGENT_SESSION_ID = /^agent-session-[a-f0-9-]{36}$/;

/** `suspended`: not running, but its provider conversation can be resumed by sending a message. */
export type AgentSessionStatus = 'starting' | 'idle' | 'running' | 'suspended' | 'closed' | 'failed';

export interface AgentSessionInfo {
  id: string;
  provider: AgentProvider;
  title: string;
  cwd: string;
  model: string | null;
  effort: string | null;
  policy: AgentSessionPolicy;
  network: boolean;
  /** Acts as a Coordinator through Aha MCP; only switched while idle, and kept across restarts. */
  orchestration: boolean;
  providerSessionId: string | null;
  status: AgentSessionStatus;
  error: string | null;
  /** Questions the agent is blocked on until the person answers or skips them. */
  pendingQuestions: { requestId: string; questions: AgentQuestion[] }[];
  /** Messages written while a turn ran; each goes out once the turn before it completes. */
  queued: QueuedMessage[];
  /** A turn ended without completing (or a queued message was refused), so the queue waits for the person. */
  queuePaused: boolean;
  /** Active or paused goal set via `/goal` on the provider thread, if any. */
  goal: AgentSessionGoal | null;
  createdAt: number;
}

export interface QueuedMessage {
  id: string;
  text: string;
  attachments: AgentTurnAttachment[];
  model?: string | null;
  effort?: string | null;
  policy?: AgentSessionPolicy;
  network?: boolean;
  freshContext?: boolean;
  queuedAt: number;
}

type MessageInput = {
  text: string; model?: string | null; effort?: string | null; policy?: AgentSessionPolicy; network?: boolean;
  freshContext?: boolean; attachments?: readonly AgentTurnAttachment[];
};

// The queue lives only as long as the process: after a restart nothing is sent on its own.
type SavedSession = Omit<AgentSessionInfo, 'status' | 'error' | 'pendingQuestions' | 'queued' | 'queuePaused' | 'goal'> & { timeline: WorkerTimelineSnapshot };

interface PendingRequest {
  turnKey: string;
  request: AgentRuntimeRequest;
  resolve: (response: AgentRuntimeResponse) => void;
}

interface Session {
  info: AgentSessionInfo;
  driver: AgentSessionDriver | null;
  timeline: WorkerTimeline;
  turns: number;
  requests: Map<string, PendingRequest>;
  /** Bumped for each provider process, so a replaced process cannot change the session. */
  generation: number;
  /** The provider accepted a turn; Codex saves a thread only then, so before it there is nothing to resume. */
  hasTurns: boolean;
  /** The person interrupted while a `/goal` turn was being started; its goal must not stay active. */
  goalTurnInterrupted?: boolean;
  /** Policy the live process was launched with; Aha MCP approvals are baked in at that point. */
  launchedPolicy: AgentSessionPolicy | null;
  /** A title the person chose that the provider has not stored yet; sent when a process is next up. */
  unsyncedName: string | null;
}

export interface AgentSessionManagerDependencies {
  launch: (
    sessionId: string, cwd: string, events: AgentSessionEvents,
    options: { orchestration: boolean; policy: AgentSessionPolicy },
  ) => AgentSessionDriver;
  /** Coordinator rules for an orchestrating session's conversation; without it orchestration mode is unavailable. */
  coordinatorInstructions?: (sessionId: string, options: { policy: AgentSessionPolicy }) => string;
  /** A session stopped coordinating (switched off or closed); its pending orchestration state can go. */
  onCoordinatorEnded?: (sessionId: string) => void;
  /**
   * A message the person typed started a turn in an orchestrating session. Only these turns can
   * confirm the Coordinator's dispatch proposals; turns AhaStation starts itself never call this.
   */
  onUserTurn?: (sessionId: string) => void;
  /** An orchestrating session became idle and can take a system turn (see `wake`). */
  onCoordinatorIdle?: (sessionId: string) => void;
  /** Where sessions are kept across restarts; omitted keeps them in memory only. */
  storageFile?: () => string;
  now?: () => number;
}

const DECLINE: AgentRuntimeResponse = { decision: 'decline' };
const MAX_SAVED_SESSIONS = 20;
export const MAX_QUEUED_MESSAGES = 10;
const MAX_GOAL_OBJECTIVE_CHARS = 2_000;

const GOAL_STATUS_LABELS: Record<AgentSessionGoal['status'], string> = {
  active: '进行中',
  paused: '已暂停',
  blocked: '已阻塞',
  usageLimited: '额度受限',
  budgetLimited: '预算受限',
  complete: '已完成',
};

const describeGoal = (goal: AgentSessionGoal) => `当前目标（${GOAL_STATUS_LABELS[goal.status]}）：${goal.objective}`;

/**
 * Conversations a person drives directly, outside orchestration: no dispatch, no report_done. The
 * person in the session is the only writer and the only one who answers its approvals and questions.
 */
export class AgentSessionManager {
  private readonly sessions = new Map<string, Session>();
  private readonly timelineListeners = new Set<(change: WorkerTimelineChange) => void>();
  private readonly sessionListeners = new Set<(sessions: AgentSessionInfo[]) => void>();
  private readonly now: () => number;
  private readonly writer: AtomicSnapshotWriter | null;
  private disposing = false;

  constructor(private readonly deps: AgentSessionManagerDependencies) {
    this.now = deps.now ?? Date.now;
    this.writer = deps.storageFile ? new AtomicSnapshotWriter(deps.storageFile, { skipUnchanged: true, fileMode: 0o600, directoryMode: 0o700 }) : null;
    this.load();
  }

  onTimelineChange(listener: (change: WorkerTimelineChange) => void): () => void {
    this.timelineListeners.add(listener);
    return () => this.timelineListeners.delete(listener);
  }

  onSessionsChanged(listener: (sessions: AgentSessionInfo[]) => void): () => void {
    this.sessionListeners.add(listener);
    return () => this.sessionListeners.delete(listener);
  }

  list(): AgentSessionInfo[] {
    return [...this.sessions.values()].map((session) => structuredClone(session.info));
  }

  getTimeline(id: string): WorkerTimelineSnapshot | null {
    return this.sessions.get(id)?.timeline.snapshot() ?? null;
  }

  async create(input: {
    cwd: string; policy: AgentSessionPolicy; model?: string | null; effort?: string | null; network?: boolean;
    title?: string;
  }): Promise<AgentSessionInfo> {
    const info: AgentSessionInfo = {
      id: `agent-session-${randomUUID()}`, provider: 'codex', title: input.title?.trim() || '', cwd: input.cwd,
      model: input.model ?? null, effort: input.effort ?? null, policy: input.policy, network: input.network ?? false,
      orchestration: false, providerSessionId: null, status: 'starting', error: null, pendingQuestions: [],
      queued: [], queuePaused: false, goal: null, createdAt: this.now(),
    };
    const session = this.track(info);
    this.sessionsChanged();
    await this.connect(session);
    return structuredClone(info);
  }

  /**
   * Reopens a stored provider conversation without starting a process: the session is suspended
   * with the replayed history, and the first message resumes the thread. A conversation already
   * open here is returned as it is.
   */
  adopt(input: {
    providerSessionId: string; cwd: string; title: string; policy: AgentSessionPolicy;
    model?: string | null; effort?: string | null;
    timeline: (sessionId: string) => WorkerTimelineSnapshot;
  }): AgentSessionInfo {
    const open = [...this.sessions.values()].find((session) => session.info.providerSessionId === input.providerSessionId);
    if (open) return structuredClone(open.info);
    const id = `agent-session-${randomUUID()}`;
    const info: AgentSessionInfo = {
      id, provider: 'codex', title: input.title.trim(), cwd: input.cwd,
      model: input.model ?? null, effort: input.effort ?? null, policy: input.policy, network: false,
      orchestration: false, providerSessionId: input.providerSessionId, status: 'suspended', error: null,
      pendingQuestions: [], queued: [], queuePaused: false, goal: null, createdAt: this.now(),
    };
    const session = this.track(info, input.timeline(id));
    // The stored thread exists at the provider, so it is rejoined even if no turn replayed.
    session.hasTurns = true;
    session.timeline.setMeta({ alive: false, providerSessionId: input.providerSessionId });
    this.sessionsChanged();
    return structuredClone(info);
  }

  /**
   * Starts a turn with the person's message, resuming the provider conversation first if the
   * session was suspended or its process died. A session takes one turn at a time.
   */
  async send(id: string, input: MessageInput): Promise<TurnDelivery> {
    const session = this.sessions.get(id);
    const text = input.text.trim();
    if (!session) return { status: 'not_accepted', reason: 'Session not found' };
    if (!text && !input.attachments?.length) return { status: 'not_accepted', reason: 'Message is empty' };
    const { info } = session;
    const compact = !input.attachments?.length && /^\/compact\s*$/iu.test(text);
    if (compact && (input.freshContext || !session.hasTurns)) {
      return { status: 'not_accepted', reason: '还没有可压缩的对话记录' };
    }
    const wanted = input.policy ?? info.policy;
    if (input.freshContext) {
      if (info.status === 'running' || info.status === 'starting' || session.driver?.busy) {
        return { status: 'not_accepted', reason: `Session is ${info.status}` };
      }
      if (info.status === 'closed') return { status: 'not_accepted', reason: 'Session is closed' };
      const previous = info.policy;
      if (info.orchestration) info.policy = wanted;
      info.goal = null;
      session.hasTurns = false;
      if (info.title) session.unsyncedName = info.title;
      if (session.driver?.alive) {
        const driver = session.driver;
        session.generation++;
        session.driver = null;
        info.status = 'starting';
        this.sessionsChanged();
        await driver.close();
      }
      await this.connect(session, false);
      if ((info.status as AgentSessionStatus) !== 'idle') {
        info.policy = previous;
        this.sessionsChanged();
        return { status: 'not_accepted', reason: info.error ?? '无法开启新上下文' };
      }
    } else if (this.resumable(session)) {
      const previous = info.policy;
      if (info.orchestration) info.policy = wanted;
      await this.connect(session, true);
      if (session.info.status !== 'idle') {
        info.policy = previous;
        this.sessionsChanged();
        return { status: 'not_accepted', reason: session.info.error ?? 'Session did not resume' };
      }
    }
    if (!session.driver?.alive || session.info.status !== 'idle') {
      return { status: 'not_accepted', reason: `Session is ${session.info.status}` };
    }
    // An orchestrating session whose approval tier changed must be relaunched: the Aha MCP tool
    // approvals are baked into the process arguments, so a per-turn policy delta cannot grant
    // manage_worker under `full-auto`. The other tiers differ only per turn and need no relaunch.
    if (info.orchestration && (wanted === 'full-auto') !== (session.launchedPolicy === 'full-auto')) {
      const previous = info.policy;
      info.policy = wanted;
      const driver = session.driver;
      session.generation++;
      session.driver = null;
      info.status = 'starting';
      this.sessionsChanged();
      await driver.close();
      await this.connect(session, true);
      // connect() moved the status on; widen the type narrowed by the assignment above.
      if ((info.status as AgentSessionStatus) !== 'idle') {
        info.policy = previous;
        this.sessionsChanged();
        return { status: 'not_accepted', reason: info.error ?? 'Session did not restart' };
      }
    }
    if (compact) {
      if (!session.driver?.compactThread) {
        return { status: 'not_accepted', reason: '当前会话不支持压缩上下文' };
      }
      return this.startCompactTurn(session);
    }
    if (!session.info.title) {
      const firstSkill = input.attachments?.find((item) => item.type === 'skill');
      session.info.title = (text.split('\n')[0] || (firstSkill?.type === 'skill' ? `$${firstSkill.name}` : '附件')).slice(0, 60);
    }
    const delivery = await this.startTurn(session, text, input);
    if (delivery.status === 'accepted' && session.info.orchestration) this.deps.onUserTurn?.(id);
    // Codex may refuse to name a thread before its first turn; try again once it has one.
    if (delivery.status === 'accepted' && session.unsyncedName) void this.syncName(session);
    return delivery;
  }

  /**
   * Keeps a message for after the running turn. A session with nothing running takes it at once
   * instead, so a turn that just ended does not strand it; a paused queue stays where it is.
   */
  async enqueue(id: string, input: MessageInput): Promise<{ ok: true; queued: boolean } | { ok: false; error: string }> {
    const session = this.sessions.get(id);
    if (!session) return { ok: false, error: '会话不存在' };
    const text = input.text.trim();
    if (!text && !input.attachments?.length) return { ok: false, error: '消息是空的' };
    const { info } = session;
    const busy = info.status === 'running' || info.status === 'starting' || Boolean(session.driver?.busy);
    if (!busy && (!info.queued.length || info.queuePaused)) {
      const delivery = await this.send(id, input);
      return delivery.status === 'accepted' ? { ok: true, queued: false } : { ok: false, error: delivery.reason };
    }
    if (info.status === 'closed') return { ok: false, error: '会话已结束' };
    if (info.queued.length >= MAX_QUEUED_MESSAGES) return { ok: false, error: `最多排队 ${MAX_QUEUED_MESSAGES} 条消息` };
    info.queued.push({
      id: `q_${randomUUID().replace(/-/g, '').slice(0, 12)}`, text, attachments: [...(input.attachments ?? [])],
      ...(input.model !== undefined ? { model: input.model } : {}),
      ...(input.effort !== undefined ? { effort: input.effort } : {}),
      ...(input.policy ? { policy: input.policy } : {}),
      ...(input.network !== undefined ? { network: input.network } : {}),
      ...(input.freshContext ? { freshContext: true } : {}),
      queuedAt: this.now(),
    });
    this.sessionsChanged();
    return { ok: true, queued: true };
  }

  /** Takes a message out of the queue (to edit or drop it) and returns it. */
  unqueue(id: string, messageId: string): QueuedMessage | null {
    const session = this.sessions.get(id);
    const index = session?.info.queued.findIndex((message) => message.id === messageId) ?? -1;
    if (!session || index < 0) return null;
    const [message] = session.info.queued.splice(index, 1);
    if (!session.info.queued.length) session.info.queuePaused = false;
    this.sessionsChanged();
    return message!;
  }

  /** Lets a paused queue go on; the next message is sent now if nothing is running. */
  resumeQueue(id: string): boolean {
    const session = this.sessions.get(id);
    if (!session) return false;
    session.info.queuePaused = false;
    this.sessionsChanged();
    this.drainQueue(session);
    return true;
  }

  /** Sends the next queued message if the session can take a turn; true when one went out. */
  private drainQueue(session: Session): boolean {
    const { info } = session;
    if (info.queuePaused || !info.queued.length || this.disposing) return false;
    const idle = info.status === 'idle' && Boolean(session.driver?.alive) && !session.driver?.busy && session.requests.size === 0;
    if (!idle && !this.resumable(session)) return false;
    const next = info.queued.shift()!;
    this.sessionsChanged();
    const { id: _id, queuedAt: _at, ...message } = next;
    // send() moves the status off idle before its first await, so nothing else takes this turn.
    void this.send(info.id, message).then((delivery) => {
      if (delivery.status === 'accepted' || !this.sessions.has(info.id)) return;
      info.queuePaused = true;
      this.sessionsChanged();
    });
    return true;
  }

  /**
   * Starts a system turn in an idle orchestrating session, for Worker news and stall reminders. It
   * never resumes a stopped conversation and never counts as the person's message.
   */
  async wake(id: string, prompt: string): Promise<TurnDelivery> {
    const session = this.sessions.get(id);
    const text = prompt.trim();
    if (!session || !text) return { status: 'not_accepted', reason: 'Nothing to deliver' };
    if (!this.coordinatorState(id)?.idle) return { status: 'not_accepted', reason: 'Session is not an idle Coordinator' };
    if (session.info.queued.length && !session.info.queuePaused) return { status: 'not_accepted', reason: 'The person has messages queued' };
    return this.startTurn(session, text, {}, 'system');
  }

  private async startTurn(
    session: Session, text: string,
    input: {
      model?: string | null; effort?: string | null; policy?: AgentSessionPolicy; network?: boolean;
      freshContext?: boolean; attachments?: readonly AgentTurnAttachment[];
    },
    origin?: 'system',
    display?: { prompt?: string; commandKind?: 'compact' | 'goal' },
  ): Promise<TurnDelivery> {
    const { info, timeline } = session;
    const driver = session.driver!;
    const turnKey = `${info.id}:${++session.turns}`;
    const attachments = input.attachments ?? [];
    info.status = 'running';
    timeline.beginTurn({
      dispatchId: turnKey, prompt: display?.prompt ?? text, ...(origin ? { origin } : {}),
      ...(input.freshContext ? { freshContext: true } : {}),
      ...(display?.commandKind ? { commandKind: display.commandKind } : {}),
      attachments: attachments.map((item) => item.type === 'skill'
        ? { kind: 'skill' as const, name: item.name }
        : { kind: item.type === 'localImage' ? 'image' as const : 'file' as const, name: item.name || basename(item.path) }),
    });
    this.sessionsChanged();
    const delivery = await driver.startTurn({
      turnKey, prompt: text, ...(attachments.length ? { attachments } : {}),
      effort: input.effort ?? info.effort,
      ...(input.model && input.model !== info.model ? { model: input.model } : {}),
      ...(input.policy && input.policy !== info.policy ? { policy: input.policy } : {}),
      ...(input.network !== undefined && input.network !== info.network ? { network: input.network } : {}),
    });
    if (delivery.status === 'accepted') {
      session.hasTurns = true;
      if (input.model) info.model = input.model;
      if (input.effort !== undefined) info.effort = input.effort;
      if (input.policy) info.policy = input.policy;
      if (input.network !== undefined) info.network = input.network;
      timeline.setMeta({ model: info.model, readOnly: info.policy === 'read-only' });
    } else {
      timeline.endActiveTurn('failed', delivery.reason);
      // Unknown acceptance leaves the provider thread in an unknown state; do not send into it again.
      if (delivery.status === 'unknown_acceptance') {
        info.error = delivery.reason;
        await driver.close();
      } else if (info.status === 'running') {
        info.status = 'idle';
      }
    }
    this.sessionsChanged();
    if (delivery.status !== 'accepted') this.coordinatorSettled(session);
    return delivery;
  }

  private async startCompactTurn(session: Session): Promise<TurnDelivery> {
    const { info, timeline } = session;
    const driver = session.driver!;
    const turnKey = `${info.id}:${++session.turns}`;
    info.status = 'running';
    timeline.beginTurn({ dispatchId: turnKey, prompt: '/compact', commandKind: 'compact' });
    this.sessionsChanged();
    const delivery = await driver.compactThread!(turnKey);
    if (delivery.status !== 'accepted') {
      timeline.endActiveTurn('failed', delivery.reason);
      if (delivery.status === 'unknown_acceptance') {
        info.error = delivery.reason;
        await driver.close();
      } else if (info.status === 'running') {
        info.status = 'idle';
      }
    }
    this.sessionsChanged();
    if (delivery.status !== 'accepted') this.coordinatorSettled(session);
    return delivery;
  }

  /**
   * Starts the turn that works on a paused goal, then activates the goal (T3 Code's order). If the
   * person interrupted the turn before the activation landed, the goal is paused again.
   */
  private async startGoalTurn(session: Session, prompt: string, shown: string): Promise<TurnDelivery> {
    const driver = session.driver!;
    session.goalTurnInterrupted = false;
    const delivery = await this.startTurn(session, prompt, {}, undefined, { prompt: shown, commandKind: 'goal' });
    if (delivery.status !== 'accepted' || !driver.setGoal) return delivery;
    session.info.goal = await driver.setGoal({ status: 'active' });
    if (session.goalTurnInterrupted) session.info.goal = await driver.setGoal({ status: 'paused' });
    this.sessionsChanged();
    return delivery;
  }

  /** Reads, sets, pauses, resumes, or clears the session's Codex goal (`/goal`). */
  async runGoal(id: string, action: AgentGoalAction): Promise<
    { ok: true; goal: AgentSessionGoal | null; message: string } | { ok: false; error: string }
  > {
    const session = this.sessions.get(id);
    if (!session) return { ok: false, error: '会话不存在' };
    const { info } = session;
    if (info.status === 'closed') return { ok: false, error: '会话已结束' };
    if (action.type === 'set') {
      const objective = action.objective.trim();
      if (!objective) return { ok: false, error: '请输入目标内容' };
      if ([...objective].length > MAX_GOAL_OBJECTIVE_CHARS) {
        return { ok: false, error: `目标内容最多 ${MAX_GOAL_OBJECTIVE_CHARS} 个字符` };
      }
    }
    if (action.type === 'show' && !session.driver?.alive && !session.hasTurns) {
      return { ok: true, goal: null, message: '当前没有设定目标' };
    }
    if (!session.driver?.alive) {
      if (this.resumable(session)) {
        await this.connect(session, true);
      }
      if (!session.driver?.alive) {
        return { ok: false, error: info.error ?? '会话未连接' };
      }
    }
    const driver = session.driver;
    if (!driver.getGoal || !driver.setGoal || !driver.clearGoal) {
      return { ok: false, error: '当前会话不支持 /goal 命令' };
    }
    try {
      if (action.type === 'show') {
        const goal = await driver.getGoal();
        info.goal = goal;
        this.sessionsChanged();
        return { ok: true, goal, message: goal ? describeGoal(goal) : '当前没有设定目标' };
      }
      if (action.type === 'clear') {
        const cleared = await driver.clearGoal();
        info.goal = null;
        this.sessionsChanged();
        return { ok: true, goal: null, message: cleared ? '已清除会话目标' : '当前没有设定目标' };
      }
      if (action.type === 'pause') {
        const current = await driver.getGoal();
        if (!current) {
          info.goal = null;
          this.sessionsChanged();
          return { ok: true, goal: null, message: '当前没有设定目标' };
        }
        const goal = await driver.setGoal({ status: 'paused' });
        info.goal = goal;
        this.sessionsChanged();
        return { ok: true, goal, message: '已暂停目标推进' };
      }
      if (action.type === 'resume') {
        const current = await driver.getGoal();
        if (!current) {
          info.goal = null;
          this.sessionsChanged();
          return { ok: false, error: '当前没有设定目标' };
        }
        if (info.status === 'running' || driver.busy) {
          return { ok: false, error: '当前有进行中的轮次，请等本轮结束后再继续目标' };
        }
        // The goal stays paused until our own turn is running, so Codex cannot start one first.
        const delivery = await this.startGoalTurn(
          session, `Continue working toward the active thread goal: ${current.objective}`, '/goal resume',
        );
        if (delivery.status !== 'accepted') return { ok: false, error: delivery.reason };
        return { ok: true, goal: info.goal, message: '已继续推进目标' };
      }
      if (info.status === 'running' || driver.busy) {
        return { ok: false, error: '请等本轮结束后再设定新目标' };
      }
      const objective = action.objective.trim();
      const current = await driver.getGoal();
      if (current) await driver.clearGoal();
      // Keep paused until our turn starts so Codex does not race an extra turn.
      info.goal = await driver.setGoal({ objective, status: 'paused' });
      if (!info.title) info.title = objective.split('\n')[0]!.slice(0, 60);
      this.sessionsChanged();
      const delivery = await this.startGoalTurn(
        session,
        `This thread's goal is already set. Do not call create_goal. Start working on it now: ${objective}`,
        `/goal ${objective}`,
      );
      if (delivery.status !== 'accepted') return { ok: false, error: delivery.reason };
      if (session.info.orchestration) this.deps.onUserTurn?.(id);
      if (session.unsyncedName) void this.syncName(session);
      return { ok: true, goal: info.goal, message: '已设定并开始推进目标' };
    } catch (error) {
      return { ok: false, error: error instanceof Error ? error.message : String(error) };
    }
  }

  /** Uploads this thread and Codex logs to OpenAI (`/feedback [reason]`). */
  async uploadFeedback(
    id: string,
    reason?: string,
    fallback?: (threadId: string, reason?: string) => Promise<{ feedbackId: string }>,
  ): Promise<{ ok: true; feedbackId: string } | { ok: false; error: string }> {
    const session = this.sessions.get(id);
    if (!session) return { ok: false, error: '会话不存在' };
    if (!session.info.providerSessionId || !session.hasTurns) {
      return { ok: false, error: '请先发送至少一条消息后再提交反馈' };
    }
    try {
      if (session.driver?.alive && session.driver.uploadFeedback) {
        const res = await session.driver.uploadFeedback(reason);
        return { ok: true, feedbackId: res.feedbackId };
      }
      if (fallback) {
        const res = await fallback(session.info.providerSessionId, reason);
        return { ok: true, feedbackId: res.feedbackId };
      }
      return { ok: false, error: '当前会话未连接，无法上传反馈' };
    } catch (error) {
      return { ok: false, error: error instanceof Error ? error.message : String(error) };
    }
  }

  /** Coordinator state the orchestration service checks before trusting the session's MCP calls. */
  coordinatorState(id: string): { cwd: string; provider: string; active: boolean; idle: boolean; network: boolean } | null {
    const session = this.sessions.get(id);
    if (!session) return null;
    const { info, driver } = session;
    const live = info.status === 'starting' || info.status === 'idle' || info.status === 'running';
    const active = info.orchestration && live;
    const idle = active && info.status === 'idle' && Boolean(driver?.alive) && !driver?.busy && session.requests.size === 0;
    return { cwd: info.cwd, provider: info.provider, active, idle, network: info.network };
  }

  /**
   * Switches orchestration mode between turns. A running provider process is replaced and the same
   * conversation resumed with (or without) Aha MCP; a suspended session takes the mode on its next resume.
   */
  async setOrchestration(id: string, enabled: boolean): Promise<{ ok: true } | { ok: false; error: string }> {
    const session = this.sessions.get(id);
    if (!session) return { ok: false, error: '会话不存在' };
    const { info } = session;
    if (info.orchestration === enabled) return { ok: true };
    if (enabled && !this.deps.coordinatorInstructions) return { ok: false, error: '编排模式当前不可用' };
    if (info.status === 'starting' || info.status === 'running' || session.driver?.busy || session.requests.size > 0) {
      return { ok: false, error: '请等本轮结束后再切换编排模式' };
    }
    if (!info.providerSessionId || info.status === 'closed') return { ok: false, error: '这个会话无法切换编排模式' };
    info.orchestration = enabled;
    if (!enabled) this.deps.onCoordinatorEnded?.(id);
    const driver = session.driver;
    if (!driver?.alive) {
      this.sessionsChanged();
      return { ok: true };
    }
    session.generation++;
    session.driver = null;
    info.status = 'starting';
    this.sessionsChanged();
    await driver.close();
    await this.connect(session, true);
    // connect() moved the status on; widen the type narrowed by the assignment above.
    if ((info.status as AgentSessionStatus) === 'idle') return { ok: true };
    // Fall back to the previous mode; the conversation resumes with it on the next message.
    const error = info.error ?? '会话没有恢复';
    info.orchestration = !enabled;
    if (enabled) this.deps.onCoordinatorEnded?.(id);
    this.sessionsChanged();
    return { ok: false, error };
  }

  async interrupt(id: string): Promise<boolean> {
    const session = this.sessions.get(id);
    if (session) session.goalTurnInterrupted = true;
    return (await session?.driver?.interrupt()) ?? false;
  }

  /**
   * Retitles the session. The title is kept here at once; the provider thread is renamed too, now
   * if its process is up, otherwise the next time it starts. `synced` says whether that happened.
   */
  async rename(id: string, title: string): Promise<{ ok: true; synced: boolean } | { ok: false; error: string }> {
    const session = this.sessions.get(id);
    if (!session) return { ok: false, error: '会话不存在' };
    const name = title.replace(/\s+/gu, ' ').trim();
    if (!name) return { ok: false, error: '标题不能为空' };
    session.info.title = name;
    session.unsyncedName = name;
    this.sessionsChanged();
    return { ok: true, synced: await this.syncName(session) };
  }

  private async syncName(session: Session): Promise<boolean> {
    const { driver, unsyncedName: name } = session;
    if (!name || !driver?.alive || !driver.sessionId || !driver.setName) return false;
    const synced = await driver.setName(name);
    // A newer rename while this one was in flight keeps its own pending name.
    if (synced && session.unsyncedName === name) session.unsyncedName = null;
    return synced;
  }

  /** Answers an approval the agent is waiting on. Returns false if it is no longer pending. */
  respond(id: string, requestId: string, allow: boolean, reason?: string): boolean {
    const session = this.sessions.get(id);
    const pending = session?.requests.get(requestId);
    if (!session || !pending || pending.request.kind !== 'approval') return false;
    this.settleRequest(session, requestId, allow ? 'approved' : 'denied', reason ?? null);
    pending.resolve(allow ? { decision: 'accept' } : DECLINE);
    return true;
  }

  /** Answers (or, with null, skips) a question the agent is waiting on. */
  answer(id: string, requestId: string, answers: Record<string, string[]> | null): boolean {
    const session = this.sessions.get(id);
    const pending = session?.requests.get(requestId);
    if (!session || !pending || pending.request.kind !== 'question') return false;
    const asked = pending.request.questions ?? [];
    if (answers && asked.some((question) => !answers[question.id]?.some((value) => value.trim()))) return false;
    this.settleRequest(session, requestId, answers ? 'approved' : 'denied', null);
    if (answers) {
      const text = asked.map((question) => `${question.header || question.prompt}：${answers[question.id]!.join('、')}`).join('\n');
      session.timeline.apply({ type: 'notice', id: `answer:${requestId}`, level: 'info', text: `已回答提问\n${text}` });
    }
    pending.resolve(answers ? { answers } : DECLINE);
    return true;
  }

  /** Ends the session and forgets it, including its saved history. */
  async close(id: string): Promise<void> {
    const session = this.sessions.get(id);
    if (!session) return;
    this.sessions.delete(id);
    if (session.info.orchestration) this.deps.onCoordinatorEnded?.(id);
    await session.driver?.close(true);
    this.sessionsChanged();
  }

  /** Stops every session's process but keeps them, so they can be resumed after a restart. */
  async dispose(): Promise<void> {
    this.disposing = true;
    await Promise.all([...this.sessions.values()].map((session) => session.driver?.close(true)));
    this.writer?.writeSync(() => this.serialize());
    this.writer?.close();
  }

  private track(info: AgentSessionInfo, initial?: WorkerTimelineSnapshot): Session {
    const session: Session = {
      info, driver: null, requests: new Map(), generation: 0, launchedPolicy: null, unsyncedName: null,
      turns: initial?.turns.length ?? 0,
      // A turn that never reached the provider ends failed with no turn id and no output.
      hasTurns: initial?.turns.some((turn) => turn.status !== 'failed' || turn.turnId !== null || turn.items.length > 0) ?? false,
      timeline: new WorkerTimeline({
        workerHandle: info.id, cliId: info.provider, model: info.model, readOnly: info.policy === 'read-only',
        redact: (text) => session.driver?.redactText(text) ?? text,
        now: this.now,
        initial,
        onPatch: (change) => {
          for (const listener of this.timelineListeners) listener(change);
        },
      }),
    };
    this.sessions.set(info.id, session);
    return session;
  }

  private resumable(session: Session): boolean {
    const { status, providerSessionId } = session.info;
    return Boolean(providerSessionId) && !session.driver?.alive && (status === 'suspended' || status === 'failed');
  }

  /**
   * Starts the provider process and opens (or, with `resume`, rejoins) the conversation. A thread
   * that never took a turn cannot be rejoined, so it is replaced by a new one.
   */
  private async connect(session: Session, resume = false): Promise<void> {
    const { info, timeline } = session;
    const rejoin = resume && Boolean(info.providerSessionId) && session.hasTurns;
    info.status = 'starting';
    info.error = null;
    if (resume) this.sessionsChanged();
    const generation = ++session.generation;
    try {
      const orchestration = info.orchestration;
      const developerInstructions = orchestration
        ? this.deps.coordinatorInstructions?.(info.id, { policy: info.policy })
        : undefined;
      session.driver = this.deps.launch(info.id, info.cwd, this.eventsFor(session, generation), {
        orchestration, policy: info.policy,
      });
      session.launchedPolicy = info.policy;
      info.provider = session.driver.provider;
      info.providerSessionId = await session.driver.open({
        cwd: info.cwd, model: info.model, policy: info.policy, network: info.network,
        ...(developerInstructions ? { developerInstructions } : {}),
        ...(rejoin && info.providerSessionId ? { resumeSessionId: info.providerSessionId } : {}),
      });
      if (!session.driver.alive) throw new Error(info.error ?? 'The agent closed while starting');
      // What the provider actually uses, e.g. a default from the person's own config.
      info.model = session.driver.effectiveModel ?? info.model;
      info.effort = session.driver.effectiveEffort ?? info.effort;
      info.status = 'idle';
      timeline.setMeta({ alive: true, providerSessionId: info.providerSessionId });
      void this.syncName(session);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      info.status = 'failed';
      info.error = rejoin ? `无法恢复会话：${reason}` : reason;
      timeline.setMeta({ alive: false });
      await session.driver?.close();
    }
    this.sessionsChanged();
    this.coordinatorSettled(session);
  }

  private coordinatorSettled(session: Session): void {
    if (session.info.orchestration && session.info.status === 'idle') this.deps.onCoordinatorIdle?.(session.info.id);
  }

  private eventsFor(session: Session, generation: number): AgentSessionEvents {
    const { info, timeline } = session;
    const current = () => session.generation === generation;
    return {
      onTurnStarted: () => {},
      onTimeline: (turnKey, event) => {
        if (current() && timeline.activeTurn?.dispatchId === turnKey) timeline.apply(event);
      },
      onGoalChanged: (goal) => {
        if (!current()) return;
        info.goal = goal;
        this.sessionsChanged();
      },
      onTurnCompleted: (_turnKey, result) => {
        if (!current()) return;
        this.cancelRequests(session, 'Turn ended');
        timeline.completeTurn({
          turnId: result.turnId, stopReason: result.stopReason, usage: result.usage, error: result.error,
          status: result.stopReason === 'completed' ? 'completed'
            : result.stopReason === 'interrupted' ? 'interrupted' : 'failed',
        });
        if (info.status === 'running') info.status = 'idle';
        if (result.stopReason !== 'completed' && info.queued.length) info.queuePaused = true;
        this.sessionsChanged();
        if (!this.drainQueue(session)) this.coordinatorSettled(session);
      },
      onRequest: (turnKey, request) => {
        if (!current() || request.signal.aborted) return Promise.resolve(DECLINE);
        const requestId = `req_${randomUUID().replace(/-/g, '').slice(0, 12)}`;
        return new Promise<AgentRuntimeResponse>((resolve) => {
          session.requests.set(requestId, { turnKey, request, resolve });
          if (request.kind === 'question') {
            info.pendingQuestions.push({ requestId, questions: request.questions ?? [] });
            this.sessionsChanged();
            return;
          }
          timeline.upsertApproval({
            approvalId: requestId, dispatchId: turnKey, method: request.method,
            tool: request.method.split('/')[1] ?? request.method,
            input: request.toolInput, reason: request.message ?? null, status: 'pending',
          });
        });
      },
      onExit: (reason) => {
        if (!current()) return;
        this.cancelRequests(session, reason);
        const expected = Boolean(session.driver?.closing) && !info.error;
        timeline.endActiveTurn(expected ? 'interrupted' : 'failed', reason);
        timeline.setMeta({ alive: false });
        if (info.status !== 'failed') {
          info.status = this.disposing && info.providerSessionId ? 'suspended' : expected ? 'closed' : 'failed';
          if (info.status === 'failed') info.error ??= reason;
        }
        if (info.queued.length) info.queuePaused = true;
        if (this.sessions.has(info.id)) this.sessionsChanged();
      },
    };
  }

  private settleRequest(session: Session, requestId: string, status: 'approved' | 'denied' | 'expired', reason: string | null): void {
    const pending = session.requests.get(requestId);
    if (!pending) return;
    session.requests.delete(requestId);
    if (pending.request.kind === 'question') {
      session.info.pendingQuestions = session.info.pendingQuestions.filter((entry) => entry.requestId !== requestId);
      this.sessionsChanged();
      return;
    }
    session.timeline.upsertApproval({
      approvalId: requestId, dispatchId: pending.turnKey, method: pending.request.method,
      tool: pending.request.method.split('/')[1] ?? pending.request.method,
      input: pending.request.toolInput, reason: pending.request.message ?? null, status, resolution: reason,
    });
  }

  private cancelRequests(session: Session, reason: string): void {
    for (const [requestId, pending] of [...session.requests]) {
      this.settleRequest(session, requestId, 'expired', reason);
      pending.resolve(DECLINE);
    }
  }

  private load(): void {
    const file = this.deps.storageFile?.();
    if (!file || !existsSync(file)) return;
    let saved: SavedSession[] = [];
    try {
      const parsed = JSON.parse(readFileSync(file, 'utf8')) as { version?: number; sessions?: SavedSession[] };
      if (parsed.version === 1 && Array.isArray(parsed.sessions)) saved = parsed.sessions;
    } catch (error) {
      console.warn('[agent-sessions] ignoring unreadable saved sessions:', error);
    }
    for (const entry of saved) {
      if (!entry?.id || !AGENT_SESSION_ID.test(entry.id) || !entry.providerSessionId || !entry.timeline) continue;
      const { timeline, ...rest } = entry;
      const session = this.track({ ...rest, network: rest.network === true, orchestration: rest.orchestration === true, status: 'suspended', error: null, pendingQuestions: [], queued: [], queuePaused: false, goal: null }, timeline);
      session.timeline.setMeta({ alive: false });
    }
  }

  private serialize(): string {
    const sessions: SavedSession[] = [...this.sessions.values()]
      .filter((session) => session.info.providerSessionId)
      .sort((a, b) => b.info.createdAt - a.info.createdAt)
      .slice(0, MAX_SAVED_SESSIONS)
      .map(({ info, timeline }) => {
        const { status: _status, error: _error, pendingQuestions: _questions, queued: _queued, queuePaused: _paused, goal: _goal, ...rest } = info;
        return { ...rest, timeline: timeline.archive() };
      });
    return JSON.stringify({ version: 1, sessions });
  }

  private sessionsChanged(): void {
    if (!this.disposing) {
      void this.writer?.write(() => this.serialize()).catch((error) => {
        console.warn('[agent-sessions] failed to save sessions:', error);
      });
    }
    const sessions = this.list();
    for (const listener of this.sessionListeners) listener(sessions);
  }
}
