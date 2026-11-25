import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { chmodSync, existsSync, mkdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { createInterface } from 'node:readline';
import { augmentedPath, resolveBinaryFromPath } from '../bin-resolve.js';
import { mergedCodexSubprocessEnv } from '../settings-loader.js';
import type {
  AgentGoalStatus, AgentOutputSlice, AgentSessionDriver, AgentSessionEvents, AgentSessionGoal, AgentSessionPolicy,
  AgentTurnAttachment, TurnDelivery,
} from './agent-session-driver.js';
import { spawnWindowsCodexJob } from './windows-codex-job.js';
import type {
  WorkerTimelineEvent, WorkerTimelineFileChange, WorkerTimelineItemStatus, WorkerTimelineItemUpdate,
} from './worker-timeline.js';

export const CODEX_WORKER_ID = /^codex-worker-[a-f0-9-]{36}$/;
const POLICIES: Record<AgentSessionPolicy, { sandbox: string; sandboxType: string; approvalPolicy: string }> = {
  'read-only': { sandbox: 'read-only', sandboxType: 'readOnly', approvalPolicy: 'on-request' },
  'workspace-write': { sandbox: 'workspace-write', sandboxType: 'workspaceWrite', approvalPolicy: 'on-request' },
  // Still sandboxed to the workspace; it only stops asking.
  'full-auto': { sandbox: 'workspace-write', sandboxType: 'workspaceWrite', approvalPolicy: 'never' },
};
/**
 * Requests a client may make outside a conversation: catalogs, file search, usage limits, the skill
 * switch, and the conversation history (listing, reading, archiving stored threads). Anything that
 * runs a thread (start, resume, turns) or writes config goes through the methods of this class.
 */
export const CODEX_QUERY_METHODS = new Set([
  'model/list', 'skills/list', 'skills/config/write', 'plugin/installed', 'fuzzyFileSearch', 'account/rateLimits/read',
  'thread/list', 'thread/read', 'thread/archive', 'thread/unarchive', 'feedback/upload',
]);
const OUTPUT_LIMIT = 512 * 1024;
const STDERR_LIMIT = 4 * 1024;
const CHILD_CLOSE_TIMEOUT_MS = 3_000;

type Obj = Record<string, unknown>;
type RpcId = string | number;

interface ActiveTurn {
  turnKey: string;
  id: string | null;
  messages: Map<string, string>;
  /** Thread token total before this turn; null when unknown (first turn after a resume). */
  usageBaseline: Record<string, number> | null;
  diff: string | null;
  usage: Record<string, number> | null;
  idle: Promise<void>;
  finish: () => void;
}

class RpcRejection extends Error {}

function object(value: unknown): Obj {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Obj : {};
}

function string(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function numericFields(value: unknown): Record<string, number> {
  return Object.fromEntries(Object.entries(object(value))
    .filter((entry): entry is [string, number] => typeof entry[1] === 'number'));
}

const GOAL_STATUSES = new Set<AgentGoalStatus>(['active', 'paused', 'blocked', 'usageLimited', 'budgetLimited', 'complete']);

function parseCodexGoal(raw: unknown): AgentSessionGoal | null {
  const entry = object(raw);
  const objective = string(entry.objective)?.trim();
  const status = string(entry.status) as AgentGoalStatus | undefined;
  if (!objective || !status || !GOAL_STATUSES.has(status)) return null;
  const rawUpdated = typeof entry.updatedAt === 'number' && Number.isFinite(entry.updatedAt) ? entry.updatedAt : Date.now();
  return {
    objective,
    status,
    tokensUsed: typeof entry.tokensUsed === 'number' && Number.isFinite(entry.tokensUsed) ? entry.tokensUsed : 0,
    timeUsedSeconds: typeof entry.timeUsedSeconds === 'number' && Number.isFinite(entry.timeUsedSeconds) ? entry.timeUsedSeconds : 0,
    updatedAt: rawUpdated < 1e12 ? rawUpdated * 1000 : rawUpdated,
  };
}

function codexStatus(value: unknown): WorkerTimelineItemStatus {
  if (value === 'completed') return 'completed';
  if (value === 'failed') return 'failed';
  if (value === 'declined') return 'declined';
  return 'running';
}

function textParts(value: unknown): string {
  if (!Array.isArray(value)) return '';
  return value.map(object).map((part) => string(part.text) ?? '').filter(Boolean).join('\n');
}

function summarize(value: unknown): string {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return '';
  }
}

function codexFileChanges(value: unknown): WorkerTimelineFileChange[] {
  if (!Array.isArray(value)) return [];
  return value.map(object).map((change) => {
    const kind = object(change.kind);
    const type = string(kind.type) ?? string(change.kind);
    return {
      path: string(change.path) ?? '',
      change: type === 'add' || type === 'delete' ? type : 'update',
      movePath: string(kind.move_path) ?? string(kind.movePath) ?? null,
      diff: string(change.diff) ?? '',
      added: 0, removed: 0, truncated: false,
    };
  });
}

/** Translates a Codex ThreadItem into a protocol-agnostic timeline item update. */
export function codexItemUpdate(raw: unknown, completedAt?: number): WorkerTimelineItemUpdate | null {
  const item = object(raw);
  const id = string(item.id);
  if (!id) return null;
  const done = completedAt !== undefined ? { completedAt } : {};
  switch (item.type) {
    case 'agentMessage':
      return { id, kind: 'message', role: 'assistant', ...(typeof item.text === 'string' && item.text ? { text: item.text } : {}),
        ...(completedAt !== undefined ? { status: 'completed' as const } : {}), ...done };
    case 'reasoning': {
      const summary = Array.isArray(item.summary) ? item.summary.filter((s): s is string => typeof s === 'string').join('\n\n') : '';
      const content = Array.isArray(item.content) ? item.content.filter((s): s is string => typeof s === 'string').join('\n\n') : '';
      return { id, kind: 'reasoning', ...(summary ? { summary } : {}), ...(content ? { text: content } : {}),
        ...(completedAt !== undefined ? { status: 'completed' as const } : {}), ...done };
    }
    case 'plan':
      return { id, kind: 'plan', ...(typeof item.text === 'string' && item.text ? { text: item.text } : {}),
        ...(completedAt !== undefined ? { status: 'completed' as const } : {}), ...done };
    case 'commandExecution':
      return {
        id, kind: 'command', command: string(item.command) ?? '', cwd: string(item.cwd) ?? null,
        status: codexStatus(item.status),
        ...(typeof item.aggregatedOutput === 'string' ? { output: item.aggregatedOutput } : {}),
        ...(typeof item.exitCode === 'number' ? { exitCode: item.exitCode } : {}),
        ...(typeof item.durationMs === 'number' ? { durationMs: item.durationMs } : {}),
        ...done,
      };
    case 'fileChange':
      return { id, kind: 'file_change', status: codexStatus(item.status), files: codexFileChanges(item.changes), ...done };
    case 'mcpToolCall': {
      const result = object(item.result);
      return {
        id, kind: 'tool_call', toolType: 'mcp', server: string(item.server) ?? null, tool: string(item.tool) ?? 'tool',
        args: summarize(item.arguments), status: codexStatus(item.status),
        ...(item.result ? { output: textParts(result.content) || summarize(result.structuredContent) } : {}),
        error: string(object(item.error).message) ?? null,
        ...(typeof item.durationMs === 'number' ? { durationMs: item.durationMs } : {}),
        ...done,
      };
    }
    case 'dynamicToolCall':
      return {
        id, kind: 'tool_call', toolType: 'tool', server: string(item.namespace) ?? null, tool: string(item.tool) ?? 'tool',
        args: summarize(item.arguments), status: codexStatus(item.status),
        ...(Array.isArray(item.contentItems) ? { output: textParts(item.contentItems) } : {}),
        ...(item.success === false ? { error: 'Tool reported failure' } : {}),
        ...(typeof item.durationMs === 'number' ? { durationMs: item.durationMs } : {}),
        ...done,
      };
    case 'webSearch':
      return {
        id, kind: 'tool_call', toolType: 'search', server: null, tool: 'web_search', args: string(item.query) ?? '',
        status: completedAt !== undefined ? 'completed' : 'running', ...done,
      };
    case 'contextCompaction':
      return completedAt !== undefined
        ? { id, kind: 'notice', level: 'info', text: '上下文已压缩', ...done }
        : { id, kind: 'notice', level: 'info', text: '正在压缩上下文…' };
    case 'userMessage':
      return null;
    default: {
      const type = string(item.type);
      if (!type) return null;
      return {
        id, kind: 'tool_call', toolType: 'tool', server: null, tool: type, args: '',
        status: completedAt !== undefined ? codexStatus(item.status ?? 'completed') : 'running', ...done,
      };
    }
  }
}

/** Maps one Codex app-server notification (already scoped to the active turn) to timeline events. */
export function codexTimelineEvents(method: string, params: Obj): WorkerTimelineEvent[] {
  const itemId = string(params.itemId) ?? '';
  const delta = string(params.delta) ?? '';
  switch (method) {
    case 'item/started': {
      const update = codexItemUpdate(params.item);
      return update ? [{ type: 'item', item: update }] : [];
    }
    case 'item/completed': {
      const at = typeof params.completedAtMs === 'number' ? params.completedAtMs : Date.now();
      const update = codexItemUpdate(params.item, at);
      return update ? [{ type: 'item', item: update }] : [];
    }
    case 'item/agentMessage/delta':
      return itemId ? [{ type: 'delta', id: itemId, kind: 'message', field: 'text', delta }] : [];
    case 'item/reasoning/textDelta':
      return itemId ? [{ type: 'delta', id: itemId, kind: 'reasoning', field: 'text', delta }] : [];
    case 'item/reasoning/summaryTextDelta':
      return itemId ? [{ type: 'delta', id: itemId, kind: 'reasoning', field: 'summary', delta }] : [];
    case 'item/reasoning/summaryPartAdded':
      return itemId && typeof params.summaryIndex === 'number' && params.summaryIndex > 0
        ? [{ type: 'delta', id: itemId, kind: 'reasoning', field: 'summary', delta: '\n\n' }] : [];
    case 'item/commandExecution/outputDelta':
      return itemId ? [{ type: 'delta', id: itemId, kind: 'command', field: 'output', delta }] : [];
    case 'item/plan/delta':
      return itemId ? [{ type: 'delta', id: itemId, kind: 'plan', field: 'text', delta }] : [];
    case 'item/mcpToolCall/progress':
      return itemId ? [{ type: 'item', item: { id: itemId, kind: 'tool_call', progress: string(params.message) ?? null } }] : [];
    case 'item/fileChange/patchUpdated':
      return itemId ? [{ type: 'item', item: { id: itemId, kind: 'file_change', files: codexFileChanges(params.changes) } }] : [];
    case 'turn/plan/updated': {
      const turnId = string(params.turnId) ?? 'turn';
      const steps = (Array.isArray(params.plan) ? params.plan : []).map(object).map((entry) => ({
        step: string(entry.step) ?? '',
        status: entry.status === 'completed' ? 'completed' as const
          : entry.status === 'inProgress' ? 'in_progress' as const : 'pending' as const,
      }));
      const done = steps.length > 0 && steps.every((step) => step.status === 'completed');
      return [{
        type: 'item',
        item: { id: `plan:${turnId}`, kind: 'plan', explanation: string(params.explanation) ?? null, steps,
          status: done ? 'completed' : 'running' },
      }];
    }
    case 'error': {
      const message = string(object(params.error).message) ?? 'Codex reported an error';
      return [{ type: 'notice', level: params.willRetry ? 'warning' : 'error', text: message }];
    }
    default:
      return [];
  }
}
export function enabledCodexPilot(platform: NodeJS.Platform, setting: boolean, override?: string): boolean {
  return platform === 'win32' && (override === '1' || (override !== '0' && setting));
}

// These calls are part of the Worker dispatch contract and still use Worker-scoped auth.
const WORKER_APPROVED_MCP_TOOLS = ['report_done', 'read_board', 'ask_coordinator', 'post_message'] as const;

/**
 * Coordinator tools a session runs without a Codex prompt: reads, plus calls the orchestration
 * service already gates (delegate_task waits for the person's confirmation in chat or in the UI).
 * Tools that stop Workers or write Git history keep Codex's approval prompt.
 */
export const COORDINATOR_APPROVED_MCP_TOOLS = [
  'delegate_task', 'team_status', 'list_coding_agents', 'read_board', 'board_wait', 'team_wait',
  'post_message', 'message_worker', 'inspect_worker_diff', 'inspect_run_diff',
] as const;

/**
 * The approved tools for a session launched with `policy`. `full-auto` maps to Codex's `never`
 * approval policy, under which a tool call that still needs approval fails outright, so
 * `manage_worker` must be pre-approved there. `integrate_worker` and `finalize_run_integration`
 * are never approved: changing Git history always needs the person.
 */
export function coordinatorApprovedMcpTools(policy: AgentSessionPolicy): readonly string[] {
  return policy === 'full-auto'
    ? [...COORDINATOR_APPROVED_MCP_TOOLS, 'manage_worker']
    : COORDINATOR_APPROVED_MCP_TOOLS;
}

/**
 * `turn/start` input for a message. Images go as image items; every saved file, images included,
 * is also named with its path in the text so the agent can open it with its own tools.
 */
export function codexTurnInput(prompt: string, attachments: readonly AgentTurnAttachment[]): Obj[] {
  const saved = attachments.filter((item) => item.type !== 'skill');
  const notes = saved.map((item) => {
    const kind = item.type === 'localImage' ? 'image' : 'file';
    const name = (item.name || item.path.split(/[\\/]/).pop() || kind).replace(/"/g, "'");
    return `[Attached ${kind} "${name}" is saved at: ${item.path}]`;
  });
  const text = [prompt, ...notes].filter(Boolean).join('\n\n');
  return [
    ...(text ? [{ type: 'text', text, text_elements: [] }] : []),
    ...attachments.flatMap((item) => item.type === 'skill'
      ? [{ type: 'skill', name: item.name, path: item.path }]
      : item.type === 'localImage' ? [{ type: 'localImage', path: item.path }] : []),
  ];
}

export function codexMcpConfigArgs(
  node: string, mcp: { serverPath: string; env: Record<string, string>; approvedTools?: readonly string[] },
): string[] {
  const mcpEnv = Object.entries(mcp.env).map(([key, value]) => `${key} = ${JSON.stringify(value)}`).join(', ');
  return [
    '-c', `mcp_servers.aha.command=${JSON.stringify(node)}`,
    '-c', `mcp_servers.aha.args=${JSON.stringify([mcp.serverPath])}`,
    '-c', `mcp_servers.aha.env={ ${mcpEnv} }`,
    '-c', 'mcp_servers.aha.enabled=true',
    '-c', 'mcp_servers.aha.required=true',
    ...(mcp.approvedTools ?? WORKER_APPROVED_MCP_TOOLS)
      .flatMap((tool) => ['-c', `mcp_servers.aha.tools.${tool}.approval_mode="approve"`]),
  ];
}

export function writeMcpCredentialFile(env: Record<string, string>, credentialDir?: string): string {
  const base = credentialDir ?? join(tmpdir(), 'ahastudio-mcp-credentials');
  mkdirSync(base, { recursive: true, mode: 0o700 });
  if (process.platform !== 'win32') chmodSync(base, 0o700);
  const owner = env.AHA_WORKER_ID || env.AHA_SESSION_ID || env.AHA_TERMINAL_ID || `${process.pid}-${Date.now()}`;
  const file = join(base, `${owner.replace(/[^a-zA-Z0-9_.-]/g, '_')}.json`);
  const tmp = `${file}.${process.pid}.tmp`;
  writeFileSync(tmp, JSON.stringify(env, null, 2), { encoding: 'utf8', mode: 0o600 });
  if (process.platform !== 'win32') chmodSync(tmp, 0o600);
  renameSync(tmp, file);
  return file;
}

export function launchCodexPilot(options: {
  cwd: string;
  /**
   * Aha MCP wiring for an orchestration Worker or an orchestrating session; a plain session runs
   * without it. `requiredTool` must be listed before the thread is usable (default report_done) and
   * `approvedTools` run without a Codex approval prompt (default report_done, read_board, ask_coordinator, and post_message).
   */
  mcp?: {
    serverPath: string; env: Record<string, string>; requiredTool?: string; approvedTools?: readonly string[];
    /** Private directory used for the per-process credential file instead of a temporary directory. */
    credentialDir?: string;
  };
  events: AgentSessionEvents;
}): CodexAppServerPilot {
  const codex = resolveBinaryFromPath('codex');
  const node = resolveBinaryFromPath('node');
  if (!codex || !node) throw new Error('Codex or Node executable was not found');
  const wrapper = /\.(cmd|bat)$/i.test(codex);
  const entry = join(dirname(codex), 'node_modules', '@openai', 'codex', 'bin', 'codex.js');
  if (wrapper && !existsSync(entry)) throw new Error('Codex npm entry point was not found');

  let credentialFile: string | null = null;
  let mcp = options.mcp;
  if (mcp) {
    credentialFile = writeMcpCredentialFile(mcp.env, mcp.credentialDir);
    // Only the file path belongs in argv; scoped tokens stay inside a 0600 file.
    mcp = { ...mcp, env: { AHA_CREDENTIAL_FILE: credentialFile } };
  }
  const args = ['app-server', '--stdio', ...(mcp ? codexMcpConfigArgs(node, mcp) : [])];
  const env: NodeJS.ProcessEnv = { ...mergedCodexSubprocessEnv(), PATH: augmentedPath() };
  const executable = wrapper ? node : codex;
  const argv = wrapper ? [entry, ...args] : args;
  const child = process.platform === 'win32'
    ? spawnWindowsCodexJob(executable, argv, options.cwd, env)
    : spawn(executable, argv, { cwd: options.cwd, env, windowsHide: true, stdio: 'pipe' });
  return new CodexAppServerPilot(child, options.events, 30_000, [
    options.mcp?.env.AHA_WORKER_TOKEN, options.mcp?.env.AHA_SESSION_TOKEN, options.mcp?.env.AHA_AGENT_HOOK_TOKEN,
  ].filter((value): value is string => Boolean(value)), options.mcp ? options.mcp.requiredTool ?? 'report_done' : null,
    credentialFile ? () => rmSync(credentialFile, { force: true }) : undefined);
}

export class CodexAppServerPilot implements AgentSessionDriver {
  readonly provider = 'codex' as const;
  readonly capabilities = {
    resume: true, steer: false,
    policies: ['read-only', 'workspace-write', 'full-auto'] as readonly AgentSessionPolicy[],
  };
  private readonly pending = new Map<number, (error: Error | null, value?: unknown) => void>();
  private readonly disconnect = new AbortController();
  private seq = 0;
  private tokenTotal: Record<string, number> | null = {};
  private policy: AgentSessionPolicy = 'read-only';
  private network = false;
  private threadId: string | null = null;
  private active: ActiveTurn | null = null;
  private output = '';
  private stderrTail = Buffer.alloc(0);
  private offset = 0;
  private outputTurnKey: string | null = null;
  private disconnected = false;
  private stopping = false;
  private stopPromise: Promise<void> | null = null;
  private initialized: Promise<void> | null = null;
  private readonly childClosed: Promise<void>;
  effectiveModel: string | null = null;
  effectiveEffort: string | null = null;

  constructor(
    private readonly child: ChildProcessWithoutNullStreams,
    private readonly events: AgentSessionEvents,
    private readonly requestTimeoutMs = 30_000,
    private readonly secrets: readonly string[] = [],
    /** Aha MCP tool the thread must expose before it is usable; null for sessions without Aha MCP. */
    private readonly requiredMcpTool: string | null = 'report_done',
    private readonly cleanup?: () => void,
  ) {
    const cleanupOnce = () => {
      try { this.cleanup?.(); } catch { /* Best-effort secret file cleanup. */ }
    };
    this.childClosed = new Promise((resolve) => {
      child.once('close', (code, signal) => {
        cleanupOnce();
        resolve();
        this.transportLost(`Codex app-server exited (${code ?? signal ?? 'unknown'})`, code);
      });
      child.once('error', (error) => {
        cleanupOnce();
        resolve();
        this.transportLost(`Codex app-server process error: ${error.message}`);
      });
    });
    child.stdin.on('error', () => this.transportLost('Codex app-server stdin disconnected'));
    child.stdout.on('error', () => this.transportLost('Codex app-server stdout disconnected'));
    const stderrCapacity = STDERR_LIMIT + secrets.reduce((max, secret) => Math.max(max, Buffer.byteLength(secret)), 0);
    child.stderr.on('data', (chunk: Buffer) => {
      this.stderrTail = Buffer.concat([this.stderrTail, chunk.subarray(-stderrCapacity)])
        .subarray(-stderrCapacity);
    });
    const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
    lines.on('line', (line) => {
      try {
        this.receive(object(JSON.parse(line)));
      } catch {
        this.transportLost('Invalid Codex app-server protocol message');
      }
    });
    lines.once('close', () => {
      if (this.stopping || this.disconnected) return;
      // Prefer the child close event, which arrives after stderr has drained.
      const timer = setTimeout(() => {
        if (!this.stopping && child.exitCode === null && child.signalCode === null) {
          this.transportLost('Codex app-server stdout closed');
        }
      }, 50);
      void this.childClosed.then(() => clearTimeout(timer));
    });
  }

  get alive(): boolean { return !this.disconnected && !this.stopping; }
  get sessionId(): string | null { return this.threadId; }
  get busy(): boolean { return this.active !== null; }
  get closing(): boolean { return this.stopping; }
  get latestTurnKey(): string | null { return this.outputTurnKey; }

  async open({ cwd, model, policy, resumeSessionId, network = false, developerInstructions }: {
    cwd: string; model: string | null; policy: AgentSessionPolicy; resumeSessionId?: string; network?: boolean;
    developerInstructions?: string;
  }): Promise<string> {
    const wanted = POLICIES[policy];
    try {
      await this.initialize();
      const settings = {
        cwd, ...(model ? { model } : {}), sandbox: wanted.sandbox,
        approvalPolicy: wanted.approvalPolicy, approvalsReviewer: 'user',
        ...(developerInstructions ? { developerInstructions } : {}),
      };
      const result = object(resumeSessionId
        ? await this.request('thread/resume', { threadId: resumeSessionId, ...settings, excludeTurns: true })
        : await this.request('thread/start', settings));
      const id = string(object(result.thread).id);
      if (!id) throw new Error('Codex did not return a thread ID');
      if (resumeSessionId && id !== resumeSessionId) throw new Error('Codex resumed a different thread');
      if (object(result.sandbox).type !== wanted.sandboxType || result.approvalPolicy !== wanted.approvalPolicy) {
        throw new Error(`Codex did not confirm the requested ${policy} sandbox and approvals`);
      }
      this.policy = policy;
      this.network = network;
      this.effectiveModel = string(result.model) ?? model;
      this.effectiveEffort = string(result.reasoningEffort) ?? null;
      // The resumed thread's running token total is unknown until Codex reports it.
      if (resumeSessionId) this.tokenTotal = null;
      this.threadId = id;
      if (!this.requiredMcpTool) return id;
      // Codex may cache an empty MCP catalog prepared before the first turn.
      const inventory = object(await this.request('mcpServerStatus/list', {
        threadId: id, serverName: 'aha', detail: 'toolsAndAuthOnly',
      }));
      const servers: unknown[] = Array.isArray(inventory.data) ? inventory.data : [];
      const aha = servers.map(object).find((server) => server.name === 'aha');
      const tools = Object.values(object(aha?.tools)).map(object);
      const required = this.requiredMcpTool;
      if (!tools.some((tool) => tool.name === required || tool.name === `mcp__aha__${required}`)) {
        throw new Error(`Aha MCP did not expose ${required} to the Codex thread`);
      }
      return id;
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      throw new Error(this.withStderr(reason));
    }
  }

  /** One handshake per process, shared by `open` and `query`; a failed one may be retried. */
  private initialize(): Promise<void> {
    this.initialized ??= this.request('initialize', {
      clientInfo: { name: 'ahastation', title: 'AhaStation', version: '0.1.0' },
    }).then(
      () => this.write({ method: 'initialized' }),
      (error: unknown) => {
        this.initialized = null;
        throw error;
      },
    );
    return this.initialized;
  }

  /** A catalog or search request from `CODEX_QUERY_METHODS`; it needs no open conversation. */
  async query(method: string, params: Obj): Promise<unknown> {
    if (!CODEX_QUERY_METHODS.has(method)) throw new Error(`Codex request ${method} is not allowed here`);
    try {
      await this.initialize();
      return await this.request(method, params);
    } catch (error) {
      throw new Error(this.withStderr(error instanceof Error ? error.message : String(error)));
    }
  }

  async waitForIdle(timeoutMs: number): Promise<boolean> {
    const active = this.active;
    if (!active) return true;
    return new Promise((resolve) => {
      const timer = setTimeout(() => resolve(false), timeoutMs);
      void active.idle.then(() => {
        clearTimeout(timer);
        resolve(true);
      });
    });
  }

  async startTurn({ turnKey, prompt, effort, model, policy, network, attachments = [] }: {
    turnKey: string; prompt: string; effort?: string | null; model?: string | null; policy?: AgentSessionPolicy;
    network?: boolean; attachments?: readonly AgentTurnAttachment[];
  }): Promise<TurnDelivery> {
    if (!this.alive || !this.threadId || this.active) {
      return { status: 'not_accepted', reason: 'Codex thread is closed or busy' };
    }
    const wanted = POLICIES[policy ?? this.policy];
    const networkAccess = network ?? this.network;
    let finish!: () => void;
    const idle = new Promise<void>((resolve) => { finish = resolve; });
    const turn: ActiveTurn = {
      turnKey, id: null, messages: new Map(), diff: null, usage: null, usageBaseline: this.tokenTotal, idle, finish,
    };
    this.active = turn;
    if (this.outputTurnKey !== turnKey) {
      this.outputTurnKey = turnKey;
      this.output = '';
      this.offset = 0;
    }
    try {
      const result = object(await this.request('turn/start', {
        threadId: this.threadId,
        input: codexTurnInput(prompt, attachments),
        ...(effort ? { effort } : {}),
        ...(model ? { model } : {}),
        sandboxPolicy: wanted.sandboxType === 'readOnly'
          ? { type: 'readOnly', networkAccess }
          : { type: 'workspaceWrite', writableRoots: [], networkAccess, excludeTmpdirEnvVar: false, excludeSlashTmp: false },
        approvalPolicy: wanted.approvalPolicy, approvalsReviewer: 'user',
      }));
      const id = string(object(result.turn).id);
      if (!id) throw new Error('Codex did not acknowledge a turn ID');
      if (policy) this.policy = policy;
      if (network !== undefined) this.network = network;
      this.observeStart(turn, id);
      return { status: 'accepted' };
    } catch (error) {
      const rejected = error instanceof RpcRejection && !turn.id && this.alive;
      this.finishTurn(turn);
      return {
        status: rejected ? 'not_accepted' : 'unknown_acceptance',
        reason: error instanceof Error ? this.redact(error.message) : 'Codex delivery failed',
      };
    }
  }

  read(turnKey: string, cursor = 0, limit = 8_000, screen = false): AgentOutputSlice | null {
    if (this.outputTurnKey !== turnKey) return null;
    const start = screen ? this.offset : Math.min(Math.max(Math.floor(cursor), this.offset), this.offset + this.output.length);
    limit = Math.max(1, Math.min(256_000, Math.floor(limit)));
    const text = this.output.slice(start - this.offset, screen ? undefined : start - this.offset + limit);
    return {
      screen, text: this.redact(text), cursor: start, nextCursor: start + text.length, truncated: cursor < this.offset,
      hasMore: start + text.length < this.offset + this.output.length,
      alive: this.alive, exitCode: this.child.exitCode,
    };
  }

  /** Asks Codex to interrupt the running turn; the turn then completes as interrupted. */
  async interrupt(): Promise<boolean> {
    const active = this.active;
    if (!this.alive || !active?.id || !this.threadId) return false;
    try {
      await this.request('turn/interrupt', { threadId: this.threadId, turnId: active.id }, 5_000);
      return true;
    } catch {
      return false;
    }
  }

  async setName(name: string): Promise<boolean> {
    if (!this.alive || !this.threadId) return false;
    try {
      await this.request('thread/name/set', { threadId: this.threadId, name }, 5_000);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Starts Codex's native context compaction (`thread/compact/start`). Unlike `turn/start`, the
   * RPC result is empty and `turn/started` arrives asynchronously as a notification.
   */
  async compactThread(turnKey: string): Promise<TurnDelivery> {
    if (!this.alive || !this.threadId || this.active) {
      return { status: 'not_accepted', reason: 'Codex thread is closed or busy' };
    }
    let finish!: () => void;
    const idle = new Promise<void>((resolve) => { finish = resolve; });
    const turn: ActiveTurn = {
      turnKey, id: null, messages: new Map(), diff: null, usage: null, usageBaseline: this.tokenTotal, idle, finish,
    };
    this.active = turn;
    if (this.outputTurnKey !== turnKey) {
      this.outputTurnKey = turnKey;
      this.output = '';
      this.offset = 0;
    }
    try {
      await this.request('thread/compact/start', { threadId: this.threadId });
      return { status: 'accepted' };
    } catch (error) {
      const rejected = error instanceof RpcRejection && !turn.id && this.alive;
      this.finishTurn(turn);
      return {
        status: rejected ? 'not_accepted' : 'unknown_acceptance',
        reason: error instanceof Error ? this.redact(error.message) : 'Codex compaction failed',
      };
    }
  }

  async uploadFeedback(reason?: string): Promise<{ feedbackId: string }> {
    if (!this.alive || !this.threadId) throw new Error('Codex 会话尚未连接');
    const result = object(await this.request('feedback/upload', {
      classification: 'bug',
      includeLogs: true,
      threadId: this.threadId,
      ...(reason ? { reason } : {}),
    }, 30_000));
    return { feedbackId: string(result.threadId) || this.threadId };
  }

  async getGoal(): Promise<AgentSessionGoal | null> {
    if (!this.alive || !this.threadId) throw new Error('Codex 会话尚未连接');
    const result = object(await this.request('thread/goal/get', { threadId: this.threadId }, 10_000));
    return parseCodexGoal(result.goal);
  }

  async setGoal(input: { objective?: string; status?: 'active' | 'paused' }): Promise<AgentSessionGoal | null> {
    if (!this.alive || !this.threadId) throw new Error('Codex 会话尚未连接');
    const result = object(await this.request('thread/goal/set', {
      threadId: this.threadId,
      ...(input.objective !== undefined ? { objective: input.objective } : {}),
      ...(input.status !== undefined ? { status: input.status } : {}),
    }, 10_000));
    return parseCodexGoal(result.goal);
  }

  async clearGoal(): Promise<boolean> {
    if (!this.alive || !this.threadId) throw new Error('Codex 会话尚未连接');
    const result = object(await this.request('thread/goal/clear', { threadId: this.threadId }, 10_000));
    return result.cleared === true;
  }

  close(interrupt = false): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    this.stopPromise = Promise.resolve().then(() => this.stop(interrupt));
    return this.stopPromise;
  }

  private async stop(interrupt: boolean): Promise<void> {
    if (interrupt && !this.disconnected && this.active?.id) {
      try {
        await this.request('turn/interrupt', { threadId: this.threadId, turnId: this.active.id }, 2_000);
        await this.waitForIdle(2_000);
      } catch {
        // A lost or hung server still has to be reaped.
      }
    }
    this.disconnect.abort();
    try {
      if (this.child.pid && this.child.exitCode === null && this.child.signalCode === null) {
        this.child.kill('SIGKILL');
      }
    } catch {
      // Still release the pipes and bound the wait for the process.
    }
    this.child.stdin.destroy();
    this.child.stdout.destroy();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const timedOut = await Promise.race([
        this.childClosed.then(() => false),
        new Promise<boolean>((resolve) => {
          timer = setTimeout(() => resolve(true), CHILD_CLOSE_TIMEOUT_MS);
        }),
      ]);
      if (timedOut) this.transportLost('Timed out waiting for Codex app-server to close');
    } finally {
      clearTimeout(timer);
      this.child.stderr.destroy();
    }
  }

  private transportLost(reason: string, exitCode: number | null = null): void {
    if (this.disconnected) return;
    this.disconnected = true;
    this.disconnect.abort();
    const summary = this.withStderr(reason);
    for (const finish of this.pending.values()) finish(new Error(summary));
    if (this.active) this.finishTurn(this.active);
    try {
      this.events.onExit(summary, exitCode);
    } finally {
      void this.close();
    }
  }

  private request(method: string, params: Obj, timeout = this.requestTimeoutMs): Promise<unknown> {
    if (this.disconnected) return Promise.reject(new Error('Codex app-server is disconnected'));
    const id = ++this.seq;
    return new Promise((resolve, reject) => {
      const finish = (error: Error | null, result?: unknown) => {
        clearTimeout(timer);
        this.pending.delete(id);
        if (error) reject(error);
        else resolve(result);
      };
      const timer = setTimeout(() => finish(new Error(`Codex ${method} timed out`)), timeout);
      this.pending.set(id, finish);
      try {
        this.write({ method, id, params });
      } catch (error) {
        finish(error instanceof Error ? error : new Error('Codex stdin write failed'));
      }
    });
  }

  private write(message: Obj): void {
    this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n');
  }

  private receive(message: Obj): void {
    if (this.disconnected) return;
    const { id } = message;
    const method = string(message.method);
    const params = object(message.params);
    if (!method && typeof id === 'number') {
      this.pending.get(id)?.(message.error
        ? new RpcRejection(this.redact(string(object(message.error).message) ?? 'Codex rejected the request'))
        : null, message.result);
      return;
    }
    if (!method) return;
    if (typeof id === 'string' || typeof id === 'number') {
      void this.replyToServer(id, method, params).catch(() => this.transportLost('Codex approval response could not be sent'));
      return;
    }
    if (this.threadId && params.threadId === this.threadId) {
      if (method === 'thread/goal/updated') {
        this.events.onGoalChanged?.(parseCodexGoal(params.goal));
        return;
      }
      if (method === 'thread/goal/cleared') {
        this.events.onGoalChanged?.(null);
        return;
      }
    }
    const turn = this.active;
    if (!turn || params.threadId !== this.threadId) return;
    const reportedTurn = object(params.turn);
    const turnId = string(reportedTurn.id ?? params.turnId);
    if (turn.id && turnId !== turn.id) return;
    if (method === 'turn/started' && turnId) this.observeStart(turn, turnId);
    this.emitTimeline(turn, method, params);
    if (method === 'item/agentMessage/delta' && typeof params.delta === 'string') {
      const itemId = string(params.itemId) ?? '';
      turn.messages.set(itemId, (turn.messages.get(itemId) ?? '') + params.delta);
      this.append(params.delta);
    }
    if (method === 'item/completed') {
      const item = object(params.item);
      if (item.type === 'agentMessage' && typeof item.text === 'string') {
        const itemId = string(item.id) ?? '';
        if (!turn.messages.has(itemId)) this.append(item.text);
        turn.messages.set(itemId, item.text);
        this.append('\n');
      }
      if (item.type === 'mcpToolCall') {
        const error = string(object(item.error).message);
        this.append(`\n[MCP ${string(item.server) ?? ''}/${string(item.tool) ?? ''}: ${string(item.status) ?? 'finished'}${error ? ': ' + error : ''}]\n`);
      }
    }
    if (method === 'thread/tokenUsage/updated') {
      const tokenUsage = object(params.tokenUsage);
      const total = numericFields(tokenUsage.total);
      const last = numericFields(tokenUsage.last);
      const before = turn.usageBaseline;
      turn.usage = Object.keys(total).length === 0 || !before ? last : Object.fromEntries(
        Object.entries(total).map(([key, count]) => {
          const baseline = before[key] ?? 0;
          return [key, count >= baseline ? count - baseline : last[key] ?? count];
        }),
      );
      if (Object.keys(total).length > 0) this.tokenTotal = total;
      if (turn.usage) this.timeline(turn, { type: 'usage', usage: turn.usage });
    }
    if (method === 'turn/diff/updated') turn.diff = string(params.diff) ?? null;
    if (method === 'turn/completed' && turnId) {
      this.observeStart(turn, turnId);
      for (const entry of Array.isArray(reportedTurn.items) ? reportedTurn.items : []) {
        const item = object(entry);
        const itemId = string(item.id);
        if (item.type === 'agentMessage' && typeof item.text === 'string' && itemId && !turn.messages.has(itemId)) {
          turn.messages.set(itemId, item.text);
          this.append(item.text + '\n');
        }
      }
      this.finishTurn(turn);
      this.events.onTurnCompleted(turn.turnKey, {
        turnId, stopReason: string(reportedTurn.status) ?? 'unknown',
        finalText: this.redact([...turn.messages.values()].at(-1) ?? ''),
        usage: turn.usage,
        diff: turn.diff === null ? null : this.redact(turn.diff),
        error: this.redact(string(object(reportedTurn.error).message) ?? '') || null,
      });
    }
  }

  private observeStart(turn: ActiveTurn, id: string): void {
    if (turn.id) return;
    turn.id = id;
    this.timeline(turn, { type: 'turn_started', turnId: id });
    this.events.onTurnStarted(turn.turnKey);
  }

  private emitTimeline(turn: ActiveTurn, method: string, params: Obj): void {
    if (!this.events.onTimeline) return;
    for (const event of codexTimelineEvents(method, params)) this.timeline(turn, event);
  }

  private timeline(turn: ActiveTurn, event: WorkerTimelineEvent): void {
    if (!this.events.onTimeline) return;
    try {
      this.events.onTimeline(turn.turnKey, this.redactEvent(event));
    } catch (error) {
      console.warn('[orchestration] Codex timeline listener failed:', error);
    }
  }

  private redactEvent(event: WorkerTimelineEvent): WorkerTimelineEvent {
    if (this.secrets.length === 0) return event;
    const walk = (value: unknown): unknown => {
      if (typeof value === 'string') return this.redact(value);
      if (Array.isArray(value)) return value.map(walk);
      if (value && typeof value === 'object') {
        return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, walk(entry)]));
      }
      return value;
    };
    return walk(event) as WorkerTimelineEvent;
  }

  private finishTurn(turn: ActiveTurn): void {
    if (this.active === turn) this.active = null;
    turn.finish();
  }

  private withStderr(reason: string): string {
    const safeReason = this.redact(reason);
    if (safeReason.includes('\nCodex stderr:\n')) return safeReason;
    const tail = Buffer.from(this.redact(this.stderrTail.toString('utf8')))
      .subarray(-STDERR_LIMIT).toString('utf8').trim();
    return tail ? `${safeReason}\nCodex stderr:\n${tail}` : safeReason;
  }

  redactText(text: string): string {
    return this.redact(text);
  }

  private redact(text: string): string {
    for (const secret of this.secrets) text = text.replaceAll(secret, '<redacted>');
    return text;
  }

  private append(text: string): void {
    this.output += text;
    const overflow = this.output.length - OUTPUT_LIMIT;
    if (overflow > 0) {
      this.offset += overflow;
      this.output = this.output.slice(overflow);
    }
  }

  private async askUser(turn: ActiveTurn, method: string, params: Obj): Promise<Record<string, string[]> | null> {
    const asked = (Array.isArray(params.questions) ? params.questions : []).map(object);
    // A secret typed into the question lane would land in the transcript.
    if (asked.length === 0 || asked.some((question) => question.isSecret === true)) return null;
    const questions = asked.map((question) => ({
      id: string(question.id) ?? '', header: this.redact(string(question.header) ?? ''),
      prompt: this.redact(string(question.question) ?? ''),
      allowOther: question.isOther === true || !Array.isArray(question.options) || question.options.length === 0,
      options: (Array.isArray(question.options) ? question.options : []).map(object).map((option) => ({
        label: string(option.label) ?? '', description: string(option.description) ?? '',
      })),
    }));
    try {
      const response = await this.events.onRequest(turn.turnKey, {
        kind: 'question', method, toolInput: '', questions, signal: this.disconnect.signal,
      });
      return 'answers' in response ? response.answers : null;
    } catch {
      return null;
    }
  }

  private async replyToServer(id: RpcId, method: string, params: Obj): Promise<void> {
    const turn = this.active;
    if (method === 'item/tool/requestUserInput' && turn && !this.stopping &&
      params.threadId === this.threadId && (!turn.id || params.turnId === turn.id)) {
      const answers = await this.askUser(turn, method, params);
      if (this.disconnected) return;
      if (answers) {
        this.write({ id, result: {
          answers: Object.fromEntries(Object.entries(answers).map(([key, value]) => [key, { answers: value }])),
        } });
        return;
      }
    }
    const mcpConfirmation = method === 'mcpServer/elicitation/request';
    const approval = method === 'item/commandExecution/requestApproval' ||
      method === 'item/fileChange/requestApproval' || method === 'item/permissions/requestApproval' ||
      mcpConfirmation;
    if (!approval) {
      if (turn) this.append(`\n[Unsupported Codex request: ${method}]\n`);
      this.write({ id, error: { code: -32601, message: 'Unsupported request in the AhaStation pilot' } });
      return;
    }
    const schema = object(params.requestedSchema);
    // An approval button cannot supply arbitrary data for an MCP form.
    const supportsConfirmation = !mcpConfirmation || (
      ['form', 'openai/form', 'openaiForm'].includes(string(params.mode) ?? '') &&
      (!Array.isArray(schema.required) || schema.required.length === 0)
    );
    const matchesTurn = turn && params.threadId === this.threadId &&
      (!turn.id || params.turnId === turn.id || (mcpConfirmation && params.turnId === null));
    let allowed = false;
    if (!this.stopping && matchesTurn && supportsConfirmation) {
      try {
        const response = await this.events.onRequest(turn.turnKey, {
          kind: 'approval',
          method,
          toolInput: this.redact(mcpConfirmation
            ? JSON.stringify({ server: params.serverName, metadata: params._meta, schema })
            : string(params.command) ?? JSON.stringify(params.permissions ?? params.grantRoot ?? {})),
          message: this.redact(string(params.reason ?? params.message) ?? '') || undefined,
          signal: this.disconnect.signal,
        });
        allowed = 'decision' in response && response.decision === 'accept';
      } catch {
        // Fail closed if the host cannot record the approval.
      }
    }
    if (this.disconnected) return;
    if (mcpConfirmation) {
      this.write({ id, result: { action: allowed ? 'accept' : 'decline', content: allowed ? {} : null, _meta: null } });
    } else if (method === 'item/permissions/requestApproval') {
      this.write({ id, result: { permissions: allowed ? object(params.permissions) : {}, scope: 'turn' } });
    } else {
      const offered = params.availableDecisions;
      const decision = allowed && (!Array.isArray(offered) || offered.includes('accept')) ? 'accept' : 'decline';
      this.write({ id, result: { decision } });
    }
  }
}
