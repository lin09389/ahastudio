#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');

const SERVER_INFO = {
  name: 'aha-orchestration',
  version: '1.0.0',
};
const DEFAULT_PROTOCOL_VERSION = '2024-11-05';
const TASK_KIND_ENUM = [
  'architecture',
  'implementation',
  'debug',
  'tests',
  'review',
  'research',
  'docs',
];
const BOARD_POST_KIND_ENUM = [
  'brief',
  'post',
  'handoff',
  'question',
  'answer',
  'report',
  'approval',
  'stall',
];

function readCredentialFile() {
  const file = process.env.AHA_CREDENTIAL_FILE;
  if (!file) return {};
  try {
    const data = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (!data || typeof data !== 'object' || Array.isArray(data)) return {};
    const out = {};
    for (const [key, value] of Object.entries(data)) {
      if (/^AHA_[A-Z0-9_]+$/.test(key) && typeof value === 'string') out[key] = value;
    }
    return out;
  } catch {
    return {};
  }
}

const credentialFileEnv = readCredentialFile();

function envValue(name) {
  return process.env[name] || credentialFileEnv[name] || '';
}


function readEndpointFile() {
  const file = envValue('AHA_AGENT_HOOK_ENDPOINT_FILE');
  if (!file) return null;
  try {
    const data = JSON.parse(fs.readFileSync(file, 'utf8'));
    const port = Number(data && data.port);
    if (!Number.isInteger(port) || port <= 0 || port > 65535) return null;
    if (typeof data.token !== 'string' || !/^[a-f0-9]{16,128}$/i.test(data.token)) return null;
    return { baseUrl: `http://127.0.0.1:${port}/orchestration`, token: data.token };
  } catch {
    return null;
  }
}

function resolveOrchestrationEndpoint() {
  const fromFile = readEndpointFile();
  if (fromFile) return fromFile;
  const token = envValue('AHA_AGENT_HOOK_TOKEN');
  if (envValue('AHA_ORCHESTRATION_URL')) {
    return { baseUrl: envValue('AHA_ORCHESTRATION_URL').replace(/\/+$/, ''), token };
  }
  if (envValue('AHA_AGENT_HOOK_URL')) {
    return {
      baseUrl: envValue('AHA_AGENT_HOOK_URL').replace(/\/hook\/?$/, '') + '/orchestration',
      token,
    };
  }
  if (envValue('AHA_AGENT_HOOK_PORT')) {
    return {
      baseUrl: `http://127.0.0.1:${envValue('AHA_AGENT_HOOK_PORT')}/orchestration`,
      token,
    };
  }
  return null;
}

function callerIdentity() {
  return envValue('AHA_WORKER_ID') || envValue('AHA_SESSION_ID') || envValue('AHA_TERMINAL_ID') || '';
}

function postOrchestrationAction(action, payload, activeRequests) {
  const endpoint = resolveOrchestrationEndpoint();
  if (!endpoint) {
    return Promise.reject(
      new Error('Missing AhaStation orchestration endpoint environment variables.')
    );
  }
  const urlStr = `${endpoint.baseUrl}/${action}`;
  return new Promise((resolve, reject) => {
    const parsed = new URL(urlStr);
    const bodyText = JSON.stringify(payload || {});
    const client = parsed.protocol === 'https:' ? https : http;

    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(bodyText),
    };
    if (endpoint.token) {
      headers['x-aha-agent-hook-token'] = endpoint.token;
    }
    if (envValue('AHA_WORKER_ID')) {
      headers['x-aha-worker-id'] = envValue('AHA_WORKER_ID');
      headers['x-aha-worker-token'] = envValue('AHA_WORKER_TOKEN');
    } else if (envValue('AHA_SESSION_ID')) {
      headers['x-aha-session-id'] = envValue('AHA_SESSION_ID');
      headers['x-aha-session-token'] = envValue('AHA_SESSION_TOKEN');
    } else {
      if (envValue('AHA_TERMINAL_ID')) headers['x-aha-pty-id'] = envValue('AHA_TERMINAL_ID');
      if (envValue('AHA_ORCHESTRATION_PANE_TOKEN')) headers['x-aha-pane-token'] = envValue('AHA_ORCHESTRATION_PANE_TOKEN');
    }

    const req = client.request(
      {
        hostname: parsed.hostname,
        port: parsed.port,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers,
      },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => {
          if (activeRequests) activeRequests.delete(req);
          const text = Buffer.concat(chunks).toString('utf8');
          try {
            const json = JSON.parse(text);
            resolve({ status: res.statusCode || 500, body: json });
          } catch {
            reject(new Error(`Invalid JSON response (${res.statusCode}): ${text}`));
          }
        });
      }
    );

    if (activeRequests) activeRequests.add(req);
    req.on('error', (err) => {
      if (activeRequests) activeRequests.delete(req);
      reject(err);
    });

    req.write(bodyText);
    req.end();
  });
}

function buildDelegateTaskTool(ctx) {
  const roster = Array.isArray(ctx && ctx.roster) ? ctx.roster : [];
  const available = roster.filter((entry) => entry && entry.available);
  const availableCliIds = [];
  for (const entry of available) {
    if (typeof entry.cliId === 'string' && !availableCliIds.includes(entry.cliId)) {
      availableCliIds.push(entry.cliId);
    }
  }
  const agentEnum = ['auto', ...availableCliIds];

  const descParts = [
    'Delegate a task to a Coding Agent worker in AhaStation (spawns or reuses a worker terminal with optional isolated Git worktree).',
  ];

  if (available.length > 0) {
    const cliSummaries = available.map((a) => {
      const idleCount = Array.isArray(a.idlePanes) ? a.idlePanes.length : 0;
      const idlePart = idleCount > 0 ? `, ${idleCount} idle pane${idleCount > 1 ? 's' : ''}` : '';
      const specPart = a.specialty ? `: ${a.specialty}` : '';
      return `${a.cliId}${specPart}${idlePart}`;
    });
    descParts.push(`Available agents: ${cliSummaries.join('; ')}.`);
  }

  const confirmMode = (ctx && ctx.confirmMode) || 'chat-and-guard';
  if (confirmMode === 'chat-and-guard') {
    descParts.push(
      'Confirmation rule (2-step): before spawning workers on a new user request, first call delegate_task with confirmed=false (and authorizeReviews=true when you plan to review completed work) to register the dispatch plan without spawning, then briefly ask the user in one natural sentence to confirm the role, Coding Agent, task, worktree isolation, and automatic post-work heterogeneous review; after the user replies to confirm, call delegate_task with confirmed=true and the original proposalId. Once confirmed with authorizeReviews=true (or after confirming one review in this Run), subsequent reviewOf / reviewRun dispatches in the same Run start without re-prompting the user.'
    );
  } else if (confirmMode === 'always-ui') {
    descParts.push(
      'This call waits for the user to approve (or adjust agent/worktree) in the AhaStation UI.'
    );
  }

  return {
    name: 'delegate_task',
    description: descParts.join(' '),
    inputSchema: {
      type: 'object',
      properties: {
        role: {
          type: 'string',
          description: 'Short semantic worker role name without @ (e.g. reviewer, tester, backend).',
        },
        task: {
          type: 'string',
          description:
            'Clear, self-contained task specification for the worker (workers start with a blank context and cannot see your prior chat). Optional when reviewOf or reviewRun is set.',
        },
        reviewOf: {
          type: 'string',
          maxLength: 128,
          description:
            'Launch an isolated read-only adversarial review of this settled worker (@role or dispatchId). Automatically selects a heterogeneous Coding Agent avoiding the author CLI, snapshots the diff into a detached review worktree, and withholds author self-summary.',
        },
        reviewRun: {
          type: 'boolean',
          description:
            'Launch a post-integration Run-level overall secondary adversarial review across the combined changes of your active Run. Avoids all integrated implementation CLIs, snapshots the combined integrated commit into a detached read-only worktree, and firewalls worker verbal summaries.',
        },
        reviewFocus: {
          type: 'string',
          maxLength: 400,
          description:
            'Optional review focus angle when reviewOf or reviewRun is set (enables parallel multi-perspective L2 reviews, e.g. "security & state transitions" vs "edge cases & tests").',
        },
        context: {
          type: 'string',
          maxLength: 16000,
          description:
            'Curated background from the coordinator: user goal, already-confirmed decisions, and pitfalls to avoid.',
        },
        constraints: {
          type: 'array',
          maxItems: 20,
          items: { type: 'string', minLength: 1, maxLength: 400 },
          description:
            'Explicit boundaries and guardrails (e.g. files not to touch, platform/API requirements).',
        },
        files: {
          type: 'array',
          maxItems: 30,
          items: { type: 'string', minLength: 1, maxLength: 400 },
          description:
            'Key file paths, symbols, or design doc paths already located by the coordinator so the worker can jump straight in.',
        },
        acceptance: {
          type: 'string',
          maxLength: 4000,
          description:
            'Definition of done and verification commands/checks expected before calling report_done.',
        },
        background: { type: 'string', maxLength: 16000, description: 'Alias for context.' },
        taskContext: {
          type: 'object',
          properties: {
            background: { type: 'string', maxLength: 16000 },
            constraints: { type: 'array', maxItems: 20, items: { type: 'string', minLength: 1, maxLength: 400 } },
            files: { type: 'array', maxItems: 30, items: { type: 'string', minLength: 1, maxLength: 400 } },
            acceptance: { type: 'string', maxLength: 4000 },
          },
        },
        proposalId: {
          type: 'string',
          maxLength: 128,
          description: 'Return the Proposal ID after the user confirms that plan.',
        },
        agent: {
          type: 'string',
          enum: agentEnum,
          description: 'Target Coding Agent CLI or "auto" to pick the best fit by taskKind.',
        },
        taskKind: {
          type: 'string',
          enum: TASK_KIND_ENUM,
          description: 'Task category used when agent is "auto" (prefers heterogeneous review).',
        },
        worktree: {
          type: 'boolean',
          description: 'Whether to isolate the worker in a dedicated Git worktree.',
        },
        model: {
          type: 'string',
          description: 'Optional model override for the spawned Coding Agent.',
        },
        effort: {
          type: 'string',
          description: 'Optional reasoning effort override (requires model).',
        },
        terminal: {
          type: 'string',
          description: 'Optional existing idle pane ptyId to reuse.',
        },
        confirmed: {
          type: 'boolean',
          description: 'Set to true only after the user has explicitly confirmed dispatch in chat.',
        },
        authorizeReviews: {
          type: 'boolean',
          description:
            'When true on a confirmed proposal, pre-authorizes subsequent isolated heterogeneous reviews (reviewOf / reviewRun) in the same Run so the user is not prompted repeatedly.',
        },
        planSummary: {
          type: 'string',
          description: 'One-sentence human-readable summary of what this worker will do.',
        },
      },
    },
  };
}

function buildPostMessageTool(ctx) {
  const isWorker = Boolean(ctx && ctx.role === 'worker');
  return {
    name: 'post_message',
    description: isWorker
      ? 'Post a message to the shared run board. Mention @coordinator or an active sibling @role (e.g. @verifier) to wake them, cite earlier posts by #seq (e.g. #4), or post unaddressed status updates without waking anyone. Worker @all is visible on the board but never wakes peers.'
      : 'Post a message to the shared run board. Mention @role to answer a pending question, steer an active worker, or reactivate a settled worker; mention @all to broadcast to all active workers; cite earlier posts by #seq.',
    inputSchema: {
      type: 'object',
      properties: {
        text: {
          type: 'string',
          description: 'Message text to post on the shared board (supports @role mentions and #seq references).',
        },
        kind: {
          type: 'string',
          enum: BOARD_POST_KIND_ENUM,
          description: 'Optional board post kind (defaults to handoff for peer mentions or post otherwise).',
        },
        subject: {
          type: 'string',
          description: 'Optional short subject line for the post.',
        },
      },
      required: ['text'],
    },
  };
}

function buildReadBoardTool() {
  return {
    name: 'read_board',
    description:
      'Read messages from the shared run board (oldest first) alongside a compact team status summary. Pass since=<seq> to read only posts after that sequence number, or seqs=[...] to pull specific posts without advancing your cursor.',
    inputSchema: {
      type: 'object',
      properties: {
        since: {
          type: 'number',
          description: 'Return posts with boardSeq strictly greater than this number.',
        },
        seqs: {
          type: 'array',
          items: { type: 'number' },
          description: 'Fetch only these specific board sequence numbers without advancing lastReadSeq.',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of posts to return (default 20).',
        },
      },
    },
  };
}

function buildBoardWaitTool() {
  return {
    name: 'board_wait',
    description:
      'Sleep reactively until another member posts to the shared board (never poll in a loop). Pass since=<lastReadSeq> so messages posted while you were busy wake you immediately. Defaults to mentionsOnly=true (wakes on @your-role, reports/questions/approvals, or coordinator @all); pass mentionsOnly=false to wake on any new post.',
    inputSchema: {
      type: 'object',
      properties: {
        since: {
          type: 'number',
          description: 'Highest boardSeq you have already read (defaults to your server-tracked lastReadSeq).',
        },
        mentionsOnly: {
          type: 'boolean',
          description: 'Wake only for posts addressing you or privileged @all (default: true).',
        },
        timeoutMs: {
          type: 'number',
          description: 'Maximum wait duration in milliseconds (default 120000).',
        },
        peek: {
          type: 'boolean',
          description: 'If true, inspect board posts immediately without waiting or advancing your cursor.',
        },
      },
    },
  };
}

function buildTeamWaitTool() {
  return {
    name: 'team_wait',
    description:
      'Alias of board_wait: wait reactively for board updates, worker completions, questions, permission approval requests, or turn-stop escalations (never poll in a loop).',
    inputSchema: {
      type: 'object',
      properties: {
        since: {
          type: 'number',
          description: 'Highest boardSeq you have already read (defaults to your server-tracked lastReadSeq).',
        },
        mentionsOnly: {
          type: 'boolean',
          description: 'Wake only for posts addressing you or privileged @all (default: true).',
        },
        timeoutMs: {
          type: 'number',
          description: 'Maximum wait duration in milliseconds (default 120000).',
        },
        peek: {
          type: 'boolean',
          description: 'If true, read unread updates immediately without waiting or acknowledging.',
        },
      },
    },
  };
}

function buildToolsForContext(ctx) {
  if (ctx && ctx.role === 'worker') {
    const tools = [
      {
        name: 'report_done',
        description: 'Report task completion, failure, or blocker back to your coordinator.',
        inputSchema: {
          type: 'object',
          properties: {
            outcome: {
              type: 'string',
              enum: ['completed', 'failed', 'blocked', 'escalated', 'abandoned'],
              description: 'Final outcome (default: completed).',
            },
            summary: {
              type: 'string',
              description: 'Concise summary of what changed, verification results, or root cause.',
            },
            verdict: {
              type: 'string',
              enum: ['pass', 'reject'],
              description:
                'For adversarial review tasks only: final verdict ("pass" or "reject"). Any P0/P1 finding forces reject.',
            },
            findings: {
              type: 'array',
              maxItems: 25,
              description: 'For adversarial review tasks only: concrete verified findings.',
              items: {
                type: 'object',
                properties: {
                  severity: { type: 'string', enum: ['P0', 'P1', 'P2'] },
                  location: { type: 'string', maxLength: 240 },
                  title: { type: 'string', maxLength: 240 },
                  evidence: { type: 'string', maxLength: 1200 },
                  fix: { type: 'string', maxLength: 800 },
                },
                required: ['severity', 'location', 'title', 'evidence'],
              },
            },
          },
          required: ['summary'],
        },
      },
      {
        name: 'ask_coordinator',
        description:
          'Ask your coordinator a clarifying or blocking question instead of guessing (waits for reply by default).',
        inputSchema: {
          type: 'object',
          properties: {
            question: {
              type: 'string',
              description: 'The question to ask the coordinator.',
            },
            options: {
              type: 'array',
              items: { type: 'string' },
              description: 'Optional discrete choices.',
            },
            wait: {
              type: 'boolean',
              description: 'Wait for the coordinator answer before returning (default: true).',
            },
            timeoutMs: {
              type: 'number',
              description: 'Wait timeout in milliseconds.',
            },
          },
          required: ['question'],
        },
      },
      buildPostMessageTool(ctx),
      buildReadBoardTool(),
      buildBoardWaitTool(),
    ];
    if (ctx.canDelegate) {
      tools.push(buildDelegateTaskTool(ctx), buildTeamWaitTool());
    }
    return tools;
  }

  return [
    {
      name: 'list_coding_agents',
      description:
        'List Coding Agent CLIs available in AhaStation along with their specialties, default models, MCP readiness, and idle panes.',
      inputSchema: {
        type: 'object',
        properties: {},
      },
    },
    buildDelegateTaskTool(ctx),
    {
      name: 'team_status',
      description:
        'View active and settled workers managed by this pane (roles, CLIs, stages, open questions/approvals) and pending dispatch proposals.',
      inputSchema: {
        type: 'object',
        properties: {
          includeSettled: {
            type: 'boolean',
            description: 'Include completed/failed/released workers (default: true).',
          },
        },
      },
    },
    buildPostMessageTool(ctx),
    buildReadBoardTool(),
    buildBoardWaitTool(),
    buildTeamWaitTool(),
    {
      name: 'message_worker',
      description:
        'Reply to a worker pending question, steer an active worker, or reactivate a settled worker by @role (or dispatch id).',
      inputSchema: {
        type: 'object',
        properties: {
          role: {
            type: 'string',
            description: 'Target worker @role name or dispatch id.',
          },
          message: {
            type: 'string',
            description: 'Reply or follow-up instructions for the worker.',
          },
          subject: {
            type: 'string',
            description: 'Optional short subject line when steering or reactivating.',
          },
        },
        required: ['role', 'message'],
      },
    },
    {
      name: 'manage_worker',
      description:
        'Inspect terminal output, approve/deny a pending permission request, release resources, or stop a worker by @role (or dispatch id).',
      inputSchema: {
        type: 'object',
        properties: {
          role: {
            type: 'string',
            description: 'Target worker @role name or dispatch id.',
          },
          action: {
            type: 'string',
            enum: ['read', 'approve', 'deny', 'release', 'stop'],
            description: 'Management action to perform on the target worker.',
          },
          reason: {
            type: 'string',
            description: 'Optional reason when denying an approval or stopping a worker.',
          },
          force: {
            type: 'boolean',
            description: 'Force release/stop even with unread mail or dirty worktree.',
          },
          screen: {
            type: 'boolean',
            description: 'For action="read", read visible terminal screen (default: true).',
          },
        },
        required: ['role', 'action'],
      },
    },
    {
      name: 'inspect_worker_diff',
      description:
        'Read-only inspection of a worker real Git changes (baseCommit..snapshot including committed and uncommitted files), diff stat, bounded unified diff, and recommended adversarial review tier (L0/L1/L2).',
      inputSchema: {
        type: 'object',
        properties: {
          role: {
            type: 'string',
            description: 'Target worker @role name, dispatch id, or task id.',
          },
          files: {
            type: 'array',
            maxItems: 30,
            items: { type: 'string', minLength: 1, maxLength: 400 },
            description: 'Optional relative file paths to inspect full diff for specific files.',
          },
          maxDiffBytes: {
            type: 'number',
            description: 'Optional diff byte budget (1024..131072, default 32768).',
          },
        },
        required: ['role'],
      },
    },
    {
      name: 'integrate_worker',
      description:
        'Safely check or integrate a settled worker changes into the main workspace or the Run integration branch aha/run/<runId> (default mode="squash", targetBranch="auto", releaseAfter=true). When main is clean, integrates directly into main; when main has uncommitted user edits, seamlessly routes into aha/run/<runId> without touching dirty files in main. Pass targetBranch="main" to require direct integration into main.',
      inputSchema: {
        type: 'object',
        properties: {
          role: {
            type: 'string',
            description: 'Target worker @role name, dispatch id, or task id.',
          },
          mode: {
            type: 'string',
            enum: ['check', 'squash', 'merge'],
            description: 'Integration mode: "check" (read-only conflict check), "squash" (default, one clean commit), or "merge".',
          },
          targetBranch: {
            type: 'string',
            enum: ['main', 'integration', 'auto'],
            description: 'Target branch strategy: "auto" (default: use main if clean, otherwise seamlessly route to aha/run/<runId>, or continue active aha/run/<runId>), "integration" (always use aha/run/<runId>), or "main" (require clean main branch).',
          },
          expectedSnapshotOid: {
            type: 'string',
            description: 'Optional snapshot OID from review/diff to guarantee no changes occurred after review.',
          },
          commitMessage: {
            type: 'string',
            maxLength: 500,
            description: 'Optional commit message for squash/merge integration.',
          },
          releaseAfter: {
            type: 'boolean',
            description: 'Automatically release worker + review panes, worktrees, and temporary branches after integration (default: true).',
          },
          deleteBranch: {
            type: 'boolean',
            description: 'Delete the owned aha/worker/* branch on release (default: true).',
          },
          force: {
            type: 'boolean',
            description: 'Skip automatic reviewed-snapshot OID check if intentionally integrating an updated snapshot.',
          },
        },
        required: ['role'],
      },
    },
    {
      name: 'inspect_run_diff',
      description:
        'Read-only inspection of the combined integrated changes across a Run (baseCommit..integrationHead on aha/run/<runId> or main), including integrated workers, commits, diff stat, bounded unified diff, integration status, and Run-level secondary adversarial review status.',
      inputSchema: {
        type: 'object',
        properties: {
          runId: {
            type: 'string',
            description: 'Optional run ID (defaults to this coordinator active run).',
          },
          files: {
            type: 'array',
            maxItems: 30,
            items: { type: 'string', minLength: 1, maxLength: 400 },
            description: 'Optional relative file paths to inspect full diff for specific files.',
          },
          maxDiffBytes: {
            type: 'number',
            description: 'Optional diff byte budget (1024..131072, default 32768).',
          },
        },
      },
    },
    {
      name: 'finalize_run_integration',
      description:
        'Check or promote an active Run integration branch (aha/run/<runId>) back into the main branch once the main workspace is clean (default mode="squash"). Enforces Run-level adversarial review gates if started, protects against dirty main / ignored-file collisions / conflicts, and cleans up the _run-* worktree and aha/run/* branch.',
      inputSchema: {
        type: 'object',
        properties: {
          runId: {
            type: 'string',
            description: 'Optional run ID (defaults to this coordinator active run).',
          },
          mode: {
            type: 'string',
            enum: ['check', 'squash', 'merge'],
            description: 'Promotion mode: "check" (read-only conflict check), "squash" (default, one clean commit), or "merge".',
          },
          expectedSnapshotOid: {
            type: 'string',
            description: 'Optional commit OID of aha/run/<runId> HEAD to guarantee no drift since review.',
          },
          commitMessage: {
            type: 'string',
            maxLength: 500,
            description: 'Optional commit message when promoting into main.',
          },
          deleteBranch: {
            type: 'boolean',
            description: 'Delete the aha/run/<runId> branch via atomic CAS after promotion (default: true).',
          },
          keepWorktree: {
            type: 'boolean',
            description: 'Retain the _run-* integration worktree after promotion (default: false).',
          },
        },
      },
    },
  ];
}

function formatAgentsText(agents) {
  if (!Array.isArray(agents) || agents.length === 0) {
    return 'No coding agents discovered.';
  }
  return agents
    .map((a) => {
      const status = a.available ? 'available' : !a.installed ? 'not-installed' : 'disabled';
      const idle =
        Array.isArray(a.idlePanes) && a.idlePanes.length > 0
          ? `, idlePanes=[${a.idlePanes.map((p) => p.ptyId).join(', ')}]`
          : '';
      const model = a.defaultModel ? `, model=${a.defaultModel}` : '';
      const mcp = a.mcpReady ? `mcp=${a.mcpMode}` : 'mcp=fallback';
      return `- ${a.cliId} (${a.label}) [${status}; ${mcp}${model}${idle}]: ${a.specialty}`;
    })
    .join('\n');
}

function formatTeamStatusText(ctx, includeSettled) {
  const workers = Array.isArray(ctx && ctx.workers) ? ctx.workers : [];
  const filtered =
    includeSettled === false
      ? workers.filter((w) => w.status === 'pending' || w.status === 'dispatched')
      : workers;
  const lines = [];
  if (filtered.length === 0) {
    lines.push('No managed workers.');
  } else {
    lines.push(`Managed workers (${filtered.length}):`);
    for (const w of filtered) {
      const roleLabel = w.role ? `@${w.role}` : w.dispatchId;
      const extras = [];
      if (w.cliId) extras.push(w.cliId);
      extras.push(w.stage || w.status);
      if (w.branch) extras.push(`branch=${w.branch}`);
      if (w.openQuestionCount > 0) extras.push(`${w.openQuestionCount} pending question(s)`);
      if (w.openApprovalCount > 0) extras.push(`${w.openApprovalCount} pending approval(s)`);
      lines.push(`- ${roleLabel} (${w.dispatchId}): ${extras.join(', ')}`);
    }
  }
  const pendingProposals = Array.isArray(ctx && ctx.pendingProposals) ? ctx.pendingProposals : [];
  if (pendingProposals.length > 0) {
    lines.push('', `Pending dispatch proposals (${pendingProposals.length}):`);
    for (const p of pendingProposals) {
      lines.push(
        `- ${p.id}: ${p.role ? `@${p.role}` : '(unnamed)'} -> ${p.resolvedAgent} (worktree=${p.worktree})`
      );
    }
  }
  return lines.join('\n');
}

function formatDelegateResultText(res) {
  if (!res || typeof res !== 'object') return 'Unexpected empty response.';
  if (res.status === 'confirmation_required') {
    const p = res.proposal || {};
    const lines = [
      res.message || 'Dispatch requires user confirmation before starting.',
      `Proposal: ${p.id || ''}`,
      ...(p.role ? [`Role: @${p.role}`] : []),
      `Agent: ${p.resolvedAgent || p.requestedAgent || 'auto'}`,
      `Worktree: ${p.worktree ? 'yes' : 'no'}`,
      ...(p.planSummary ? [`Plan: ${p.planSummary}`] : []),
    ];
    return lines.join('\n');
  }
  if (res.status === 'rejected') {
    return `Dispatch cancelled: ${res.message || 'Rejected by user.'}`;
  }
  const lines = [
    `Worker started${res.role ? ` (@${res.role})` : ''}.`,
    `Agent: ${res.agent}${res.fallbackFrom ? ` (fallback from ${res.fallbackFrom})` : ''}`,
    ...(res.selectionReason ? [`Selection: ${res.selectionReason}`] : []),
    ...(res.dispatch && res.dispatch.id ? [`Dispatch: ${res.dispatch.id}`] : []),
    ...(typeof res.briefBoardSeq === 'number' ? [`Board: #${res.briefBoardSeq}`] : []),
    ...(res.ptyId ? [`Pane: ${res.ptyId}`] : []),
    ...(res.worktreePath ? [`Worktree: ${res.worktreePath}`] : []),
    ...(res.branch ? [`Branch: ${res.branch}`] : []),
    'Use board_wait (or team_wait) to wait reactively for board updates, completions, questions, or approvals.',
  ];
  return lines.join('\n');
}

function formatMessageWorkerText(res) {
  if (!res || typeof res !== 'object') return 'Message delivered.';
  const seqPart =
    res.message && typeof res.message.boardSeq === 'number' ? ` (#${res.message.boardSeq})` : '';
  if (res.mode === 'replied') {
    return `Answered pending question${seqPart} for worker (${res.dispatch ? res.dispatch.id : ''}).`;
  }
  if (res.mode === 'reactivated') {
    const nextId = res.followUp && res.followUp.dispatch ? res.followUp.dispatch.id : '';
    return `Reactivated settled worker${seqPart} as ${nextId}. Use board_wait for its reply.`;
  }
  return `Message delivered${seqPart} to worker (${res.dispatch ? res.dispatch.id : ''}).`;
}

function formatBoardPostText(res) {
  if (!res || typeof res !== 'object') return 'Posted to board.';
  const seq = typeof res.boardSeq === 'number' ? `#${res.boardSeq}` : '(unnumbered)';
  const head = typeof res.headSeq === 'number' ? ` (head=#${res.headSeq})` : '';
  const wokeList = Array.isArray(res.woke) ? res.woke.filter(Boolean) : [];
  const wokePart =
    wokeList.length > 0
      ? ` Woke: ${wokeList.map((role) => `@${role}`).join(', ')}.`
      : ' (no members woken).';
  const escalatedPart = res.escalatedToCoordinator ? ' Escalated to @coordinator.' : '';
  return `Posted ${seq}${head}.${wokePart}${escalatedPart}`;
}

function formatBoardReadText(res) {
  if (!res || typeof res !== 'object') return '(no board posts)';
  const posts = Array.isArray(res.posts) ? res.posts : [];
  const header = `[Board run=${res.runId || ''} | head=#${res.headSeq ?? 0} | lastRead=#${res.lastReadSeq ?? 0} | posts=${posts.length}]`;
  const parts = [header];
  if (typeof res.formatted === 'string' && res.formatted.trim()) {
    parts.push(res.formatted.trim());
  } else {
    parts.push('(no board posts)');
  }
  if (typeof res.teamSummary === 'string' && res.teamSummary.trim()) {
    parts.push(res.teamSummary.trim());
  }
  return parts.join('\n\n');
}

function formatBoardWaitText(res) {
  if (!res || typeof res !== 'object') return 'No new board updates.';
  const posts = Array.isArray(res.posts) ? res.posts : [];
  const head = res.headSeq ?? 0;
  const lastRead = res.lastReadSeq ?? 0;
  const summary =
    typeof res.teamSummary === 'string' && res.teamSummary.trim() ? `\n\n${res.teamSummary.trim()}` : '';
  if (posts.length > 0 && typeof res.formatted === 'string' && res.formatted.trim()) {
    return `[Board head=#${head} | lastRead=#${lastRead}]\n${res.formatted.trim()}${summary}`;
  }
  if (res.timedOut) {
    return `No new board updates (wait timed out; head=#${head}, lastRead=#${lastRead}).${summary}`;
  }
  if (res.cancelled) {
    return `Board wait cancelled (head=#${head}, lastRead=#${lastRead}).${summary}`;
  }
  return `No new board updates (head=#${head}, lastRead=#${lastRead}).${summary}`;
}

function formatManageWorkerText(res) {
  if (!res || typeof res !== 'object') return 'Action completed.';
  switch (res.action) {
    case 'read': {
      const out = res.output || {};
      const body = typeof out.text === 'string' && out.text.length > 0 ? out.text : '(empty)';
      return `[${out.dispatchId || ''} | ${out.screen ? 'screen' : 'log'} | ${out.alive ? 'alive' : 'exited'}]\n${body}`;
    }
    case 'approve':
      return `Approved permission ${res.approval ? res.approval.approvalId : ''} (${res.approval ? res.approval.toolName : ''}).`;
    case 'deny':
      return `Denied permission ${res.approval ? res.approval.approvalId : ''} (${res.approval ? res.approval.reason || '' : ''}).`;
    case 'release':
      return `Released worker ${res.dispatch ? res.dispatch.id : ''} (closedPane=${Boolean(res.closedPane)}, removedWorktree=${Boolean(res.removedWorktree)}).`;
    case 'stop':
      return `Stopped worker ${res.dispatch ? res.dispatch.id : ''} (closedPane=${Boolean(res.closedPane)}, removedWorktree=${Boolean(res.removedWorktree)}).`;
    default:
      return JSON.stringify(res, null, 2);
  }
}

function toolText(text, isError = false) {
  return {
    content: [{ type: 'text', text: String(text) }],
    ...(isError ? { isError: true } : {}),
  };
}

function nonEmptyStr(value) {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function finiteNonNegativeInt(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return undefined;
  return Math.max(0, Math.floor(value));
}

function finitePositiveInt(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return undefined;
  const n = Math.floor(value);
  return n >= 1 ? n : undefined;
}

function positiveIntArray(value) {
  if (!Array.isArray(value)) return undefined;
  const parsed = value.map(finitePositiveInt).filter((n) => n !== undefined);
  return parsed.length > 0 ? parsed : undefined;
}

async function handleToolCall(name, rawArgs, activeRequests) {
  if (!callerIdentity()) {
    return toolText(
      'AhaStation orchestration tools are only available inside an AhaStation terminal.',
      true
    );
  }
  const args = rawArgs && typeof rawArgs === 'object' ? rawArgs : {};

  const callAction = async (action, payload) => {
    const res = await postOrchestrationAction(action, payload, activeRequests);
    if (!res.body || res.body.ok !== true) {
      const rawCode = res.body && typeof res.body.code === 'string' ? res.body.code : '';
      const code = rawCode ? `[${rawCode}] ` : '';
      const errMsg = (res.body && res.body.error) || `HTTP ${res.status}`;
      return { ok: false, code: rawCode, errorText: `${code}${errMsg}` };
    }
    return { ok: true, result: res.body.result };
  };

  switch (name) {
    case 'list_coding_agents': {
      const res = await callAction('agents', {});
      if (!res.ok) return toolText(res.errorText, true);
      return toolText(formatAgentsText(res.result && res.result.agents));
    }
    case 'delegate_task': {
      const role = nonEmptyStr(args.role);
      const agent = nonEmptyStr(args.agent);
      const taskKind = nonEmptyStr(args.taskKind ?? args.task_kind);
      const model = nonEmptyStr(args.model);
      const effort = nonEmptyStr(args.effort);
      const terminal = nonEmptyStr(args.terminal);
      const planSummary = nonEmptyStr(args.planSummary ?? args.plan_summary);
      const reviewOf = nonEmptyStr(args.reviewOf ?? args.review_of);
      const rawReviewRun = args.reviewRun !== undefined ? args.reviewRun : args.review_run;
      const reviewRun =
        typeof rawReviewRun === 'boolean'
          ? rawReviewRun
          : nonEmptyStr(rawReviewRun);
      const reviewFocus = nonEmptyStr(args.reviewFocus ?? args.review_focus);
      const context = nonEmptyStr(args.context) || nonEmptyStr(args.background);
      const proposalId = nonEmptyStr(args.proposalId);
      const taskContext = args.taskContext;

      const payload = {
        ...(role ? { role } : {}),
        ...(typeof args.task === 'string' && args.task.trim() ? { task: args.task } : {}),
        ...(reviewOf ? { reviewOf } : {}),
        ...(reviewRun !== undefined ? { reviewRun } : {}),
        ...(reviewFocus ? { reviewFocus } : {}),
        ...(context ? { context } : {}),
        ...(taskContext !== undefined ? { taskContext } : {}),
        ...(proposalId ? { proposalId } : {}),
        ...(args.constraints !== undefined ? { constraints: args.constraints } : {}),
        ...(args.files !== undefined ? { files: args.files } : {}),
        ...(args.acceptance !== undefined ? { acceptance: args.acceptance } : {}),
        ...(agent ? { agent } : {}),
        ...(taskKind ? { taskKind } : {}),
        ...(typeof args.worktree === 'boolean' ? { worktree: args.worktree } : {}),
        ...(model ? { model } : {}),
        ...(effort ? { effort } : {}),
        ...(terminal ? { terminal } : {}),
        ...(typeof args.confirmed === 'boolean' ? { confirmed: args.confirmed } : {}),
        ...(typeof (args.authorizeReviews ?? args.authorize_reviews) === 'boolean'
          ? { authorizeReviews: Boolean(args.authorizeReviews ?? args.authorize_reviews) }
          : {}),
        ...(planSummary ? { planSummary } : {}),
        callerCwd: process.cwd(),
      };
      const res = await callAction('delegate-task', payload);
      if (!res.ok) return toolText(res.errorText, true);
      return toolText(formatDelegateResultText(res.result));
    }
    case 'team_status': {
      const res = await callAction('mcp-context', {});
      if (!res.ok) return toolText(res.errorText, true);
      const includeSettled =
        typeof args.includeSettled === 'boolean'
          ? args.includeSettled
          : typeof args.include_settled === 'boolean'
            ? args.include_settled
            : undefined;
      return toolText(formatTeamStatusText(res.result, includeSettled));
    }
    case 'post_message': {
      const kind = nonEmptyStr(args.kind);
      const subject = nonEmptyStr(args.subject);
      const res = await callAction('board-post', {
        text: typeof args.text === 'string' ? args.text : '',
        ...(kind ? { kind } : {}),
        ...(subject ? { subject } : {}),
      });
      if (!res.ok) return toolText(res.errorText, true);
      return toolText(formatBoardPostText(res.result));
    }
    case 'read_board': {
      const since = finiteNonNegativeInt(args.since);
      const seqs = positiveIntArray(args.seqs);
      const limit = finitePositiveInt(args.limit);
      const res = await callAction('board-read', {
        ...(since !== undefined ? { since } : {}),
        ...(seqs ? { seqs } : {}),
        ...(limit !== undefined ? { limit } : {}),
      });
      if (!res.ok) return toolText(res.errorText, true);
      return toolText(formatBoardReadText(res.result));
    }
    case 'board_wait':
    case 'team_wait': {
      const peek = args.peek === true;
      const since = finiteNonNegativeInt(args.since);
      const mentionsOnly =
        typeof args.mentionsOnly === 'boolean'
          ? args.mentionsOnly
          : typeof args.mentions_only === 'boolean'
            ? args.mentions_only
            : undefined;
      const timeoutMs = finiteNonNegativeInt(args.timeoutMs ?? args.timeout_ms);
      if (peek) {
        const res = await callAction('board-read', {
          ...(since !== undefined ? { since } : {}),
          markRead: false,
        });
        if (!res.ok) {
          if (name === 'team_wait' && res.code === 'run_not_found') {
            return toolText('No unread worker updates.');
          }
          return toolText(res.errorText, true);
        }
        return toolText(formatBoardReadText(res.result));
      }
      const res = await callAction('board-wait', {
        ...(since !== undefined ? { since } : {}),
        ...(mentionsOnly !== undefined ? { mentionsOnly } : {}),
        ...(timeoutMs !== undefined ? { timeoutMs } : {}),
      });
      if (!res.ok) {
        if (name === 'team_wait' && res.code === 'run_not_found') {
          const fallback = await callAction('check', {
            from: callerIdentity(),
            wait: true,
            peek: false,
            format: true,
            ...(timeoutMs !== undefined ? { timeoutMs } : {}),
          });
          if (!fallback.ok) return toolText(fallback.errorText, true);
          const fr = fallback.result || {};
          if (fr.count > 0 && typeof fr.formatted === 'string' && fr.formatted.trim()) {
            return toolText(fr.formatted);
          }
          if (fr.timedOut) return toolText('No new worker updates (wait timed out).');
          if (fr.cancelled) return toolText('Wait cancelled.');
          return toolText('No unread worker updates.');
        }
        return toolText(res.errorText, true);
      }
      return toolText(formatBoardWaitText(res.result));
    }
    case 'message_worker': {
      const subject = nonEmptyStr(args.subject);
      const res = await callAction('role-message', {
        role: typeof args.role === 'string' ? args.role : '',
        message: typeof args.message === 'string' ? args.message : '',
        ...(subject ? { subject } : {}),
      });
      if (!res.ok) return toolText(res.errorText, true);
      return toolText(formatMessageWorkerText(res.result));
    }
    case 'manage_worker': {
      const reason = nonEmptyStr(args.reason);
      const res = await callAction('role-manage', {
        role: typeof args.role === 'string' ? args.role : '',
        action: typeof args.action === 'string' ? args.action : '',
        ...(reason ? { reason } : {}),
        ...(typeof args.force === 'boolean' ? { force: args.force } : {}),
        ...(typeof args.screen === 'boolean' ? { screen: args.screen } : {}),
      });
      if (!res.ok) return toolText(res.errorText, true);
      return toolText(formatManageWorkerText(res.result));
    }
    case 'inspect_worker_diff': {
      const target = nonEmptyStr(args.role ?? args.worker ?? args.dispatchId ?? args.dispatch_id);
      const files = Array.isArray(args.files)
        ? args.files.map(nonEmptyStr).filter(Boolean)
        : undefined;
      const maxDiffBytes = finitePositiveInt(args.maxDiffBytes ?? args.max_diff_bytes);
      const res = await callAction('worker-diff', {
        ...(target ? { worker: target } : {}),
        ...(files && files.length > 0 ? { files } : {}),
        ...(maxDiffBytes !== undefined ? { maxDiffBytes } : {}),
      });
      if (!res.ok) return toolText(res.errorText, true);
      const r = res.result || {};
      const lines = [
        `Worker @${r.role || target} (${r.dispatchId || ''}) · review=${r.reviewStatus || 'none'} (R${r.reviewRound || 0}) · integration=${r.integrationStatus || 'none'}`,
        r.isGitRepo
          ? `Git: branch=${r.branch || '(detached)'} | base=${r.baseCommit || 'none'} | head=${r.headCommit || 'none'} | snapshotOid=${r.snapshotOid || 'none'} | commitsAhead=${r.commitsAhead || 0} | uncommitted=${(r.uncommittedFiles || []).length}`
          : `Non-git workspace: ${r.worktreePath || ''}`,
        `Recommended review tier: ${r.recommendedReviewTier || 'L0'} (${r.tierReason || ''})`,
      ];
      if (Array.isArray(r.stagedDivergentFiles) && r.stagedDivergentFiles.length > 0) {
        lines.push(
          `Warning: staged vs working-tree divergence detected in: ${r.stagedDivergentFiles.join(', ')} (reconcile before review/integration).`,
        );
      }
      if (Array.isArray(r.commits) && r.commits.length > 0) {
        lines.push('Commits:', ...r.commits.map((c) => `  - ${c.sha} ${c.subject}`));
      }
      if (r.diffStat) {
        lines.push('Diff stat:', r.diffStat);
      }
      if (r.diffError) {
        lines.push(`Diff error: ${r.diffError}`);
      } else if (r.diff) {
        lines.push('', r.diff);
      } else if (r.isGitRepo) {
        lines.push('No diff changes.');
      }
      return toolText(lines.join('\n'));
    }
    case 'integrate_worker': {
      const target = nonEmptyStr(args.role ?? args.worker ?? args.dispatchId ?? args.dispatch_id);
      const mode = nonEmptyStr(args.mode);
      const targetBranch = nonEmptyStr(args.targetBranch ?? args.target_branch);
      const expectedSnapshotOid = nonEmptyStr(args.expectedSnapshotOid ?? args.expected_snapshot_oid);
      const commitMessage = nonEmptyStr(args.commitMessage ?? args.commit_message);
      const res = await callAction('worker-integrate', {
        ...(target ? { worker: target } : {}),
        ...(mode ? { mode } : {}),
        ...(targetBranch ? { targetBranch } : {}),
        ...(expectedSnapshotOid ? { expectedSnapshotOid } : {}),
        ...(commitMessage ? { commitMessage } : {}),
        ...(typeof args.releaseAfter === 'boolean' ? { releaseAfter: args.releaseAfter } : {}),
        ...(typeof args.deleteBranch === 'boolean' ? { deleteBranch: args.deleteBranch } : {}),
        ...(typeof args.force === 'boolean' ? { force: args.force } : {}),
      });
      if (!res.ok) return toolText(res.errorText, true);
      const r = res.result || {};
      const lines = [
        `Integration [${r.mode || 'squash'}] for @${r.role || target}${r.targetBranch ? ` -> ${r.targetBranch}` : ''}: ${r.message || ''}`,
        ...(r.usedRunIntegrationBranch
          ? [`Run integration branch active (${r.targetBranch || 'aha/run/*'}). Call finalize_run_integration once the main workspace is clean.`]
          : []),
        ...(r.integratedCommit ? [`Commit: ${r.integratedCommit}`] : []),
        ...(r.snapshotOid ? [`Snapshot: ${r.snapshotOid}`] : []),
        ...(Array.isArray(r.conflictFiles) && r.conflictFiles.length > 0
          ? [`Conflicts: ${r.conflictFiles.join(', ')}`]
          : []),
        ...(Array.isArray(r.ignoredCollisionFiles) && r.ignoredCollisionFiles.length > 0
          ? [`Ignored file collisions: ${r.ignoredCollisionFiles.join(', ')}`]
          : []),
        ...(r.released
          ? [`Cleaned up worker resources${r.releasedReviews ? ` + ${r.releasedReviews} review helper(s)` : ''}.`]
          : []),
        ...(r.releaseSkippedReason ? [`Release skipped: ${r.releaseSkippedReason}`] : []),
        ...(Array.isArray(r.cleanupWarnings) && r.cleanupWarnings.length > 0
          ? ['Cleanup warnings:', ...r.cleanupWarnings.map((w) => `  - ${w}`)]
          : []),
      ];
      return toolText(lines.join('\n'));
    }
    case 'inspect_run_diff': {
      const runId = nonEmptyStr(args.runId ?? args.run_id);
      const files = Array.isArray(args.files)
        ? args.files.map(nonEmptyStr).filter(Boolean)
        : undefined;
      const maxDiffBytes = finitePositiveInt(args.maxDiffBytes ?? args.max_diff_bytes);
      const res = await callAction('run-diff', {
        ...(runId ? { runId } : {}),
        ...(files && files.length > 0 ? { files } : {}),
        ...(maxDiffBytes !== undefined ? { maxDiffBytes } : {}),
      });
      if (!res.ok) return toolText(res.errorText, true);
      const r = res.result || {};
      const lines = [
        `Run ${r.runId || ''} · integration=${r.integrationStatus || 'none'}${r.integrationBranch ? ` (${r.integrationBranch})` : ''} · runReview=${r.runReviewStatus || 'none'} (R${r.runReviewRound || 0})`,
        r.hasIntegratedChanges
          ? `Git: branch=${r.branch || '(detached)'} | base=${r.baseCommit || 'none'} | target=${r.targetCommit || 'none'} | commits=${r.commitsAhead || 0} | files=${(r.changedFiles || []).length}`
          : (r.message || 'No integrated changes recorded for this run yet.'),
        `Recommended review tier: ${r.recommendedReviewTier || 'L0'} (${r.tierReason || ''})`,
      ];
      if (Array.isArray(r.integratedWorkers) && r.integratedWorkers.length > 0) {
        lines.push(
          'Integrated workers:',
          ...r.integratedWorkers.map(
            (w) => `  - @${w.role} (${w.dispatchId}${w.cliId ? `, ${w.cliId}` : ''})${w.integratedCommit ? ` @ ${String(w.integratedCommit).slice(0, 10)}` : ''}`,
          ),
        );
      }
      if (Array.isArray(r.commits) && r.commits.length > 0) {
        lines.push('Commits:', ...r.commits.map((c) => `  - ${c.sha} ${c.subject}`));
      }
      if (r.diffStat) {
        lines.push('Diff stat:', r.diffStat);
      }
      if (r.diffError) {
        lines.push(`Diff error: ${r.diffError}`);
      } else if (r.diff) {
        lines.push('', r.diff);
      }
      return toolText(lines.join('\n'));
    }
    case 'finalize_run_integration': {
      const runId = nonEmptyStr(args.runId ?? args.run_id);
      const mode = nonEmptyStr(args.mode);
      const expectedSnapshotOid = nonEmptyStr(args.expectedSnapshotOid ?? args.expected_snapshot_oid);
      const commitMessage = nonEmptyStr(args.commitMessage ?? args.commit_message);
      const res = await callAction('run-finalize', {
        ...(runId ? { runId } : {}),
        ...(mode ? { mode } : {}),
        ...(expectedSnapshotOid ? { expectedSnapshotOid } : {}),
        ...(commitMessage ? { commitMessage } : {}),
        ...(typeof args.deleteBranch === 'boolean' ? { deleteBranch: args.deleteBranch } : {}),
        ...(typeof args.keepWorktree === 'boolean' ? { keepWorktree: args.keepWorktree } : {}),
      });
      if (!res.ok) return toolText(res.errorText, true);
      const r = res.result || {};
      const lines = [
        `Run finalization [${r.mode || 'squash'}] ${r.integrationBranch || ''} -> ${r.targetBranch || 'main'}: ${r.message || ''}`,
        ...(r.integratedCommit ? [`Commit: ${r.integratedCommit}`] : []),
        ...(r.snapshotOid ? [`Run snapshot: ${r.snapshotOid}`] : []),
        ...(Array.isArray(r.conflictFiles) && r.conflictFiles.length > 0
          ? [`Conflicts: ${r.conflictFiles.join(', ')}`]
          : []),
        ...(Array.isArray(r.ignoredCollisionFiles) && r.ignoredCollisionFiles.length > 0
          ? [`Ignored file collisions: ${r.ignoredCollisionFiles.join(', ')}`]
          : []),
        ...(r.removedWorktree
          ? [`Cleaned up run integration worktree${r.removedBranch ? ` and branch ${r.integrationBranch}` : ''}${r.releasedReviews ? ` + ${r.releasedReviews} run review helper(s)` : ''}.`]
          : []),
        ...(Array.isArray(r.cleanupWarnings) && r.cleanupWarnings.length > 0
          ? ['Cleanup warnings:', ...r.cleanupWarnings.map((w) => `  - ${w}`)]
          : []),
      ];
      return toolText(lines.join('\n'));
    }
    case 'report_done': {
      const outcome = nonEmptyStr(args.outcome);
      const verdict = nonEmptyStr(args.verdict);
      const res = await callAction('worker-done-self', {
        ...(outcome ? { outcome } : {}),
        ...(verdict ? { verdict } : {}),
        ...(Array.isArray(args.findings) ? { findings: args.findings } : {}),
        summary: typeof args.summary === 'string' ? args.summary : '',
      });
      if (!res.ok) return toolText(res.errorText, true);
      const d = res.result && res.result.dispatch;
      const seqPart =
        res.result && res.result.message && typeof res.result.message.boardSeq === 'number'
          ? ` as #${res.result.message.boardSeq}`
          : '';
      return toolText(
        `Completion report recorded${seqPart} (${d ? `${d.id}: ${d.status}` : 'settled'}).`
      );
    }
    case 'ask_coordinator': {
      const timeoutMs = finiteNonNegativeInt(args.timeoutMs ?? args.timeout_ms);
      const options = Array.isArray(args.options)
        ? args.options.map(nonEmptyStr).filter(Boolean)
        : [];
      const payload = {
        question: typeof args.question === 'string' ? args.question : '',
        ...(options.length > 0 ? { options } : {}),
        wait: args.wait !== false,
        ...(timeoutMs !== undefined ? { timeoutMs } : {}),
      };
      const res = await callAction('ask-self', payload);
      if (!res.ok) return toolText(res.errorText, true);
      const r = res.result || {};
      if (typeof r.answer === 'string') {
        return toolText(`Coordinator reply: ${r.answer}`);
      }
      if (r.timedOut) {
        return toolText(`Question ${r.messageId || ''} sent, but timed out waiting for reply.`);
      }
      return toolText(`Question ${r.messageId || ''} sent to coordinator (pending reply).`);
    }
    default:
      return toolText(`Unknown tool: ${name}`, true);
  }
}

function encodeFrame(messageObj, framing) {
  const json = JSON.stringify(messageObj);
  if (framing === 'content-length') {
    const bytes = Buffer.byteLength(json, 'utf8');
    return `Content-Length: ${bytes}\r\n\r\n${json}`;
  }
  return `${json}\n`;
}

function extractFramesFromBuffer(buffer) {
  const frames = [];
  let offset = 0;

  while (offset < buffer.length) {
    // Skip leading CR/LF whitespace between newline-delimited frames.
    while (offset < buffer.length && (buffer[offset] === 0x0d || buffer[offset] === 0x0a)) {
      offset += 1;
    }
    if (offset >= buffer.length) break;

    // Check if the frame begins with an LSP-style Content-Length / Content-Type header.
    const peekLen = Math.min(32, buffer.length - offset);
    const peekStr = buffer.subarray(offset, offset + peekLen).toString('ascii');
    if (
      buffer.length - offset < 16 &&
      buffer.indexOf(0x0a, offset) === -1 &&
      /^content-/i.test(peekStr)
    ) {
      break;
    }
    if (/^(?:content-length|content-type)\s*:/i.test(peekStr)) {
      const crlfIdx = buffer.indexOf('\r\n\r\n', offset);
      const lfIdx = buffer.indexOf('\n\n', offset);
      let headerEnd = -1;
      let sepLen = 0;
      if (crlfIdx !== -1 && (lfIdx === -1 || crlfIdx < lfIdx)) {
        headerEnd = crlfIdx;
        sepLen = 4;
      } else if (lfIdx !== -1) {
        headerEnd = lfIdx;
        sepLen = 2;
      }
      if (headerEnd === -1) break;

      const headerBlock = buffer.subarray(offset, headerEnd).toString('ascii');
      const match = /content-length\s*:\s*(\d+)/i.exec(headerBlock);
      if (!match) {
        offset = headerEnd + sepLen;
        continue;
      }
      const contentLength = Number(match[1]);
      const bodyStart = headerEnd + sepLen;
      if (buffer.length < bodyStart + contentLength) break;

      const bodyStr = buffer.subarray(bodyStart, bodyStart + contentLength).toString('utf8');
      frames.push({ framing: 'content-length', raw: bodyStr });
      offset = bodyStart + contentLength;
      continue;
    }

    // Standard MCP stdio transport: newline-delimited JSON-RPC.
    const nlIdx = buffer.indexOf(0x0a, offset);
    if (nlIdx === -1) break;
    const line = buffer.subarray(offset, nlIdx).toString('utf8').replace(/\r$/, '').trim();
    offset = nlIdx + 1;
    if (line.length > 0) {
      frames.push({ framing: 'newline', raw: line });
    }
  }

  return {
    frames,
    remainder: offset > 0 ? buffer.subarray(offset) : buffer,
  };
}

function runMcpServer(options = {}) {
  const input = options.input || process.stdin;
  const output = options.output || process.stdout;
  const activeRequests = new Set();
  let buffer = Buffer.alloc(0);
  let inFlight = 0;
  let stdinEnded = false;
  let resolveDone;
  const donePromise = new Promise((resolve) => {
    resolveDone = resolve;
  });

  const maybeFinish = () => {
    if (stdinEnded && inFlight === 0) {
      resolveDone();
    }
  };

  const writeResponse = (msg, framing) => {
    try {
      output.write(encodeFrame(msg, framing));
    } catch {
      // Ignore broken pipe when host disconnects.
    }
  };

  const dispatchMessage = async (msg, framing) => {
    if (!msg || typeof msg !== 'object') return;
    const hasId = Object.prototype.hasOwnProperty.call(msg, 'id') && msg.id !== undefined;
    const id = msg.id;
    const method = typeof msg.method === 'string' ? msg.method : '';
    const params = msg.params && typeof msg.params === 'object' ? msg.params : {};

    // Notifications (no id) require no response.
    if (!hasId) {
      return;
    }

    try {
      if (method === 'initialize') {
        const protocolVersion =
          typeof params.protocolVersion === 'string' && params.protocolVersion.trim()
            ? params.protocolVersion.trim()
            : DEFAULT_PROTOCOL_VERSION;
        writeResponse(
          {
            jsonrpc: '2.0',
            id,
            result: {
              protocolVersion,
              capabilities: {
                tools: { listChanged: false },
              },
              serverInfo: SERVER_INFO,
            },
          },
          framing
        );
        return;
      }

      if (method === 'ping') {
        writeResponse({ jsonrpc: '2.0', id, result: {} }, framing);
        return;
      }

      if (method === 'tools/list') {
        if (!callerIdentity()) {
          writeResponse({ jsonrpc: '2.0', id, result: { tools: [] } }, framing);
          return;
        }
        let ctx = null;
        try {
          const res = await postOrchestrationAction('mcp-context', {}, activeRequests);
          if (res.body && res.body.ok === true && res.body.result) {
            ctx = res.body.result;
          }
        } catch {
          writeResponse({ jsonrpc: '2.0', id, result: { tools: [] } }, framing);
          return;
        }
        if (!ctx) {
          writeResponse({ jsonrpc: '2.0', id, result: { tools: [] } }, framing);
          return;
        }
        writeResponse(
          { jsonrpc: '2.0', id, result: { tools: buildToolsForContext(ctx) } },
          framing
        );
        return;
      }

      if (method === 'tools/call') {
        const toolName = typeof params.name === 'string' ? params.name : '';
        const result = await handleToolCall(toolName, params.arguments, activeRequests);
        writeResponse({ jsonrpc: '2.0', id, result }, framing);
        return;
      }

      writeResponse(
        {
          jsonrpc: '2.0',
          id,
          error: {
            code: -32601,
            message: `Method not found: ${method}`,
          },
        },
        framing
      );
    } catch (err) {
      writeResponse(
        {
          jsonrpc: '2.0',
          id,
          error: {
            code: -32603,
            message: err instanceof Error ? err.message : String(err),
          },
        },
        framing
      );
    }
  };

  const processFrame = (frame) => {
    let parsed;
    try {
      parsed = JSON.parse(frame.raw);
    } catch {
      writeResponse(
        {
          jsonrpc: '2.0',
          id: null,
          error: { code: -32700, message: 'Parse error' },
        },
        frame.framing
      );
      return;
    }
    inFlight += 1;
    Promise.resolve(dispatchMessage(parsed, frame.framing)).finally(() => {
      inFlight -= 1;
      maybeFinish();
    });
  };

  input.on('data', (chunk) => {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, 'utf8');
    buffer = buffer.length === 0 ? buf : Buffer.concat([buffer, buf]);
    const extracted = extractFramesFromBuffer(buffer);
    buffer = extracted.remainder;
    for (const frame of extracted.frames) {
      processFrame(frame);
    }
  });

  input.on('end', () => {
    const trailing = buffer.toString('utf8').trim();
    buffer = Buffer.alloc(0);
    if (trailing.startsWith('{')) {
      processFrame({ framing: 'newline', raw: trailing });
    }
    stdinEnded = true;
    maybeFinish();
  });

  const abortActive = () => {
    for (const req of activeRequests) {
      try {
        req.destroy();
      } catch {
        // ignore
      }
    }
    activeRequests.clear();
  };

  input.on('close', () => {
    stdinEnded = true;
    maybeFinish();
  });
  process.once('SIGINT', () => {
    abortActive();
    stdinEnded = true;
    resolveDone();
  });
  process.once('SIGTERM', () => {
    abortActive();
    stdinEnded = true;
    resolveDone();
  });

  if (typeof input.resume === 'function') {
    input.resume();
  }

  return donePromise;
}

if (require.main === module) {
  runMcpServer().catch(() => {
    process.exitCode = 1;
  });
}

module.exports = {
  runMcpServer,
  extractFramesFromBuffer,
  encodeFrame,
  buildToolsForContext,
  resolveOrchestrationEndpoint,
};
