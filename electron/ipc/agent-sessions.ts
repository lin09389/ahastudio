import { randomUUID } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { basename, isAbsolute, join, relative, resolve } from 'node:path';
import { z } from 'zod';
import type { AgentTurnAttachment } from '../orchestration/agent-session-driver.js';
import { AGENT_SESSION_ID, type AgentSessionManager } from '../orchestration/agent-session-manager.js';
import type { CodexCatalog } from '../orchestration/codex-catalog.js';
import { codexThreadTimeline } from '../orchestration/codex-thread-history.js';
import { WorkerTimelinePatchCoalescer } from '../orchestration/worker-timeline.js';
import type { IpcContext } from './context.js';
import { handleSecure, mainWindowSenderPolicy } from './validators.js';

const id = z.string().regex(AGENT_SESSION_ID);
const policy = z.enum(['read-only', 'workspace-write', 'full-auto']);
const optionalName = z.string().trim().min(1).max(200).nullish();
const absolutePath = z.string().min(1).max(2048).refine((value) => isAbsolute(value), 'Path must be absolute');

const IMAGE_EXTENSIONS = { 'image/png': '.png', 'image/jpeg': '.jpg', 'image/gif': '.gif', 'image/webp': '.webp' } as const;
const MAX_IMAGE_BYTES = 10 * 1024 * 1024;
const MAX_FILE_BYTES = 20 * 1024 * 1024;
const MAX_ATTACHMENTS = 20;


function privateDirectory(path: string): void {
  mkdirSync(path, { recursive: true, mode: 0o700 });
  if (process.platform !== 'win32') chmodSync(path, 0o700);
}

function writePrivateFile(path: string, bytes: Buffer): void {
  writeFileSync(path, bytes, { mode: 0o600 });
  if (process.platform !== 'win32') chmodSync(path, 0o600);
}

const attachmentSchema = z.discriminatedUnion('type', [
  z.object({ type: z.literal('skill'), name: z.string().trim().min(1).max(200), path: absolutePath }).strict(),
  z.object({ type: z.literal('localImage'), path: absolutePath, name: z.string().trim().min(1).max(200).optional() }).strict(),
  z.object({ type: z.literal('file'), path: absolutePath, name: z.string().trim().min(1).max(200).optional() }).strict(),
]);

/** A file name safe on every platform, keeping the extension the agent will look at. */
const safeFileName = (name: string) =>
  basename(name).replace(/[<>:"/\\|?*\u0000-\u001f]/gu, '_').replace(/^\.+/u, '_').slice(0, 120) || 'file';

const createSchema = z.object({
  cwd: z.string().min(1).max(1024).optional(),
  policy,
  model: optionalName,
  effort: optionalName,
  network: z.boolean().optional(),
}).strict();
const sendSchema = z.object({
  id, text: z.string().max(512_000), policy: policy.optional(), model: optionalName, effort: optionalName,
  network: z.boolean().optional(), freshContext: z.boolean().optional(),
  attachments: z.array(attachmentSchema).max(MAX_ATTACHMENTS).optional(),
}).strict().refine((input) => input.text.trim() || input.attachments?.length, 'Message is empty');
const goalActionSchema = z.discriminatedUnion('type', [
  z.object({ type: z.literal('show') }).strict(),
  z.object({ type: z.literal('clear') }).strict(),
  z.object({ type: z.literal('pause') }).strict(),
  z.object({ type: z.literal('resume') }).strict(),
  z.object({ type: z.literal('set'), objective: z.string().max(8_000) }).strict(),
]);
const goalSchema = z.object({ id, action: goalActionSchema }).strict();
const feedbackSchema = z.object({ id, reason: z.string().max(4_000).optional() }).strict();
const unqueueSchema = z.object({ id, messageId: z.string().min(1).max(64) }).strict();
const catalogSchema = z.object({ id, reload: z.boolean().optional() }).strict();
const searchSchema = z.object({ id, query: z.string().max(200) }).strict();
const skillSwitchSchema = z.object({ id, path: absolutePath, enabled: z.boolean() }).strict();
const imageSchema = z.object({
  id,
  name: z.string().trim().min(1).max(200),
  mime: z.enum(Object.keys(IMAGE_EXTENSIONS) as [keyof typeof IMAGE_EXTENSIONS, ...(keyof typeof IMAGE_EXTENSIONS)[]]),
  // Base64 grows by 4/3; the decoded size is checked again below.
  data: z.string().min(1).max(Math.ceil(MAX_IMAGE_BYTES / 3) * 4 + 4),
}).strict();
const fileSchema = z.object({
  id,
  name: z.string().trim().min(1).max(200),
  data: z.string().min(1).max(Math.ceil(MAX_FILE_BYTES / 3) * 4 + 4),
}).strict();
const answerSchema = z.object({
  id, requestId: z.string().min(1).max(64),
  answers: z.record(z.string().max(200), z.array(z.string().max(4_000)).max(20)).nullable(),
}).strict();
const targetSchema = z.object({ id }).strict();
// Matches MAX_SESSION_TITLE in src/lib/session-title.ts; the renderer checks it in characters first.
const renameSchema = z.object({ id, title: z.string().max(400).refine((value) => [...value.trim()].length <= 80, '标题最多 80 个字') }).strict();
const orchestrationSchema = z.object({ id, enabled: z.boolean() }).strict();
const threadId = z.string().min(1).max(200).regex(/^[\w-]+$/u);
const historySchema = z.object({
  cwd: absolutePath, search: z.string().max(200).optional(), archived: z.boolean().optional(),
  cursor: z.string().min(1).max(2048).nullish(),
}).strict();
const openHistorySchema = z.object({ threadId, policy }).strict();
const archiveSchema = z.object({ threadId, archived: z.boolean() }).strict();
const respondSchema = z.object({
  id, requestId: z.string().min(1).max(64), allow: z.boolean(), reason: z.string().max(2_000).optional(),
}).strict();

const fail = (error: unknown) => ({ ok: false as const, error: error instanceof Error ? error.message : String(error) });

const isInside = (root: string, path: string) => {
  const rel = relative(root, path);
  return Boolean(rel) && !rel.startsWith('..') && !isAbsolute(rel);
};

export function registerAgentSessionsIpc(
  ctx: IpcContext,
  manager: AgentSessionManager,
  options: {
    /** Models, skills, plugins, and file search; without it the composer offers none of them. */
    catalog?: CodexCatalog;
    /** Where pasted images are kept, one folder per session; without it images cannot be attached. */
    attachmentsDir?: () => string;
  } = {},
): { dispose: () => void } {
  const authorize = mainWindowSenderPolicy(() => ctx.liveWindow()?.webContents.id ?? null);
  const { catalog } = options;
  const sessionCwd = (sessionId: string) => {
    const cwd = manager.list().find((session) => session.id === sessionId)?.cwd;
    if (!cwd) throw new Error('会话不存在');
    return cwd;
  };
  const requireCatalog = () => {
    if (!catalog) throw new Error('当前环境无法读取 Codex 目录');
    return catalog;
  };
  const imagesDir = (sessionId: string) => {
    if (!options.attachmentsDir) throw new Error('当前环境不支持图片附件');
    return join(options.attachmentsDir(), sessionId);
  };
  // A message may only carry skills Codex listed for the session's folder and images saved for it here.
  const checkAttachments = async (sessionId: string, attachments: readonly AgentTurnAttachment[]) => {
    const cwd = sessionCwd(sessionId);
    for (const attachment of attachments) {
      if (attachment.type === 'skill') {
        if (!await requireCatalog().findSkill(cwd, attachment.path)) throw new Error(`技能 ${attachment.name} 不可用`);
      } else {
        const path = resolve(attachment.path);
        if (!isInside(imagesDir(sessionId), path) || !existsSync(path)) throw new Error('附件已失效，请重新添加');
      }
    }
  };

  // Images of sessions that no longer exist (closed, or dropped from history) are removed at start.
  if (options.attachmentsDir) {
    privateDirectory(options.attachmentsDir());
    try {
      const known = new Set(manager.list().map((session) => session.id));
      for (const entry of readdirSync(options.attachmentsDir(), { withFileTypes: true })) {
        if (entry.isDirectory() && !known.has(entry.name)) {
          rmSync(join(options.attachmentsDir(), entry.name), { recursive: true, force: true });
        }
      }
    } catch {
      // No attachments folder yet.
    }
  }
  const secure = <T extends z.ZodTypeAny>(
    channel: string, schema: T, handler: (input: z.infer<T>) => Promise<Record<string, unknown>> | Record<string, unknown>,
  ) => handleSecure(`agent-sessions:${channel}`, {
    schema, authorize, authorizeError: 'Sender is not the main window',
    handler: async (input) => {
      try {
        return { ok: true, ...(await handler(input)) };
      } catch (error) {
        return fail(error);
      }
    },
  });

  secure('list', z.object({}).strict(), () => ({ sessions: manager.list() }));
  secure('create', createSchema, async (input) => {
    const cwd = input.cwd ?? ctx.getCurrentCwd();
    if (!cwd || !isAbsolute(cwd) || !existsSync(cwd) || !statSync(cwd).isDirectory()) {
      throw new Error('请选择一个项目文件夹后再新建会话');
    }
    return { session: await manager.create({ cwd, policy: input.policy, model: input.model, effort: input.effort, network: input.network }) };
  });
  secure('send', sendSchema, async ({ id: sessionId, ...message }) => {
    if (message.attachments?.length) await checkAttachments(sessionId, message.attachments);
    const delivery = await manager.send(sessionId, message);
    if (delivery.status !== 'accepted') throw new Error(delivery.reason);
    return {};
  });
  secure('enqueue', sendSchema, async ({ id: sessionId, ...message }) => {
    if (message.attachments?.length) await checkAttachments(sessionId, message.attachments);
    const result = await manager.enqueue(sessionId, message);
    if (!result.ok) throw new Error(result.error);
    return { queued: result.queued };
  });
  secure('unqueue', unqueueSchema, (input) => {
    const message = manager.unqueue(input.id, input.messageId);
    if (!message) throw new Error('这条消息已经发出或已移除');
    return { message };
  });
  secure('resume-queue', targetSchema, (input) => {
    if (!manager.resumeQueue(input.id)) throw new Error('会话不存在');
    return {};
  });
  secure('models', z.object({}).strict(), async () => ({ models: await requireCatalog().listModels() }));
  secure('rate-limits', z.object({}).strict(), async () => ({ limits: await requireCatalog().readRateLimits() }));
  secure('skills', catalogSchema, async (input) => ({
    skills: await requireCatalog().listSkills(sessionCwd(input.id), input.reload),
  }));
  secure('set-skill-enabled', skillSwitchSchema, async (input) => {
    await requireCatalog().setSkillEnabled(sessionCwd(input.id), input.path, input.enabled);
    return {};
  });
  secure('plugins', catalogSchema, async (input) => ({ plugins: await requireCatalog().listPlugins(sessionCwd(input.id)) }));
  secure('search-files', searchSchema, async (input) => ({
    files: input.query.trim() ? await requireCatalog().searchFiles(sessionCwd(input.id), input.query.trim()) : [],
  }));
  secure('attach-image', imageSchema, (input) => {
    sessionCwd(input.id);
    const bytes = Buffer.from(input.data, 'base64');
    if (!bytes.length || bytes.length > MAX_IMAGE_BYTES) throw new Error('图片不能超过 10MB');
    const dir = imagesDir(input.id);
    privateDirectory(dir);
    const path = join(dir, `${randomUUID()}${IMAGE_EXTENSIONS[input.mime]}`);
    writePrivateFile(path, bytes);
    return { attachment: { type: 'localImage', path, name: basename(input.name) } };
  });
  // Any other file keeps its own name (inside a fresh folder), since the agent reads it by path.
  secure('attach-file', fileSchema, (input) => {
    sessionCwd(input.id);
    const bytes = Buffer.from(input.data, 'base64');
    if (!bytes.length) throw new Error('文件是空的或无法读取');
    if (bytes.length > MAX_FILE_BYTES) throw new Error('文件不能超过 20MB');
    const dir = join(imagesDir(input.id), randomUUID());
    privateDirectory(dir);
    const name = safeFileName(input.name);
    const path = join(dir, name);
    writePrivateFile(path, bytes);
    return { attachment: { type: 'file', path, name } };
  });
  secure('interrupt', targetSchema, async (input) => ({ interrupted: await manager.interrupt(input.id) }));
  secure('respond', respondSchema, (input) => {
    if (!manager.respond(input.id, input.requestId, input.allow, input.reason)) throw new Error('这个授权请求已经结束');
    return {};
  });
  secure('answer', answerSchema, (input) => {
    if (!manager.answer(input.id, input.requestId, input.answers)) throw new Error('这个提问已经结束，或还有问题没有回答');
    return {};
  });
  secure('close', targetSchema, async (input) => {
    await manager.close(input.id);
    if (options.attachmentsDir) rmSync(imagesDir(input.id), { recursive: true, force: true });
    return {};
  });
  secure('set-orchestration', orchestrationSchema, async (input) => {
    const result = await manager.setOrchestration(input.id, input.enabled);
    if (!result.ok) throw new Error(result.error);
    return {};
  });
  secure('rename', renameSchema, async (input) => {
    const result = await manager.rename(input.id, input.title);
    if (!result.ok) throw new Error(result.error);
    return { synced: result.synced };
  });
  secure('goal', goalSchema, async (input) => {
    const result = await manager.runGoal(input.id, input.action);
    if (!result.ok) throw new Error(result.error);
    return { goal: result.goal, message: result.message };
  });
  secure('upload-feedback', feedbackSchema, async (input) => {
    const result = await manager.uploadFeedback(
      input.id,
      input.reason,
      catalog ? (tid, reason) => catalog.uploadFeedback(tid, reason) : undefined,
    );
    if (!result.ok) throw new Error(result.error);
    return { feedbackId: result.feedbackId };
  });
  secure('history', historySchema, async (input) => requireCatalog().listThreads(input));
  secure('open-history', openHistorySchema, async (input) => {
    const thread = await requireCatalog().readThread(input.threadId);
    const cwd = typeof thread.cwd === 'string' ? thread.cwd : '';
    if (!cwd || !isAbsolute(cwd) || !existsSync(cwd)) throw new Error('这个会话的项目文件夹已不存在');
    const name = typeof thread.name === 'string' ? thread.name.trim() : '';
    const preview = typeof thread.preview === 'string' ? thread.preview : '';
    const title = name || preview.split(/\r?\n/u).map((line) => line.trim()).find(Boolean)?.slice(0, 60) || '';
    const model = typeof thread.model === 'string' && thread.model ? thread.model : null;
    const effort = typeof thread.reasoningEffort === 'string' && thread.reasoningEffort ? thread.reasoningEffort : null;
    return {
      session: manager.adopt({
        providerSessionId: input.threadId, cwd, title, policy: input.policy, model, effort,
        timeline: (sessionId) => codexThreadTimeline(sessionId, thread, { model, readOnly: input.policy === 'read-only' }),
      }),
    };
  });
  secure('archive-history', archiveSchema, async (input) => {
    await requireCatalog().setThreadArchived(input.threadId, input.archived);
    return {};
  });
  secure('timeline', targetSchema, (input) => ({ timeline: manager.getTimeline(input.id) }));

  const send = (channel: string, payload: unknown) => {
    const win = ctx.liveWindow();
    if (win && !win.isDestroyed() && !win.webContents.isDestroyed()) win.webContents.send(channel, payload);
  };
  // Same channel and shape as Worker timelines, so one renderer subscription serves both.
  const patches = new WorkerTimelinePatchCoalescer((patch) => send('orchestration:worker-timeline-patch', patch), 40);
  const offTimeline = manager.onTimelineChange((change) => {
    if (ctx.liveWindow()) patches.push(change);
  });
  const offSessions = manager.onSessionsChanged((sessions) => send('agent-sessions:changed', sessions));

  return {
    dispose: () => {
      offTimeline();
      offSessions();
      patches.dispose();
    },
  };
}
