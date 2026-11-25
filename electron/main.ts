import { app, BrowserWindow, nativeTheme, protocol, screen, session } from 'electron';
import { existsSync, mkdirSync, readFileSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { extname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  AgentHookServer,
  formatAdditionalContextHookStdout,
  formatPostToolContextHookStdout,
  supportsPostToolContextInjection,
} from './agent/agent-hook-server.js';
import { AgentHookInstaller } from './agent/agent-hook-installer.js';
import {
  AGENT_MCP_CLI_IDS,
  AgentMcpInstaller,
  type AgentMcpCliId,
} from './agent/agent-mcp-installer.js';
import { AgentStatusStore } from './agent/agent-status-store.js';
import { coordinatorInstructions } from './orchestration/coordinator-prompt.js';
import { registerAgentIpc } from './ipc/agent.js';
import { registerAgentHooksIpc } from './ipc/agent-hooks.js';
import { registerAgentMcpIpc } from './ipc/agent-mcp.js';
import { registerAsrIpc } from './ipc/asr.js';
import type { IpcContext } from './ipc/context.js';
import { registerDesktopIpc } from './ipc/desktop.js';
import { registerDialogIpc } from './ipc/dialog.js';
import { registerGitIpc } from './ipc/git.js';
import { registerSettingsIpc } from './ipc/settings.js';
import { registerSettingsWindowIpc } from './ipc/settings-window.js';
import { registerShellPtyIpc } from './ipc/shell-pty.js';
import { isOrchestrationCliId, resolveMcpLaunchArgs } from './local-cli.js';
import { registerWorkspaceFsIpc } from './ipc/workspace-fs.js';
import { getPtyHost } from './pty-host.js';
import {
  buildRendererSecurityHeaders,
  resolveAppAssetPath,
  themeBackgroundColor,
} from './renderer-security.js';
import {
  flushSettingsWrites,
  getSettings,
  normalizeOrchestrationSettings,
} from './store.js';
import { getDefaultWorkspaceStateStore } from './persistence/workspace-state-store.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const APP_ROOT = resolve(__dirname, '..');
const DIST_DIR = join(APP_ROOT, 'dist');
const PRELOAD_PATH = join(__dirname, 'preload.cjs');
const DEV_SERVER_URL = process.env.VITE_DEV_SERVER_URL;
const DEV_ORIGIN = DEV_SERVER_URL ? new URL(DEV_SERVER_URL).origin : null;

protocol.registerSchemesAsPrivileged([
  {
    scheme: 'app',
    privileges: {
      standard: true,
      secure: true,
      supportFetchAPI: true,
      stream: true,
      corsEnabled: false,
    },
  },
]);

if (process.platform === 'linux') {
  app.disableHardwareAcceleration();
}

try {
  const marker = join(app.getPath('userData'), '.gpu-init-marker');
  if (existsSync(marker)) {
    app.disableHardwareAcceleration();
  }
  mkdirSync(app.getPath('userData'), { recursive: true });
  writeFileSync(marker, String(Date.now()), 'utf8');
  app.once('browser-window-created', () => {
    setTimeout(() => {
      try { unlinkSync(marker); } catch { /* ignore */ }
    }, 5000).unref();
  });
} catch { /* non-fatal */ }

let mainWindow: BrowserWindow | null = null;
let currentCwd: string | undefined;
let agentHookServer: AgentHookServer | null = null;
let disposeAgentIpc: (() => void) | null = null;
let disposeAgentHooks: (() => void) | null = null;
let disposeAgentMcp: (() => void) | null = null;
let disposeOrchestration: (() => void) | null = null;
let stopCodexWorkersOnQuit: (() => Promise<void> | null) | null = null;
let waitingForCodexShutdown = false;

const MIME_TYPES: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.mjs': 'application/javascript; charset=utf-8',
  '.cjs': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.wasm': 'application/wasm',
  '.onnx': 'application/octet-stream',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.txt': 'text/plain; charset=utf-8',
};

function isTrustedRendererUrl(rawUrl: string): boolean {
  try {
    const parsed = new URL(rawUrl);
    if (parsed.origin === 'app://bundle') return true;
    if (DEV_ORIGIN && parsed.origin === DEV_ORIGIN) return true;
  } catch { /* invalid URL -> untrusted */ }
  return false;
}

function registerAppProtocol(): void {
  const securityHeaders = buildRendererSecurityHeaders({
    isDev: Boolean(DEV_SERVER_URL),
    devServerUrl: DEV_SERVER_URL,
  });

  session.defaultSession.webRequest.onHeadersReceived((details, callback) => {
    if (!isTrustedRendererUrl(details.url)) {
      callback({ responseHeaders: details.responseHeaders });
      return;
    }
    callback({
      responseHeaders: {
        ...details.responseHeaders,
        ...securityHeaders,
      },
    });
  });

  protocol.handle('app', (request) => {
    const resolved = resolveAppAssetPath(DIST_DIR, request.url);
    if (!resolved) {
      return new Response('Forbidden', { status: 403 });
    }
    if (!existsSync(resolved) || !statSync(resolved).isFile()) {
      return new Response('Not found', { status: 404 });
    }
    const body = readFileSync(resolved);
    const mime = MIME_TYPES[extname(resolved).toLowerCase()] ?? 'application/octet-stream';
    const headers = new Headers({ 'content-type': mime });
    for (const [k, vals] of Object.entries(securityHeaders)) {
      if (vals.length > 0) headers.set(k, vals.join(', '));
    }
    return new Response(body, { status: 200, headers });
  });
}

function installSecurityGuards(win: BrowserWindow): void {
  win.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
  win.webContents.on('will-navigate', (event, targetUrl) => {
    if (isTrustedRendererUrl(targetUrl)) return;
    event.preventDefault();
  });
}

function createWindow(): void {
  const win = new BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 720,
    minHeight: 520,
    title: 'AhaStation Local CLI',
    backgroundColor: themeBackgroundColor(nativeTheme.shouldUseDarkColors),
    webPreferences: {
      preload: PRELOAD_PATH,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  installSecurityGuards(win);
  mainWindow = win;

  const onThemeUpdated = () => {
    if (!win.isDestroyed()) {
      win.setBackgroundColor(themeBackgroundColor(nativeTheme.shouldUseDarkColors));
    }
  };
  nativeTheme.on('updated', onThemeUpdated);

  win.on('closed', () => {
    nativeTheme.removeListener('updated', onThemeUpdated);
    if (mainWindow === win) mainWindow = null;
  });

  if (DEV_SERVER_URL) {
    void win.loadURL(DEV_SERVER_URL);
  } else {
    void win.loadURL('app://bundle/index.html');
  }
}

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.on('second-instance', () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.focus();
    } else if (app.isReady()) {
      createWindow();
    }
  });
}

app.whenReady().then(async () => {
  registerAppProtocol();

  session.defaultSession.setPermissionCheckHandler((_wc, permission, requestingOrigin) => {
    if (
      permission === 'media' ||
      permission === 'clipboard-read' ||
      permission === 'clipboard-sanitized-write'
    ) {
      return isTrustedRendererUrl(requestingOrigin);
    }
    return false;
  });

  session.defaultSession.setPermissionRequestHandler((wc, permission, callback) => {
    if (permission === 'media') {
      callback(isTrustedRendererUrl(wc.getURL()));
      return;
    }
    callback(false);
  });

  const agentStatusStore = new AgentStatusStore();
  agentHookServer = new AgentHookServer(agentStatusStore);
  try {
    agentHookServer.setPaneKeyFile(join(app.getPath('userData'), 'orchestration-pane.key'));
  } catch (error) {
    console.warn('[orchestration] could not retain pane identities across restarts:', error);
  }
  agentHookServer.setEndpointFile(join(app.getPath('userData'), 'agent-hook-endpoint.json'));
  let defaultMcpServerSourcePath: string | undefined;
  try {
    const {
      createOrchestrationBinShim,
      resolveDefaultOrchestrationMcpServerPath,
    } = await import('./orchestration/orchestration-bin-shim.js');
    defaultMcpServerSourcePath = resolveDefaultOrchestrationMcpServerPath();
    const binShim = createOrchestrationBinShim({
      binDir: join(app.getPath('userData'), 'orchestration-bin'),
    });
    agentHookServer.setOrchestrationBinShim(binShim);
  } catch (error) {
    console.warn('[orchestration] failed to initialize CLI bin shim:', error);
  }

  const mcpInstaller = new AgentMcpInstaller(
    defaultMcpServerSourcePath ? { serverSourcePath: defaultMcpServerSourcePath } : undefined,
  );
  let claudeMcpConfigPath: string | undefined;
  try {
    mcpInstaller.refreshServerIfInstalled();
    claudeMcpConfigPath = mcpInstaller.ensureClaudeMcpConfigFile();
  } catch (error) {
    console.warn('[agent-mcp] failed to prepare MCP launch config:', error);
  }
  const resolveActiveMcpServerPath = (): string | undefined => {
    if (existsSync(mcpInstaller.serverPath)) return mcpInstaller.serverPath;
    if (defaultMcpServerSourcePath && existsSync(defaultMcpServerSourcePath)) {
      return defaultMcpServerSourcePath;
    }
    return existsSync(mcpInstaller.sourcePath) ? mcpInstaller.sourcePath : undefined;
  };

  try {
    await agentHookServer.start();
  } catch (error) {
    console.warn('[agent-hooks] loopback server failed to start:', error);
  }

  const ipcCtx: IpcContext = {
    liveWindow: () => (mainWindow && !mainWindow.isDestroyed() ? mainWindow : null),
    getCurrentCwd: () => currentCwd,
    daemon: {
      dataDir: join(app.getPath('userData'), 'terminal-daemon'),
      runtimePath: process.execPath,
    },
    userDataDir: () => app.getPath('userData'),
    agentStatusStore,
    agentHookEnv: (ptyId) => agentHookServer?.envForPane(ptyId) ?? { AHA_TERMINAL_ID: ptyId },
    mcpLaunchArgsForCli: (cliId) => {
      const serverPath = resolveActiveMcpServerPath();
      if (!serverPath) return [];
      if (cliId === 'claude') {
        try {
          claudeMcpConfigPath = mcpInstaller.ensureClaudeMcpConfigFile();
        } catch {
          /* fall back to previously written config if present */
        }
        return claudeMcpConfigPath && existsSync(claudeMcpConfigPath)
          ? resolveMcpLaunchArgs(cliId, { claudeMcpConfigPath })
          : [];
      }
      if (cliId === 'codex') {
        return resolveMcpLaunchArgs(cliId, { mcpServerPath: serverPath });
      }
      return [];
    },
  };

  const shellPtyCtrl = registerShellPtyIpc(ipcCtx);
  disposeAgentIpc = registerAgentIpc(ipcCtx, {
    store: agentStatusStore,
    hookServer: agentHookServer,
    sendInput: shellPtyCtrl.sendInput,
  }).dispose;

  const { OrchestrationStore } = await import('./orchestration/orchestration-store.js');
  const { OrchestrationService } = await import('./orchestration/orchestration-service.js');
  const { ACTIVE_DISPATCH_STATUSES } = await import(
    './orchestration/orchestration-lifecycle-graph.js'
  );
  const { buildAgentRoster, selectAgentForTask } = await import(
    './orchestration/agent-roster.js'
  );
  const { dispatchOrchestrationHttpAction } = await import(
    './orchestration/orchestration-http-router.js'
  );
  const { dispatchCompanionHttpAction } = await import(
    './orchestration/companion-cli-router.js'
  );
  const { syncBundledOrchestrationSkills } = await import(
    './orchestration/orchestration-skill-sync.js'
  );
  const { registerOrchestrationIpc } = await import('./ipc/orchestration.js');

  try {
    syncBundledOrchestrationSkills();
  } catch (error) {
    console.warn('[orchestration] failed to sync bundled orchestration skills:', error);
  }

  const resolveFallbackCwd = (): string | undefined =>
    currentCwd ?? shellPtyCtrl.tracker.listTerminals()[0]?.cwd;

  const hookInstaller = new AgentHookInstaller();
  const mcpCliIdSet = new Set<string>(AGENT_MCP_CLI_IDS);
  const { coordinatorApprovedMcpTools, enabledCodexPilot, launchCodexPilot } =
    await import('./orchestration/codex-app-server-pilot.js');
  const orchestrationStore = OrchestrationStore.forUserDataDir(() => app.getPath('userData'));
  const orchestrationService = new OrchestrationService({
    store: orchestrationStore,
    agentStatusStore,
    writePty: shellPtyCtrl.sendInput,
    spawnWorkerPty: async ({ cliId, cwd, extraArgs }) => {
      if (!cliId || !isOrchestrationCliId(cliId)) {
        throw new Error(`Worker CLI must be an agent CLI: ${cliId ?? '(none)'}`);
      }
      // The dispatch prompt is typed as soon as this resolves; let the agent UI start first.
      return shellPtyCtrl.createPty({
        cliId,
        cwd,
        extraArgs,
        appearance: nativeTheme.shouldUseDarkColors ? 'dark' : 'light',
        waitForReady: { quietMs: 1_500, maxMs: 20_000 },
      });
    },
    codexPilot: {
      enabled: () => enabledCodexPilot(
        process.platform,
        normalizeOrchestrationSettings(getSettings().orchestration).codexAppServerPilot ?? false,
        process.env.AHA_CODEX_APP_SERVER_PILOT,
      ),
      launch: (workerId, cwd, events) => {
        const mcpServerPath = resolveActiveMcpServerPath();
        if (!agentHookServer || !mcpServerPath) throw new Error('Aha MCP Server is unavailable');
        return launchCodexPilot({ cwd, events, mcp: {
          serverPath: mcpServerPath, env: agentHookServer.envForWorker(workerId),
          credentialDir: join(app.getPath('userData'), 'mcp-credentials'),
        } });
      },
    },
    closeWorkerPty: (ptyId) => shellPtyCtrl.closePty(ptyId),
    isPtyAlive: shellPtyCtrl.isPtyAlive,
    isAgentCliId: isOrchestrationCliId,
    terminalTracker: shellPtyCtrl.tracker,
    getDefaultCwd: resolveFallbackCwd,
    getConfirmMode: () =>
      normalizeOrchestrationSettings(getSettings().orchestration).confirmBeforeDispatch,
    getDefaultWorktree: () =>
      normalizeOrchestrationSettings(getSettings().orchestration).defaultWorktree,
    getPeerMessagingMode: () =>
      normalizeOrchestrationSettings(getSettings().orchestration).peerMessagingMode,
    stallWatchdogIntervalMs: 10_000,
    stallThresholdMs: 25_000,
    isMcpEnabledForCli: (cliId) => {
      if (!mcpCliIdSet.has(cliId)) return false;
      try {
        const state = mcpInstaller.status(cliId as AgentMcpCliId).state;
        return state === 'launch-managed' || state === 'installed';
      } catch {
        return false;
      }
    },
    listCodingAgents: (opts) => {
      const settings = normalizeOrchestrationSettings(getSettings().orchestration);
      let hookStatuses: ReturnType<AgentHookInstaller['listStatus']> = [];
      try {
        hookStatuses = hookInstaller.listStatus();
      } catch {
        /* ignore unreadable hook config */
      }
      let mcpStatuses: ReturnType<AgentMcpInstaller['listStatus']> = [];
      try {
        mcpStatuses = mcpInstaller.listStatus();
      } catch {
        /* ignore unreadable MCP config */
      }

      const idlePanes: Array<{ ptyId: string; cliId: string; cwd: string | null }> = [];
      if (opts?.includeIdlePanes !== false) {
        const busyPanes = new Set<string>();
        for (const dispatch of orchestrationStore.listDispatches()) {
          if (!ACTIVE_DISPATCH_STATUSES.has(dispatch.status)) continue;
          if (dispatch.assigneePaneId) busyPanes.add(dispatch.assigneePaneId);
          if (dispatch.assigneeHandle) busyPanes.add(dispatch.assigneeHandle);
        }
        for (const pane of agentStatusStore.list()) {
          if (opts?.callerPaneId && pane.ptyId === opts.callerPaneId) continue;
          if (!shellPtyCtrl.isPtyAlive(pane.ptyId)) continue;
          if (!pane.agentId || !isOrchestrationCliId(pane.agentId)) continue;
          if (pane.state !== 'idle' && pane.state !== 'done') continue;
          if (busyPanes.has(pane.ptyId)) continue;
          const boundRun = orchestrationStore.getCurrentRunForConsumer(pane.ptyId);
          if (boundRun && boundRun.coordinatorPaneId === pane.ptyId) continue;
          idlePanes.push({
            ptyId: pane.ptyId,
            cliId: pane.agentId,
            cwd: shellPtyCtrl.tracker.getTerminal(pane.ptyId)?.cwd ?? null,
          });
        }
      }

      return buildAgentRoster({
        settings,
        hookStatuses,
        mcpStatuses,
        idlePanes,
      });
    },
    selectAgentForTask: (opts) => selectAgentForTask(opts),
    turnStartObserveTimeoutMs: 2_000,
    startConfirmationTimeoutMs: 60_000,
    heartbeatTimeoutMs: 300_000,
    staleWarningTimeoutMs: 900_000,
    staleSweepIntervalMs: 15_000,
  });
  ipcCtx.orchestrationService = orchestrationService;
  agentHookServer.setOrchestrationHandler((action, payload, context) =>
    dispatchOrchestrationHttpAction(orchestrationService, action, payload, context)
  );
  agentHookServer.setPermissionHookInterceptor((req) =>
    orchestrationService.handlePermissionHookRequest(req)
  );
  agentHookServer.setPostToolInterceptor((req) => {
    if (!supportsPostToolContextInjection(req.provider, req.eventName)) {
      return null;
    }
    const text = orchestrationService.consumeMidTurnBoardInjections(req.ptyId);
    if (!text) return null;
    const hookStdout = formatPostToolContextHookStdout(req.provider, req.eventName, text);
    return hookStdout ? { hookStdout } : null;
  });
  agentHookServer.setUserPromptInterceptor((req) => {
    orchestrationService.recordUserPromptTurn(req.ptyId, 'hook');
    if (!orchestrationService.isPaneOrchestrationMode(req.ptyId)) {
      return null;
    }
    const mcpCtx = orchestrationService.getMcpContextForPane(req.ptyId);
    if (mcpCtx.role === 'worker' && !mcpCtx.canDelegate) {
      return null;
    }
    const hookStdout = formatAdditionalContextHookStdout(
      req.provider,
      req.eventName,
      coordinatorInstructions(mcpCtx),
    );
    return hookStdout ? { hookStdout } : null;
  });
  agentHookServer.setCompanionHandler((namespace, action, payload, context) =>
    dispatchCompanionHttpAction(
      {
        terminalTracker: shellPtyCtrl.tracker,
        agentStatusStore,
        writePty: shellPtyCtrl.sendInput,
        createPty: (opts) => shellPtyCtrl.createPty({
          ...opts,
          appearance: nativeTheme.shouldUseDarkColors ? 'dark' : 'light',
        }),
        closePty: shellPtyCtrl.closePty,
        getDefaultCwd: resolveFallbackCwd,
        orchestrationService: () => orchestrationService,
        onWorktreeTerminalSpawned: (ev) => {
          ipcCtx.liveWindow()?.webContents.send('orchestration:pane-spawned', {
            ptyId: ev.ptyId,
            runId: '',
            taskId: ev.branch ?? 'worktree',
            cwd: ev.cwd,
            worktreePath: ev.cwd,
            branch: ev.branch,
            cliId: ev.cliId,
          });
        },
      },
      namespace,
      action,
      payload,
      context
    )
  );
  const orchestrationIpc = registerOrchestrationIpc(ipcCtx);
  const { AgentSessionManager } = await import('./orchestration/agent-session-manager.js');
  const { registerAgentSessionsIpc } = await import('./ipc/agent-sessions.js');
  const agentSessions = new AgentSessionManager({
    launch: (sessionId, cwd, events, { orchestration, policy }) => {
      if (!orchestration) return launchCodexPilot({ cwd, events });
      const mcpServerPath = resolveActiveMcpServerPath();
      if (!agentHookServer || !mcpServerPath) throw new Error('Aha MCP Server is unavailable');
      return launchCodexPilot({
        cwd, events,
        mcp: {
          serverPath: mcpServerPath, env: agentHookServer.envForSession(sessionId),
          credentialDir: join(app.getPath('userData'), 'mcp-credentials'),
          requiredTool: 'delegate_task', approvedTools: coordinatorApprovedMcpTools(policy),
        },
      });
    },
    coordinatorInstructions: (sessionId, { policy }) =>
      coordinatorInstructions({
        ...orchestrationService.getMcpContextForPane(sessionId),
        session: { policy },
      }),
    onCoordinatorEnded: (sessionId) => orchestrationService.endSessionCoordinator(sessionId),
    onUserTurn: (sessionId) => orchestrationService.recordUserPromptTurn(sessionId, 'session'),
    onCoordinatorIdle: (sessionId) => orchestrationService.sessionCoordinatorIdle(sessionId),
    storageFile: () => join(app.getPath('userData'), 'agent-sessions.json'),
  });
  orchestrationService.setSessionCoordinators({
    get: (id) => agentSessions.coordinatorState(id),
    wake: (id, prompt) => agentSessions.wake(id, prompt),
  });
  const { CodexCatalog } = await import('./orchestration/codex-catalog.js');
  const noSessionEvents = {
    onTurnStarted: () => {}, onTurnCompleted: () => {}, onExit: () => {},
    onRequest: async () => ({ decision: 'decline' as const }),
  };
  // A separate app-server without a conversation, so catalogs never touch a session's thread.
  const codexCatalog = new CodexCatalog(() => launchCodexPilot({ cwd: app.getPath('home'), events: noSessionEvents }));
  const agentSessionsIpc = registerAgentSessionsIpc(ipcCtx, agentSessions, {
    catalog: codexCatalog,
    attachmentsDir: () => join(app.getPath('userData'), 'agent-session-attachments'),
  });
  const offPtyExit = shellPtyCtrl.onPtyExit((ptyId) => {
    orchestrationService.markPtyExited(ptyId);
  });
  stopCodexWorkersOnQuit = () => {
    // Always disposed so the final state is saved; sessions resume on the next launch.
    const sessions = agentSessions.dispose();
    const workers = orchestrationService.hasCodexWorkers()
      ? orchestrationService.stopCodexWorkers().then(() => orchestrationService.waitForPendingWrites())
      : null;
    return Promise.all([sessions, workers, codexCatalog.dispose()]).then(() => undefined);
  };
  disposeOrchestration = () => {
    offPtyExit();
    agentSessionsIpc.dispose();
    orchestrationIpc.dispose();
    orchestrationService.dispose();
  };
  disposeAgentHooks = registerAgentHooksIpc().dispose;
  disposeAgentMcp = registerAgentMcpIpc(mcpInstaller).dispose;
  registerGitIpc(ipcCtx);
  registerWorkspaceFsIpc(ipcCtx);
  registerDialogIpc(ipcCtx);
  registerAsrIpc();
  registerSettingsIpc();
  registerSettingsWindowIpc();
  registerDesktopIpc();

  const broadcastDisplayChanged = () => {
    for (const win of BrowserWindow.getAllWindows()) {
      if (!win.isDestroyed()) {
        win.webContents.send('display:changed');
      }
    }
  };
  screen.on('display-metrics-changed', broadcastDisplayChanged);
  screen.on('display-added', broadcastDisplayChanged);
  screen.on('display-removed', broadcastDisplayChanged);

  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('before-quit', (event) => {
  if (waitingForCodexShutdown) {
    event.preventDefault();
    return;
  }
  const close = stopCodexWorkersOnQuit?.();
  stopCodexWorkersOnQuit = null;
  if (close) {
    event.preventDefault();
    waitingForCodexShutdown = true;
    void close.catch((error) => {
      console.warn('[orchestration] Codex shutdown did not complete:', error);
    }).finally(() => {
      waitingForCodexShutdown = false;
      app.quit();
    });
    return;
  }
  try {
    disposeOrchestration?.();
    disposeOrchestration = null;
  } catch { /* ignore */ }
  try {
    disposeAgentHooks?.();
    disposeAgentHooks = null;
  } catch { /* ignore */ }
  try {
    disposeAgentMcp?.();
    disposeAgentMcp = null;
  } catch { /* ignore */ }
  try {
    disposeAgentIpc?.();
    disposeAgentIpc = null;
  } catch { /* ignore */ }
  try {
    void agentHookServer?.stop();
    agentHookServer = null;
  } catch { /* ignore */ }
  try {
    getPtyHost().killAll();
  } catch { /* ignore */ }
  try {
    getDefaultWorkspaceStateStore(() => app.getPath('userData')).flushSync();
  } catch (error) {
    console.error('[workspace-state] flush on quit failed:', error);
  }
  flushSettingsWrites();
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});
