import { dialog } from 'electron';
import { readFileSync, existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

export interface ClaudeEnv {
  ANTHROPIC_API_KEY?: string;
  ANTHROPIC_BASE_URL?: string;
  ANTHROPIC_AUTH_TOKEN?: string;
  [k: string]: string | undefined;
}

export function loadClaudeEnv(): ClaudeEnv {
  const path = join(homedir(), '.claude', 'settings.json');
  if (!existsSync(path)) return {};
  try {
    const raw = readFileSync(path, 'utf8');
    const parsed = JSON.parse(raw);
    const env = (parsed?.env ?? {}) as ClaudeEnv;
    return env;
  } catch (err) {
    console.error('[settings-loader] failed to parse Claude settings:', err);
    // dialog is imported statically at the top — the old lazy
    // require('electron') threw ReferenceError in the ESM main process, so
    // this warning silently never appeared.
    try {
      dialog.showMessageBoxSync({
        type: 'warning',
        title: '配置警告',
        message: 'Claude 配置文件解析失败',
        detail: `~/.claude/settings.json 格式有误，SDK 会话可能无法正常认证。\n\n错误: ${err instanceof Error ? err.message : String(err)}`,
      });
    } catch { /* dialog may not be available during early startup */ }
    return {};
  }
}

// Whitelist of env vars we forward from the Electron main process into the
// Claude SDK subprocess. The old code did `{ ...process.env }`, which leaked
// every credential the user happened to have exported in their shell rc
// (AWS_*, GITHUB_TOKEN, OpenAI keys, DATABASE_URL, …) into a process that
// talks to a remote LLM and can shell out arbitrary commands. Anything not
// listed here gets dropped; Anthropic credentials still come from
// the process environment or ~/.claude/settings.json.
const ENV_ALLOWLIST: ReadonlySet<string> = new Set([
  // Process essentials
  'PATH',
  'HOME',
  'SHELL',
  'USER',
  'USERNAME',
  'LOGNAME',
  // Windows equivalents
  'USERPROFILE',
  'APPDATA',
  'LOCALAPPDATA',
  'ComSpec',
  'SYSTEMROOT',
  'SystemRoot',
  // Temp dirs (cross-platform)
  'TMPDIR',
  'TEMP',
  'TMP',
  // Locale
  'LANG',
  'LC_ALL',
  'LC_CTYPE',
  'LC_MESSAGES',
  'LANGUAGE',
  // TTY
  'TERM',
  'COLORTERM',
  // TLS roots — corporate MITM proxies need NODE_EXTRA_CA_CERTS / SSL_CERT_FILE
  'SSL_CERT_FILE',
  'SSL_CERT_DIR',
  'NODE_EXTRA_CA_CERTS',
  // Outbound proxy (both casings — some tools only read one)
  'HTTP_PROXY',
  'HTTPS_PROXY',
  'NO_PROXY',
  'ALL_PROXY',
  'http_proxy',
  'https_proxy',
  'no_proxy',
  'all_proxy',
  // Node runtime knobs the SDK or its deps may honor
  'NODE_OPTIONS',
  'NODE_PATH',
  // NODE_TLS_REJECT_UNAUTHORIZED=0 disables TLS verification and enables
  // MITM on the API connection. Only forward it when the value is NOT '0'.
  // Corporate proxy users should rely on NODE_EXTRA_CA_CERTS instead.
  // (Handled specially in mergedSubprocessEnv below.)
]);

// Prefix allowlist — forward anything starting with these. ANTHROPIC_* covers
// API_KEY/BASE_URL/AUTH_TOKEN/MODEL/etc.; CLAUDE_* covers internal Claude
// Code envs like CLAUDE_PROJECT_DIR; XDG_* lets Claude Code find non-default
// config dirs.
const ENV_PREFIX_ALLOWLIST: readonly string[] = ['ANTHROPIC_', 'CLAUDE_', 'XDG_'];

function isEnvAllowed(name: string): boolean {
  if (ENV_ALLOWLIST.has(name) || name.toUpperCase() === 'PATH') return true;
  for (const pfx of ENV_PREFIX_ALLOWLIST) {
    if (name.startsWith(pfx)) return true;
  }
  return false;
}

function sanitizedAllowlistedEnv(isAllowed: (name: string) => boolean): NodeJS.ProcessEnv {
  const out: NodeJS.ProcessEnv = {};
  for (const [k, v] of Object.entries(process.env)) {
    if (typeof v !== 'string' || !isAllowed(k)) continue;
    // Strip dangerous NODE_OPTIONS flags (--inspect*, --require, -r) to
    // prevent debug port exposure and arbitrary code loading via subprocess.
    if (k === 'NODE_OPTIONS') {
      const cleaned = v
        .replace(/--inspect[^\s]*/g, '')
        .replace(/--require[^\s]*/g, '')
        .replace(/(?:^|\s)-r\s+[^\s]+/g, ' ')
        .trim();
      if (cleaned) out[k] = cleaned;
      continue;
    }
    out[k] = v;
  }
  return out;
}

function loadCodexProviderEnvKeys(): ReadonlySet<string> {
  const codexHome = process.env.CODEX_HOME?.trim() || join(homedir(), '.codex');
  const path = join(codexHome, 'config.toml');
  if (!existsSync(path)) return new Set();
  try {
    const keys = new Set<string>();
    const text = readFileSync(path, 'utf8');
    for (const match of text.matchAll(/^\s*env_key\s*=\s*["']([A-Za-z_][A-Za-z0-9_]*)["']/gm)) {
      keys.add(match[1]);
    }
    return keys;
  } catch {
    return new Set();
  }
}

const CODEX_ENV_PREFIX_ALLOWLIST: readonly string[] = ['OPENAI_', 'CODEX_', 'XDG_'];

function isCodexEnvAllowed(name: string, providerEnvKeys: ReadonlySet<string>): boolean {
  if (providerEnvKeys.has(name)) return true;
  if (ENV_ALLOWLIST.has(name) || name.toUpperCase() === 'PATH') return true;
  for (const pfx of CODEX_ENV_PREFIX_ALLOWLIST) {
    if (name.startsWith(pfx)) return true;
  }
  return false;
}

/** Environment for the Codex app-server: provider credentials yes, unrelated ambient secrets no. */
export function mergedCodexSubprocessEnv(): NodeJS.ProcessEnv {
  // Custom Codex providers name their credential variable explicitly in config.toml;
  // forward only those declared keys rather than reopening the whole process environment.
  return sanitizedAllowlistedEnv((name) => isCodexEnvAllowed(name, loadCodexProviderEnvKeys()));
}

export function mergedSubprocessEnv(): NodeJS.ProcessEnv {
  const claudeEnv = loadClaudeEnv();
  const out = sanitizedAllowlistedEnv(isEnvAllowed);
  // Overlay ~/.claude/settings.json env. User explicitly authored that file,
  // so we trust every key in it (and let it override allowlisted process env).
  for (const [k, v] of Object.entries(claudeEnv)) {
    if (typeof v === 'string' && v.length > 0) out[k] = v;
  }
  return out;
}
