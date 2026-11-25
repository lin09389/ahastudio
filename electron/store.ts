import { app, safeStorage } from 'electron';
import {
  chmodSync,
  constants,
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import { migrateSettings, type LegacySettings } from './settings-migration.js';

export interface VoicePrint {
  embedding: number[];
  model: string;
  secondsCaptured: number;
  enrolledAt: number;
}

export interface XfyunAsrCredentials {
  appId?: string;
  apiKey?: string;
  apiSecret?: string;
}

export const ORCHESTRATION_CONFIRM_MODES = ['chat-and-guard', 'always-ui', 'auto'] as const;
export type OrchestrationConfirmMode = (typeof ORCHESTRATION_CONFIRM_MODES)[number];

export const ORCHESTRATION_PEER_MESSAGING_MODES = ['controlled-p2p', 'hub-only'] as const;
export type OrchestrationPeerMessagingMode = (typeof ORCHESTRATION_PEER_MESSAGING_MODES)[number];

export const ORCHESTRATION_TASK_KINDS = [
  'architecture',
  'implementation',
  'debug',
  'tests',
  'review',
  'research',
  'docs',
] as const;
export type OrchestrationTaskKind = (typeof ORCHESTRATION_TASK_KINDS)[number];

export interface OrchestrationAgentProfile {
  enabled?: boolean;
  specialty?: string;
  defaultModel?: string;
  defaultEffort?: string;
}

export interface OrchestrationSettings {
  confirmBeforeDispatch: OrchestrationConfirmMode;
  defaultWorktree: boolean;
  peerMessagingMode: OrchestrationPeerMessagingMode;
  codexAppServerPilot?: boolean;
  agentProfiles: Record<string, OrchestrationAgentProfile>;
}

export const DEFAULT_ORCHESTRATION_SETTINGS: OrchestrationSettings = {
  confirmBeforeDispatch: 'chat-and-guard',
  defaultWorktree: true,
  peerMessagingMode: 'controlled-p2p',
  agentProfiles: {},
};

const FORBIDDEN_RECORD_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

export function normalizeOrchestrationSettings(
  raw?: Partial<OrchestrationSettings> | null,
): OrchestrationSettings {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return {
      confirmBeforeDispatch: DEFAULT_ORCHESTRATION_SETTINGS.confirmBeforeDispatch,
      defaultWorktree: DEFAULT_ORCHESTRATION_SETTINGS.defaultWorktree,
      peerMessagingMode: DEFAULT_ORCHESTRATION_SETTINGS.peerMessagingMode,
      agentProfiles: {},
    };
  }

  const confirmBeforeDispatch: OrchestrationConfirmMode =
    raw.confirmBeforeDispatch === 'chat-and-guard' ||
    raw.confirmBeforeDispatch === 'always-ui' ||
    raw.confirmBeforeDispatch === 'auto'
      ? raw.confirmBeforeDispatch
      : DEFAULT_ORCHESTRATION_SETTINGS.confirmBeforeDispatch;

  const defaultWorktree =
    typeof raw.defaultWorktree === 'boolean'
      ? raw.defaultWorktree
      : DEFAULT_ORCHESTRATION_SETTINGS.defaultWorktree;

  const peerMessagingMode: OrchestrationPeerMessagingMode =
    raw.peerMessagingMode === 'controlled-p2p' || raw.peerMessagingMode === 'hub-only'
      ? raw.peerMessagingMode
      : DEFAULT_ORCHESTRATION_SETTINGS.peerMessagingMode;

  const agentProfiles: Record<string, OrchestrationAgentProfile> = {};
  if (raw.agentProfiles && typeof raw.agentProfiles === 'object' && !Array.isArray(raw.agentProfiles)) {
    for (const [rawKey, rawProfile] of Object.entries(raw.agentProfiles)) {
      const cliId = rawKey.trim();
      if (!cliId || FORBIDDEN_RECORD_KEYS.has(cliId)) continue;
      if (!rawProfile || typeof rawProfile !== 'object' || Array.isArray(rawProfile)) continue;
      const cleaned: OrchestrationAgentProfile = {};
      if (typeof rawProfile.enabled === 'boolean') cleaned.enabled = rawProfile.enabled;
      if (typeof rawProfile.specialty === 'string') {
        const sanitizedSpecialty = rawProfile.specialty.replace(/[\r\n]+/g, ' ').trim().slice(0, 300);
        if (sanitizedSpecialty) cleaned.specialty = sanitizedSpecialty;
      }
      if (typeof rawProfile.defaultModel === 'string') {
        const trimmedModel = rawProfile.defaultModel.trim();
        if (
          trimmedModel &&
          trimmedModel.length <= 128 &&
          !trimmedModel.startsWith('-') &&
          !/[\s\r\n\0]/.test(trimmedModel)
        ) {
          cleaned.defaultModel = trimmedModel;
        }
      }
      if (typeof rawProfile.defaultEffort === 'string') {
        const trimmedEffort = rawProfile.defaultEffort.trim();
        if (trimmedEffort && trimmedEffort.length <= 32 && !/[\s\r\n\0]/.test(trimmedEffort)) {
          cleaned.defaultEffort = trimmedEffort;
        }
      }
      if (Object.keys(cleaned).length > 0) {
        agentProfiles[cliId] = cleaned;
      }
    }
  }

  return {
    confirmBeforeDispatch,
    defaultWorktree,
    peerMessagingMode,
    ...(typeof raw.codexAppServerPilot === 'boolean' ? { codexAppServerPilot: raw.codexAppServerPilot } : {}),
    agentProfiles,
  };
}

export interface Settings {
  schemaVersion?: 2;
  voiceLockEnabled?: boolean;
  voicePrint?: VoicePrint;
  voicePolishEnabled?: boolean;
  voicePolishApiKey?: string;
  voicePolishApiKeyEncrypted?: string;
  voicePolishBaseUrl?: string;
  voicePolishModel?: string;
  handheldMode?: 'auto' | 'desktop' | 'handheld';
  xfyunAsr?: XfyunAsrCredentials;
  xfyunAsrEncrypted?: string;
  orchestration?: OrchestrationSettings;
}

let cachedSettings: Settings | null = null;
let writeTimer: ReturnType<typeof setTimeout> | null = null;

function settingsFilePath(): string {
  return join(app.getPath('userData'), 'settings.json');
}

function decryptString(b64: string): string {
  if (!safeStorage.isEncryptionAvailable()) throw new Error('Settings encryption is unavailable');
  try {
    return safeStorage.decryptString(Buffer.from(b64, 'base64'));
  } catch {
    throw new Error('Failed to decrypt settings');
  }
}

function encryptString(plain: string): string {
  if (!safeStorage.isEncryptionAvailable()) throw new Error('Settings encryption is unavailable');
  try {
    return safeStorage.encryptString(plain).toString('base64');
  } catch {
    throw new Error('Failed to encrypt settings');
  }
}

function decodeLoadedSettings(raw: Settings): Settings {
  const out: Settings = { ...raw };
  if (raw.voicePolishApiKeyEncrypted) {
    out.voicePolishApiKey = decryptString(raw.voicePolishApiKeyEncrypted);
  }
  if (raw.xfyunAsrEncrypted) {
    out.xfyunAsr = JSON.parse(decryptString(raw.xfyunAsrEncrypted)) as XfyunAsrCredentials;
  }
  return out;
}

function encodeSettingsForDisk(settings: Settings): Settings {
  const disk: Settings = { ...settings };
  if (disk.voicePolishApiKey) {
    disk.voicePolishApiKeyEncrypted = encryptString(disk.voicePolishApiKey);
    delete disk.voicePolishApiKey;
  } else if (!disk.voicePolishApiKeyEncrypted) {
    delete disk.voicePolishApiKey;
  }
  const xfyun = disk.xfyunAsr;
  if (xfyun && (xfyun.appId || xfyun.apiKey || xfyun.apiSecret)) {
    disk.xfyunAsrEncrypted = encryptString(JSON.stringify(xfyun));
    delete disk.xfyunAsr;
  } else if (!disk.xfyunAsrEncrypted) {
    delete disk.xfyunAsr;
  }
  return disk;
}

function writeSettingsNow(settings: Settings): void {
  const file = settingsFilePath();
  mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
  if (process.platform !== 'win32') chmodSync(dirname(file), 0o700);
  const tmp = `${file}.${process.pid}.tmp`;
  writeFileSync(tmp, JSON.stringify(encodeSettingsForDisk(settings), null, 2), { encoding: 'utf8', mode: 0o600 });
  if (process.platform !== 'win32') chmodSync(tmp, 0o600);
  renameSync(tmp, file);
}

function backupV1Settings(): void {
  const file = settingsFilePath();
  const backup = `${file}.v1.bak`;
  if (!existsSync(file) || existsSync(backup)) return;
  copyFileSync(file, backup, constants.COPYFILE_EXCL);
  if (process.platform !== 'win32') chmodSync(backup, 0o600);
}

function scheduleSettingsWrite(): void {
  if (writeTimer) clearTimeout(writeTimer);
  writeTimer = setTimeout(() => {
    writeTimer = null;
    if (cachedSettings) writeSettingsNow(cachedSettings);
  }, 50);
  writeTimer.unref();
}

export function getSettings(): Settings {
  if (cachedSettings) return cachedSettings;
  const file = settingsFilePath();
  if (process.platform !== 'win32') {
    try {
      mkdirSync(dirname(file), { recursive: true, mode: 0o700 });
      chmodSync(dirname(file), 0o700);
      if (existsSync(file)) chmodSync(file, 0o600);
    } catch {}
  }
  if (!existsSync(file)) {
    cachedSettings = { schemaVersion: 2 };
    return cachedSettings;
  }

  const raw = JSON.parse(readFileSync(file, 'utf8')) as LegacySettings;
  const migration = migrateSettings(raw ?? {});
  if (migration.settings.orchestration !== undefined) {
    migration.settings.orchestration = normalizeOrchestrationSettings(migration.settings.orchestration);
  }
  if (migration.changed) {
    backupV1Settings();
    writeSettingsNow(migration.settings);
  }
  cachedSettings = decodeLoadedSettings(migration.settings);
  return cachedSettings;
}

export function updateSettings(patch: Partial<Settings>): Settings {
  const next: Settings = { ...getSettings(), ...patch, schemaVersion: 2 };
  for (const key of Object.keys(patch) as (keyof Settings)[]) {
    if (patch[key] === undefined) delete next[key];
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'voicePolishApiKey')) {
    delete next.voicePolishApiKeyEncrypted;
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'xfyunAsr')) {
    delete next.xfyunAsrEncrypted;
  }
  encodeSettingsForDisk(next);
  cachedSettings = next;
  scheduleSettingsWrite();
  return cachedSettings;
}

export function clearVoicePrint(): Settings {
  return updateSettings({ voicePrint: undefined, voiceLockEnabled: false });
}

export function flushSettingsWrites(): void {
  if (writeTimer) {
    clearTimeout(writeTimer);
    writeTimer = null;
  }
  if (cachedSettings) writeSettingsNow(cachedSettings);
}
