import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AtomicSnapshotWriter } from '../dist-electron/persistence/atomic-snapshot-writer.js';
import {
  codexMcpConfigArgs,
  writeMcpCredentialFile,
} from '../dist-electron/orchestration/codex-app-server-pilot.js';
import { mergedCodexSubprocessEnv } from '../dist-electron/settings-loader.js';

test('Codex app-server environment keeps provider variables but drops unrelated credentials', () => {
  const keys = ['OPENAI_API_KEY', 'CODEX_HOME', 'AWS_SECRET_ACCESS_KEY', 'DATABASE_URL', 'GITHUB_TOKEN'];
  const saved = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  try {
    process.env.OPENAI_API_KEY = 'provider-key';
    process.env.CODEX_HOME = '/tmp/codex-home';
    process.env.AWS_SECRET_ACCESS_KEY = 'aws-secret';
    process.env.DATABASE_URL = 'postgres://user:pass@host/db';
    process.env.GITHUB_TOKEN = 'github-secret';
    const env = mergedCodexSubprocessEnv();
    assert.equal(env.OPENAI_API_KEY, 'provider-key');
    assert.equal(env.CODEX_HOME, '/tmp/codex-home');
    assert.equal(env.AWS_SECRET_ACCESS_KEY, undefined);
    assert.equal(env.DATABASE_URL, undefined);
    assert.equal(env.GITHUB_TOKEN, undefined);
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});

test('MCP credentials are written privately and argv carries only the credential file path', () => {
  const dir = mkdtempSync(join(tmpdir(), 'aha-privacy-creds-'));
  try {
    const secret = 'worker-token-secret';
    const file = writeMcpCredentialFile({
      AHA_WORKER_ID: 'codex-worker-test',
      AHA_WORKER_TOKEN: secret,
      AHA_AGENT_HOOK_ENDPOINT_FILE: '/tmp/endpoint.json',
    }, dir);
    assert.deepEqual(JSON.parse(readFileSync(file, 'utf8')).AHA_WORKER_TOKEN, secret);
    const args = codexMcpConfigArgs('node', {
      serverPath: 'aha-mcp-server.cjs',
      env: { AHA_CREDENTIAL_FILE: file },
    }).join('\n');
    assert.match(args, /AHA_CREDENTIAL_FILE/);
    assert.doesNotMatch(args, new RegExp(secret));
    if (process.platform !== 'win32') {
      assert.equal(statSync(file).mode & 0o777, 0o600);
      assert.equal(statSync(dir).mode & 0o777, 0o700);
    }
  } finally {
    rmSync(dir, { force: true, recursive: true });
  }
});

test('private snapshot writes use 0600 files, 0700 directories, and migrate existing modes', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'aha-privacy-store-'));
  try {
    const file = join(dir, 'nested', 'state.json');
    const writer = new AtomicSnapshotWriter(() => file, {
      fileMode: 0o600,
      directoryMode: 0o700,
      backupSuffix: '.bak',
    });
    await writer.write(() => JSON.stringify({ ok: true }));
    await writer.waitForPendingWrite();
    if (process.platform !== 'win32') {
      assert.equal(statSync(file).mode & 0o777, 0o600);
      assert.equal(statSync(`${file}.bak`).mode & 0o777, 0o600);
      assert.equal(statSync(join(dir, 'nested')).mode & 0o777, 0o700);
    }
    assert.equal(readFileSync(file, 'utf8'), JSON.stringify({ ok: true }));
    assert.ok(readdirSync(join(dir, 'nested')).includes('state.json'));
  } finally {
    rmSync(dir, { force: true, recursive: true });
  }
});
