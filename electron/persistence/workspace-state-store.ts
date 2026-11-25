import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { AtomicSnapshotWriter } from './atomic-snapshot-writer.js';

export type WorkspaceSnapshotData = Record<string, unknown>;

export class WorkspaceStateStore {
  private readonly writer: AtomicSnapshotWriter;
  private latestState: WorkspaceSnapshotData | null = null;
  private dirty = false;
  private writeGeneration = 0;

  constructor(private readonly resolvePath: () => string) {
    this.writer = new AtomicSnapshotWriter(resolvePath, { skipUnchanged: true, fileMode: 0o600, directoryMode: 0o700 });
  }

  read(): WorkspaceSnapshotData | null {
    if (this.latestState !== null) return this.latestState;
    try {
      const raw = readFileSync(this.resolvePath(), 'utf8');
      this.writer.primeCommittedContent(raw);
      const parsed = JSON.parse(raw) as unknown;
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        this.latestState = parsed as WorkspaceSnapshotData;
        return this.latestState;
      }
      return null;
    } catch {
      return null;
    }
  }

  async write(state: WorkspaceSnapshotData): Promise<{ ok: true }> {
    this.latestState = state;
    this.dirty = true;
    const generation = ++this.writeGeneration;
    const serialized = JSON.stringify(state);
    await this.writer.write(() => serialized);
    // An older write finishing must not hide a newer state that has not reached disk yet.
    if (generation === this.writeGeneration) this.dirty = false;
    return { ok: true };
  }

  flushSync(): void {
    this.writer.close();
    if (!this.dirty || this.latestState === null) return;
    const serialized = JSON.stringify(this.latestState);
    try {
      this.writer.writeSync(() => serialized);
      this.dirty = false;
    } catch (error) {
      // Quit must not be blocked by a failed save, but the loss should be diagnosable.
      console.error('[workspace-state] synchronous flush failed:', error);
    }
  }

  waitForPendingWrite(): Promise<void> {
    return this.writer.waitForPendingWrite();
  }
}

let defaultStore: WorkspaceStateStore | null = null;

export function getDefaultWorkspaceStateStore(getUserDataDir: () => string): WorkspaceStateStore {
  if (!defaultStore) {
    defaultStore = new WorkspaceStateStore(() => join(getUserDataDir(), 'workspace-state.json'));
  }
  return defaultStore;
}
