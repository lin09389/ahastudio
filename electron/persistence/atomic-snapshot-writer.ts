// Ported/adapted from Orca (https://github.com/stablyai/orca).
// Copyright (c) 2026 Lovecast Inc. — MIT License. See THIRD_PARTY_NOTICES.md.

import { chmodSync, existsSync, mkdirSync, unlinkSync, writeFileSync } from 'node:fs'
import { chmod, mkdir, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'
import {
  durableWriteTempPath,
  removeStaleDurableWriteTempFiles,
  writeFileDurableIfCurrent,
  writeFileDurableSync
} from './durable-file-write.js'

const STALE_WRITE_TEMP_AGE_MS = 24 * 60 * 60 * 1000

export type AtomicSnapshotWriterOptions = {
  fileMode?: number
  /** Reapplied best-effort on POSIX; Windows permissions use ACLs. */
  directoryMode?: number
  skipUnchanged?: boolean
  /** When set, every committed snapshot is also written to `<file><suffix>` for crash recovery. */
  backupSuffix?: string
}

/** Coalesce async temp/rename writes; quit paths can synchronously supersede pending generations. */
export class AtomicSnapshotWriter {
  private writeGeneration = 0
  private lastCommittedGeneration = 0
  private lastCommittedContent: string | null = null
  private writeRequested = false
  private closed = false
  private pendingWrite: Promise<void> | null = null
  private pendingSerialize: (() => string) | null = null
  private readonly staleTempCleanup: Promise<void>
  private inFlightAsyncTmpFile: string | null = null

  constructor(
    private readonly resolveFile: () => string,
    private readonly options: AtomicSnapshotWriterOptions = {}
  ) {
    this.staleTempCleanup = removeStaleDurableWriteTempFiles(resolveFile(), {
      minimumAgeMs: STALE_WRITE_TEMP_AGE_MS
    })
    this.applyPrivatePermissionsToExistingFiles(resolveFile())
  }

  private applyPrivatePermissionsToExistingFiles(file: string): void {
    if (this.options.fileMode === undefined || process.platform === 'win32') return
    const paths = [file, ...(this.options.backupSuffix ? [`${file}${this.options.backupSuffix}`] : [])]
    for (const path of paths) {
      if (!existsSync(path)) continue
      try {
        chmodSync(path, this.options.fileMode)
      } catch {}
    }
  }

  write(serialize: () => string): Promise<void> {
    if (this.closed) {
      return Promise.resolve()
    }
    this.writeRequested = true
    this.pendingSerialize = serialize
    // drainWrites awaits before clearing pendingWrite, so this assignment wins.
    this.pendingWrite ??= this.drainWrites()
    return this.pendingWrite
  }

  writeSync(serialize: () => string): void {
    const file = this.resolveFile()
    this.prepareDirectorySync(dirname(file))
    if (this.inFlightAsyncTmpFile) {
      try {
        unlinkSync(this.inFlightAsyncTmpFile)
        this.inFlightAsyncTmpFile = null
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
          void this.write(serialize).catch((writeError) => {
            console.error('[atomic-snapshot-writer] Failed to write snapshot:', writeError)
          })
          throw error
        }
      }
    }
    const { tmpFile, json, generation } = this.preparePayload(file, serialize)
    if (this.isUnchanged(json)) {
      // Fence older async renames even when the newest bytes are already on disk.
      this.lastCommittedGeneration = Math.max(this.lastCommittedGeneration, generation)
      return
    }
    writeFileDurableSync(tmpFile, file, json, this.options.fileMode)
    this.lastCommittedGeneration = Math.max(this.lastCommittedGeneration, generation)
    this.lastCommittedContent = json
    this.writeBackupSync(file, json)
  }

  private writeBackupSync(file: string, json: string): void {
    if (!this.options.backupSuffix) {
      return
    }
    try {
      writeFileSync(`${file}${this.options.backupSuffix}`, json, {
        encoding: 'utf-8',
        mode: this.options.fileMode
      })
    } catch (error) {
      console.error('[atomic-snapshot-writer] Failed to write snapshot backup:', error)
    }
  }

  private async writeBackup(file: string, json: string): Promise<void> {
    if (!this.options.backupSuffix) {
      return
    }
    try {
      await writeFile(`${file}${this.options.backupSuffix}`, json, {
        encoding: 'utf-8',
        mode: this.options.fileMode
      })
    } catch (error) {
      console.error('[atomic-snapshot-writer] Failed to write snapshot backup:', error)
    }
  }

  waitForPendingWrite(): Promise<void> {
    return this.pendingWrite ?? Promise.resolve()
  }

  primeCommittedContent(content: string | null): void {
    this.lastCommittedContent = content
  }

  /** Prevent async publication and drop queued work; synchronous quit writes remain allowed. */
  close(): void {
    this.closed = true
    this.writeRequested = false
    this.pendingSerialize = null
  }

  private isUnchanged(json: string): boolean {
    return this.options.skipUnchanged === true && json === this.lastCommittedContent
  }

  private preparePayload(
    finalPath: string,
    serialize: () => string
  ): {
    tmpFile: string
    json: string
    generation: number
  } {
    const generation = ++this.writeGeneration
    return {
      tmpFile: durableWriteTempPath(finalPath),
      json: serialize(),
      generation
    }
  }

  private prepareDirectorySync(dir: string): void {
    mkdirSync(dir, { recursive: true, mode: this.options.directoryMode })
    if (this.options.directoryMode !== undefined && process.platform !== 'win32') {
      try {
        chmodSync(dir, this.options.directoryMode)
      } catch {}
    }
  }

  private async prepareDirectory(dir: string): Promise<void> {
    await mkdir(dir, {
      recursive: true,
      mode: this.options.directoryMode
    }).catch(() => {})
    if (this.options.directoryMode !== undefined && process.platform !== 'win32') {
      await chmod(dir, this.options.directoryMode).catch(() => {})
    }
  }

  private async drainWrites(): Promise<void> {
    try {
      await this.staleTempCleanup
      let firstError: unknown = null
      while (this.writeRequested && !this.closed) {
        this.writeRequested = false
        const serialize = this.pendingSerialize!
        try {
          await this.writeToDiskAsync(serialize)
        } catch (error) {
          firstError ??= error
          if (!this.writeRequested) {
            throw error
          }
        }
      }
      if (firstError) {
        throw firstError
      }
    } finally {
      // Clear synchronously with the empty-queue observation so later callers start a fresh drain.
      this.pendingWrite = null
    }
  }

  private async writeToDiskAsync(serialize: () => string): Promise<void> {
    const file = this.resolveFile()
    await this.prepareDirectory(dirname(file))
    if (this.closed) {
      return
    }
    const { tmpFile, json, generation } = this.preparePayload(file, serialize)
    if (this.isUnchanged(json)) {
      return
    }
    this.inFlightAsyncTmpFile = tmpFile
    try {
      let published = false
      try {
        published = await writeFileDurableIfCurrent(
          tmpFile,
          file,
          json,
          () => !this.closed && this.lastCommittedGeneration < generation,
          this.options.fileMode
        )
      } catch (err) {
        if (
          (err as NodeJS.ErrnoException).code !== 'ENOENT' ||
          this.lastCommittedGeneration < generation
        ) {
          throw err
        }
      }
      if (published) {
        this.lastCommittedGeneration = Math.max(this.lastCommittedGeneration, generation)
        this.lastCommittedContent = json
        await this.writeBackup(file, json)
      }
    } finally {
      if (this.inFlightAsyncTmpFile === tmpFile) {
        this.inFlightAsyncTmpFile = null
      }
    }
  }
}
