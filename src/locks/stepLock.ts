import { Redis } from 'ioredis';
import { randomUUID, createHash } from 'crypto';
import { db, pool } from '../db/db.js';
import { sql } from 'drizzle-orm';

export type LockProvider = 'REDIS' | 'POSTGRES_ADVISORY';

export class StepLock {
  private redis: Redis;
  private key: string;
  private value: string;
  private ttlMs: number;
  private provider: LockProvider = 'REDIS';
  private advisoryLockKey1: number;
  private advisoryLockKey2: number;
  private pgClient: any = null;

  constructor(redis: Redis, workflowRunId: string, stepName: string, ttlMs = 30000) {
    this.redis = redis;
    this.key = `vaultflow:step-lock:${workflowRunId}:${stepName}`;
    this.value = randomUUID();
    this.ttlMs = ttlMs;

    const hash = createHash('sha256').update(this.key).digest();
    this.advisoryLockKey1 = hash.readInt32BE(0);
    this.advisoryLockKey2 = hash.readInt32BE(4);
  }

  getProvider(): LockProvider {
    return this.provider;
  }

  getTtlMs(): number {
    return this.ttlMs;
  }

  async acquire(): Promise<boolean> {
    try {
      if (this.redis) {
        const result = await this.redis.set(this.key, this.value, 'PX', this.ttlMs, 'NX');
        if (result === 'OK') {
          this.provider = 'REDIS';
          return true;
        }
        return false;
      }
    } catch {
      // Redis connection failed or unavailable; failover to PostgreSQL 64-bit advisory lock
    }

    try {
      if (!this.pgClient && pool && typeof pool.connect === 'function') {
        try {
          this.pgClient = await pool.connect();
        } catch {
          this.pgClient = null;
        }
      }

      const res: any = this.pgClient
        ? await this.pgClient.query('SELECT pg_try_advisory_lock($1, $2) as acquired', [
            this.advisoryLockKey1,
            this.advisoryLockKey2,
          ])
        : await db.execute(
            sql`SELECT pg_try_advisory_lock(${this.advisoryLockKey1}, ${this.advisoryLockKey2}) as acquired`
          );

      const rows = Array.isArray(res) ? res : (res?.rows || [res]);
      const firstRow = rows[0];
      const acquired = Boolean(
        firstRow &&
          (firstRow.acquired === true ||
            firstRow.acquired === 't' ||
            firstRow.acquired === 1 ||
            firstRow.pg_try_advisory_lock === true)
      );

      if (acquired) {
        this.provider = 'POSTGRES_ADVISORY';
        return true;
      }

      if (this.pgClient) {
        this.pgClient.release();
        this.pgClient = null;
      }
    } catch {
      if (this.pgClient) {
        try {
          this.pgClient.release();
        } catch {
          // ignore release error on disconnected client
        }
        this.pgClient = null;
      }
    }

    return false;
  }

  async release(): Promise<boolean> {
    if (this.provider === 'REDIS') {
      try {
        const luaScript = `
          if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
          else
            return 0
          end
        `;
        const result = await this.redis.eval(luaScript, 1, this.key, this.value);
        return result === 1;
      } catch {
        return false;
      }
    }

    if (this.provider === 'POSTGRES_ADVISORY') {
      try {
        const res: any = this.pgClient
          ? await this.pgClient.query('SELECT pg_advisory_unlock($1, $2) as released', [
              this.advisoryLockKey1,
              this.advisoryLockKey2,
            ])
          : await db.execute(
              sql`SELECT pg_advisory_unlock(${this.advisoryLockKey1}, ${this.advisoryLockKey2}) as released`
            );

        const rows = Array.isArray(res) ? res : (res?.rows || [res]);
        const firstRow = rows[0];
        return Boolean(
          firstRow &&
            (firstRow.released === true ||
              firstRow.released === 't' ||
              firstRow.released === 1 ||
              firstRow.pg_advisory_unlock === true)
        );
      } catch {
        return false;
      } finally {
        if (this.pgClient) {
          try {
            this.pgClient.release();
          } catch {
            // ignore release error on cleanup
          }
          this.pgClient = null;
        }
      }
    }

    return false;
  }

  async renew(extraTtlMs: number = this.ttlMs): Promise<boolean> {
    if (this.provider === 'REDIS') {
      try {
        const luaScript = `
          if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
          else
            return 0
          end
        `;
        const result = await this.redis.eval(luaScript, 1, this.key, this.value, extraTtlMs);
        return result === 1;
      } catch {
        return false;
      }
    }

    return true;
  }

  async extend(extraTtlMs: number = this.ttlMs): Promise<boolean> {
    return this.renew(extraTtlMs);
  }
}
