/**
 * lockWorker.ts — subprocess for the SIGKILL fault injection test.
 *
 * Protocol:
 *   1. Reads WORKFLOW_RUN_ID, STEP_NAME, LOCK_TTL_MS from env.
 *   2. Acquires the distributed Redis lock for that step.
 *   3. Writes "LOCK_ACQUIRED:{pid}" to stdout so the parent knows it
 *      can SIGKILL this process.
 *   4. Sleeps indefinitely — the parent is expected to kill this process
 *      while the lock is held, simulating a real worker crash.
 *
 * This file is only ever executed as a child_process via processKillFaultTest.
 */
import { Redis } from 'ioredis';
import { StepLock } from '../../locks/stepLock.js';

const workflowRunId = process.env.WORKFLOW_RUN_ID;
const stepName = process.env.STEP_NAME;
const lockTtlMs = parseInt(process.env.LOCK_TTL_MS ?? '5000', 10);

if (!workflowRunId || !stepName) {
  process.stderr.write('lockWorker: WORKFLOW_RUN_ID and STEP_NAME are required\n');
  process.exit(2);
}

const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379', {
  // Disable auto-reconnect so a dead worker doesn't silently re-subscribe.
  maxRetriesPerRequest: 0,
  lazyConnect: false,
});

async function run(): Promise<void> {
  const lock = new StepLock(redis, workflowRunId!, stepName!, lockTtlMs);
  const acquired = await lock.acquire();

  if (!acquired) {
    process.stderr.write(`lockWorker: failed to acquire lock for ${stepName}\n`);
    process.exit(1);
  }

  // Signal the parent: we hold the lock — safe to SIGKILL us now.
  process.stdout.write(`LOCK_ACQUIRED:${process.pid}\n`);

  // Hold the lock forever. The parent will kill this process via SIGKILL.
  // SIGKILL cannot be trapped, so no cleanup runs — that's the whole point.
  await new Promise<never>(() => {});
}

run().catch((err: Error) => {
  process.stderr.write(`lockWorker error: ${err.message}\n`);
  process.exit(1);
});
