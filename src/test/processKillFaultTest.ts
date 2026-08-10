/**
 * processKillFaultTest.ts
 *
 * Multi-process SIGKILL fault injection test.
 *
 * This test exercises the exact failure mode that in-process promise-racing
 * cannot cover: a worker process is killed by the OS while holding a
 * distributed lock, after which a second worker must pick up the step and
 * complete it exactly once.
 *
 * Sequence
 * ────────
 * 1. Insert a real workflow_run row in Postgres.
 * 2. Spawn Worker A (lockWorker.js) as a separate OS process.
 * 3. Worker A acquires the Redis lock and signals its PID via stdout.
 * 4. Parent sends SIGKILL to Worker A — no cleanup runs, lock TTL is the
 *    only mechanism that will release it.
 * 5. Worker B (executeStep in this process) spin-waits for the lock,
 *    acquires it after TTL expiry, executes the step, and commits.
 * 6. Assertions against real Postgres rows verify exactly-once completion.
 *
 * Requirements
 * ────────────
 * docker-compose up -d must be running (Postgres + Redis).
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';
import { eq, and } from 'drizzle-orm';
import { db, pool } from '../db/db.js';
import { workflows, workflowRuns, stepExecutions, stepCheckpoints } from '../db/schema.js';
import { executeStep } from '../engine/executeStep.js';
import { WorkflowRunStatus } from '../types/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Short TTL so the test runs quickly; long enough to be stable under load.
const LOCK_TTL_MS = 5000;
// Worker B must wait longer than the TTL for the dead lock to expire.
const LOCK_WAIT_MS = LOCK_TTL_MS * 3;

const WORKER_SCRIPT = path.resolve(__dirname, 'worker', 'lockWorker.js');

/**
 * Returns a promise that resolves with the first line from the child's stdout
 * that contains `marker`, or rejects after `timeoutMs`.
 */
function waitForLine(
  proc: ReturnType<typeof spawn>,
  marker: string,
  timeoutMs = 10_000
): Promise<string> {
  return new Promise((resolve, reject) => {
    let buffer = '';

    const timer = setTimeout(() => {
      reject(new Error(`Timed out (${timeoutMs}ms) waiting for "${marker}" from child`));
    }, timeoutMs);

    proc.stdout!.on('data', (chunk: Buffer) => {
      buffer += chunk.toString();
      const match = buffer.split('\n').find((line) => line.includes(marker));
      if (match) {
        clearTimeout(timer);
        resolve(match.trim());
      }
    });

    proc.on('exit', (code) => {
      clearTimeout(timer);
      if (code !== null && code !== 0) {
        reject(new Error(`Child exited with code ${code} before emitting "${marker}"`));
      }
    });
  });
}

async function runFaultTest(): Promise<void> {
  const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379');

  try {
    console.log('--- StratStep Multi-Process SIGKILL Fault Injection Test ---\n');

    // ── 1. Create a real workflow run in Postgres ─────────────────────────
    const [wf] = await db
      .insert(workflows)
      .values({ name: `fault-test-${Date.now()}` })
      .returning();

    const [run] = await db
      .insert(workflowRuns)
      .values({ workflowId: wf.id, status: WorkflowRunStatus.RUNNING })
      .returning();

    const workflowRunId = run.id;
    const stepName = 'chargePaymentFaultStep';
    console.log(`Workflow run: ${workflowRunId}`);

    // ── 2. Spawn Worker A ────────────────────────────────────────────────
    console.log('Spawning Worker A...');
    const workerA = spawn(process.execPath, [WORKER_SCRIPT], {
      env: {
        ...process.env,
        WORKFLOW_RUN_ID: workflowRunId,
        STEP_NAME: stepName,
        LOCK_TTL_MS: String(LOCK_TTL_MS),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    // Surface worker errors so failures are diagnosable.
    workerA.stderr!.on('data', (d: Buffer) =>
      process.stderr.write(`[worker-a stderr] ${d}`)
    );

    // ── 3. Wait for Worker A to hold the lock ────────────────────────────
    const lockLine = await waitForLine(workerA, 'LOCK_ACQUIRED');
    const workerAPid = parseInt(lockLine.split(':')[1]!, 10);
    console.log(`Worker A (pid ${workerAPid}) holds the lock.`);

    // ── 4. SIGKILL Worker A ──────────────────────────────────────────────
    console.log('Sending SIGKILL to Worker A...');
    process.kill(workerAPid, 'SIGKILL');
    await new Promise<void>((resolve) => workerA.on('exit', () => resolve()));
    console.log(
      `Worker A dead. Redis lock will expire naturally in ≤${LOCK_TTL_MS}ms.\n`
    );

    // ── 5. Worker B acquires the lock and completes the step ─────────────
    console.log('Worker B: waiting for lock and executing step...');
    let workerBExecuted = false;

    const result = await executeStep({
      workflowRunId,
      stepName,
      inputPayload: { amount: 4900, currency: 'usd' },
      redis,
      lockTtlMs: LOCK_TTL_MS,
      maxLockWaitMs: LOCK_WAIT_MS,
      stepFn: async () => {
        workerBExecuted = true;
        console.log('  Worker B: stepFn executing...');
        // Simulate real work inside the step.
        await new Promise((r) => setTimeout(r, 50));
        return { status: 'charged', chargeId: 'ch_fault_test_b' };
      },
    });

    assert.equal(workerBExecuted, true, 'Worker B stepFn must run (not cached)');
    assert.equal(result.cached, false, 'Result must not be served from stale cache');
    assert.equal(result.outputPayload?.status, 'charged', 'Output payload correct');
    console.log('  Worker B: step committed successfully.\n');

    // ── 6. Assert DB state ───────────────────────────────────────────────
    const execRows = await db
      .select()
      .from(stepExecutions)
      .where(
        and(
          eq(stepExecutions.workflowRunId, workflowRunId),
          eq(stepExecutions.stepName, stepName)
        )
      );

    const completedRows = execRows.filter((r) => r.status === 'COMPLETED');
    assert.equal(
      completedRows.length,
      1,
      `Expected exactly 1 COMPLETED execution, got ${completedRows.length}`
    );

    const checkpointRows = await db
      .select()
      .from(stepCheckpoints)
      .where(
        and(
          eq(stepCheckpoints.workflowRunId, workflowRunId),
          eq(stepCheckpoints.stepName, stepName),
          eq(stepCheckpoints.isActive, true)
        )
      );

    assert.equal(
      checkpointRows.length,
      1,
      `Expected exactly 1 active checkpoint, got ${checkpointRows.length}`
    );

    console.log('✓  step_executions: exactly 1 COMPLETED row');
    console.log('✓  step_checkpoints: exactly 1 active row');
    console.log('\n--- SIGKILL fault injection test PASSED ---\n');
  } catch (err) {
    console.error('\n--- FAULT TEST FAILED ---');
    console.error(err);
    process.exitCode = 1;
  } finally {
    await redis.quit();
    await pool.end();
  }
}

runFaultTest();
