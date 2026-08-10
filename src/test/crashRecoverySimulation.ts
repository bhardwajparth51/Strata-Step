import { Redis } from 'ioredis';
import { db, pool } from '../db/db.js';
import { workflows, workflowRuns } from '../db/schema.js';
import { recoverStaleWorkflowRuns } from '../engine/recovery.js';
import { WorkflowRunRepository } from '../db/repositories/workflowRunRepository.js';
import { WorkflowRunStatus } from '../types/index.js';
import { sql } from 'drizzle-orm';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

async function runCrashRecoverySimulation() {
  console.log('running worker crash recovery simulation...\n');

  try {
    const [wf] = await db
      .insert(workflows)
      .values({ name: 'Crash Recovery Workflow' })
      .onConflictDoNothing()
      .returning();

    const workflowId = wf ? wf.id : (await db.query.workflows.findFirst())!.id;

    // Simulate a workflow run started by Worker A that crashed 60s ago
    const sixtySecondsAgo = new Date(Date.now() - 60000);
    const [crashedRun] = await db
      .insert(workflowRuns)
      .values({
        workflowId,
        status: WorkflowRunStatus.RUNNING,
        currentStep: 'generateAiVideo',
        startedAt: sixtySecondsAgo,
        heartbeatAt: sixtySecondsAgo,
      })
      .returning();

    console.log(`created simulated crashed workflow run id: ${crashedRun.id}\n`);

    // Run Recovery Worker (stale timeout = 5000ms)
    console.log('recovery worker scanning for crashed runs');
    const recoveryResult = await recoverStaleWorkflowRuns(5000);
    console.log('recovery scan result:', recoveryResult, '\n');

    // Assert crashed run transitioned to FAILED
    const updatedRun = await WorkflowRunRepository.findById(crashedRun.id);
    if (!updatedRun) throw new Error('Workflow run missing');

    console.log(`updated workflow status: ${updatedRun.status}`);
    console.log(`error summary: ${updatedRun.errorSummary}`);

    if (updatedRun.status !== WorkflowRunStatus.FAILED) {
      throw new Error(`Crash Recovery Failure: Expected FAILED status, got ${updatedRun.status}`);
    }

    if (!updatedRun.errorSummary?.includes('Worker Crash Detected')) {
      throw new Error('Crash Recovery Failure: Missing Worker Crash error summary');
    }

    console.log('\ncrash recovery simulation passed cleanly\n');
  } catch (err) {
    console.error('crash recovery simulation error:', err);
    process.exitCode = 1;
  } finally {
    await redis.quit();
    await pool.end();
  }
}

runCrashRecoverySimulation();
