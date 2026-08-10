import { Redis } from 'ioredis';
import { db, pool } from '../db/db.js';
import { workflows, workflowRuns } from '../db/schema.js';
import { executeStep } from '../engine/executeStep.js';
import { WorkflowRunStatus } from '../types/index.js';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

async function runConcurrencySimulation() {
  console.log('running concurrency simulation...\n');

  try {
    const [wf] = await db
      .insert(workflows)
      .values({ name: 'Concurrency Simulation Workflow' })
      .onConflictDoNothing()
      .returning();

    const workflowId = wf ? wf.id : (await db.query.workflows.findFirst())!.id;

    const [run] = await db
      .insert(workflowRuns)
      .values({ workflowId, status: WorkflowRunStatus.RUNNING })
      .returning();

    console.log(`workflow run id: ${run.id}`);

    let executionCount = 0;
    const workerPromises = Array.from({ length: 10 }, (_, idx) =>
      executeStep({
        workflowRunId: run.id,
        stepName: 'concurrentWorkerStep',
        inputPayload: { workerId: idx },
        redis,
        maxLockWaitMs: 5000,
        stepFn: async (input) => {
          executionCount++;
          console.log(`  worker #${input.workerId} acquired lock and is executing stepFn...`);
          await new Promise((resolve) => setTimeout(resolve, 300));
          return { processedByWorker: input.workerId };
        },
      })
    );

    const results = await Promise.all(workerPromises);
    console.log(`\nactual stepFn executions across 10 workers: ${executionCount}`);

    if (executionCount !== 1) {
      throw new Error(`Concurrency Failure: Expected 1 execution, got ${executionCount}`);
    }

    const cachedCount = results.filter((r) => r.cached).length;
    console.log(`cached double-check checkpoint hits: ${cachedCount}`);
    if (cachedCount !== 9) {
      throw new Error(`Concurrency Failure: Expected 9 cached hits, got ${cachedCount}`);
    }

    console.log('concurrency simulation passed cleanly\n');
  } catch (err) {
    console.error('concurrency simulation error:', err);
    process.exitCode = 1;
  } finally {
    await redis.quit();
    await pool.end();
  }
}

runConcurrencySimulation();
