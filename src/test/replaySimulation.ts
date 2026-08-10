import { Redis } from 'ioredis';
import { db, pool } from '../db/db.js';
import { workflows, workflowRuns } from '../db/schema.js';
import { executeStep } from '../engine/executeStep.js';
import { replayWorkflow } from '../engine/replayWorkflow.js';
import { WorkflowRunRepository } from '../db/repositories/workflowRunRepository.js';
import { WorkflowRunStatus } from '../types/index.js';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

async function runSimulation() {
  console.log('running workflow engine simulation...\n');

  try {
    // 1. Setup Test Workflow Definition
    const [wf] = await db
      .insert(workflows)
      .values({
        name: 'Simulated Order Processing Workflow',
        description: 'End-to-End Replay & Concurrency Verification Test',
      })
      .onConflictDoNothing()
      .returning();

    const workflowId = wf ? wf.id : (await db.query.workflows.findFirst())!.id;

    // 2. Create Workflow Run Instance
    const [run] = await db
      .insert(workflowRuns)
      .values({
        workflowId,
        status: WorkflowRunStatus.RUNNING,
      })
      .returning();

    console.log(`created workflow run id: ${run.id}\n`);

    // ----------------------------------------------------------------------
    // SCENARIO 1: 10 CONCURRENT WORKERS DISTRIBUTED LOCKING TEST
    // ----------------------------------------------------------------------
    console.log('scenario 1: 10 concurrent workers double-check lock test');
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

    const workerResults = await Promise.all(workerPromises);
    console.log(`  actual stepFn executions across 10 workers: ${executionCount}`);

    if (executionCount !== 1) {
      throw new Error(`Double-Check Locking Failure: Expected exactly 1 execution, got ${executionCount}`);
    }
    console.log('10 workers coordinated cleanly - stepFn executed ONCE, 9 workers received cached checkpoint\n');

    // ----------------------------------------------------------------------
    // SCENARIO 2: FIRST COMPLETE WORKFLOW EXECUTION PASS
    // ----------------------------------------------------------------------
    console.log('scenario 2: first complete workflow execution pass');

    // Step 1: Charge Stripe with Idempotency Key
    const step1 = await executeStep({
      workflowRunId: run.id,
      stepName: 'chargeCustomerStripe',
      inputPayload: { amount: 4900, currency: 'usd' },
      redis,
      stepFn: async (input, ctx) => {
        console.log(`  processing stripe charge of $${input.amount / 100} with idempotency key: ${ctx.idempotencyKey}`);
        return { chargeId: 'ch_simulated_999', status: 'paid', idempotencyKey: ctx.idempotencyKey };
      },
    });
    console.log('step 1 result:', step1, '\n');

    // Step 2: Generate Video (Simulated first attempt)
    const step2 = await executeStep({
      workflowRunId: run.id,
      stepName: 'generateAiVideo',
      inputPayload: { chargeId: step1.outputPayload.chargeId, prompt: 'Cinematic drone shot' },
      redis,
      stepFn: async (input, ctx) => {
        console.log(`  generating video (attempt #${ctx.attemptNumber}) for charge: ${input.chargeId}`);
        return { videoUrl: 'https://cdn.stratstep.dev/renders/v_initial.mp4', status: 'rendered' };
      },
    });
    console.log('step 2 result:', step2, '\n');

    // Step 3: Send Fulfillment Email
    const step3 = await executeStep({
      workflowRunId: run.id,
      stepName: 'sendFulfillmentEmail',
      inputPayload: { videoUrl: step2.outputPayload.videoUrl, userEmail: 'founder@startup.com' },
      redis,
      stepFn: async (input, ctx) => {
        console.log(`  sending video link to: ${input.userEmail} (key: ${ctx.idempotencyKey})`);
        return { messageId: 'msg_initial_100', delivered: true };
      },
    });
    console.log('step 3 result:', step3, '\n');

    // Complete Initial Run
    await WorkflowRunRepository.updateStatus(run.id, WorkflowRunStatus.COMPLETED);
    console.log('initial workflow run completed. all step checkpoints saved to postgresql.\n');

    // ----------------------------------------------------------------------
    // SCENARIO 3: CHECKPOINT REPLAY & DOWNSTREAM INVALIDATION PROOF
    // ----------------------------------------------------------------------
    console.log('scenario 3: checkpoint replay from step 2 and invalidation');

    const stepPipeline = ['chargeCustomerStripe', 'generateAiVideo', 'sendFulfillmentEmail'];
    const replayResult = await replayWorkflow(run.id, 'generateAiVideo', stepPipeline);
    console.log(`replay response: reset status to RUNNING. Invalidated ${replayResult.invalidatedCount} downstream checkpoints.\n`);

    interface ChargeOutput {
      chargeId: string;
      status: string;
      idempotencyKey: string;
    }

    // Step 1 re-run (Step function execution skipped via PostgreSQL checkpoint hit)
    const step1Replay = await executeStep<{ amount: number; currency: string }, ChargeOutput>({
      workflowRunId: run.id,
      stepName: 'chargeCustomerStripe',
      inputPayload: { amount: 4900, currency: 'usd' },
      redis,
      stepFn: async (): Promise<ChargeOutput> => {
        throw new Error('CRITICAL BUG: Stripe was called again on replay!');
      },
    });
    console.log('step 1 replay result (checkpoint hit):', step1Replay, '\n');

    // Step 2 re-run (Attempt #2, succeeds with heartbeat renewal test)
    console.log('testing step with lock heartbeat renewal');
    const step2Replay = await executeStep({
      workflowRunId: run.id,
      stepName: 'generateAiVideo',
      inputPayload: { chargeId: step1Replay.outputPayload.chargeId, prompt: 'Cinematic 4K render' },
      redis,
      lockTtlMs: 1000, // Short 1s lock TTL
      stepFn: async (input, ctx) => {
        console.log(`  re-rendering video for charge: ${input.chargeId} (attempt #${ctx.attemptNumber})`);
        console.log('  simulating long step duration (2500ms) with 1000ms lock TTL...');
        await new Promise((r) => setTimeout(r, 2500)); // Will trigger multiple lock renewals
        return { videoUrl: 'https://cdn.stratstep.dev/renders/v_replayed_4k.mp4', status: 'rendered' };
      },
    });
    console.log('step 2 replay result (attempt #2):', step2Replay, '\n');

    // Step 3 re-run (Previously COMPLETED checkpoint was INVALIDATED, so stepFn MUST execute again)
    let step3ReExecuted = false;
    const step3Replay = await executeStep({
      workflowRunId: run.id,
      stepName: 'sendFulfillmentEmail',
      inputPayload: { videoUrl: step2Replay.outputPayload.videoUrl, userEmail: 'founder@startup.com' },
      redis,
      stepFn: async (input, ctx) => {
        step3ReExecuted = true;
        console.log(`  re-sending video link to: ${input.userEmail} (key: ${ctx.idempotencyKey})`);
        return { messageId: 'msg_replayed_200', delivered: true };
      },
    });

    if (!step3ReExecuted) {
      throw new Error('Replay Invalidation Failure: Downstream Step 3 was not re-executed after replay!');
    }
    console.log('step 3 replay result (re-executed downstream):', step3Replay, '\n');

    // Finalize Workflow Run Status
    await WorkflowRunRepository.updateStatus(run.id, WorkflowRunStatus.COMPLETED);
    console.log('simulation passed cleanly');

  } catch (error) {
    console.error('simulation error:', error);
    process.exitCode = 1;
  } finally {
    await redis.quit();
    await pool.end();
  }
}

runSimulation();


