import { Redis } from 'ioredis';
import { db, pool } from '../db/db.js';
import { workflows, workflowRuns } from '../db/schema.js';
import { executeStep } from '../engine/executeStep.js';
import { StripeAdapter } from '../adapters/stripeAdapter.js';
import { WorkflowRunStatus } from '../types/index.js';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

async function runIdempotencySimulation() {
  console.log('running idempotency failover simulation...\n');

  try {
    const stripeAdapter = new StripeAdapter();

    const [wf] = await db
      .insert(workflows)
      .values({ name: 'Idempotency Failover Workflow' })
      .onConflictDoNothing()
      .returning();

    const workflowId = wf ? wf.id : (await db.query.workflows.findFirst())!.id;

    const [run] = await db
      .insert(workflowRuns)
      .values({ workflowId, status: WorkflowRunStatus.RUNNING })
      .returning();

    console.log(`workflow run id: ${run.id}\n`);

    // ----------------------------------------------------------------------
    // WORKER A: Calls Stripe successfully, but CRASHES before DB write
    // ----------------------------------------------------------------------
    console.log('worker A: executing stripe charge and simulating crash before DB write');
    let workerACrashed = false;
    let workerAChargeId = '';

    try {
      await executeStep({
        workflowRunId: run.id,
        stepName: 'chargeCustomerStripe',
        inputPayload: { amount: 9900, currency: 'usd' },
        redis,
        stepFn: async (input, ctx) => {
          const res = await stripeAdapter.createCharge(input.amount, input.currency, ctx.idempotencyKey);
          workerAChargeId = res.chargeId;
          console.log('  worker process simulated crash after call');
          throw new Error('WORKER_CRASH_SIMULATION_ERROR');
        },
      });
    } catch (err: any) {
      workerACrashed = true;
      console.log('  worker A crashed as expected\n');
    }

    // ----------------------------------------------------------------------
    // WORKER B: Retries the failed step with SAME idempotency key
    // ----------------------------------------------------------------------
    console.log('worker B: retrying failed step with same idempotency key');
    const workerBStep = await executeStep({
      workflowRunId: run.id,
      stepName: 'chargeCustomerStripe',
      inputPayload: { amount: 9900, currency: 'usd' },
      redis,
      stepFn: async (input, ctx) => {
        return await stripeAdapter.createCharge(input.amount, input.currency, ctx.idempotencyKey);
      },
    });

    console.log('\nstep result for worker B:', workerBStep.outputPayload, '\n');

    // VERIFICATION: Check external API metrics
    console.log('metrics verification:');
    console.log(`total requests handled by adapter: ${stripeAdapter.totalRequestsHandled}`);
    console.log(`actual external charges created: ${stripeAdapter.externalChargeCount}`);
    console.log(`reused existing charge: ${workerBStep.outputPayload.reusedExistingCharge}`);

    if (stripeAdapter.externalChargeCount !== 1) {
      throw new Error(`Idempotency Failure: Expected 1 external charge, got ${stripeAdapter.externalChargeCount}`);
    }

    if (workerBStep.outputPayload.chargeId !== workerAChargeId) {
      throw new Error('Idempotency Failure: Worker B received a different charge ID!');
    }

    console.log('\nidempotency failover simulation passed cleanly\n');

  } catch (err) {
    console.error('idempotency simulation error:', err);
    process.exitCode = 1;
  } finally {
    await redis.quit();
    await pool.end();
  }
}

runIdempotencySimulation();
