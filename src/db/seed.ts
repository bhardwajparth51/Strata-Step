import { db, pool } from './db.js';
import { workflows, workflowRuns, stepExecutions } from './schema.js';

async function seed() {
  console.log('Seeding VaultFlow database...');

  try {
    // 1. Insert Workflow Definition
    const [createdWorkflow] = await db.insert(workflows).values({
      name: 'Order Processing & AI Fulfillment',
      description: 'Processes user order, charges Stripe, renders AI video thumbnail, and dispatches email notification.',
    }).onConflictDoNothing().returning();

    if (!createdWorkflow) {
      console.log('Workflow already exists. Fetching existing...');
      const existing = await db.query.workflows.findFirst();
      if (!existing) throw new Error('Could not find existing workflow');
      console.log(`Using Workflow: ${existing.name} (${existing.id})`);
    } else {
      console.log(`Workflow Created: ${createdWorkflow.name} (${createdWorkflow.id})`);
    }

    const targetWorkflowId = createdWorkflow ? createdWorkflow.id : (await db.query.workflows.findFirst())!.id;

    // 2. Insert Workflow Run
    const [createdRun] = await db.insert(workflowRuns).values({
      workflowId: targetWorkflowId,
      status: 'FAILED',
      currentStep: 'generateAiVideo',
      errorSummary: 'AI Vendor API Gateway Timeout (504)',
      startedAt: new Date(),
    }).returning();

    console.log(`Workflow Run Created: ${createdRun.id}`);

    // 3. Insert Step Executions (Checkpoint Events)
    await db.insert(stepExecutions).values([
      {
        workflowRunId: createdRun.id,
        stepName: 'chargeCustomerStripe',
        status: 'COMPLETED',
        inputPayload: { amount: 2900, currency: 'usd', customerId: 'cust_8832' },
        outputPayload: { status: 'succeeded', chargeId: 'ch_3N9x821' },
        executionTimeMs: 340,
        attemptNumber: 1,
        idempotencyKey: 'idemp_charge_8832_1',
      },
      {
        workflowRunId: createdRun.id,
        stepName: 'generateAiVideo',
        status: 'FAILED',
        inputPayload: { chargeId: 'ch_3N9x821', prompt: 'Modern logistics dashboard 3D render' },
        outputPayload: null,
        errorMessage: 'HTTP 504: AI Generation Worker Timeout after 30000ms',
        executionTimeMs: 30000,
        attemptNumber: 1,
        idempotencyKey: 'idemp_video_8832_1',
      },
      {
        workflowRunId: createdRun.id,
        stepName: 'sendFulfillmentEmail',
        status: 'SKIPPED',
        inputPayload: { recipient: 'user@example.com' },
        outputPayload: null,
        executionTimeMs: 0,
        attemptNumber: 1,
        idempotencyKey: 'idemp_email_8832_1',
      },
    ]);

    console.log('Seed steps inserted successfully.');
  } catch (error) {
    console.error('Error during seeding:', error);
  } finally {
    await pool.end();
  }
}

seed();
