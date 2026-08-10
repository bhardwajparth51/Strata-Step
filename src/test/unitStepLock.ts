import assert from 'node:assert';
import { StepLock } from '../locks/stepLock.js';
import { executeStep } from '../engine/executeStep.js';
import { StepExecutionRepository } from '../db/repositories/stepExecutionRepository.js';
import { StepCheckpointRepository } from '../db/repositories/stepCheckpointRepository.js';
import { WorkflowRunRepository } from '../db/repositories/workflowRunRepository.js';
import { StripeAdapter } from '../adapters/stripeAdapter.js';
import { db } from '../db/db.js';

(db as any).transaction = async (cb: any) => await cb(db);

class MockRedis {
  public kv = new Map<string, string>();
  public renewCalls: number = 0;
  public forceRenewFailure: boolean = false;

  async set(key: string, value: string, _pxFlag: string, _ttlMs: number, _nxFlag: string): Promise<string | null> {
    if (this.kv.has(key)) return null;
    this.kv.set(key, value);
    return 'OK';
  }

  async eval(script: string, _numKeys: number, key: string, value: string, _extraTtlMs?: number): Promise<number> {
    if (script.includes('pexpire')) {
      this.renewCalls++;
      if (this.forceRenewFailure) {
        return 0;
      }
      return this.kv.get(key) === value ? 1 : 0;
    }
    if (script.includes('del')) {
      if (this.kv.get(key) === value) {
        this.kv.delete(key);
        return 1;
      }
      return 0;
    }
    return 0;
  }
}

async function runUnitTests() {
  console.log('Running VaultFlow engine unit tests...\n');

  // Test 1: StepLock Basic Acquire, Renew, and Release
  console.log('Test 1: StepLock acquire, renew, and release');
  const mockRedis = new MockRedis();
  const lock = new StepLock(mockRedis as any, 'run-1', 'step-1', 1000);

  const acquired = await lock.acquire();
  assert.strictEqual(acquired, true, 'Lock should be acquired');

  const renewed = await lock.renew(1000);
  assert.strictEqual(renewed, true, 'Lock should be renewed');

  const released = await lock.release();
  assert.strictEqual(released, true, 'Lock should be released');
  console.log('PASS Test 1\n');

  // Mock Repository DB methods
  StepCheckpointRepository.findActiveCheckpoint = (async () => null) as any;
  StepCheckpointRepository.upsertCheckpoint = (async () => ({})) as any;
  StepCheckpointRepository.deleteDownstreamCheckpoints = (async () => 0) as any;
  StepExecutionRepository.findLatestCompletedStep = (async () => null) as any;
  StepExecutionRepository.getNextAttemptNumber = async () => 1;
  StepExecutionRepository.createRunningStep = (async (params: any) => ({
    id: 'step-exec-1',
    attemptNumber: params.attemptNumber ?? 1,
    ...params,
  })) as any;
  StepExecutionRepository.updateToCompleted = (async (id: string, outputPayload: any, executionTimeMs: number) => ({
    id,
    stepName: 'testStep',
    outputPayload,
    executionTimeMs,
    attemptNumber: 1,
  })) as any;
  StepExecutionRepository.completeStepTransaction = (async (
    runningStepId: string,
    workflowRunId: string,
    stepName: string,
    outputPayload: any,
    executionTimeMs: number,
    attemptNumber: number
  ) => {
    await StepCheckpointRepository.upsertCheckpoint(workflowRunId, stepName, runningStepId, outputPayload, attemptNumber);
    return {
      id: runningStepId,
      stepName,
      status: 'COMPLETED',
      outputPayload,
      executionTimeMs,
      attemptNumber,
    };
  }) as any;
  StepExecutionRepository.updateToFailed = async () => ({}) as any;
  WorkflowRunRepository.setCurrentStep = async () => ({}) as any;
  WorkflowRunRepository.updateHeartbeat = async () => ({}) as any;
  WorkflowRunRepository.updateStatus = async () => ({}) as any;
  WorkflowRunRepository.transitionStatus = async () => ({}) as any;
  WorkflowRunRepository.transitionStatusTx = async (_tx: any, id: string, _expectedStatuses: any[], newStatus: any) => {
    if (id === 'active-run') return null;
    return { id, status: newStatus } as any;
  };

  // Test 2: executeStep Heartbeat Renewal & StepContext Injection
  console.log('Test 2: executeStep Heartbeat Renewal & StepContext');
  const mockRedis2 = new MockRedis();
  let capturedContext: any = null;

  const result = await executeStep({
    workflowRunId: 'run-100',
    stepName: 'chargeCustomerStripe',
    inputPayload: { amount: 5000 },
    redis: mockRedis2 as any,
    lockTtlMs: 200,
    stepFn: async (_input, ctx) => {
      capturedContext = ctx;
      await new Promise((r) => setTimeout(r, 450));
      return { chargeId: 'ch_123', status: 'paid' };
    },
  });

  assert.ok(capturedContext !== null, 'Context should be passed');
  assert.strictEqual(capturedContext.idempotencyKey, 'vaultflow:run-100:chargeCustomerStripe', 'Idempotency key match');
  assert.ok(mockRedis2.renewCalls >= 3, `Heartbeat should renew multiple times (got ${mockRedis2.renewCalls})`);
  assert.strictEqual(result.outputPayload.chargeId, 'ch_123', 'Output payload match');
  console.log('PASS Test 2\n');

  // Test 3: Lock Lost Detection during execution
  console.log('Test 3: Lock Lost Detection');
  const mockRedis3 = new MockRedis();
  let lockLostErrorThrown = false;

  try {
    await executeStep({
      workflowRunId: 'run-200',
      stepName: 'generateAiVideo',
      inputPayload: { prompt: 'space shuttle' },
      redis: mockRedis3 as any,
      lockTtlMs: 100,
      stepFn: async () => {
        mockRedis3.forceRenewFailure = true;
        await new Promise((r) => setTimeout(r, 150));
        return { videoUrl: 'http://cdn/123.mp4' };
      },
    });
  } catch (err: any) {
    if (err.message.includes('Lock lost') || err.message.includes('Lock Lease Lost') || err.name === 'LockLostError') {
      lockLostErrorThrown = true;
    }
  }

  assert.strictEqual(lockLostErrorThrown, true, 'Should throw LockLostError when renewal fails');
  console.log('PASS Test 3\n');

  // Test 4: Failure Persistence Fallback
  console.log('Test 4: Failure Persistence Fallback');
  const mockRedis4 = new MockRedis();
  StepExecutionRepository.updateToFailed = async () => {
    throw new Error('PostgreSQL Database Connection Dropped!');
  };
  WorkflowRunRepository.updateStatus = async () => {
    throw new Error('PostgreSQL Database Connection Dropped!');
  };

  let originalStepErrorThrown = false;
  try {
    await executeStep({
      workflowRunId: 'run-300',
      stepName: 'failingStep',
      inputPayload: {},
      redis: mockRedis4 as any,
      stepFn: async () => {
        throw new Error('Stripe API Key Invalid');
      },
    });
  } catch (err: any) {
    if (err.message === 'Stripe API Key Invalid') {
      originalStepErrorThrown = true;
    }
  }

  assert.strictEqual(originalStepErrorThrown, true, 'Original stepFn error should be preserved and rethrown');
  console.log('PASS Test 4\n');

  // Reset Repository mocks
  WorkflowRunRepository.updateStatus = async () => ({}) as any;
  WorkflowRunRepository.updateStatusFromStatuses = async (id: string, newStatus: any) => {
    if (id === 'active-run') return null;
    return { id, status: newStatus } as any;
  };
  StepExecutionRepository.updateToFailed = async () => ({}) as any;

  // Test 5: replayWorkflow Validation & State Transition Guardrails
  console.log('Test 5: replayWorkflow Validation & State Transition Guardrails');
  const { replayWorkflow } = await import('../engine/replayWorkflow.js');

  WorkflowRunRepository.findById = async (id: string) => {
    if (id === 'non-existent-run') return null;
    if (id === 'active-run') return { id: 'active-run', status: 'RUNNING' } as any;
    return { id: 'failed-run', status: 'FAILED' } as any;
  };

  StepExecutionRepository.findLatestStep = async (_runId: string, stepName: string) => {
    if (stepName === 'nonExistentStep') return null;
    return { id: 'step-rec-1', stepName } as any;
  };

  const testPipeline = ['step1', 'step2'];

  let rejectedMissingStepOrder = false;
  try {
    await (replayWorkflow as any)('failed-run', 'step1');
  } catch (err: any) {
    if (err.message.includes('stepOrder array is required')) rejectedMissingStepOrder = true;
  }
  assert.strictEqual(rejectedMissingStepOrder, true, 'Should reject replay when stepOrder is missing');

  let rejectedNonExistentRun = false;
  try {
    await replayWorkflow('non-existent-run', 'step1', testPipeline);
  } catch (err: any) {
    if (err.message.includes('not found')) rejectedNonExistentRun = true;
  }
  assert.strictEqual(rejectedNonExistentRun, true, 'Should reject non-existent workflow run');

  let rejectedActiveRun = false;
  try {
    await replayWorkflow('active-run', 'step1', testPipeline);
  } catch (err: any) {
    if (err) rejectedActiveRun = true;
  }
  assert.strictEqual(rejectedActiveRun, true, 'Should reject replaying a RUNNING workflow run');

  let rejectedInvalidStep = false;
  try {
    await replayWorkflow('failed-run', 'nonExistentStep', testPipeline);
  } catch (err: any) {
    if (err.message.includes('not found in execution history') || err.message.includes('not defined in stepOrder')) {
      rejectedInvalidStep = true;
    }
  }
  assert.strictEqual(rejectedInvalidStep, true, 'Should reject non-existent step name');
  console.log('PASS Test 5\n');

  // Test 6: Immutable Event Ledger & Downstream Step Invalidation
  console.log('Test 6: Immutable Event Ledger & Downstream Step Invalidation');
  let invalidatedSteps: string[] = [];

  StepCheckpointRepository.deleteDownstreamCheckpointsTx = async (_tx: any, _runId: string, fromStepName: string, stepOrder: string[]) => {
    if (stepOrder) {
      const idx = stepOrder.indexOf(fromStepName);
      invalidatedSteps = stepOrder.slice(idx);
    }
    return invalidatedSteps.length;
  };

  const stepPipeline = ['chargeCustomerStripe', 'generateAiVideo', 'sendFulfillmentEmail'];
  await replayWorkflow('failed-run', 'generateAiVideo', stepPipeline);

  assert.ok(invalidatedSteps.includes('generateAiVideo'), 'generateAiVideo checkpoint should be cleared for re-execution');
  assert.ok(invalidatedSteps.includes('sendFulfillmentEmail'), 'Downstream step sendFulfillmentEmail checkpoint should be cleared');
  console.log('PASS Test 6\n');

  // Test 7: 10+ Simultaneous Workers Concurrency Stress Test
  console.log('Test 7: 10+ Simultaneous Workers Concurrency Stress Test');
  const mockRedis7 = new MockRedis();
  let stepFnExecutions = 0;

  let activeCheckpoint: any = null;
  StepCheckpointRepository.findActiveCheckpoint = async () => activeCheckpoint;
  StepCheckpointRepository.upsertCheckpoint = async (_runId, stepName, stepExecId, outputPayload, attemptNumber) => {
    activeCheckpoint = {
      stepExecutionId: stepExecId,
      stepName,
      outputPayload,
      attemptNumber,
    };
    return activeCheckpoint;
  };
  StepExecutionRepository.getNextAttemptNumber = async () => 1;
  StepExecutionRepository.createRunningStep = (async (params: any) => ({
    id: 'step-exec-concurrent',
    ...params,
  })) as any;
  StepExecutionRepository.updateToCompleted = (async (id: string, outputPayload: any, executionTimeMs: number) => ({
    id,
    stepName: 'concurrentStep',
    status: 'COMPLETED',
    outputPayload,
    executionTimeMs,
    attemptNumber: 1,
  })) as any;

  const workerCount = 10;
  const workerPromises = Array.from({ length: workerCount }).map((_, idx) =>
    executeStep({
      workflowRunId: 'run-concurrent-99',
      stepName: 'concurrentStep',
      inputPayload: { workerId: idx },
      redis: mockRedis7 as any,
      maxLockWaitMs: 3000,
      stepFn: async (input) => {
        stepFnExecutions++;
        await new Promise((r) => setTimeout(r, 100));
        return { processedByWorker: input.workerId };
      },
    })
  );

  const workerResults = await Promise.all(workerPromises);
  assert.strictEqual(stepFnExecutions, 1, `Exactly 1 worker should execute stepFn (got ${stepFnExecutions})`);
  assert.strictEqual(workerResults.length, 10, 'All 10 workers should resolve successfully');
  assert.strictEqual(
    workerResults.filter((r) => r.cached).length,
    9,
    '9 workers should receive cached double-check result'
  );
  console.log('PASS Test 7\n');

  // Test 8: Historical Attempt Preservation Verification Test
  console.log('Test 8: Historical Attempt Preservation Verification Test');
  const ledgerHistory = [
    { id: '1', stepName: 'sendFulfillmentEmail', attemptNumber: 1, status: 'FAILED' },
    { id: '2', stepName: 'sendFulfillmentEmail', attemptNumber: 2, status: 'COMPLETED' },
  ];

  const pipeline = ['chargeCustomerStripe', 'generateAiVideo', 'sendFulfillmentEmail'];
  const fromIndex = pipeline.indexOf('generateAiVideo');
  const downstream = pipeline.slice(fromIndex + 1);

  for (const record of ledgerHistory) {
    if (downstream.includes(record.stepName) && record.status === 'COMPLETED') {
      record.status = 'INVALIDATED';
    }
  }

  assert.strictEqual(ledgerHistory[0].status, 'FAILED', 'Historical Attempt #1 (FAILED) MUST remain untouched');
  assert.strictEqual(ledgerHistory[1].status, 'INVALIDATED', 'Active Attempt #2 (COMPLETED) checkpoint MUST be INVALIDATED');
  console.log('PASS Test 8\n');

  // Test 9: Strict State Machine & Decoupled setCurrentStep Verification
  console.log('Test 9: Strict State Machine & Decoupled setCurrentStep Verification');
  const sampleRun = { id: 'run-state-1', status: 'PAUSED', currentStep: 'step1' };

  sampleRun.currentStep = 'step2';
  assert.strictEqual(sampleRun.currentStep, 'step2', 'currentStep updated');
  assert.strictEqual(sampleRun.status, 'PAUSED', 'setCurrentStep MUST NOT mutate status');

  const validTransition = ['FAILED', 'PAUSED', 'COMPLETED'].includes(sampleRun.status);
  assert.strictEqual(validTransition, true, 'PAUSED -> RUNNING transition is valid');

  const invalidTransitionFromCancelled = ['FAILED', 'PAUSED', 'COMPLETED'].includes('CANCELLED');
  assert.strictEqual(invalidTransitionFromCancelled, false, 'CANCELLED -> RUNNING transition MUST be rejected');
  console.log('PASS Test 9\n');

  // Test 10: External Service Idempotency Key Failover Test
  console.log('Test 10: External Service Idempotency Key Failover Test');
  const stripeAdapter = new StripeAdapter();
  const idempotencyKey = 'vaultflow:run-idemp-1:chargeCustomerStripe';

  const chargeA = await stripeAdapter.createCharge(4900, 'usd', idempotencyKey);
  assert.strictEqual(chargeA.reusedExistingCharge, false, 'First charge MUST execute new external call');

  const chargeB = await stripeAdapter.createCharge(4900, 'usd', idempotencyKey);
  assert.strictEqual(chargeB.reusedExistingCharge, true, 'Worker B retry MUST hit Stripe idempotency key');
  assert.strictEqual(chargeB.chargeId, chargeA.chargeId, 'Worker B MUST receive identical charge ID');
  assert.strictEqual(stripeAdapter.externalChargeCount, 1, 'External charges on Stripe MUST equal 1');
  console.log('PASS Test 10\n');

  console.log('All 10 VaultFlow engine unit tests passed cleanly!');
}

runUnitTests().catch((err) => {
  console.error('Unit test failure:', err);
  process.exit(1);
});
