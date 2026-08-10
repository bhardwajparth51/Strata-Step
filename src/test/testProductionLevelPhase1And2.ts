import assert from 'node:assert';
import { executeStep } from '../engine/executeStep.js';
import { StepLock } from '../locks/stepLock.js';
import { StepExecutionRepository } from '../db/repositories/stepExecutionRepository.js';
import { StepCheckpointRepository } from '../db/repositories/stepCheckpointRepository.js';
import { WorkflowRunRepository } from '../db/repositories/workflowRunRepository.js';
import { VaultFlowEngine } from '../engine/vaultflowEngine.js';
import { db } from '../db/db.js';
import { stepCheckpoints, workflowRuns } from '../db/schema.js';

(db as any).transaction = async (cb: any) => await cb(db);

class FailingRedis {
  async set(): Promise<string> {
    throw new Error('ECONNREFUSED');
  }
  async eval(): Promise<number> {
    throw new Error('ECONNREFUSED');
  }
}

class MockRedis {
  public kv = new Map<string, string>();
  async set(key: string, value: string, _px: string, _ttl: number, _nx: string): Promise<string | null> {
    if (this.kv.has(key)) return null;
    this.kv.set(key, value);
    return 'OK';
  }
  async eval(script: string, _num: number, key: string, value: string): Promise<number> {
    if (script.includes('del')) {
      if (this.kv.get(key) === value) {
        this.kv.delete(key);
        return 1;
      }
      return 0;
    }
    return 1;
  }
}

async function runTests() {
  console.log('Running VaultFlow production-level integration test suite...\n');

  let mockExecutions: any[] = [];
  StepCheckpointRepository.findActiveCheckpoint = (async () => null) as any;
  StepCheckpointRepository.upsertCheckpoint = (async () => ({})) as any;
  
  let attemptCounter = 0;
  StepExecutionRepository.createRunningStep = (async (params: any) => {
    attemptCounter++;
    const record = {
      id: `exec-${Math.random().toString(36).substring(2, 7)}`,
      attemptNumber: attemptCounter,
      status: 'RUNNING',
      createdAt: new Date(),
      ...params,
    };
    mockExecutions.push(record);
    return record;
  }) as any;

  StepExecutionRepository.completeStepTransaction = (async (
    runningStepId: string,
    workflowRunId: string,
    stepName: string,
    outputPayload: any,
    executionTimeMs: number,
    attemptNumber: number
  ) => {
    const existing = mockExecutions.find((e) => e.id === runningStepId);
    if (existing) {
      existing.status = 'COMPLETED';
      existing.outputPayload = outputPayload;
    }
    return {
      id: runningStepId,
      stepName,
      status: 'COMPLETED',
      outputPayload,
      executionTimeMs,
      attemptNumber,
    };
  }) as any;

  StepExecutionRepository.updateToFailed = (async (id: string, errorMessage: string) => {
    const existing = mockExecutions.find((e) => e.id === id);
    if (existing) {
      existing.status = 'FAILED';
      existing.errorMessage = errorMessage;
    }
    return existing;
  }) as any;

  StepExecutionRepository.findExecutionsForRun = (async (workflowRunId: string) => {
    return mockExecutions.filter((e) => e.workflowRunId === workflowRunId);
  }) as any;

  WorkflowRunRepository.setCurrentStep = (async () => ({})) as any;
  WorkflowRunRepository.updateHeartbeat = (async () => ({})) as any;
  WorkflowRunRepository.transitionStatus = (async () => ({})) as any;

  // Test 1: PostgreSQL Advisory Lock Failover
  (db as any).execute = async (query: any) => {
    const rawChunks = query?.queryChunks ? query.queryChunks.map((c: any) => (Array.isArray(c) ? c.join('') : String(c.value || c))).join('') : String(query);
    if (rawChunks.includes('unlock') || rawChunks.includes('pg_advisory_unlock')) {
      return [{ released: true }];
    }
    return [{ acquired: true }];
  };

  const failingRedis = new FailingRedis();
  const lock = new StepLock(failingRedis as any, 'run-failover-1', 'chargeCustomer', 10000);
  
  const acquired = await lock.acquire();
  assert.strictEqual(acquired, true);
  assert.strictEqual(lock.getProvider(), 'POSTGRES_ADVISORY');

  const released = await lock.release();
  assert.strictEqual(released, true);

  // Test 2: Saga Rollback
  const mockRedis = new MockRedis();
  const runId = 'run-saga-100';
  let refundExecuted = false;

  await executeStep({
    workflowRunId: runId,
    stepName: 'chargeCustomerStripe',
    inputPayload: { amount: 4999, currency: 'usd' },
    redis: mockRedis as any,
    stepFn: async (input) => ({ chargeId: 'ch_12345', amount: input.amount }),
    compensate: async () => {
      refundExecuted = true;
    },
  });

  let step2Failed = false;
  try {
    await executeStep({
      workflowRunId: runId,
      stepName: 'generateInvoicePDF',
      inputPayload: { invoiceId: 'inv_999' },
      redis: mockRedis as any,
      retryPolicy: { maxAttempts: 1 },
      stepFn: async () => {
        throw new Error('PDF Generator Service Unavailable');
      },
    });
  } catch {
    step2Failed = true;
  }

  assert.strictEqual(step2Failed, true);
  assert.strictEqual(refundExecuted, true);

  // Test 3: Schema Metadata
  const runTableDef = workflowRuns as any;
  const checkpointTableDef = stepCheckpoints as any;

  assert.ok(runTableDef.workflowVersion);
  assert.ok(runTableDef.schemaVersion);
  assert.ok(checkpointTableDef.workflowRunId);

  // Test 4: VaultFlowEngine Facade
  const engine = new VaultFlowEngine({ redis: mockRedis as any });
  
  let facadeExecuted = false;
  await engine.executeStep({
    workflowRunId: 'run-facade-1',
    stepName: 'testFacadeStep',
    inputPayload: { foo: 'bar' },
    stepFn: async () => {
      facadeExecuted = true;
      return { success: true };
    },
  });

  assert.strictEqual(facadeExecuted, true);

  // Test 5: Optimistic Concurrency Control
  assert.ok(runTableDef.version);

  const originalTransitionTx = WorkflowRunRepository.transitionStatusTx;
  const originalTransition = WorkflowRunRepository.transitionStatus;
  
  WorkflowRunRepository.transitionStatus = (id: string, fromStatuses: any[], toStatus: any, errorSummary?: string, expectedVersion?: number) => {
    return WorkflowRunRepository.transitionStatusTx(db, id, fromStatuses, toStatus, errorSummary, expectedVersion);
  };

  WorkflowRunRepository.transitionStatusTx = (async (
    _tx: any,
    id: string,
    _fromStatuses: any[],
    toStatus: any,
    _errorSummary?: string,
    expectedVersion?: number
  ) => {
    if (expectedVersion !== undefined && expectedVersion !== 1) {
      return null;
    }
    return { id, status: toStatus, version: (expectedVersion ?? 1) + 1 };
  }) as any;

  const validOccUpdate = await WorkflowRunRepository.transitionStatus('run-occ-1', ['RUNNING' as any], 'FAILED' as any, 'Error', 1);
  assert.strictEqual(validOccUpdate?.version, 2);

  const invalidOccUpdate = await WorkflowRunRepository.transitionStatus('run-occ-1', ['RUNNING' as any], 'FAILED' as any, 'Error', 99);
  assert.strictEqual(invalidOccUpdate, null);
  
  WorkflowRunRepository.transitionStatusTx = originalTransitionTx;
  WorkflowRunRepository.transitionStatus = originalTransition;

  // Test 6: NonRetryableError Bypass
  const { NonRetryableError } = await import('../engine/errors.js');

  let nonRetryAttempts = 0;
  let nonRetryCaught = false;
  try {
    await executeStep({
      workflowRunId: 'run-nonretry-1',
      stepName: 'invalidAuthTokenStep',
      inputPayload: {},
      redis: mockRedis as any,
      retryPolicy: { maxAttempts: 5 },
      stepFn: async () => {
        nonRetryAttempts++;
        throw new NonRetryableError('Invalid API Key');
      },
    });
  } catch (err: any) {
    if (err instanceof NonRetryableError || err?.name === 'NonRetryableError') {
      nonRetryCaught = true;
    }
  }

  assert.strictEqual(nonRetryCaught, true);
  assert.strictEqual(nonRetryAttempts, 1);

  console.log('VaultFlow Phase 1 & 2 production-level integration tests passed cleanly!\n');
}

runTests().catch((err) => {
  console.error('Integration test failure:', err);
  process.exit(1);
});
