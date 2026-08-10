import assert from 'node:assert';
import { executeStep } from '../engine/executeStep.js';
import { StepExecutionRepository } from '../db/repositories/stepExecutionRepository.js';
import { StepCheckpointRepository } from '../db/repositories/stepCheckpointRepository.js';
import { WorkflowRunRepository } from '../db/repositories/workflowRunRepository.js';
import { db } from '../db/db.js';

(db as any).transaction = async (cb: any) => await cb(db);

class MockRedis {
  public kv = new Map<string, string>();
  public renewCalls = 0;
  public forceRenewFailure = false;

  async set(key: string, value: string, _pxFlag: string, _ttlMs: number, _nxFlag: string): Promise<string | null> {
    if (this.kv.has(key)) return null;
    this.kv.set(key, value);
    return 'OK';
  }

  async eval(script: string, _numKeys: number, key: string, value: string): Promise<number> {
    if (script.includes('pexpire')) {
      this.renewCalls++;
      if (this.forceRenewFailure) return 0;
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

async function runHardeningTests() {
  StepCheckpointRepository.findActiveCheckpoint = (async () => null) as any;
  StepCheckpointRepository.upsertCheckpoint = (async () => ({})) as any;
  let mockAttempts = 0;
  StepExecutionRepository.createRunningStep = (async (params: any) => {
    mockAttempts++;
    return {
      id: `exec-${Math.random().toString(36).substring(2, 7)}`,
      attemptNumber: mockAttempts,
      ...params,
    };
  }) as any;
  StepExecutionRepository.completeStepTransaction = (async (
    runningStepId: string,
    workflowRunId: string,
    stepName: string,
    outputPayload: any,
    executionTimeMs: number,
    attemptNumber: number
  ) => ({
    id: runningStepId,
    stepName,
    status: 'COMPLETED',
    outputPayload,
    executionTimeMs,
    attemptNumber,
  })) as any;
  StepExecutionRepository.updateToFailed = (async () => ({})) as any;
  WorkflowRunRepository.setCurrentStep = (async () => ({})) as any;
  WorkflowRunRepository.updateHeartbeat = (async () => ({})) as any;
  WorkflowRunRepository.transitionStatus = (async () => ({})) as any;

  const mockRedis = new MockRedis();

  let attemptsSeen = 0;
  const retryResult = await executeStep({
    workflowRunId: 'run-hardening-1',
    stepName: 'transientFlakyStep',
    inputPayload: { amount: 100 },
    redis: mockRedis as any,
    retryPolicy: {
      maxAttempts: 3,
      initialIntervalMs: 20,
      backoffCoefficient: 1.5,
    },
    stepFn: async (_input, context) => {
      attemptsSeen++;
      if (context.attemptNumber < 3) {
        throw new Error(`Transient error on attempt ${context.attemptNumber}`);
      }
      return { success: true, attemptsTaken: context.attemptNumber };
    },
  });

  assert.strictEqual(attemptsSeen, 3);
  assert.strictEqual(retryResult.attemptNumber, 3);

  const mockRedis2 = new MockRedis();
  let abortSignalTriggered = false;
  let lockLostErrorCaught = false;

  try {
    await executeStep({
      workflowRunId: 'run-hardening-2',
      stepName: 'longRunningCall',
      inputPayload: {},
      redis: mockRedis2 as any,
      lockTtlMs: 100,
      stepFn: async (_input, context) => {
        mockRedis2.forceRenewFailure = true;

        context.abortSignal.addEventListener('abort', () => {
          abortSignalTriggered = true;
        });

        await new Promise((r) => setTimeout(r, 120));

        if (context.abortSignal.aborted) {
          throw new Error('Aborted by lock lost signal');
        }
        return { success: true };
      },
    });
  } catch (err: any) {
    if (err.message.includes('Lock lost') || err.message.includes('Aborted')) {
      lockLostErrorCaught = true;
    }
  }

  assert.strictEqual(abortSignalTriggered, true);
  assert.strictEqual(lockLostErrorCaught, true);

  let softInvalidated = false;
  StepCheckpointRepository.deleteDownstreamCheckpointsTx = (async (_tx: any, _runId: string, _fromStepName: string, stepOrder: string[]) => {
    softInvalidated = true;
    return stepOrder.length;
  }) as any;

  const { replayWorkflow } = await import('../engine/replayWorkflow.js');
  WorkflowRunRepository.findById = (async () => ({ id: 'run-3', status: 'FAILED' })) as any;
  StepExecutionRepository.findLatestStep = (async () => ({ id: 's1', stepName: 'step1' })) as any;
  WorkflowRunRepository.transitionStatusTx = (async () => ({ id: 'run-3', status: 'RUNNING' })) as any;

  await replayWorkflow('run-3', 'step1', ['step1', 'step2']);
  assert.strictEqual(softInvalidated, true);
}

runHardeningTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
