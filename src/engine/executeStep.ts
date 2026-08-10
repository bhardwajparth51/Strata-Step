import { Redis } from 'ioredis';
import { StepExecutionRepository } from '../db/repositories/stepExecutionRepository.js';
import { StepCheckpointRepository } from '../db/repositories/stepCheckpointRepository.js';
import { WorkflowRunRepository } from '../db/repositories/workflowRunRepository.js';
import { StepLock } from '../locks/stepLock.js';
import { StepExecutionStatus, StepOutput, WorkflowRunStatus, StepContext, RetryPolicy, StepCompensateFn } from '../types/index.js';
import { SagaEngine } from './sagaEngine.js';
import { LockAcquisitionTimeoutError, LockLostError, NonRetryableError } from './errors.js';

export interface ExecuteStepParams<TInput, TOutput> {
  workflowRunId: string;
  stepName: string;
  inputPayload: TInput;
  redis: Redis;
  stepFn: (input: TInput, context: StepContext) => Promise<TOutput>;
  compensate?: StepCompensateFn<TInput> | string;
  maxLockWaitMs?: number;
  lockTtlMs?: number;
  retryPolicy?: RetryPolicy;
}

export async function executeStep<TInput, TOutput>(
  params: ExecuteStepParams<TInput, TOutput>
): Promise<StepOutput<TOutput>> {
  const {
    workflowRunId,
    stepName,
    inputPayload,
    redis,
    stepFn,
    compensate,
    maxLockWaitMs = 10000,
    lockTtlMs = 30000,
    retryPolicy,
  } = params;

  const maxAttempts = retryPolicy?.maxAttempts ?? 1;
  const initialIntervalMs = retryPolicy?.initialIntervalMs ?? 1000;
  const backoffCoefficient = retryPolicy?.backoffCoefficient ?? 2.0;
  const maxIntervalMs = retryPolicy?.maxIntervalMs ?? 30000;

  const existingCheckpoint = await StepCheckpointRepository.findActiveCheckpoint(workflowRunId, stepName);
  if (existingCheckpoint) {
    return {
      stepExecutionId: existingCheckpoint.stepExecutionId,
      stepName: existingCheckpoint.stepName,
      status: StepExecutionStatus.COMPLETED,
      outputPayload: existingCheckpoint.outputPayload as TOutput,
      cached: true,
      attemptNumber: existingCheckpoint.attemptNumber,
    };
  }

  const lock = new StepLock(redis, workflowRunId, stepName, lockTtlMs);
  const startTime = Date.now();
  let acquired = false;
  let currentSleepMs = 50;

  while (!acquired) {
    acquired = await lock.acquire();
    if (acquired) break;

    const elapsed = Date.now() - startTime;
    const remaining = maxLockWaitMs - elapsed;
    if (remaining <= 0) break;

    const jitter = Math.floor(Math.random() * 20);
    const sleepMs = Math.min(remaining, currentSleepMs + jitter);
    await new Promise((resolve) => setTimeout(resolve, sleepMs));

    currentSleepMs = Math.min(500, Math.floor(currentSleepMs * 1.5));
  }

  if (!acquired) {
    throw new LockAcquisitionTimeoutError(`Lock acquisition timeout for step '${stepName}' in run ${workflowRunId}`);
  }

  const abortController = new AbortController();
  let lockLost = false;
  let isHeartbeatRunning = true;
  let heartbeatTimer: NodeJS.Timeout | null = null;
  const heartbeatIntervalMs = Math.floor(lock.getTtlMs() / 2);

  const scheduleHeartbeat = () => {
    if (!isHeartbeatRunning) return;

    heartbeatTimer = setTimeout(async () => {
      try {
        const renewed = await lock.renew();
        if (!renewed) {
          lockLost = true;
          abortController.abort(new LockLostError(`Lock lost for step '${stepName}' in run ${workflowRunId}`));
        } else {
          await WorkflowRunRepository.updateHeartbeat(workflowRunId).catch(() => {});
        }
      } catch {
        lockLost = true;
        abortController.abort(new LockLostError(`Lock renewal exception for step '${stepName}'`));
      } finally {
        if (isHeartbeatRunning && !lockLost) {
          scheduleHeartbeat();
        }
      }
    }, heartbeatIntervalMs);

    if (heartbeatTimer && typeof heartbeatTimer.unref === 'function') {
      heartbeatTimer.unref();
    }
  };

  scheduleHeartbeat();

  try {
    const recheckedCheckpoint = await StepCheckpointRepository.findActiveCheckpoint(workflowRunId, stepName);
    if (recheckedCheckpoint) {
      return {
        stepExecutionId: recheckedCheckpoint.stepExecutionId,
        stepName: recheckedCheckpoint.stepName,
        status: StepExecutionStatus.COMPLETED,
        outputPayload: recheckedCheckpoint.outputPayload as TOutput,
        cached: true,
        attemptNumber: recheckedCheckpoint.attemptNumber,
      };
    }

    await WorkflowRunRepository.setCurrentStep(workflowRunId, stepName);
    await WorkflowRunRepository.updateHeartbeat(workflowRunId);

    let attemptCount = 0;
    let lastError: any = null;

    while (attemptCount < maxAttempts) {
      attemptCount++;

      if (lockLost || abortController.signal.aborted) {
        throw new LockLostError(`Lock lost during execution of step '${stepName}'`);
      }

      const idempotencyKey = `vaultflow:${workflowRunId}:${stepName}`;
      const runningStep = await StepExecutionRepository.createRunningStep({
        workflowRunId,
        stepName,
        inputPayload,
        idempotencyKey,
      });

      const stepContext: StepContext = {
        workflowRunId,
        stepName,
        attemptNumber: runningStep.attemptNumber,
        idempotencyKey,
        abortSignal: abortController.signal,
      };

      const execStartTime = Date.now();

      try {
        const outputPayload = await stepFn(inputPayload, stepContext);

        if (lockLost || abortController.signal.aborted) {
          throw new LockLostError(`Lock lost during execution of step '${stepName}'`);
        }

        const executionTimeMs = Date.now() - execStartTime;

        const completedStep = await StepExecutionRepository.completeStepTransaction(
          runningStep.id,
          workflowRunId,
          stepName,
          outputPayload,
          executionTimeMs,
          runningStep.attemptNumber
        );

        if (compensate) {
          SagaEngine.registerCompensation(workflowRunId, stepName, compensate, inputPayload, stepContext);
        }

        return {
          stepExecutionId: completedStep.id,
          stepName: completedStep.stepName,
          status: StepExecutionStatus.COMPLETED,
          outputPayload,
          cached: false,
          attemptNumber: completedStep.attemptNumber,
        };
      } catch (err: any) {
        lastError = err;
        const executionTimeMs = Date.now() - execStartTime;
        const errorMessage = lastError?.message || String(lastError);

        try {
          await StepExecutionRepository.updateToFailed(runningStep.id, errorMessage, executionTimeMs);
        } catch {
          // ignore DB error on attempt log write to preserve original exception
        }

        const isNonRetryable = lastError instanceof NonRetryableError || (lastError?.name === 'NonRetryableError');

        if (!isNonRetryable && attemptCount < maxAttempts && !lockLost && !abortController.signal.aborted) {
          const backoffDelay = Math.min(
            maxIntervalMs,
            Math.floor(initialIntervalMs * Math.pow(backoffCoefficient, attemptCount - 1) + Math.random() * 50)
          );
          await new Promise((resolve) => setTimeout(resolve, backoffDelay));
        } else if (isNonRetryable) {
          break;
        }
      }
    }

    const finalErrorMessage = lastError?.message || String(lastError);
    try {
      await WorkflowRunRepository.transitionStatus(
        workflowRunId,
        [WorkflowRunStatus.RUNNING],
        WorkflowRunStatus.FAILED,
        finalErrorMessage
      );
    } catch {
      // preserve lastError
    }

    if (await SagaEngine.hasRegisteredCompensations(workflowRunId)) {
      await SagaEngine.rollbackWorkflowSaga(workflowRunId).catch(() => {});
    }

    throw lastError;
  } finally {
    isHeartbeatRunning = false;
    if (heartbeatTimer) {
      clearTimeout(heartbeatTimer);
    }
    await lock.release();
  }
}
