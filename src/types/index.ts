export * from '../engine/errors.js';

export enum WorkflowRunStatus {
  PENDING = 'PENDING',
  RUNNING = 'RUNNING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
  PAUSED = 'PAUSED',
}


export enum StepExecutionStatus {
  RUNNING = 'RUNNING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  SKIPPED = 'SKIPPED',
}

export interface RetryPolicy {
  maxAttempts?: number;
  initialIntervalMs?: number;
  backoffCoefficient?: number;
  maxIntervalMs?: number;
}

export interface StepContext {
  workflowRunId: string;
  stepName: string;
  attemptNumber: number;
  idempotencyKey: string;
  abortSignal: AbortSignal;
}

export type StepCompensateFn<TInput = any> = (
  inputPayload: TInput,
  context: StepContext
) => Promise<void>;

export interface StepInput<T = unknown> {
  workflowRunId: string;
  stepName: string;
  payload: T;
  idempotencyKey?: string;
  retryPolicy?: RetryPolicy;
  compensate?: StepCompensateFn<T>;
}

export interface StepOutput<T = unknown> {
  stepExecutionId: string;
  stepName: string;
  status: StepExecutionStatus;
  outputPayload: T;
  cached: boolean;
  attemptNumber: number;
}

export interface SagaRollbackResult {
  workflowRunId: string;
  compensatedSteps: string[];
  failedCompensations: Array<{ stepName: string; error: string }>;
  success: boolean;
}



