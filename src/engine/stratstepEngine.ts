import { Redis } from 'ioredis';
import { executeStep, ExecuteStepParams } from './executeStep.js';
import { replayWorkflow } from './replayWorkflow.js';
import { recoverStaleWorkflowRuns } from './recovery.js';
import { SagaEngine } from './sagaEngine.js';
import { StepCompensateFn, StepOutput } from '../types/index.js';

export interface StratStepEngineConfig {
  redis: Redis;
}

export class StratStepEngine {
  constructor(private config: StratStepEngineConfig) {}

  async executeStep<TInput, TOutput>(
    params: Omit<ExecuteStepParams<TInput, TOutput>, 'redis'>
  ): Promise<StepOutput<TOutput>> {
    return executeStep({
      ...params,
      redis: this.config.redis,
    });
  }

  registerCompensationHandler(name: string, fn: StepCompensateFn) {
    SagaEngine.registerHandler(name, fn);
  }

  async replay(workflowRunId: string, fromStepName: string, stepOrder: string[]) {
    return replayWorkflow(workflowRunId, fromStepName, stepOrder);
  }

  async recoverStale(staleTimeoutMs = 30000) {
    return recoverStaleWorkflowRuns(staleTimeoutMs);
  }

  async rollbackSaga(workflowRunId: string) {
    return SagaEngine.rollbackWorkflowSaga(workflowRunId);
  }
}
