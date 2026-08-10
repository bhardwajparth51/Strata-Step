import { StepExecutionRepository } from '../db/repositories/stepExecutionRepository.js';
import { SagaCompensationRepository } from '../db/repositories/sagaCompensationRepository.js';
import { SagaRollbackResult, StepCompensateFn, StepContext, StepExecutionStatus } from '../types/index.js';

interface RegisteredCompensation {
  stepName: string;
  compensateFn: StepCompensateFn;
  compensationName?: string;
  inputPayload: any;
  context: StepContext;
}

export class SagaEngine {
  private static compensationRegistry = new Map<string, Map<string, RegisteredCompensation>>();
  private static handlerRegistry = new Map<string, StepCompensateFn>();

  static registerHandler(name: string, compensateFn: StepCompensateFn) {
    this.handlerRegistry.set(name, compensateFn);
  }

  static async registerCompensation(
    workflowRunId: string,
    stepName: string,
    compensate: StepCompensateFn | string,
    inputPayload: any,
    context: StepContext
  ) {
    let compensateFn: StepCompensateFn | undefined;
    let compensationName: string | undefined;

    if (typeof compensate === 'string') {
      compensationName = compensate;
      compensateFn = this.handlerRegistry.get(compensationName);
    } else {
      compensateFn = compensate;
      compensationName = `${stepName}_compensate`;
    }

    if (!this.compensationRegistry.has(workflowRunId)) {
      this.compensationRegistry.set(workflowRunId, new Map());
    }

    if (compensateFn) {
      this.compensationRegistry.get(workflowRunId)!.set(stepName, {
        stepName,
        compensateFn,
        compensationName,
        inputPayload,
        context,
      });
    }

    try {
      await SagaCompensationRepository.createCompensation({
        workflowRunId,
        stepName,
        compensationName: compensationName || stepName,
        inputPayload,
      });
    } catch {
      // non-blocking DB persistence log for saga compensation record
    }
  }

  static async hasRegisteredCompensations(workflowRunId: string): Promise<boolean> {
    const runRegistry = this.compensationRegistry.get(workflowRunId);
    if (runRegistry && runRegistry.size > 0) {
      return true;
    }

    try {
      const dbCompensations = await SagaCompensationRepository.findPendingCompensationsForRun(workflowRunId);
      return dbCompensations.length > 0;
    } catch {
      return false;
    }
  }

  static async rollbackWorkflowSaga(workflowRunId: string): Promise<SagaRollbackResult> {
    const runRegistry = this.compensationRegistry.get(workflowRunId);
    const compensatedSteps: string[] = [];
    const failedCompensations: Array<{ stepName: string; error: string }> = [];

    let dbCompensations: any[] = [];
    try {
      dbCompensations = await SagaCompensationRepository.findPendingCompensationsForRun(workflowRunId);
    } catch {
      // fallback to memory registry if DB check fails
    }

    const completedExecutions = await StepExecutionRepository.findExecutionsForRun(workflowRunId);
    const completedSteps = completedExecutions
      .filter((e) => e.status === StepExecutionStatus.COMPLETED)
      .reverse();

    for (const stepExec of completedSteps) {
      const memReg = runRegistry?.get(stepExec.stepName);
      const dbReg = dbCompensations.find((c) => c.stepName === stepExec.stepName);

      const fn = memReg?.compensateFn || (dbReg ? this.handlerRegistry.get(dbReg.compensationName) : undefined);
      const inputPayload = memReg?.inputPayload || dbReg?.inputPayload;
      const context: StepContext = memReg?.context || {
        workflowRunId,
        stepName: stepExec.stepName,
        attemptNumber: stepExec.attemptNumber,
        idempotencyKey: stepExec.idempotencyKey || '',
        abortSignal: new AbortController().signal,
      };

      if (fn) {
        try {
          await fn(inputPayload, context);
          compensatedSteps.push(stepExec.stepName);
          if (dbReg) {
            await SagaCompensationRepository.markCompensated(dbReg.id).catch(() => {});
          }
        } catch (err: any) {
          const errMsg = err?.message || String(err);
          if (dbReg) {
            await SagaCompensationRepository.markFailed(dbReg.id, errMsg).catch(() => {});
          }
          failedCompensations.push({
            stepName: stepExec.stepName,
            error: errMsg,
          });
        }
      }
    }

    this.compensationRegistry.delete(workflowRunId);
    return {
      workflowRunId,
      compensatedSteps,
      failedCompensations,
      success: failedCompensations.length === 0,
    };
  }

  static clearRegistry(workflowRunId: string) {
    this.compensationRegistry.delete(workflowRunId);
  }
}
