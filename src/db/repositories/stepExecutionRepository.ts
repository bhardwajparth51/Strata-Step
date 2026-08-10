import { db } from '../db.js';
import { stepExecutions } from '../schema.js';
import { StepExecutionStatus } from '../../types/index.js';
import { StepCheckpointRepository } from './stepCheckpointRepository.js';
import { eq, and, desc } from 'drizzle-orm';

export class StepExecutionRepository {
  static async findLatestCompletedStep(workflowRunId: string, stepName: string) {
    const records = await db
      .select()
      .from(stepExecutions)
      .where(
        and(
          eq(stepExecutions.workflowRunId, workflowRunId),
          eq(stepExecutions.stepName, stepName),
          eq(stepExecutions.status, StepExecutionStatus.COMPLETED)
        )
      )
      .orderBy(desc(stepExecutions.attemptNumber))
      .limit(1);

    return records[0] || null;
  }

  static async findExecutionsForRun(workflowRunId: string) {
    return await db
      .select()
      .from(stepExecutions)
      .where(eq(stepExecutions.workflowRunId, workflowRunId))
      .orderBy(stepExecutions.createdAt);
  }

  static async findLatestStep(workflowRunId: string, stepName: string) {
    const records = await db
      .select()
      .from(stepExecutions)
      .where(
        and(
          eq(stepExecutions.workflowRunId, workflowRunId),
          eq(stepExecutions.stepName, stepName)
        )
      )
      .orderBy(desc(stepExecutions.attemptNumber))
      .limit(1);

    return records[0] || null;
  }

  static async getNextAttemptNumberTx(tx: any, workflowRunId: string, stepName: string): Promise<number> {
    const records = await tx
      .select({ attemptNumber: stepExecutions.attemptNumber })
      .from(stepExecutions)
      .where(
        and(
          eq(stepExecutions.workflowRunId, workflowRunId),
          eq(stepExecutions.stepName, stepName)
        )
      )
      .orderBy(desc(stepExecutions.attemptNumber))
      .limit(1);

    return records.length > 0 ? records[0].attemptNumber + 1 : 1;
  }

  static async getNextAttemptNumber(workflowRunId: string, stepName: string): Promise<number> {
    return await this.getNextAttemptNumberTx(db, workflowRunId, stepName);
  }

  static async createRunningStep(params: {
    workflowRunId: string;
    stepName: string;
    inputPayload: unknown;
    attemptNumber?: number;
    idempotencyKey: string;
  }) {
    return await db.transaction(async (tx) => {
      const attemptNumber = params.attemptNumber ?? await this.getNextAttemptNumberTx(tx, params.workflowRunId, params.stepName);
      
      const [created] = await tx
        .insert(stepExecutions)
        .values({
          workflowRunId: params.workflowRunId,
          stepName: params.stepName,
          status: StepExecutionStatus.RUNNING,
          inputPayload: params.inputPayload as any,
          attemptNumber,
          idempotencyKey: params.idempotencyKey,
        })
        .returning();

      return created;
    });
  }

  static async completeStepTransaction(
    runningStepId: string,
    workflowRunId: string,
    stepName: string,
    outputPayload: unknown,
    executionTimeMs: number,
    attemptNumber: number
  ) {
    return await db.transaction(async (tx) => {
      const [completedStep] = await tx
        .update(stepExecutions)
        .set({
          status: StepExecutionStatus.COMPLETED,
          outputPayload: outputPayload as any,
          executionTimeMs,
        })
        .where(eq(stepExecutions.id, runningStepId))
        .returning();

      await StepCheckpointRepository.upsertCheckpointTx(
        tx,
        workflowRunId,
        stepName,
        completedStep.id,
        outputPayload,
        attemptNumber
      );

      return completedStep;
    });
  }

  static async updateToCompleted(id: string, outputPayload: unknown, executionTimeMs: number) {
    const [updated] = await db
      .update(stepExecutions)
      .set({
        status: StepExecutionStatus.COMPLETED,
        outputPayload: outputPayload as any,
        executionTimeMs,
      })
      .where(eq(stepExecutions.id, id))
      .returning();

    return updated;
  }

  static async updateToFailed(id: string, errorMessage: string, executionTimeMs: number) {
    const [updated] = await db
      .update(stepExecutions)
      .set({
        status: StepExecutionStatus.FAILED,
        errorMessage,
        executionTimeMs,
      })
      .where(eq(stepExecutions.id, id))
      .returning();

    return updated;
  }

  static async invalidateStepsAfter(workflowRunId: string, fromStepName: string, stepOrder: string[]): Promise<number> {
    return await StepCheckpointRepository.deleteDownstreamCheckpoints(workflowRunId, fromStepName, stepOrder);
  }
}
