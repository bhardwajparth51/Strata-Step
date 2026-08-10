import { db } from '../db.js';
import { stepCheckpoints } from '../schema.js';
import { eq, and, inArray } from 'drizzle-orm';

export class StepCheckpointRepository {
  static async findActiveCheckpoint(workflowRunId: string, stepName: string) {
    return await db.query.stepCheckpoints.findFirst({
      where: and(
        eq(stepCheckpoints.workflowRunId, workflowRunId),
        eq(stepCheckpoints.stepName, stepName),
        eq(stepCheckpoints.isActive, true)
      ),
    });
  }

  static async upsertCheckpointTx(
    tx: any,
    workflowRunId: string,
    stepName: string,
    stepExecutionId: string,
    outputPayload: any,
    attemptNumber: number
  ) {
    const values = {
      workflowRunId,
      stepName,
      stepExecutionId,
      outputPayload: outputPayload ?? {},
      attemptNumber,
      isActive: true,
      updatedAt: new Date(),
    };

    const [checkpoint] = await tx
      .insert(stepCheckpoints)
      .values(values)
      .onConflictDoUpdate({
        target: [stepCheckpoints.workflowRunId, stepCheckpoints.stepName],
        set: {
          stepExecutionId,
          outputPayload: outputPayload ?? {},
          attemptNumber,
          isActive: true,
          updatedAt: new Date(),
        },
      })
      .returning();

    return checkpoint;
  }

  static async upsertCheckpoint(
    workflowRunId: string,
    stepName: string,
    stepExecutionId: string,
    outputPayload: any,
    attemptNumber: number
  ) {
    return await this.upsertCheckpointTx(
      db,
      workflowRunId,
      stepName,
      stepExecutionId,
      outputPayload,
      attemptNumber
    );
  }

  static async deleteDownstreamCheckpointsTx(
    tx: any,
    workflowRunId: string,
    fromStepName: string,
    stepOrder: string[]
  ): Promise<number> {
    const fromIndex = stepOrder.indexOf(fromStepName);
    if (fromIndex === -1) {
      throw new Error(`Step '${fromStepName}' is not defined in stepOrder.`);
    }

    const replaySteps = stepOrder.slice(fromIndex);
    if (replaySteps.length === 0) {
      return 0;
    }

    const updatedRows = await tx
      .update(stepCheckpoints)
      .set({ isActive: false, updatedAt: new Date() })
      .where(
        and(
          eq(stepCheckpoints.workflowRunId, workflowRunId),
          inArray(stepCheckpoints.stepName, replaySteps),
          eq(stepCheckpoints.isActive, true)
        )
      )
      .returning();

    return updatedRows.length;
  }

  static async deleteDownstreamCheckpoints(
    workflowRunId: string,
    fromStepName: string,
    stepOrder: string[]
  ): Promise<number> {
    return await this.deleteDownstreamCheckpointsTx(db, workflowRunId, fromStepName, stepOrder);
  }
}
