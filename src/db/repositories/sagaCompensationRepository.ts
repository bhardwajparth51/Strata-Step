import { db } from '../db.js';
import { sagaCompensations } from '../schema.js';
import { eq, and, desc } from 'drizzle-orm';

export interface CreateSagaCompensationParams {
  workflowRunId: string;
  stepName: string;
  compensationName: string;
  inputPayload: unknown;
}

export class SagaCompensationRepository {
  static async createCompensation(params: CreateSagaCompensationParams) {
    const [record] = await db
      .insert(sagaCompensations)
      .values({
        workflowRunId: params.workflowRunId,
        stepName: params.stepName,
        compensationName: params.compensationName,
        inputPayload: params.inputPayload as any,
        status: 'PENDING',
      })
      .returning();

    return record;
  }

  static async findPendingCompensationsForRun(workflowRunId: string) {
    return await db
      .select()
      .from(sagaCompensations)
      .where(
        and(
          eq(sagaCompensations.workflowRunId, workflowRunId),
          eq(sagaCompensations.status, 'PENDING')
        )
      )
      .orderBy(desc(sagaCompensations.createdAt));
  }

  static async markCompensated(id: string) {
    const [updated] = await db
      .update(sagaCompensations)
      .set({ status: 'COMPENSATED' })
      .where(eq(sagaCompensations.id, id))
      .returning();

    return updated;
  }

  static async markFailed(id: string, errorMessage: string) {
    const [updated] = await db
      .update(sagaCompensations)
      .set({ status: 'FAILED', errorMessage })
      .where(eq(sagaCompensations.id, id))
      .returning();

    return updated;
  }
}
