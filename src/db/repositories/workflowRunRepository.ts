import { db } from '../db.js';
import { workflowRuns } from '../schema.js';
import { WorkflowRunStatus } from '../../types/index.js';
import { eq, and, inArray, sql, lt, or, isNull } from 'drizzle-orm';

export class WorkflowRunRepository {
  static async findById(id: string) {
    return await db.query.workflowRuns.findFirst({
      where: eq(workflowRuns.id, id),
    });
  }

  static async updateHeartbeat(id: string) {
    const [updated] = await db
      .update(workflowRuns)
      .set({ heartbeatAt: new Date() })
      .where(eq(workflowRuns.id, id))
      .returning();

    return updated;
  }

  static async findStaleRuns(staleTimeoutMs = 30000) {
    const cutoff = new Date(Date.now() - staleTimeoutMs);

    return await db.query.workflowRuns.findMany({
      where: and(
        eq(workflowRuns.status, WorkflowRunStatus.RUNNING),
        or(
          lt(workflowRuns.heartbeatAt, cutoff),
          and(isNull(workflowRuns.heartbeatAt), lt(workflowRuns.startedAt, cutoff))
        )
      ),
    });
  }

  static async transitionStatusTx(
    tx: any,
    id: string,
    fromStatuses: WorkflowRunStatus[],
    toStatus: WorkflowRunStatus,
    errorSummary?: string,
    expectedVersion?: number
  ) {
    const values: any = {
      status: toStatus,
      version: sql`${workflowRuns.version} + 1`,
    };

    if (toStatus === WorkflowRunStatus.COMPLETED || toStatus === WorkflowRunStatus.FAILED) {
      values.completedAt = new Date();
    } else if (toStatus === WorkflowRunStatus.RUNNING) {
      values.completedAt = null;
      values.errorSummary = null;
      values.heartbeatAt = new Date();
    }

    if (toStatus === WorkflowRunStatus.FAILED && errorSummary) {
      values.errorSummary = errorSummary;
    }

    const whereConditions = [
      eq(workflowRuns.id, id),
      inArray(workflowRuns.status, fromStatuses),
    ];

    if (expectedVersion !== undefined) {
      whereConditions.push(eq(workflowRuns.version, expectedVersion));
    }

    const [updated] = await tx
      .update(workflowRuns)
      .set(values)
      .where(and(...whereConditions))
      .returning();

    return updated || null;
  }

  static async transitionStatus(
    id: string,
    fromStatuses: WorkflowRunStatus[],
    toStatus: WorkflowRunStatus,
    errorSummary?: string,
    expectedVersion?: number
  ) {
    return await this.transitionStatusTx(db, id, fromStatuses, toStatus, errorSummary, expectedVersion);
  }

  static async updateStatus(id: string, status: WorkflowRunStatus, errorSummary?: string) {
    const allStatuses = Object.values(WorkflowRunStatus);
    return await this.transitionStatus(id, allStatuses, status, errorSummary);
  }

  static async updateStatusFromStatuses(
    id: string,
    newStatus: WorkflowRunStatus,
    expectedStatuses: WorkflowRunStatus[],
    errorSummary?: string
  ) {
    return await this.transitionStatus(id, expectedStatuses, newStatus, errorSummary);
  }

  static async setCurrentStep(id: string, stepName: string) {
    const [updated] = await db
      .update(workflowRuns)
      .set({ currentStep: stepName })
      .where(eq(workflowRuns.id, id))
      .returning();

    return updated;
  }
}
