import { WorkflowRunRepository } from '../db/repositories/workflowRunRepository.js';
import { WorkflowRunStatus } from '../types/index.js';

export interface RecoverStaleRunsResult {
  recoveredCount: number;
  recoveredRunIds: string[];
}

export async function recoverStaleWorkflowRuns(
  staleTimeoutMs: number = 30000
): Promise<RecoverStaleRunsResult> {
  const staleRuns = await WorkflowRunRepository.findStaleRuns(staleTimeoutMs);
  const recoveredRunIds: string[] = [];

  for (const run of staleRuns) {
    const errorSummary = `Worker crash detected: run '${run.id}' heartbeat timed out`;
    const updated = await WorkflowRunRepository.transitionStatus(
      run.id,
      [WorkflowRunStatus.RUNNING],
      WorkflowRunStatus.FAILED,
      errorSummary
    );

    if (updated) {
      recoveredRunIds.push(run.id);
    }
  }

  return {
    recoveredCount: recoveredRunIds.length,
    recoveredRunIds,
  };
}
