import { db } from '../db/db.js';
import { StepExecutionRepository } from '../db/repositories/stepExecutionRepository.js';
import { StepCheckpointRepository } from '../db/repositories/stepCheckpointRepository.js';
import { WorkflowRunRepository } from '../db/repositories/workflowRunRepository.js';
import { WorkflowRunStatus } from '../types/index.js';

export async function replayWorkflow(
  workflowRunId: string,
  fromStepName: string,
  stepOrder: string[]
) {
  if (!stepOrder || stepOrder.length === 0) {
    throw new Error('Replay rejected: stepOrder array is required.');
  }

  const fromIndex = stepOrder.indexOf(fromStepName);
  if (fromIndex === -1) {
    throw new Error(`Replay rejected: step '${fromStepName}' is not defined in stepOrder.`);
  }

  const run = await WorkflowRunRepository.findById(workflowRunId);
  if (!run) {
    throw new Error(`Replay rejected: workflow run '${workflowRunId}' not found.`);
  }

  const stepRecord = await StepExecutionRepository.findLatestStep(workflowRunId, fromStepName);
  if (!stepRecord) {
    throw new Error(`Replay rejected: step '${fromStepName}' not found in execution history.`);
  }

  const allowedReplayStatuses: WorkflowRunStatus[] = [
    WorkflowRunStatus.FAILED,
    WorkflowRunStatus.PAUSED,
    WorkflowRunStatus.COMPLETED,
  ];

  const replayResult = await db.transaction(async (tx) => {
    const invalidatedCount = await StepCheckpointRepository.deleteDownstreamCheckpointsTx(
      tx,
      workflowRunId,
      fromStepName,
      stepOrder
    );

    const updatedRun = await WorkflowRunRepository.transitionStatusTx(
      tx,
      workflowRunId,
      allowedReplayStatuses,
      WorkflowRunStatus.RUNNING
    );

    if (!updatedRun) {
      throw new Error(`Replay rejected: run '${workflowRunId}' in status '${run.status}' cannot be replayed.`);
    }

    return {
      updatedRun,
      invalidatedCount,
    };
  });

  return {
    ...replayResult.updatedRun,
    invalidatedCount: replayResult.invalidatedCount,
  };
}
