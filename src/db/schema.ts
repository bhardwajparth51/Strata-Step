import { pgTable, uuid, varchar, text, timestamp, integer, jsonb, unique, index, primaryKey, boolean, pgEnum } from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';

export const workflowRunStatusEnum = pgEnum('workflow_run_status', [
  'PENDING',
  'RUNNING',
  'COMPLETED',
  'FAILED',
  'CANCELLED',
  'PAUSED',
]);

export const stepExecutionStatusEnum = pgEnum('step_execution_status', [
  'RUNNING',
  'COMPLETED',
  'FAILED',
  'SKIPPED',
]);

export const workflows = pgTable('workflows', {
  id: uuid('id').defaultRandom().primaryKey(),
  name: varchar('name', { length: 255 }).notNull().unique(),
  description: text('description'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
});

export const workflowRuns = pgTable('workflow_runs', {
  id: uuid('id').defaultRandom().primaryKey(),
  workflowId: uuid('workflow_id').notNull().references(() => workflows.id, { onDelete: 'cascade' }),
  status: workflowRunStatusEnum('status').default('PENDING').notNull(),
  currentStep: varchar('current_step', { length: 255 }),
  errorSummary: text('error_summary'),
  workflowVersion: integer('workflow_version').default(1).notNull(),
  schemaVersion: varchar('schema_version', { length: 50 }).default('v1').notNull(),
  version: integer('version').default(1).notNull(),
  startedAt: timestamp('started_at', { withTimezone: true }).defaultNow().notNull(),
  heartbeatAt: timestamp('heartbeat_at', { withTimezone: true }),
  completedAt: timestamp('completed_at', { withTimezone: true }),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  idxStatus: index('idx_workflow_runs_status').on(table.status),
  idxWorkflowId: index('idx_workflow_runs_workflow_id').on(table.workflowId),
  idxStatusHeartbeat: index('idx_runs_status_heartbeat').on(table.status, table.heartbeatAt),
}));

export const stepExecutions = pgTable('step_executions', {
  id: uuid('id').defaultRandom().primaryKey(),
  workflowRunId: uuid('workflow_run_id').notNull().references(() => workflowRuns.id, { onDelete: 'cascade' }),
  stepName: varchar('step_name', { length: 255 }).notNull(),
  status: stepExecutionStatusEnum('status').notNull(),
  inputPayload: jsonb('input_payload').default(sql`'{}'::jsonb`).notNull(),
  outputPayload: jsonb('output_payload'),
  errorMessage: text('error_message'),
  executionTimeMs: integer('execution_time_ms'),
  attemptNumber: integer('attempt_number').default(1).notNull(),
  idempotencyKey: varchar('idempotency_key', { length: 255 }),
  workflowVersion: integer('workflow_version').default(1).notNull(),
  schemaVersion: varchar('schema_version', { length: 50 }).default('v1').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  idxRunStep: index('idx_step_executions_run_step').on(table.workflowRunId, table.stepName),
  idxIdempotencyKey: index('idx_idempotency_key').on(table.idempotencyKey),
  uqStepAttempt: unique('uq_step_attempt').on(table.workflowRunId, table.stepName, table.attemptNumber),
}));

export const stepCheckpoints = pgTable('step_checkpoints', {
  workflowRunId: uuid('workflow_run_id').notNull().references(() => workflowRuns.id, { onDelete: 'cascade' }),
  stepName: varchar('step_name', { length: 255 }).notNull(),
  stepExecutionId: uuid('step_execution_id').notNull().references(() => stepExecutions.id, { onDelete: 'cascade' }),
  outputPayload: jsonb('output_payload'),
  attemptNumber: integer('attempt_number').notNull(),
  isActive: boolean('is_active').default(true).notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.workflowRunId, table.stepName] }),
  idxCheckpointsRunActive: index('idx_checkpoints_run_active').on(table.workflowRunId, table.isActive),
}));

export const sagaCompensations = pgTable('saga_compensations', {
  id: uuid('id').defaultRandom().primaryKey(),
  workflowRunId: uuid('workflow_run_id').notNull().references(() => workflowRuns.id, { onDelete: 'cascade' }),
  stepName: varchar('step_name', { length: 255 }).notNull(),
  compensationName: varchar('compensation_name', { length: 255 }).notNull(),
  inputPayload: jsonb('input_payload').default(sql`'{}'::jsonb`).notNull(),
  status: varchar('status', { length: 50 }).default('PENDING').notNull(),
  errorMessage: text('error_message'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  idxSagaRunStep: index('idx_saga_compensations_run_step').on(table.workflowRunId, table.stepName),
}));
