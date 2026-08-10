# StratStep — Durable Execution & Workflow Engine

[![TypeScript](https://img.shields.io/badge/TypeScript-5.3-blue.svg)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-v20+-green.svg)](https://nodejs.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7-red.svg)](https://redis.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**StratStep** is a high-performance, fault-tolerant **Durable Execution Engine** built with Node.js, TypeScript, PostgreSQL (Drizzle ORM), and Redis.

It guarantees **at-most-once step execution under concurrency** and **at-least-once workflow completion**, protecting long-running background workflows against process crashes, worker network partitioning, stampeding herd API spikes, and distributed race conditions.

---

## 🚀 Key Features

### 🔒 1. Dual-Layer Distributed Locking with Failover
- **Primary Redis Locking:** Uses single-key `SET key token PX ttl NX` locking with atomic Lua release scripts checking token ownership (`KEYS[1] == ARGV[1]`).
- **PostgreSQL 64-Bit Advisory Lock Failover:** If Redis becomes unreachable, `StepLock` automatically fails over to PostgreSQL 64-bit advisory locks (`pg_try_advisory_lock(int4, int4)`) derived from SHA256 digest chunks, eliminating collision risks across millions of step keys.
- **Lock Renewal Heartbeat:** Long-running step executions automatically renew lock TTLs every `ttl / 2` via background heartbeat timers (`unref()`'d).
- **AbortSignal Integration:** If a lock is lost during step execution, an `AbortController` signal cancels active downstream HTTP/Stripe API requests immediately.

### ⚡ 2. Double-Check Checkpoint Cache
- Prevents the **stampeding herd problem**.
- Checks `StepCheckpointRepository.findActiveCheckpoint()` before lock acquisition (fast path) and re-checks *after* acquiring the distributed lock.
- If multiple worker processes attempt to run the same step simultaneously, only **one** worker executes the step function, while the remaining workers receive the cached result instantly.

### 🔄 3. State-Preserving Workflow Replays & Invalidation
- Supports replaying workflow runs from any historical step.
- Soft-invalidates downstream checkpoints (`isActive = false`) for re-execution while preserving historical execution logs in `step_executions` for auditing.

### 🛡️ 4. Saga Compensations & Crash Recovery
- Supports forward-execution and reverse (LIFO) Saga compensation rollbacks on step failures.
- Dual in-memory closure registration and string-registered compensation handlers for multi-worker process safety.
- Sweeps stale workflow runs whose heartbeat timed out and transitions them to `FAILED` with diagnostic logs.

---

## 🏗️ System Architecture

```text
                        ┌──────────────────────────────┐
                        │      StratStep Engine        │
                        │                              │
                        │   executeStep()              │
                        │   replay()                   │
                        │   recoverStale()             │
                        └──────────────┬───────────────┘
                                       │
                ┌──────────────────────┼──────────────────────┐
                │                      │                      │
       ┌────────▼────────┐    ┌────────▼────────┐    ┌────────▼────────┐
       │   PostgreSQL    │    │      Redis      │    │ External APIs   │
       │                 │    │                 │    │                 │
       │ Workflow Ledger │    │ Distributed     │    │ Stripe / OpenAI │
       │ Step Executions │    │ Locks           │    │ (Idempotency)   │
       │ Checkpoints     │    │ Heartbeat Lease │    │                 │
       └─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## 🗄️ Database Schema & Data Model

StratStep uses PostgreSQL via [Drizzle ORM](https://orm.drizzle.team/):

- `workflows`: Workflow definitions and versioning.
- `workflow_runs`: Execution runs with optimistic concurrency control (`version`), `status` (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, `PAUSED`), and heartbeat tracking.
- `step_executions`: Immutable event ledger storing input/output payloads, execution time, attempt numbers, idempotency keys, and status (`RUNNING`, `COMPLETED`, `FAILED`, `SKIPPED`, `INVALIDATED`).
- `step_checkpoints`: Active output checkpoints keyed by `(workflow_run_id, step_name)`.
- `saga_compensations`: Saga compensation registrations for automatic rollback.

---

## 🛠️ Quickstart & Local Setup

### 1. Prerequisites
- **Node.js**: v20+
- **Docker & Docker Compose** (optional, for local Postgres & Redis)

### 2. Install Dependencies
```bash
npm install
```

### 3. Start PostgreSQL & Redis Services
```bash
docker-compose up -d
```

### 4. Build TypeScript
```bash
npm run build
```

### 5. Seed Database
```bash
npm run db:seed
```

---

## 🧪 Testing

StratStep includes comprehensive unit tests, production hardening tests, and stress tests:

```bash
# Run unit test suite (StepLock, executeStep, heartbeats, OCC, SAGAs)
npm run test:unit

# Run production-level integration test suite
npm run test:prod-level

# Run production hardening tests
npm run test:hardening
```

---

## 💻 Code Example: Basic Workflow Step

```typescript
import Redis from 'ioredis';
import { StratStepEngine } from 'stratstep';

const redis = new Redis();
const engine = new StratStepEngine({ redis });

async function processOrder(workflowRunId: string) {
  // Execute Step 1: Charge Payment
  const paymentResult = await engine.executeStep({
    workflowRunId,
    stepName: 'chargePayment',
    inputPayload: { amount: 4900, currency: 'usd' },
    retryPolicy: { maxAttempts: 3, initialIntervalMs: 1000 },
    compensate: async (input, ctx) => {
      console.log(`[Saga] Refunding payment for run ${ctx.workflowRunId}`);
    },
    stepFn: async (input, ctx) => {
      // Input payload & StepContext provided
      return { chargeId: 'ch_12345', status: 'paid' };
    },
  });

  console.log('Step 1 completed:', paymentResult.outputPayload);
}
```

---

## 📄 License

This project is open-source software licensed under the [MIT License](LICENSE).
