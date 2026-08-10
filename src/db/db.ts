import { drizzle } from 'drizzle-orm/node-postgres';
import pkg from 'pg';
const { Pool } = pkg;
import * as schema from './schema.js';

const connectionString = process.env.DATABASE_URL || 'postgres://vaultflow:vaultflow@localhost:5432/vaultflow';

export const pool = new Pool({
  connectionString,
  max: 25,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

export const db = drizzle(pool, { schema });
