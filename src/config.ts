import 'dotenv/config';

export const config = {
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/conveyor',
  testDatabaseUrl:
    process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/conveyor_test',
  port: Number(process.env.PORT ?? 3000),
  demoMode: (process.env.DEMO_MODE ?? 'true').toLowerCase() === 'true',
};
