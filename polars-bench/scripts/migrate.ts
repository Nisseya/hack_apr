import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import postgres from "postgres";

const url =
  process.env.DATABASE_URL ||
  "postgresql://postgres:postgres@localhost:5432/polars_bench";

async function main() {
  console.log("→ connecting to", url.replace(/:[^:@]*@/, ":***@"));
  const client = postgres(url, { max: 1 });
  const db = drizzle(client);

  console.log("→ running migrations");
  await migrate(db, { migrationsFolder: "./db/migrations" });

  console.log("✓ migrations applied");
  await client.end();
}

main().catch((err) => {
  console.error("migration failed", err);
  process.exit(1);
});
