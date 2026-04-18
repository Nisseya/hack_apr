import {
  pgTable,
  text,
  timestamp,
  boolean,
  integer,
  jsonb,
  real,
  pgEnum,
  primaryKey,
  unique,
} from "drizzle-orm/pg-core";
import { createId } from "@/lib/ids";
import { relations } from "drizzle-orm";

// ---- better-auth core tables (names/columns must match better-auth defaults) ----

export const user = pgTable("user", {
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  email: text("email").notNull().unique(),
  emailVerified: boolean("emailVerified").notNull().default(false),
  image: text("image"),
  createdAt: timestamp("createdAt").notNull().defaultNow(),
  updatedAt: timestamp("updatedAt").notNull().defaultNow(),
});

export const session = pgTable("session", {
  id: text("id").primaryKey(),
  expiresAt: timestamp("expiresAt").notNull(),
  token: text("token").notNull().unique(),
  createdAt: timestamp("createdAt").notNull().defaultNow(),
  updatedAt: timestamp("updatedAt").notNull().defaultNow(),
  ipAddress: text("ipAddress"),
  userAgent: text("userAgent"),
  userId: text("userId")
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
});

export const account = pgTable("account", {
  id: text("id").primaryKey(),
  accountId: text("accountId").notNull(),
  providerId: text("providerId").notNull(),
  userId: text("userId")
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
  accessToken: text("accessToken"),
  refreshToken: text("refreshToken"),
  idToken: text("idToken"),
  accessTokenExpiresAt: timestamp("accessTokenExpiresAt"),
  refreshTokenExpiresAt: timestamp("refreshTokenExpiresAt"),
  scope: text("scope"),
  password: text("password"),
  createdAt: timestamp("createdAt").notNull().defaultNow(),
  updatedAt: timestamp("updatedAt").notNull().defaultNow(),
});

export const verification = pgTable("verification", {
  id: text("id").primaryKey(),
  identifier: text("identifier").notNull(),
  value: text("value").notNull(),
  expiresAt: timestamp("expiresAt").notNull(),
  createdAt: timestamp("createdAt").notNull().defaultNow(),
  updatedAt: timestamp("updatedAt").notNull().defaultNow(),
});

// ---- Application domain ----

export const submissionKindEnum = pgEnum("submission_kind", ["test", "global"]);
export const submissionStatusEnum = pgEnum("submission_status", [
  "queued",
  "running",
  "done",
  "failed",
]);
export const joinStatusEnum = pgEnum("join_request_status", [
  "pending",
  "accepted",
  "rejected",
]);

export const team = pgTable("team", {
  id: text("id").primaryKey().$defaultFn(() => createId()),
  name: text("name").notNull().unique(),
  slug: text("slug").notNull().unique(),
  description: text("description"),
  vmUrl: text("vm_url"),
  ownerId: text("owner_id")
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

export const teamMember = pgTable(
  "team_member",
  {
    teamId: text("team_id")
      .notNull()
      .references(() => team.id, { onDelete: "cascade" }),
    userId: text("user_id")
      .notNull()
      .references(() => user.id, { onDelete: "cascade" }),
    role: text("role").notNull().default("member"), // 'owner' | 'member'
    joinedAt: timestamp("joined_at").notNull().defaultNow(),
  },
  (t) => ({
    pk: primaryKey({ columns: [t.teamId, t.userId] }),
    // a user can only be member of ONE team
    uniqueUser: unique("team_member_unique_user").on(t.userId),
  }),
);

export const joinRequest = pgTable(
  "join_request",
  {
    id: text("id").primaryKey().$defaultFn(() => createId()),
    teamId: text("team_id")
      .notNull()
      .references(() => team.id, { onDelete: "cascade" }),
    userId: text("user_id")
      .notNull()
      .references(() => user.id, { onDelete: "cascade" }),
    status: joinStatusEnum("status").notNull().default("pending"),
    message: text("message"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    resolvedAt: timestamp("resolved_at"),
  },
  (t) => ({
    uniquePendingPerTeam: unique("join_request_unique_pending").on(
      t.teamId,
      t.userId,
    ),
  }),
);

export const submission = pgTable("submission", {
  id: text("id").primaryKey().$defaultFn(() => createId()),
  teamId: text("team_id")
    .notNull()
    .references(() => team.id, { onDelete: "cascade" }),
  submittedByUserId: text("submitted_by_user_id")
    .notNull()
    .references(() => user.id, { onDelete: "set null" }),
  repoUrl: text("repo_url").notNull(),
  kind: submissionKindEnum("kind").notNull(),
  /**
   * For kind='test': one of the public benchmark names accepted by the
   * FastAPI backend ('select' | 'filters' | 'joins' | 'window_functions' |
   * 'aggregations' | 'full_pipeline'), or null = backend default benchmark.
   * For kind='global': always null (the backend uses the final benchmark).
   */
  benchmarkName: text("benchmark_name"),
  status: submissionStatusEnum("status").notNull().default("queued"),
  // Aggregates (filled as benchmark runs / when done)
  totalQuestions: integer("total_questions").notNull().default(0),
  completedQuestions: integer("completed_questions").notNull().default(0),
  exactMatchCount: integer("exact_match_count").notNull().default(0),
  avgGenerationSeconds: real("avg_generation_seconds"),
  avgExecutionSeconds: real("avg_execution_seconds"),
  peakRamMb: real("peak_ram_mb"),
  peakGpuMb: real("peak_gpu_mb"),
  compositeScore: real("composite_score"),
  accuracy: real("accuracy"), // 0..1
  /**
   * For kind='global': the raw score returned by POST /submit_final.
   * Used directly for the global leaderboard ranking (higher is better).
   */
  finalScore: real("final_score"),
  errorMessage: text("error_message"),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  startedAt: timestamp("started_at"),
  finishedAt: timestamp("finished_at"),
});

export const benchmarkResult = pgTable(
  "benchmark_result",
  {
    id: text("id").primaryKey().$defaultFn(() => createId()),
    submissionId: text("submission_id")
      .notNull()
      .references(() => submission.id, { onDelete: "cascade" }),
    questionId: text("question_id").notNull(),
    questionText: text("question_text").notNull(),
    generatedCode: text("generated_code"),
    stdout: text("stdout"),
    stderr: text("stderr"),
    success: boolean("success"),
    exactMatch: boolean("exact_match"),
    generationDurationSeconds: real("generation_duration_seconds"),
    executionDurationSeconds: real("execution_duration_seconds"),
    peakRamMb: real("peak_ram_mb"),
    peakGpuMb: real("peak_gpu_mb"),
    generatedHash: text("generated_hash"),
    goldHash: text("gold_hash"),
    generatedShape: jsonb("generated_shape"),
    goldShape: jsonb("gold_shape"),
    generatedColumns: jsonb("generated_columns"),
    goldColumns: jsonb("gold_columns"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (t) => ({
    uniqueQuestion: unique("benchmark_result_unique_question").on(
      t.submissionId,
      t.questionId,
    ),
  }),
);

// ---- Relations ----

export const userRelations = relations(user, ({ one, many }) => ({
  membership: one(teamMember, {
    fields: [user.id],
    references: [teamMember.userId],
  }),
  ownedTeams: many(team),
}));

export const teamRelations = relations(team, ({ one, many }) => ({
  owner: one(user, { fields: [team.ownerId], references: [user.id] }),
  members: many(teamMember),
  submissions: many(submission),
  joinRequests: many(joinRequest),
}));

export const teamMemberRelations = relations(teamMember, ({ one }) => ({
  team: one(team, { fields: [teamMember.teamId], references: [team.id] }),
  user: one(user, { fields: [teamMember.userId], references: [user.id] }),
}));

export const joinRequestRelations = relations(joinRequest, ({ one }) => ({
  team: one(team, { fields: [joinRequest.teamId], references: [team.id] }),
  user: one(user, { fields: [joinRequest.userId], references: [user.id] }),
}));

export const submissionRelations = relations(submission, ({ one, many }) => ({
  team: one(team, { fields: [submission.teamId], references: [team.id] }),
  submittedBy: one(user, {
    fields: [submission.submittedByUserId],
    references: [user.id],
  }),
  results: many(benchmarkResult),
}));

export const benchmarkResultRelations = relations(benchmarkResult, ({ one }) => ({
  submission: one(submission, {
    fields: [benchmarkResult.submissionId],
    references: [submission.id],
  }),
}));

// ---- Types ----
export type User = typeof user.$inferSelect;
export type Team = typeof team.$inferSelect;
export type TeamMember = typeof teamMember.$inferSelect;
export type JoinRequest = typeof joinRequest.$inferSelect;
export type Submission = typeof submission.$inferSelect;
export type BenchmarkResult = typeof benchmarkResult.$inferSelect;
