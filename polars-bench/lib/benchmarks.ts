/**
 * Keep this list in sync with PUBLIC_BENCHMARKS in the FastAPI backend.
 * Source: https://github.com/Nisseya/hack_apr/blob/master/main.py
 */
export const TEST_BENCHMARKS = [
  {
    value: "select",
    label: "Select",
    description: "Column selection and projection",
  },
  {
    value: "filters",
    label: "Filters",
    description: "Row filtering (where clauses, predicates)",
  },
  {
    value: "joins",
    label: "Joins",
    description: "Inner, left, anti joins on multiple tables",
  },
  {
    value: "window_functions",
    label: "Window functions",
    description: "Rolling, rank, lag/lead windowed aggregates",
  },
  {
    value: "aggregations",
    label: "Aggregations",
    description: "Group-by, sum, mean, pivots",
  },
  {
    value: "full_pipeline",
    label: "Full pipeline",
    description: "End-to-end Polars pipelines combining the above",
  },
] as const;

export type TestBenchmark = (typeof TEST_BENCHMARKS)[number]["value"];

export const TEST_BENCHMARK_VALUES = TEST_BENCHMARKS.map(
  (b) => b.value,
) as readonly TestBenchmark[];

export function isValidTestBenchmark(value: unknown): value is TestBenchmark {
  return (
    typeof value === "string" &&
    (TEST_BENCHMARK_VALUES as readonly string[]).includes(value)
  );
}
