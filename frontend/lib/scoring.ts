import type { Submission } from "@/db/schema";

/**
 * Composite score (0..100) taking into account accuracy + performance.
 *
 * Formula:
 *   score = accuracy * 70 + perf * 30
 *
 * Where perf is a normalized inverse of total latency and peak memory.
 * For leaderboards we take the BEST (max composite) submission per team.
 */
export function computeCompositeScore(s: {
  accuracy: number | null;
  avgGenerationSeconds: number | null;
  avgExecutionSeconds: number | null;
  peakRamMb: number | null;
  peakGpuMb: number | null;
}): number {
  const accuracy = s.accuracy ?? 0;

  // Normalize latency: anything under 2s gen is "perfect", above 60s is 0
  const gen = s.avgGenerationSeconds ?? 60;
  const exec = s.avgExecutionSeconds ?? 10;
  const latency = gen + exec;

  // perf_latency in [0, 1]
  const perfLatency = Math.max(0, Math.min(1, 1 - (latency - 2) / 58));

  // Memory normalization: under 4GB = perfect, above 40GB = 0
  const ram = s.peakRamMb ?? 0;
  const gpu = s.peakGpuMb ?? 0;
  const mem = Math.max(ram, gpu);
  const perfMem = Math.max(0, Math.min(1, 1 - (mem - 4096) / (40960 - 4096)));

  const perf = perfLatency * 0.6 + perfMem * 0.4;

  return accuracy * 70 + perf * 30;
}
