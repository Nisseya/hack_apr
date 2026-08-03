import { Badge } from "./ui/badge";
import { cn } from "@/lib/utils";

type Status = "queued" | "running" | "done" | "failed";

const map: Record<
  Status,
  { variant: "default" | "destructive" | "warning" | "muted" | "success"; label: string; dot: string }
> = {
  queued: { variant: "muted", label: "Queued", dot: "bg-muted-foreground" },
  running: { variant: "warning", label: "Running", dot: "bg-amber animate-pulse-dot" },
  done: { variant: "success", label: "Done", dot: "bg-primary" },
  failed: { variant: "destructive", label: "Failed", dot: "bg-destructive" },
};

export function SubmissionStatusBadge({ status }: { status: Status }) {
  const m = map[status];
  return (
    <Badge variant={m.variant}>
      <span className={cn("inline-block h-1.5 w-1.5 rounded-full", m.dot)} />
      {m.label}
    </Badge>
  );
}
