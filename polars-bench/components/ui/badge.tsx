import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center gap-1.5 px-2 py-0.5 font-mono text-[10px] uppercase tracking-widest border",
  {
    variants: {
      variant: {
        default: "bg-primary/10 text-primary border-primary/40",
        secondary: "bg-secondary text-secondary-foreground border-border",
        outline: "bg-transparent text-muted-foreground border-border",
        success: "bg-primary/10 text-primary border-primary/40",
        destructive: "bg-destructive/10 text-destructive border-destructive/40",
        warning: "bg-amber/10 text-amber border-amber/40",
        muted: "bg-muted text-muted-foreground border-border",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return (
    <div className={cn(badgeVariants({ variant }), className)} {...props} />
  );
}
