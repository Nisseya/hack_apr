"use client";

import * as React from "react";
import {
  Toast,
  ToastClose,
  ToastDescription,
  ToastProvider,
  ToastTitle,
  ToastViewport,
} from "./toast";

type ToastVariant = "default" | "destructive";

type ToastItem = {
  id: string;
  title?: string;
  description?: string;
  variant?: ToastVariant;
};

const listeners: Array<(toasts: ToastItem[]) => void> = [];
let memory: ToastItem[] = [];

function emit() {
  listeners.forEach((l) => l(memory));
}

export function toast(opts: Omit<ToastItem, "id">) {
  const id = Math.random().toString(36).slice(2);
  memory = [...memory, { id, ...opts }];
  emit();
  setTimeout(() => {
    memory = memory.filter((t) => t.id !== id);
    emit();
  }, 5000);
  return id;
}

export function useToast() {
  const [toasts, setToasts] = React.useState<ToastItem[]>(memory);
  React.useEffect(() => {
    listeners.push(setToasts);
    return () => {
      const idx = listeners.indexOf(setToasts);
      if (idx >= 0) listeners.splice(idx, 1);
    };
  }, []);
  return { toasts, toast };
}

export function Toaster() {
  const { toasts } = useToast();
  return (
    <ToastProvider>
      {toasts.map(({ id, title, description, variant }) => (
        <Toast key={id} variant={variant}>
          <div className="grid gap-1">
            {title && <ToastTitle>{title}</ToastTitle>}
            {description && <ToastDescription>{description}</ToastDescription>}
          </div>
          <ToastClose />
        </Toast>
      ))}
      <ToastViewport />
    </ToastProvider>
  );
}
