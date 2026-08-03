"""GPU pool manager backed by RunPod pods.

The pool keeps a set of long-lived, warm GPU pods (models already loaded in
memory). The backend routes benchmark jobs to a free pod instead of running
them locally. Scaling is manual: the operator can grow or shrink the pool via
the admin API, and the pool manager provisions/terminates pods accordingly.

A lightweight in-memory registry tracks which pod is busy and which is free.
This is intentionally simple (single orchestrator process). For multi-replica
deployments, swap the registry for a shared store (Redis/Postgres).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from runpod_client import Pod, RunPodClient, RunPodError

logger = logging.getLogger(__name__)

DEFAULT_IMAGE = os.environ.get(
    "RUNPOD_POOL_IMAGE", "nisseya/hack-apr-worker:latest"
)
DEFAULT_GPU = os.environ.get("RUNPOD_POOL_GPU", "A100-80GB")
DEFAULT_GPU_COUNT = int(os.environ.get("RUNPOD_POOL_GPU_COUNT", "1"))
DEFAULT_VOLUME_MOUNT = os.environ.get("RUNPOD_POOL_VOLUME_MOUNT", "/workspace")
DEFAULT_VOLUME_SIZE_GB = int(os.environ.get("RUNPOD_POOL_VOLUME_SIZE_GB", "100"))
DEFAULT_IDLE_TIMEOUT = int(os.environ.get("RUNPOD_POOL_IDLE_TIMEOUT", "0"))


@dataclass
class PoolWorker:
    """A worker pod plus its local bookkeeping."""

    pod: Pod
    busy: bool = False
    last_seen: float = field(default_factory=time.time)


class GpuPool:
    """Manages a pool of warm RunPod GPU pods."""

    def __init__(
        self,
        client: RunPodClient | None = None,
        image: str = DEFAULT_IMAGE,
        gpu_type: str = DEFAULT_GPU,
        gpu_count: int = DEFAULT_GPU_COUNT,
        volume_mount: str = DEFAULT_VOLUME_MOUNT,
        volume_size_gb: int = DEFAULT_VOLUME_SIZE_GB,
        idle_timeout: int = DEFAULT_IDLE_TIMEOUT,
        on_worker_ready: Callable[[str], None] | None = None,
    ) -> None:
        self.client = client or RunPodClient()
        self.image = image
        self.gpu_type = gpu_type
        self.gpu_count = gpu_count
        self.volume_mount = volume_mount
        self.volume_size_gb = volume_size_gb
        self.idle_timeout = idle_timeout
        self.on_worker_ready = on_worker_ready

        self._workers: dict[str, PoolWorker] = {}
        self._lock = threading.RLock()
        self._target_size = int(os.environ.get("RUNPOD_POOL_SIZE", "1"))

    # ---- Registry ----

    def _sync_from_runpod(self) -> None:
        """Reconcile local registry with actual RunPod pods."""
        try:
            pods = self.client.list_pods()
        except RunPodError as exc:
            logger.warning("Could not list RunPod pods: %s", exc)
            return

        remote_ids = {p.id for p in pods}
        # Drop pods that no longer exist remotely.
        for pod_id in list(self._workers):
            if pod_id not in remote_ids:
                del self._workers[pod_id]

        # Add/refresh known pods.
        for pod in pods:
            if pod.id in self._workers:
                self._workers[pod.id].pod = pod
            else:
                self._workers[pod.id] = PoolWorker(pod=pod)

    # ---- Pool sizing ----

    @property
    def target_size(self) -> int:
        return self._target_size

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._workers)

    def set_target_size(self, size: int) -> None:
        """Manually scale the pool up or down."""
        if size < 0:
            raise ValueError("Pool size cannot be negative")
        with self._lock:
            self._target_size = size
        self.reconcile()

    def reconcile(self) -> None:
        """Bring the pool to its target size (provision/terminate pods)."""
        with self._lock:
            self._sync_from_runpod()

            running = [w for w in self._workers.values() if w.pod.is_running]

            # Terminate excess running pods (least recently used first).
            while len(running) > self._target_size:
                victim = min(running, key=lambda w: w.last_seen)
                try:
                    self.client.terminate_pod(victim.pod.id)
                    logger.info("Terminating excess pod %s", victim.pod.id)
                except RunPodError as exc:
                    logger.warning("Failed to terminate pod %s: %s", victim.pod.id, exc)
                running.remove(victim)
                del self._workers[victim.pod.id]

            # Provision until we reach target (counting provisioning pods too).
            while len(self._workers) < self._target_size:
                self._provision_one()

    def _provision_one(self) -> None:
        name = f"hack-apr-worker-{int(time.time())}"
        try:
            pod = self.client.create_pod(
                name=name,
                image=self.image,
                gpu_type=self.gpu_type,
                gpu_count=self.gpu_count,
                env={"RUNPOD_POOL_WORKER": "1"},
                volume_mount=self.volume_mount,
                volume_size_gb=self.volume_size_gb,
                idle_timeout=self.idle_timeout,
            )
        except RunPodError as exc:
            logger.error("Failed to provision pod: %s", exc)
            return
        self._workers[pod.id] = PoolWorker(pod=pod)
        logger.info("Provisioning pod %s (%s)", pod.id, pod.name)

    # ---- Job routing ----

    def acquire_worker(self, timeout: float = 1800.0) -> PoolWorker:
        """Return a free, running worker, waiting for one to become ready."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                self._sync_from_runpod()
                ready = [
                    w
                    for w in self._workers.values()
                    if w.pod.is_running and w.pod.http_url and not w.busy
                ]
                if ready:
                    worker = min(ready, key=lambda w: w.last_seen)
                    worker.busy = True
                    worker.last_seen = time.time()
                    if self.on_worker_ready:
                        self.on_worker_ready(worker.pod.http_url)
                    return worker
            time.sleep(5)
        raise RunPodError("No free GPU worker available in the pool")

    def release_worker(self, pod_id: str) -> None:
        with self._lock:
            worker = self._workers.get(pod_id)
            if worker:
                worker.busy = False
                worker.last_seen = time.time()

    def health(self) -> dict:
        with self._lock:
            self._sync_from_runpod()
            return {
                "target_size": self._target_size,
                "size": len(self._workers),
                "running": sum(1 for w in self._workers.values() if w.pod.is_running),
                "busy": sum(1 for w in self._workers.values() if w.busy),
                "workers": [
                    {
                        "id": w.pod.id,
                        "name": w.pod.name,
                        "running": w.pod.is_running,
                        "busy": w.busy,
                        "url": w.pod.http_url,
                        "gpu": w.pod.gpu,
                    }
                    for w in self._workers.values()
                ],
            }
