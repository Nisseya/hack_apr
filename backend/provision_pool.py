"""Provision the GPU pool on RunPod.

Builds & pushes the worker image (if needed), then creates the pool pods and
prints their status. Manual scaling afterwards is done via the orchestrator's
POST /pool/scale endpoint.

Usage:
    RUNPOD_API_KEY=... python provision_pool.py --size 2 [--gpu A100-80GB] [--push]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

from gpu_pool import GpuPool
from runpod_client import RunPodClient, RunPodError

DEFAULT_IMAGE = os.environ.get("RUNPOD_POOL_IMAGE", "nisseya/hack-apr-worker:latest")


def build_and_push(image: str) -> None:
    print(f"Building worker image {image} ...")
    subprocess.run(["docker", "build", "-t", image, "-f", "worker/Dockerfile", "."], check=True)
    print(f"Pushing {image} ...")
    subprocess.run(["docker", "push", image], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision the RunPod GPU pool")
    parser.add_argument("--size", type=int, default=1, help="Number of worker pods")
    parser.add_argument("--gpu", default=os.environ.get("RUNPOD_POOL_GPU", "A100-80GB"))
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--push", action="store_true", help="Build & push the image first")
    args = parser.parse_args()

    if args.push:
        build_and_push(args.image)

    client = RunPodClient()
    pool = GpuPool(client=client, image=args.image, gpu_type=args.gpu)
    pool.set_target_size(args.size)
    pool.reconcile()

    print(f"\nPool target: {pool.target_size}")
    for _ in range(60):
        health = pool.health()
        print(f"  running={health['running']} size={health['size']}")
        if health["running"] >= args.size:
            break
        time.sleep(10)

    print("\nPool health:")
    print(pool.health())


if __name__ == "__main__":
    try:
        main()
    except RunPodError as exc:
        print(f"RunPod error: {exc}", file=sys.stderr)
        sys.exit(1)
