"""Thin client for the RunPod GraphQL API (pods).

Only the operations needed to manage a pool of GPU pods are implemented:
create, list, get status, terminate. Uses the public RunPod GraphQL
endpoint with an API key from the RUNPOD_API_KEY environment variable.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

import requests

RUNPOD_API_URL = "https://api.runpod.io/graphql"


class RunPodError(RuntimeError):
    """Raised when the RunPod API returns an error."""


@dataclass
class Pod:
    """A RunPod pod (GPU instance) in the pool."""

    id: str
    name: str
    desired_status: str
    runtime: dict[str, Any] | None = None
    machine: dict[str, Any] | None = None
    image_name: str | None = None
    env: dict[str, str] = field(default_factory=dict)

    @property
    def is_running(self) -> bool:
        return self.desired_status == "RUNNING" and self.runtime is not None

    @property
    def http_url(self) -> str | None:
        """Public HTTP URL of the pod's first exposed port, if any."""
        if not self.runtime:
            return None
        ports = self.runtime.get("ports") or []
        for port in ports:
            if port.get("ip") and port.get("privatePort") == 8000:
                return f"http://{port['ip']}:{port.get('publicPort', 8000)}"
        for port in ports:
            if port.get("ip"):
                return f"http://{port['ip']}:{port.get('publicPort', 8000)}"
        return None

    @property
    def gpu(self) -> str | None:
        if not self.machine:
            return None
        gpus = self.machine.get("gpus") or []
        if gpus:
            return gpus[0].get("type")
        return None


class RunPodClient:
    """Minimal RunPod GraphQL client for managing pods."""

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.environ.get("RUNPOD_API_KEY", "")
        if not self.api_key:
            raise RunPodError(
                "RUNPOD_API_KEY is not set. Set it to manage the GPU pool."
            )
        self.session = requests.Session()
        self.session.headers.update({"api-key": self.api_key})

    def _query(self, query: str, variables: dict | None = None) -> dict:
        resp = self.session.post(
            RUNPOD_API_URL,
            json={"query": query, "variables": variables or {}},
            timeout=30,
        )
        if resp.status_code != 200:
            raise RunPodError(f"RunPod API HTTP {resp.status_code}: {resp.text[:300]}")
        payload = resp.json()
        if "errors" in payload:
            raise RunPodError(payload["errors"][0]["message"])
        return payload.get("data", {})

    # ---- Pods ----

    def list_pods(self) -> list[Pod]:
        query = """
        query Pods($input: PodsInput) {
          myself {
            pods(input: $input) {
              id
              name
              desiredStatus
              runtime { ports { ip privatePort publicPort } }
              machine { gpus { type } }
              imageName
              env
            }
          }
        }
        """
        data = self._query(query, {"input": {}})
        pods = (data.get("myself") or {}).get("pods") or []
        return [
            Pod(
                id=p["id"],
                name=p.get("name") or "",
                desired_status=p.get("desiredStatus") or "",
                runtime=p.get("runtime"),
                machine=p.get("machine"),
                image_name=p.get("imageName"),
                env=p.get("env") or {},
            )
            for p in pods
        ]

    def create_pod(
        self,
        name: str,
        image: str,
        gpu_type: str,
        gpu_count: int = 1,
        env: dict[str, str] | None = None,
        volume_mount: str | None = None,
        volume_size_gb: int = 50,
        idle_timeout: int = 0,
    ) -> Pod:
        """Create a pod. Returns the created pod (may still be provisioning)."""
        query = """
        mutation PodCreate($input: PodCreateInput!) {
          podFindAndDeployOnDemand(input: $input) {
            id
            name
            desiredStatus
            runtime { ports { ip privatePort publicPort } }
            machine { gpus { type } }
            imageName
            env
          }
        }
        """
        input_dict: dict[str, Any] = {
            "name": name,
            "imageName": image,
            "gpuTypeId": gpu_type,
            "gpuCount": gpu_count,
            "cloudType": "SECURE",
            "startSsh": False,
            "env": env or {},
        }
        if volume_mount:
            input_dict["volumeInGb"] = volume_size_gb
            input_dict["volumeMountPath"] = volume_mount
        if idle_timeout > 0:
            input_dict["idleTimeout"] = idle_timeout

        data = self._query(query, {"input": input_dict})
        pod = data.get("podFindAndDeployOnDemand") or {}
        return Pod(
            id=pod.get("id") or "",
            name=pod.get("name") or "",
            desired_status=pod.get("desiredStatus") or "",
            runtime=pod.get("runtime"),
            machine=pod.get("machine"),
            image_name=pod.get("imageName"),
            env=pod.get("env") or {},
        )

    def get_pod(self, pod_id: str) -> Pod | None:
        query = """
        query Pod($input: PodGetInput!) {
          pod(input: $input) {
            id
            name
            desiredStatus
            runtime { ports { ip privatePort publicPort } }
            machine { gpus { type } }
            imageName
            env
          }
        }
        """
        data = self._query(query, {"input": {"podId": pod_id}})
        pod = data.get("pod") or {}
        if not pod:
            return None
        return Pod(
            id=pod.get("id") or "",
            name=pod.get("name") or "",
            desired_status=pod.get("desiredStatus") or "",
            runtime=pod.get("runtime"),
            machine=pod.get("machine"),
            image_name=pod.get("imageName"),
            env=pod.get("env") or {},
        )

    def terminate_pod(self, pod_id: str) -> None:
        query = """
        mutation PodTerminate($input: PodTerminateInput!) {
          podTerminate(input: $input) { id }
        }
        """
        self._query(query, {"input": {"podId": pod_id}})

    def wait_until_running(
        self,
        pod_id: str,
        timeout: float = 900.0,
        poll_interval: float = 10.0,
    ) -> Pod:
        """Poll until the pod is running and exposes an HTTP URL."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            pod = self.get_pod(pod_id)
            if pod and pod.is_running and pod.http_url:
                return pod
            time.sleep(poll_interval)
        raise RunPodError(f"Pod {pod_id} did not become ready in time")
