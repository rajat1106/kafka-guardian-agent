"""Service-level actions against a real Kubernetes cluster.

Selected with ACTUATOR_MODE=k8s. Kept deliberately thin: it maps the same
action types the simulator handles onto the Kubernetes API, so switching
from demo to real infrastructure is a config change rather than a rewrite.

Requires the `kubernetes` package and a working kubeconfig or in-cluster
service account.
"""

from __future__ import annotations

import asyncio
import os
import time

import structlog

from guardian_platform.contracts import Action, ActionType

from .base import Actuator, ExecutionResult

log = structlog.get_logger(__name__)

_OWNED = {
    ActionType.SCALE_CONSUMER_GROUP,
    ActionType.RESTART_SERVICE,
    ActionType.NO_OP,
}


class KubernetesActuator(Actuator):
    name = "kubernetes"

    def __init__(self, namespace: str | None = None) -> None:
        self._namespace = namespace or os.getenv("K8S_NAMESPACE", "default")
        self._apps = None

    def handles(self, action_type: ActionType) -> bool:
        return action_type in _OWNED

    def _api(self):
        if self._apps is None:
            from kubernetes import client, config

            try:
                config.load_incluster_config()
            except Exception:  # noqa: BLE001 — running outside the cluster
                config.load_kube_config()
            self._apps = client.AppsV1Api()
        return self._apps

    async def execute(self, action: Action) -> ExecutionResult:
        started = time.perf_counter()
        try:
            detail = await asyncio.to_thread(self._execute_sync, action)
            ok = True
        except Exception as exc:  # noqa: BLE001
            detail, ok = f"{type(exc).__name__}: {exc}", False
            log.warning("k8s_action_failed", action=action.type.value, error=str(exc))
        return ExecutionResult(
            ok, detail, self.name, int((time.perf_counter() - started) * 1000)
        )

    def _execute_sync(self, action: Action) -> str:
        if action.type is ActionType.NO_OP:
            return "no action taken"

        apps = self._api()
        if action.type is ActionType.SCALE_CONSUMER_GROUP:
            replicas = int(action.params["replicas"])
            apps.patch_namespaced_deployment_scale(
                name=action.target,
                namespace=self._namespace,
                body={"spec": {"replicas": replicas}},
            )
            return f"scaled deployment {action.target} to {replicas} replicas"

        if action.type is ActionType.RESTART_SERVICE:
            # The standard rollout-restart trick: touch an annotation so the
            # deployment controller recreates the pods.
            from datetime import datetime, timezone

            stamp = datetime.now(timezone.utc).isoformat()
            apps.patch_namespaced_deployment(
                name=action.target,
                namespace=self._namespace,
                body={"spec": {"template": {"metadata": {"annotations": {
                    "guardian.kafka/restartedAt": stamp
                }}}}},
            )
            return f"triggered rolling restart of {action.target}"

        return f"unhandled action {action.type.value}"
