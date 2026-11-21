"""Watch pod events from a Kubernetes cluster."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from collections.abc import Iterable
from typing import Any

from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException


def load_kubernetes_config() -> None:
    """Use in-cluster credentials when available, otherwise local kubeconfig."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def describe_pod(pod: Any) -> dict[str, Any]:
    """Return the useful fields for a pod event."""
    metadata = pod.metadata
    status = pod.status
    containers = status.container_statuses if status and status.container_statuses else []

    return {
        "name": metadata.name,
        "namespace": metadata.namespace,
        "phase": status.phase if status else "Unknown",
        "pod_ip": status.pod_ip if status else None,
        "host_ip": status.host_ip if status else None,
        "ready": is_pod_ready(pod),
        "restarts": sum(item.restart_count or 0 for item in containers),
        "node": getattr(pod.spec, "node_name", None) if getattr(pod, "spec", None) else None,
    }


def is_pod_ready(pod: Any) -> bool:
    """Check whether a pod has a Ready=True condition."""
    conditions = (
        pod.status.conditions
        if pod.status and pod.status.conditions
        else []
    )
    return any(
        condition.type == "Ready" and condition.status == "True"
        for condition in conditions
    )


def format_event(event_type: str, pod: Any) -> str:
    """Format one pod event as readable JSON."""
    payload = {"event": event_type, **describe_pod(pod)}
    return json.dumps(payload, sort_keys=True)


def iter_pod_events(
    core_api: client.CoreV1Api,
    namespace: str,
    label_selector: str | None = None,
    timeout_seconds: int = 300,
) -> Iterable[tuple[str, Any]]:
    """Yield pod events, reconnecting when the watch stream closes."""
    resource_watcher = watch.Watch()

    while True:
        try:
            stream = resource_watcher.stream(
                core_api.list_namespaced_pod,
                namespace=namespace,
                label_selector=label_selector,
                timeout_seconds=timeout_seconds,
            )
            for event in stream:
                yield event["type"], event["object"]
        except ApiException as error:
            if error.status == 410:
                print(
                    "Event history expired. Reconnecting with a fresh pod watch.",
                    file=sys.stderr,
                )
            else:
                print(
                    f"Kubernetes API error {error.status}: {error.reason}",
                    file=sys.stderr,
                )
            time.sleep(2)
        except (ConnectionError, TimeoutError):
            print("Connection interrupted. Reconnecting...", file=sys.stderr)
            time.sleep(2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Watch Kubernetes pod events and print their state."
    )
    parser.add_argument(
        "-n",
        "--namespace",
        default="watchline",
        help="Namespace to watch (default: watchline)",
    )
    parser.add_argument(
        "-l",
        "--label-selector",
        help="Only include pods matching this Kubernetes label selector",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Seconds per watch stream before reconnecting (default: 300)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_kubernetes_config()

    stopping = False

    def stop_watching(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True
        print("\nStopping Watchline.", file=sys.stderr)

    signal.signal(signal.SIGINT, stop_watching)
    signal.signal(signal.SIGTERM, stop_watching)

    core_api = client.CoreV1Api()
    print(
        f"Watching pods in namespace '{args.namespace}'. "
        "Press Ctrl+C to stop.",
        flush=True,
    )

    try:
        for event_type, pod in iter_pod_events(
            core_api,
            args.namespace,
            args.label_selector,
            args.timeout,
        ):
            if stopping:
                break
            print(format_event(event_type, pod), flush=True)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

