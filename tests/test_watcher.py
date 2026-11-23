from types import SimpleNamespace
import threading

from urllib3.exceptions import ProtocolError

from watchline.watcher import (
    describe_pod,
    format_event,
    is_pod_ready,
    iter_pod_events,
)


def pod_status(phase="Running", ready=True):
    return SimpleNamespace(
        phase=phase,
        pod_ip="10.244.0.10",
        host_ip="172.18.0.2",
        conditions=[
            SimpleNamespace(type="Ready", status="True" if ready else "False")
        ],
        container_statuses=[
            SimpleNamespace(restart_count=2),
            SimpleNamespace(restart_count=1),
        ],
    )


def pod(phase="Running", ready=True):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name="pod-demo-test",
            namespace="watchline",
            labels={"kubernetes.io/hostname": "watchline-worker"},
        ),
        status=pod_status(phase, ready),
        spec=SimpleNamespace(node_name="watchline-worker"),
    )


def test_is_pod_ready_uses_ready_condition():
    assert is_pod_ready(pod()) is True
    assert is_pod_ready(pod(ready=False)) is False


def test_describe_pod_returns_state_summary():
    result = describe_pod(pod())

    assert result["name"] == "pod-demo-test"
    assert result["namespace"] == "watchline"
    assert result["phase"] == "Running"
    assert result["pod_ip"] == "10.244.0.10"
    assert result["ready"] is True
    assert result["restarts"] == 3
    assert result["node"] == "watchline-worker"


def test_format_event_produces_json():
    result = format_event("MODIFIED", pod(ready=False))

    assert '"event": "MODIFIED"' in result
    assert '"name": "pod-demo-test"' in result
    assert '"ready": false' in result


def test_dropped_connection_reconnects():
    calls = 0
    stop_event = threading.Event()

    class FakeWatch:
        def stream(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ProtocolError("connection dropped")
            yield {"type": "ADDED", "object": pod()}
            stop_event.set()

    fake_watch = FakeWatch()
    events = iter_pod_events(
        core_api=SimpleNamespace(list_namespaced_pod=lambda **_kwargs: None),
        namespace="watchline",
        timeout_seconds=5,
        stop_event=stop_event,
        resource_watcher=fake_watch,
    )

    observed = list(events)

    assert calls == 2
    assert len(observed) == 1
    assert observed[0][0] == "ADDED"
    assert observed[0][1].metadata.name == "pod-demo-test"


def test_stop_event_prevents_reconnection():
    calls = 0
    stop_event = threading.Event()
    stop_event.set()

    class FakeWatch:
        def stream(self, *_args, **_kwargs):
            nonlocal calls
            calls += 1
            yield {"type": "ADDED", "object": pod()}

    events = iter_pod_events(
        core_api=SimpleNamespace(list_namespaced_pod=lambda **_kwargs: None),
        namespace="watchline",
        stop_event=stop_event,
        resource_watcher=FakeWatch(),
    )

    assert list(events) == []
    assert calls == 0

