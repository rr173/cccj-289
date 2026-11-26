"""Independent end-to-end demo — no real customer system required.

Starts a throw-away SQLite DB, the ingest API (uvicorn subprocess), the
controllable HTTP receiver, and real delivery worker subprocesses.  Retry
waits are compressed (0.1s / 0.4s / 1.6s instead of 1s / 4s / 16s) so the
whole run takes a few seconds; the *scheduling logic* is identical and the
exact 1/4/16 schedule is additionally verified in ``tests/test_delivery.py``
with an injected fake clock.

Run:  ./run_demo.sh        (or: PYTHONPATH=.pylibs python3 demo.py)
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

import httpx

from webhook_delivery.receiver import Receiver, Response, by_flow, by_seq

ROOT = Path(__file__).resolve().parent
BACKOFF = (0.1, 0.4, 1.6)
TIMEOUT = 0.5
LEASE = 2.0


# --------------------------------------------------------------------------- infra
def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Demo:
    def __init__(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="webhook-demo-")
        self.db = str(Path(self.tmp.name) / "demo.db")
        self.port = free_port()
        self.api_proc: subprocess.Popen | None = None
        self.workers: list[subprocess.Popen] = []
        self.client = httpx.Client(base_url=self.api_url, timeout=10.0)
        self.receivers: list[Receiver] = []

    @property
    def api_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def env(self) -> dict[str, str]:
        env = dict(os.environ)
        extra = str(ROOT / ".pylibs")
        env["PYTHONPATH"] = extra + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        return env

    def start_api(self) -> None:
        self.api_proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "webhook_delivery.api:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=str(ROOT),
            env={**self.env(), "WEBHOOK_DB": self.db},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.api_proc.poll() is not None:
                raise RuntimeError("API failed to start:\n" + self.api_proc.stderr.read().decode())
            try:
                if httpx.get(self.api_url + "/health", timeout=0.5).status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.05)
        raise RuntimeError("API did not become healthy")

    def start_worker(
        self,
        *,
        lease: float = LEASE,
        name: str = "w",
        concurrency: int = 1,
        timeout: float = TIMEOUT,
        extra: list[str] | None = None,
    ) -> subprocess.Popen:
        log_path = Path(self.tmp.name) / f"{name}.log"
        log_file = open(log_path, "ab")
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "webhook_delivery.worker",
                "--db",
                self.db,
                "--backoff",
                ",".join(str(x) for x in BACKOFF),
                "--request-timeout",
                str(timeout),
                "--lease",
                str(lease),
                "--poll",
                "0.02",
                "--concurrency",
                str(concurrency),
                *(extra or []),
            ],
            cwd=str(ROOT),
            env=self.env(),
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        proc._demo_log = log_path  # type: ignore[attr-defined]
        self.workers.append(proc)
        return proc

    def stop_workers(self, timeout: float = 5.0) -> None:
        """Stop all delivery processes so each scene is deterministic.

        This is test-harness hygiene, not a service requirement: in production
        any number of workers may run concurrently and steal each other's due
        work; the scenes simply want to control who is alive.
        """
        for proc in self.workers:
            if proc.poll() is None:
                proc.terminate()
        for proc in self.workers:
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        self.workers.clear()

    def receiver(self) -> Receiver:
        recv = Receiver().start()
        self.receivers.append(recv)
        return recv

    def close(self) -> None:
        self.stop_workers(timeout=1.0)
        for recv in self.receivers:
            recv.stop()
        if self.api_proc and self.api_proc.poll() is None:
            self.api_proc.terminate()
            try:
                self.api_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.api_proc.kill()
        self.client.close()
        self.tmp.cleanup()

    # --------------------------------------------------------------- API sugar
    def customer(self, name: str) -> dict:
        r = self.client.post("/customers", json={"name": name})
        r.raise_for_status()
        return r.json()

    def subscribe(self, customer_id: str, url: str) -> dict:
        r = self.client.post("/subscriptions", json={"customer_id": customer_id, "target_url": url})
        r.raise_for_status()
        return r.json()

    def event(self, customer_id: str, flow_id: str, caller_event_id: str, payload: dict) -> httpx.Response:
        return self.client.post(
            "/events",
            json={
                "customer_id": customer_id,
                "flow_id": flow_id,
                "caller_event_id": caller_event_id,
                "payload": payload,
            },
        )

    def delivery(self, delivery_id: str) -> dict:
        r = self.client.get(f"/deliveries/{delivery_id}")
        r.raise_for_status()
        return r.json()

    def wait_status(self, delivery_id: str, statuses: set[str] | tuple[str, ...], timeout: float = 10.0) -> dict:
        if isinstance(statuses, str):
            statuses = {statuses}
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = self.delivery(delivery_id)
            if last["status"] in statuses:
                return last
            time.sleep(0.03)
        raise AssertionError(f"delivery {delivery_id} stuck in {last and last['status']}: {last}")


# --------------------------------------------------------------------------- output
def delivery_for(event_response: dict, subscription: dict) -> str:
    """Pick this subscription's delivery out of the event fan-out."""
    matches = [d["delivery_id"] for d in event_response["deliveries"] if d["subscription_id"] == subscription["id"]]
    if len(matches) != 1:
        raise AssertionError(f"expected 1 delivery for {subscription['id']}, got {matches}")
    return matches[0]


STEP = 0

def title(text: str) -> None:
    global STEP
    STEP += 1
    print(f"\n\033[1;36m══ {STEP}. {text}\033[0m")


def info(label: str, value: object) -> None:
    print(f"   {label:<34} {value}")


def ok(text: str) -> None:
    print(f"   \033[1;32m✔ {text}\033[0m")


def check(condition: bool, text: str) -> None:
    if not condition:
        raise AssertionError(text)
    ok(text)


# --------------------------------------------------------------------------- scenes
def scene_identity_and_cutoff(d: Demo) -> tuple[dict, dict, Receiver]:
    title("订阅创建前后的可见性 + 事件幂等")
    customer = d.customer("Acme SaaS")
    recv = d.receiver()

    before = d.event(customer["id"], "flow-cutoff", "boot-1", {"n": 0}).json()
    check(before["seq"] == 1, f"订阅前事件已入库，seq={before['seq']}，投递数={len(before['deliveries'])}")
    check(before["deliveries"] == [], "订阅创建前的事件不产生投递")

    sub = d.subscribe(customer["id"], recv.url)
    info("subscription", f"{sub['id']} target_version={sub['target_version']}")

    first = d.event(customer["id"], "flow-a", "caller-1", {"amount": 10}).json()
    check(first["deliveries"][0]["subscription_id"] == sub["id"], "订阅创建后的事件对新订阅产生投递")

    replay = d.event(customer["id"], "flow-a", "caller-1", {"amount": 10}).json()
    check(replay["id"] == first["id"] and replay["seq"] == first["seq"] and replay["replayed"], "相同 caller_event_id + 相同内容重送返回原事件")

    conflict = d.event(customer["id"], "flow-a", "caller-1", {"amount": 999})
    check(conflict.status_code == 409 and conflict.json()["error"]["code"] == "caller_event_conflict", "相同 caller_event_id + 不同内容被拒绝 (409)")

    second = d.event(customer["id"], "flow-a", "caller-2", {"amount": 20}).json()
    check(second["seq"] == first["seq"] + 1, f"同一业务流序号递增: {first['seq']} -> {second['seq']}")
    return customer, sub, recv


def scene_timeout_retry_order(d: Demo, customer: dict, sub: dict, recv: Receiver) -> None:
    title("超时/非 2xx 重试（1/4/16 压缩为 0.1/0.4/1.6）+ 同流顺序 + 跨流独立")
    # flow-a seq1: 500, 500, timeout, then 200 (3 failures, 3 waits, then success)
    recv.program(by_seq(1), [
        Response(status=500),
        Response(status=502),
        Response(status=200, delay_seconds=TIMEOUT * 3),  # client times out
        Response(status=200),
    ])
    recv.program(by_seq(2), [Response(status=200)])
    recv.program(by_flow("flow-b"), [Response(status=200)])

    d.start_worker(name="w-main", concurrency=2)
    deliveries = d.client.get(f"/subscriptions/{sub['id']}/deliveries").json()
    by_caller = {x["event"]["caller_event_id"]: x for x in deliveries}
    seq1 = by_caller["caller-1"]
    seq2 = by_caller["caller-2"]

    # Wait until the worker really is alive and has taken its first shot at
    # flow-a/seq1 (subprocess startup is what it is).
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if d.delivery(seq1["delivery_id"])["attempts_count"] >= 1:
            break
        time.sleep(0.02)
    else:
        raise AssertionError("worker never attempted flow-a seq1")

    # A different flow must not be blocked by flow-a's failures.
    other = d.event(customer["id"], "flow-b", "b-1", {"x": 1}).json()
    other_dlv = d.wait_status(other["deliveries"][0]["delivery_id"], "succeeded", timeout=10)
    check(other_dlv["status"] == "succeeded", "flow-b 在 flow-a 重试期间独立成功（跨流不互相阻塞）")
    check(d.delivery(seq1["delivery_id"])["status"] != "succeeded", "此时 flow-a/seq1 仍在重试，确实未阻塞 flow-b")

    final = d.wait_status(seq1["delivery_id"], "succeeded", timeout=15)
    attempts = final["attempts"]
    for a in attempts:
        info(f"attempt {a['attempt_number']}", f"{a['outcome']:<15} http={a['http_status']} v={a['target_version']}")
    check(len(attempts) == 4, "共 4 次尝试（500、502、超时、200）")
    check([a["outcome"] for a in attempts[:3]] == ["failed_http", "failed_http", "timeout"], "前三次分别记录 failed_http / failed_http / timeout")

    # Reconstruct the waits from recorded send times; they must follow 0.1/0.4/1.6.
    d2 = d.wait_status(seq2["delivery_id"], "succeeded", timeout=10)
    check(d2["status"] == "succeeded", "前一个事件确认成功后，同流 seq2 才被投递")
    recv_seq1 = sorted(recv.records_for(seq1["delivery_id"]), key=lambda r: r.received_at)
    recv_seq2 = sorted(recv.records_for(seq2["delivery_id"]), key=lambda r: r.received_at)
    check(len(recv_seq1) == 4, "接收端共收到 4 次 seq1 请求，且每次 delivery_id 相同")
    check(all(r.delivery_id == seq1["delivery_id"] for r in recv_seq1), "每次重试携带稳定 delivery_id")
    check(recv_seq2 and recv_seq2[0].received_at >= recv_seq1[-1].received_at, "seq2 的首次投递晚于 seq1 的成功尝试")
    gaps = [round(recv_seq1[i + 1].received_at - recv_seq1[i].received_at, 2) for i in range(3)]
    info("observed gaps (s)", gaps)
    info("compressed schedule", BACKOFF)
    # Measured at the receiver, so each gap also contains the previous request's
    # own duration.  Attempt 3 timed out after 0.5s, hence 1.6 + 0.5 ≈ 2.1s.
    check(0.08 <= gaps[0] < 0.25, "第 1 次失败后等待约 0.1s")
    check(0.35 <= gaps[1] < 0.6, "第 2 次失败后等待约 0.4s")
    check(2.0 <= gaps[2] < 2.5, "第 3 次失败后等待约 1.6s + 0.5s 超时（生产为 1/4/16）")
    d.stop_workers()


def scene_needs_attention_and_admin(d: Demo, customer: dict, sub: dict, recv: Receiver) -> None:
    title("重试耗尽进入待人工处理，阻塞后续事件，管理员跳过/重试")
    recv.program(by_flow("flow-c"), [Response(status=500)])
    d.start_worker(name="w-admin")
    time.sleep(0.2)  # let the subprocess finish importing

    e3 = d.event(customer["id"], "flow-c", "c-3", {"n": 3}).json()
    e4 = d.event(customer["id"], "flow-c", "c-4", {"n": 4}).json()
    dlv3 = e3["deliveries"][0]["delivery_id"]
    dlv4 = e4["deliveries"][0]["delivery_id"]

    parked = d.wait_status(dlv3, "needs_attention", timeout=15)
    info("seq3 status", parked["status"])
    info("seq3 last_error", parked["last_error"])
    info("seq3 attempts", len(parked["attempts"]))
    check(len(parked["attempts"]) == 4, "4 次失败后不再自动重试，进入 needs_attention")
    time.sleep(0.3)
    blocked = d.delivery(dlv4)
    check(blocked["status"] == "pending" and blocked["blocked"], f"seq4 被阻塞: {blocked['block_reason']}")
    check(recv.records_for(dlv4) == [], "seq4 从未被投递（不会悄悄放行）")

    # Administrator retry with a reason still hits a failing target -> parked again.
    r = d.client.post(f"/deliveries/{dlv3}/retry", json={"reason": "customer fixed their endpoint", "actor": "ops"})
    check(r.status_code == 200 and r.json()["status"] == "pending", "管理员说明原因后可重新入队")
    d.wait_status(dlv3, "needs_attention", timeout=15)
    ok("再次失败后重新进入 needs_attention")

    # Operator skips seq3; seq4 must then flow.
    recv.program(by_flow("flow-c"), [Response(status=200)])
    r = d.client.post(f"/deliveries/{dlv3}/skip", json={"reason": "event superseded by manual correction", "actor": "ops"})
    check(r.status_code == 200 and r.json()["status"] == "administratively_skipped", "管理员说明原因后跳过阻塞项")
    seq4 = d.wait_status(dlv4, "succeeded", timeout=10)
    check(seq4["status"] == "succeeded", "跳过阻塞项后，同流后一个事件恢复投递并成功")
    info("seq3 admin_actions", [(a["action"], a["reason"]) for a in d.delivery(dlv3)["admin_actions"]])

    bad = d.client.post(f"/deliveries/{dlv3}/skip", json={"reason": "  "})
    check(bad.status_code == 422, "跳过必须提供原因")
    d.stop_workers()


def scene_target_switch_late_reply(d: Demo, customer: dict) -> None:
    title("修改订阅目标：旧地址的晚到响应不能把新地址的记录标成成功")
    # A fresh customer keeps this customer's other subscriptions (from earlier
    # scenes) out of the fan-out, so ``deliveries`` has exactly one entry.
    customer = d.customer("Acme — target switch")
    old = d.receiver()
    new = d.receiver()
    sub = d.subscribe(customer["id"], old.url)
    old.program(None, [Response(status=200, delay_seconds=1.5)])

    ev = d.event(customer["id"], "flow-switch", "sw-1", {"v": "payload"}).json()
    dlv = delivery_for(ev, sub)

    # A generous client timeout on purpose: the old endpoint *does* answer 200,
    # just late.  That late success must not count for the new endpoint.
    d.start_worker(name="w-switch", lease=10.0, timeout=10.0)
    check(old.wait_for(lambda rs: len(rs) == 1, timeout=5), "旧地址已收到请求（响应将延迟 1.5s）")

    patched = d.client.patch(
        f"/subscriptions/{sub['id']}",
        json={"customer_id": customer["id"], "target_url": new.url, "expected_target_version": 1},
    )
    patched.raise_for_status()
    info("target_version", f"1 -> {patched.json()['target_version']}")

    final = d.wait_status(dlv, "succeeded", timeout=10)
    check(final["target_url"] == new.url and final["target_version"] == 2, "最终成功发生在新的目标地址/版本上")
    check(len(new.records_for(dlv)) == 1, "新地址收到一次请求")
    outcomes = [(a["target_version"], a["outcome"], a["http_status"]) for a in final["attempts"]]
    info("attempts (v, outcome, http)", outcomes)
    check(
        outcomes == [(1, "superseded_target", 200), (2, "succeeded", 200)],
        "旧地址的晚到 200 被记为 superseded_target，未标记成功；新地址的 200 才使投递成功",
    )
    check(len(old.records_for(dlv)) == 1, "旧地址未因切换而被重发")
    old.wait_for(lambda rs: rs[0].response_status is not None, timeout=5)
    info("old target actually answered", old.records_for(dlv)[0].response_status)
    check(old.records_for(dlv)[0].response_status == 200, "旧地址确实返回了 200（只是发生在配置更改之后）")
    d.stop_workers()


def scene_crash_restart(d: Demo, customer: dict) -> None:
    title("投递进程崩溃后重启：恢复未完成工作，接收方按 delivery_id 去重")
    customer = d.customer("Acme — crash recovery")
    recv = d.receiver()
    sub = d.subscribe(customer["id"], recv.url)
    # First request is accepted but the reply takes 2s (longer than we let the
    # worker live).  Every subsequent request succeeds immediately.
    recv.program(None, [Response(status=200, delay_seconds=2.0), Response(status=200)])

    ev = d.event(customer["id"], "flow-crash", "crash-1", {"crash": True}).json()
    dlv = delivery_for(ev, sub)

    proc = d.start_worker(name="w-crash", lease=1.0, timeout=10.0)
    check(recv.wait_for(lambda rs: len(rs) >= 1, timeout=5), "接收端收到请求（worker 仍在等待响应）")
    check(recv.records[0].delivery_id == dlv, "崩溃前的请求携带投递的 delivery_id")
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)
    info("killed worker", f"pid={proc.pid}")
    mid = d.delivery(dlv)
    info("delivery status right after SIGKILL", mid["status"])
    check(mid["status"] == "in_progress", "崩溃时投递处于 in_progress（租约仍有效）")

    d.start_worker(name="w-restart", lease=1.0, timeout=10.0)
    resumed = d.wait_status(dlv, "succeeded", timeout=15)
    check(resumed["status"] == "succeeded", "重启后的投递进程领取并完成了遗留工作")
    check(any(a["outcome"] == "expired" for a in resumed["attempts"]), "过期租约的尝试被标记为 expired（结果未知）")
    check(recv.wait_for(lambda rs: len(recv.records_for(dlv)) >= 2, timeout=5), "接收端收到第二次请求（重试）")
    ids = [r.delivery_id for r in recv.records_for(dlv)]
    info("delivery_ids seen by receiver", ids)
    check(len(set(ids)) == 1 and ids[0] == dlv, "两次请求携带完全相同的 delivery_id")

    # Receiver-side dedupe using the stable id (this is the receiver's job;
    # the service does not and cannot promise exactly-once over the network).
    seen: set[str] = set()
    unique_processed = []
    for r in recv.records_for(dlv):
        if r.delivery_id in seen:
            continue
        seen.add(r.delivery_id)
        unique_processed.append(r)
    check(len(unique_processed) == 1, "接收方按 delivery_id 去重后只处理一次")


def main() -> int:
    d = Demo()
    try:
        d.start_api()
        print("\033[1mWebhook delivery demo\033[0m")
        info("db", d.db)
        info("api", d.api_url)
        info("compressed backoff", BACKOFF)
        info("request timeout", TIMEOUT)

        customer, sub, recv = scene_identity_and_cutoff(d)
        scene_timeout_retry_order(d, customer, sub, recv)
        scene_needs_attention_and_admin(d, customer, sub, recv)
        scene_target_switch_late_reply(d, customer)
        scene_crash_restart(d, customer)

        print("\n\033[1;32mAll scenarios passed.\033[0m")
        return 0
    except AssertionError as exc:
        print(f"\n\033[1;31mDEMO FAILED: {exc}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        d.close()


if __name__ == "__main__":
    raise SystemExit(main())
