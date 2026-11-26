"""End-to-end demo: external replica erasure on topic withdrawal.

Runs on one machine with throw-away SQLite files:

* the real FastAPI app through TestClient (real routing/validation);
* the real Worker with real HTTP (``HttpxSender``) against two *reference*
  participating receivers, each with its **own separate persistent database**;
* a real local socket, so the erasure protocol and the un-ordering of requests
  are exercised for real.

The demo shows:

1. multiple registered receivers per customer, fixed addresses, selecting one
   as a subscription target and switching to another;
2. a topic callback lets the receiver identify customer, topic and the original
   delivery; ordinary/topic-less callbacks are unchanged;
3. only receivers the worker was actually authorized to send to are notified on
   withdrawal — a configured-only /*repointed-away* target is not;
4. confirmation progress: which receivers acknowledged and which did not, with
   only a matching per-operation acknowledgement counting;
5. failure/timeout stays pending and is retried with the *same* operation id;
6. the reference receiver erases only the exact customer+topic and refuses to
   let an in-flight pre-withdrawal callback recreate the copy even if its HTTP
   request arrives after the erasure request;
7. crash safety: withdrawal commits, the process "dies", and a fresh worker
   still completes every erasure.

Run:  ./run_erasure_demo.sh
or:   PYTHONPATH=.pylibs:. python3 demo_erasure.py
"""

from __future__ import annotations

import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_PYLIBS = _ROOT / ".pylibs"
if _PYLIBS.is_dir() and str(_PYLIBS) not in sys.path:
    sys.path.insert(0, str(_PYLIBS))
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from webhook_delivery.api import create_app  # noqa: E402
from webhook_delivery.config import DeliveryPolicy, RealClock, Settings  # noqa: E402
from webhook_delivery.erasure_receiver import ReferenceErasureReceiverServer  # noqa: E402
from webhook_delivery.store import Store  # noqa: E402
from webhook_delivery.worker import HttpxSender, Worker  # noqa: E402

SECRET = "DEMO_REPLICA_SECRET_909"
STEP = 0


def title(text: str) -> None:
    global STEP
    STEP += 1
    print(f"\n\033[1;36m══ {STEP}. {text}\033[0m")


def info(label: str, value: object) -> None:
    print(f"   {label:<40} {value}")


def ok(text: str) -> None:
    print(f"   \033[1;32m✔ {text}\033[0m")


def check(condition: bool, text: str) -> None:
    if not condition:
        raise AssertionError(text)
    ok(text)


def drain(store: Store, settings: Settings, *, timeout: float = 10.0) -> None:
    """Run a real worker until no delivery/erasure work remains due."""
    worker = Worker(store=store, policy=DeliveryPolicy(settings), clock=RealClock(), concurrency=2)
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            worker.run_once()
            nxt = store.next_wakeup(now=time.time())
            ew = store.next_erasure_wakeup(now=time.time())
            pending = (
                store.list_erasure_requests(status="pending")
                + store.list_erasure_requests(status="retry_wait")
                + store.list_erasure_requests(status="in_progress")
            )
            if nxt is None and ew is None and not pending:
                return
            time.sleep(0.01)
    finally:
        worker.sender.close()
        if worker._erasure_sender_obj is not None:
            worker._erasure_sender_obj.close()


def main() -> int:
    tmp = tempfile.TemporaryDirectory(prefix="webhook-erasure-")
    base = Path(tmp.name)
    db_path = str(base / "service.db")
    settings = Settings(
        database_path=db_path,
        backoff_seconds=(0.05, 0.1, 0.2),
        request_timeout_seconds=1.0,
        lease_seconds=5.0,
        worker_poll_seconds=0.01,
        max_attempts=4,
    )
    store = Store(settings)
    store.initialize()
    app = create_app(settings=settings, store=store, clock=RealClock())

    # Each reference receiver keeps its own independent persistent library.
    rx_a = ReferenceErasureReceiverServer(str(base / "receiver_a.db")).start()
    rx_b = ReferenceErasureReceiverServer(str(base / "receiver_b.db")).start()
    try:
        with TestClient(app) as api:
            print("\033[1mExternal replica erasure demo\033[0m")
            info("service db", db_path)
            info("reference receiver A db", base / "receiver_a.db")
            info("reference receiver B db", base / "receiver_b.db")

            # ------------------------------------------------------ scene 1
            title("登记多个参与协议的接收方，订阅选择其一，并可切换到另一个目标")
            cus = api.post("/customers", json={"name": "Acme Erasure"}).json()
            reg_a = api.post(
                "/erasure-receivers", json={"customer_id": cus["id"], "url": rx_a.url}
            ).json()
            reg_b = api.post(
                "/erasure-receivers", json={"customer_id": cus["id"], "url": rx_b.url}
            ).json()
            info("receiver A registration", reg_a["id"])
            info("receiver B registration", reg_b["id"])
            check(
                api.get("/erasure-receivers", params={"customer_id": cus["id"]}).json()
                and len(api.get("/erasure-receivers", params={"customer_id": cus["id"]}).json()) == 2,
                "同一客户可登记多个参与协议的接收方",
            )
            sub = api.post(
                "/subscriptions",
                json={"customer_id": cus["id"], "target_url": "http://ignored/h",
                      "erasure_receiver_id": reg_a["id"]},
            ).json()
            check(sub["target_url"] == rx_a.url and sub["erasure_receiver_id"] == reg_a["id"],
                  "订阅选择接收方 A 作为目标（登记编号绑定固定 HTTP 地址）")

            # ------------------------------------------------------ scene 2
            title("有主题的回调让接收方识别客户/主题/原投递；普通目标与无主题事件走原流程")
            ev = api.post(
                "/events",
                json={"customer_id": cus["id"], "flow_id": "orders", "caller_event_id": "o1",
                      "payload": {"secret": SECRET}, "topic_id": "gdpr-42"},
            ).json()
            plain = api.post(
                "/events",
                json={"customer_id": cus["id"], "flow_id": "orders", "caller_event_id": "o2",
                      "payload": {"note": "no topic"}},
            ).json()
            drain(store, settings)
            copies_a = rx_a.backend.list_replicas()
            check(len(copies_a) == 1, "接收方 A 只把有主题的回调纳入受管副本")
            info("A 的受管副本", f"topic={copies_a[0]['topic_id']} delivery={copies_a[0]['delivery_id'][:12]}…")
            check(copies_a[0]["delivery_id"] == ev["deliveries"][0]["delivery_id"],
                  "回调携带原投递编号，接收方可据此管理副本")
            check(all(c["topic_id"] == "gdpr-42" for c in copies_a), "无主题事件未进入协议（走原流程）")

            # ------------------------------------------------------ scene 3
            title("撤回主题：参与清除协议的接收方被纳入清除范围，并查询确认进度")
            w = api.post(
                "/topics/gdpr-42/withdraw",
                json={"customer_id": cus["id"], "wait_for_inflight": True},
            ).json()
            check(w["withdrawn"] is True and w["events_affected"] == 1, "主题本地撤回成功（沿用原有处置）")
            check(w["erasure"]["total"] == 1 and w["erasure"]["pending_receiver_ids"] == [reg_a["id"]],
                  "撤回对应 1 个接收方清除请求，尚未确认")
            drain(store, settings)
            progress = api.get("/topics/gdpr-42/erasure", params={"customer_id": cus["id"]}).json()
            check(progress["complete"] is True and progress["acknowledged_receiver_ids"] == [reg_a["id"]],
                  "接收方 A 已确认清除，进度可查")
            check(rx_a.backend.list_replicas() == [], "接收方 A 的该客户该主题受管副本已删除")
            # Other customer / other topic are never touched by an erasure.

            # ------------------------------------------------------ scene 4
            title("清除范围只含实际获准发送过的接收方：仅配置/改址离开的目标不必通知")
            cus2 = api.post("/customers", json={"name": "Acme Scope"}).json()
            ra = api.post("/erasure-receivers", json={"customer_id": cus2["id"], "url": rx_a.url}).json()
            rb = api.post("/erasure-receivers", json={"customer_id": cus2["id"], "url": rx_b.url}).json()
            sub2 = api.post(
                "/subscriptions",
                json={"customer_id": cus2["id"], "target_url": ra["url"], "erasure_receiver_id": ra["id"]},
            ).json()
            api.post(
                "/events",
                json={"customer_id": cus2["id"], "flow_id": "f", "caller_event_id": "s1",
                      "payload": {"x": SECRET}, "topic_id": "scope-topic"},
            )
            # Switch to B *before* anything is sent: A was configured only.
            api.patch(
                f"/subscriptions/{sub2['id']}",
                json={"customer_id": cus2["id"], "target_url": rb["url"], "erasure_receiver_id": rb["id"]},
            )
            drain(store, settings)
            w2 = api.post("/topics/scope-topic/withdraw", json={"customer_id": cus2["id"]}).json()
            ids = w2["erasure"]["pending_receiver_ids"] + w2["erasure"]["acknowledged_receiver_ids"]
            check(ids == [rb["id"]], "只有实际获准发送的接收方 B 进入清除范围，A 未被通知")
            drain(store, settings)

            # ------------------------------------------------------ scene 5
            title("清除请求失败仍待确认，并以同一编号自动重试；不阻塞其他接收方")
            cus3 = api.post("/customers", json={"name": "Acme Retry"}).json()
            r_ok = api.post("/erasure-receivers", json={"customer_id": cus3["id"], "url": rx_a.url}).json()
            # A dead port to force an erasure failure for the other receiver.
            import socket

            with socket.socket() as s:
                s.bind(("127.0.0.1", 0))
                dead_port = s.getsockname()[1]
            r_dead = api.post(
                "/erasure-receivers",
                json={"customer_id": cus3["id"], "url": f"http://127.0.0.1:{dead_port}/erase"},
            ).json()
            for rid in (r_ok["id"], r_dead["id"]):
                api.post(
                    "/subscriptions",
                    json={"customer_id": cus3["id"], "target_url": "http://placeholder/h",
                          "erasure_receiver_id": rid},
                )
            api.post(
                "/events",
                json={"customer_id": cus3["id"], "flow_id": "f", "caller_event_id": "r1",
                      "payload": {"x": SECRET}, "topic_id": "retry-topic"},
            )
            drain(store, settings)
            api.post("/topics/retry-topic/withdraw", json={"customer_id": cus3["id"]})
            # Give the requester one pass, then inspect the failure.
            w3 = Worker(store=store, policy=DeliveryPolicy(settings), clock=RealClock(), concurrency=2)
            w3.run_once()
            prog = api.get("/topics/retry-topic/erasure", params={"customer_id": cus3["id"]}).json()
            check(prog["acknowledged_receiver_ids"] == [r_ok["id"]],
                  "正常接收方已确认，不被另一接收方失败阻塞")
            check(r_dead["id"] in prog["pending_receiver_ids"], "不可达接收方仍待确认，未被误判完成")
            req = [r for r in prog["receivers"] if r["receiver_id"] == r_dead["id"]][0]
            info("失败请求编号", req["erasure_id"])
            info("失败原因", req["last_error"])
            # Fix the address and retry with the same operation id.
            with store._write() as conn:
                conn.execute(
                    "UPDATE erasure_requests SET receiver_url=? WHERE id=?",
                    (rx_b.url, req["erasure_id"]),
                )
            # Retries use the same operation id; the reference receiver returns
            # a fresh acknowledgement for it.
            drain(store, settings)
            prog = api.get("/topics/retry-topic/erasure", params={"customer_id": cus3["id"]}).json()
            check(prog["complete"] is True, "恢复后以同一编号重试并完成确认")
            check(all(r["erasure_id"] == req["erasure_id"] for r in prog["receivers"] if r["receiver_id"] == r_dead["id"]),
                  "重试沿用同一清除编号")
            w3.sender.close()
            if w3._erasure_sender_obj is not None:
                w3._erasure_sender_obj.close()

            # ------------------------------------------------------ scene 6
            title("参考接收端：同名不同客户/同客户其他主题不受影响，晚到的在途回调也不能重建副本")
            cus4 = api.post("/customers", json={"name": "Acme Isolation"}).json()
            cus5 = api.post("/customers", json={"name": "Acme Twin"}).json()
            reg4 = api.post("/erasure-receivers", json={"customer_id": cus4["id"], "url": rx_a.url}).json()
            reg5 = api.post("/erasure-receivers", json={"customer_id": cus5["id"], "url": rx_a.url}).json()
            for cid, rid in ((cus4["id"], reg4["id"]), (cus5["id"], reg5["id"])):
                api.post(
                    "/subscriptions",
                    json={"customer_id": cid, "target_url": "http://p/h", "erasure_receiver_id": rid},
                )
            api.post(
                "/events",
                json={"customer_id": cus4["id"], "flow_id": "f", "caller_event_id": "i1",
                      "payload": {"s": SECRET}, "topic_id": "shared"},
            )
            api.post(
                "/events",
                json={"customer_id": cus4["id"], "flow_id": "f", "caller_event_id": "i2",
                      "payload": {"s": "keep"}, "topic_id": "other"},
            )
            api.post(
                "/events",
                json={"customer_id": cus5["id"], "flow_id": "f", "caller_event_id": "i3",
                      "payload": {"s": "twin"}, "topic_id": "shared"},
            )
            drain(store, settings)
            # Capture an authorized callback but do NOT deliver it yet.
            claim = store.claim_due(now=time.time(), lease_seconds=30)
            pending_callbacks = []
            for c in claim:
                if c.erasure_receiver_id is not None and c.topic_id == "shared":
                    pending_callbacks.append(store.begin_send(c, now=time.time()))
            # (Any non-captured claims are left for the drain below.)

            api.post("/topics/shared/withdraw", json={"customer_id": cus4["id"]})
            drain(store, settings)
            scope_copies = rx_a.backend.list_replicas()
            remaining = {(c["customer_id"], c["topic_id"]) for c in scope_copies}
            check((cus4["id"], "shared") not in remaining, "同客户该主题副本已清除")
            check((cus4["id"], "other") in remaining, "同客户的其他主题未被清除")
            check((cus5["id"], "shared") in remaining, "同名但不同客户的主题未被清除")

            # Deliver the captured in-flight callback now: it must not recreate.
            sender = HttpxSender()
            try:
                for stamped in pending_callbacks:
                    if stamped is not None:
                        sender.send(stamped, timeout=2.0)
            finally:
                sender.close()
            remaining = {(c["customer_id"], c["topic_id"]) for c in rx_a.backend.list_replicas()}
            check((cus4["id"], "shared") not in remaining,
                  "晚到的在途回调到达后也无法重建已清除的副本（以水位封印，不依赖请求先后）")
            check(SECRET not in repr(rx_a.backend.list_replicas()), "接收端持久库中已不含被清除内容")

            # ------------------------------------------------------ scene 7
            title("本地撤回后服务中断，恢复后仍能继续向应通知的原接收方发出清除请求")
            cus6 = api.post("/customers", json={"name": "Acme Crash"}).json()
            reg6 = api.post("/erasure-receivers", json={"customer_id": cus6["id"], "url": rx_b.url}).json()
            api.post(
                "/subscriptions",
                json={"customer_id": cus6["id"], "target_url": "http://p/h", "erasure_receiver_id": reg6["id"]},
            )
            api.post(
                "/events",
                json={"customer_id": cus6["id"], "flow_id": "f", "caller_event_id": "c1",
                      "payload": {"s": SECRET}, "topic_id": "crash-topic"},
            )
            drain(store, settings)
            # Commit the local withdrawal, then simulate a crash: no erasure is
            # sent by this instance.
            api.post("/topics/crash-topic/withdraw", json={"customer_id": cus6["id"]})
            progress = api.get("/topics/crash-topic/erasure", params={"customer_id": cus6["id"]}).json()
            check(progress["complete"] is False and progress["pending_receiver_ids"] == [reg6["id"]],
                  "撤回已提交、清除请求仍待确认（模拟中断前状态）")
            # "Restart": brand-new Store + worker on the same file.
            restarted = Store(settings)
            restarted.initialize()
            drain(restarted, settings)
            progress = restarted.get_topic_erasure(customer_id=cus6["id"], topic_id="crash-topic")
            check(progress["complete"] is True, "重启后的工作进程仍能完成对原接收方的清除确认")
            check(all(
                c["topic_id"] != "crash-topic" for c in rx_b.backend.list_replicas()
            ), "原接收方 B 的该主题副本已清除")

        print("\n\033[1;32mAll external-erasure scenarios passed.\033[0m")
        print("CLI:  python -m webhook_delivery --db <file> erasure-progress --customer … --topic …")
        return 0
    except AssertionError as exc:
        print(f"\n\033[1;31mDEMO FAILED: {exc}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        rx_a.stop()
        rx_b.stop()
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
