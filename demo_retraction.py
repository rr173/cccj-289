"""End-to-end demo for topic withdrawal + ticket evidence retention.

Runs entirely on one machine with a throw-away SQLite database:

* the real FastAPI app through TestClient (real routing/validation);
* the real Worker lease/claim path with a scripted sender for ordering;
* a real local HTTP receiver that echoes payload content in a late 500,
  proving late results cannot bring redacted content back into the DB/API.

Run:  ./run_retraction_demo.sh
or:   PYTHONPATH=.pylibs:. python3 demo_retraction.py
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path

# Allow a direct ``python3 demo_retraction.py`` run without exporting PYTHONPATH.
_ROOT = Path(__file__).resolve().parent
_PYLIBS = _ROOT / ".pylibs"
if _PYLIBS.is_dir() and str(_PYLIBS) not in sys.path:
    sys.path.insert(0, str(_PYLIBS))
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from webhook_delivery.api import create_app  # noqa: E402
from webhook_delivery.config import DeliveryPolicy, RealClock, Settings  # noqa: E402
from webhook_delivery.receiver import Receiver, Response  # noqa: E402
from webhook_delivery.store import Store  # noqa: E402
from webhook_delivery.worker import Worker  # noqa: E402

ROOT = _ROOT
SECRET = "DEMO_SECRET_CONTENT_909"
STEP = 0


def title(text: str) -> None:
    global STEP
    STEP += 1
    print(f"\n\033[1;36m══ {STEP}. {text}\033[0m")


def info(label: str, value: object) -> None:
    print(f"   {label:<38} {value}")


def ok(text: str) -> None:
    print(f"   \033[1;32m✔ {text}\033[0m")


def check(condition: bool, text: str) -> None:
    if not condition:
        raise AssertionError(text)
    ok(text)


def db_has_secret(db_path: str, secret: str = SECRET) -> bool:
    con = sqlite3.connect(db_path)
    try:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            for row in con.execute(f"SELECT * FROM {table}"):
                if secret in repr(row):
                    return True
        return False
    finally:
        con.close()


class RecordingSender:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send(self, claim, timeout):  # noqa: ANN001
        self.sent.append(claim.id)
        from webhook_delivery.worker import SendResult

        return SendResult("succeeded", 200)


def main() -> int:
    tmp = tempfile.TemporaryDirectory(prefix="webhook-retract-")
    db_path = str(Path(tmp.name) / "retract.db")
    settings = Settings(
        database_path=db_path,
        backoff_seconds=(0.05, 0.1, 0.2),
        request_timeout_seconds=2.0,
        lease_seconds=5.0,
        worker_poll_seconds=0.01,
    )
    store = Store(settings)
    store.initialize()
    app = create_app(settings=settings, store=store, clock=RealClock())

    try:
        with TestClient(app) as api:
            print("\033[1mTopic withdrawal & ticket retention demo\033[0m")
            info("db", db_path)

            # -------------------------------------------------------- scene 1
            title("工单留存 → 按主题撤回 → 仅未结案工单可读 → 结案即清除")
            cus = api.post("/customers", json={"name": "Acme Investigation"}).json()

            ev = api.post(
                "/events",
                json={
                    "customer_id": cus["id"],
                    "flow_id": "orders",
                    "caller_event_id": "ord-1",
                    "payload": {"card_note": SECRET, "amount": 42},
                    "topic_id": "gdpr-42",
                },
            ).json()
            plain = api.post(
                "/events",
                json={
                    "customer_id": cus["id"],
                    "flow_id": "orders",
                    "caller_event_id": "ord-2",
                    "payload": {"amount": 43},
                },
            ).json()
            info("topic event / plain event seq", f"{ev['seq']} / {plain['seq']}")

            t1 = api.post("/tickets", json={"customer_id": cus["id"], "title": "investigation 4821"}).json()
            t2 = api.post("/cases", json={"customer_id": cus["id"], "title": "parallel audit"}).json()
            r1 = api.post(f"/tickets/{t1['id']}/events", json={"event_id": ev["id"]})
            r2 = api.post(f"/cases/{t2['id']}/retain", json={"event_ids": [ev["id"]]})
            check(r1.status_code == 200 and r2.status_code == 200, "同一事件可被两个工单同时留存")

            w = api.post("/topics/gdpr-42/withdraw", json={"customer_id": cus["id"]})
            check(w.status_code == 200 and w.json()["events_affected"] == 1, "按主题撤回成功")
            w2 = api.post("/topics/withdraw", json={"customer_id": cus["id"], "topic_id": "gdpr-42"})
            check(w2.json()["already_withdrawn"] is True, "撤回请求可重复且幂等")

            listing = api.get("/events", params={"customer_id": cus["id"]}).json()
            topic_row = [e for e in listing if e["id"] == ev["id"]][0]
            check(
                topic_row["withdrawn"] and topic_row["payload"] is None and topic_row["content_hash"] is None,
                "常规事件接口只剩 ID/顺序等墓碑记录，不返回 JSON 或摘要",
            )
            check(
                SECRET not in json.dumps(listing, ensure_ascii=False),
                "事件列表响应中不含原内容",
            )

            body = {"customer_id": cus["id"], "flow_id": "orders", "caller_event_id": "ord-1",
                    "payload": {"card_note": SECRET, "amount": 42}, "topic_id": "gdpr-42"}
            check(api.post("/events", json=body).status_code == 409, "原事件键重送被拒绝，不能恢复")
            body2 = dict(body, payload={"evil": "changed"})
            check(api.post("/events", json=body2).status_code == 409, "原事件键换内容重用被拒绝")
            new_topic = api.post(
                "/events",
                json={"customer_id": cus["id"], "flow_id": "orders", "caller_event_id": "ord-9",
                      "payload": {}, "topic_id": "gdpr-42"},
            )
            check(new_topic.status_code == 409, "该客户此主题的新事件被拒绝")

            e1 = api.get(f"/tickets/{t1['id']}/events").json()
            e2 = api.get(f"/tickets/{t2['id']}/evidence").json()
            check(e1[0]["payload"]["card_note"] == SECRET, "撤回后工单 T1 仍可读撤回前留存的证据")
            check(e2[0]["payload"]["card_note"] == SECRET, "工单 T2 同样可读（多工单独立留存）")

            check(api.post(f"/tickets/{t1['id']}/close", json={}).status_code == 200, "T1 结案")
            check(api.get(f"/tickets/{t1['id']}/events").status_code == 409, "结案后 T1 不能再读证据")
            check(
                api.post(f"/tickets/{t1['id']}/events", json={"event_id": plain["id"]}).status_code == 409,
                "结案后 T1 不能再追加",
            )
            check(api.get(f"/tickets/{t2['id']}/events").status_code == 200, "T2 未结案，仍可读，不受 T1 结案影响")
            check(db_has_secret(db_path), "仍有有效留存时，工单副本保留原内容（符合预期）")

            check(api.post(f"/cases/{t2['id']}/close").status_code == 200, "T2 结案")
            time.sleep(0.05)
            check(not db_has_secret(db_path), "所有有效留存消失后，正常持久数据中已无原内容")

            # -------------------------------------------------------- scene 2
            title("撤回终止未完成投递，且不阻塞同流下一条未撤回事件")
            cus2 = api.post("/customers", json={"name": "Acme Delivery"}).json()
            recv_ok = Receiver().start()
            try:
                sub = api.post(
                    "/subscriptions", json={"customer_id": cus2["id"], "target_url": "http://unused/h"}
                ).json()
                e_a = api.post(
                    "/events",
                    json={"customer_id": cus2["id"], "flow_id": "stream", "caller_event_id": "a",
                          "payload": {"n": 1}, "topic_id": "kill"}
                ).json()
                e_b = api.post(
                    "/events",
                    json={"customer_id": cus2["id"], "flow_id": "stream", "caller_event_id": "b",
                          "payload": {"n": 2}}
                ).json()
                d_a = e_a["deliveries"][0]["delivery_id"]
                d_b = e_b["deliveries"][0]["delivery_id"]

                api.post("/topics/kill/withdraw", json={"customer_id": cus2["id"]})
                check(api.get(f"/deliveries/{d_a}").json()["status"] == "withdrawn", "主题事件投递状态=withdrawn")

                sender = RecordingSender()
                worker = Worker(
                    store=store, policy=DeliveryPolicy(settings), clock=RealClock(), sender=sender
                )
                worker.run_until_idle()
                check(sender.sent == [d_b], f"worker 只发送同流后继 {d_b}（实际 {len(sender.sent)} 封）")
                check(api.get(f"/deliveries/{d_b}").json()["status"] == "succeeded", "未撤回的后继事件成功投递")
                check(api.patch(
                    f"/subscriptions/{sub['id']}",
                    json={"customer_id": cus2["id"], "target_url": recv_ok.url},
                ).status_code == 200, "修改目标地址成功")
                check(api.get(f"/deliveries/{d_a}").json()["status"] == "withdrawn", "改址不会恢复已撤回投递")
            finally:
                recv_ok.stop()

            # -------------------------------------------------------- scene 3
            title("已在发送中的请求：晚到的接收端回显不能改写交付或污染诊断记录")
            cus3 = api.post("/customers", json={"name": "Acme Echo"}).json()
            recv = Receiver().start()
            try:
                # 500 响应体回显密钥，并延迟，确保撤回事务先提交。
                recv.set_default(Response(status=500, delay_seconds=0.5, body=json.dumps({"echo": SECRET})))
                api.post("/subscriptions", json={"customer_id": cus3["id"], "target_url": recv.url})
                ev3 = api.post(
                    "/events",
                    json={"customer_id": cus3["id"], "flow_id": "echo-flow", "caller_event_id": "echo-1",
                          "payload": {"secret": SECRET}, "topic_id": "echo-topic"}
                ).json()
                d3 = ev3["deliveries"][0]["delivery_id"]

                worker = Worker(store=store, policy=DeliveryPolicy(settings), clock=RealClock())
                thread = threading.Thread(target=worker.run_once, name="demo-worker")
                thread.start()
                check(recv.wait_for(lambda rs: len(rs) >= 1, timeout=5), "真实 HTTP 请求已经发出，接收端收到")

                t0 = time.monotonic()
                wr = api.post(
                    "/topics/echo-topic/withdraw",
                    json={"customer_id": cus3["id"], "wait_for_inflight": True, "inflight_timeout": 5.0},
                )
                elapsed = time.monotonic() - t0
                thread.join(timeout=5)
                check(not thread.is_alive(), "worker 的在途请求已结束")
                info("withdraw waited (s)", f"{elapsed:.2f}")
                check(wr.status_code == 200, "撤回接口等待本主题在途请求结束后成功返回")

                detail = api.get(f"/deliveries/{d3}").json()
                check(detail["status"] == "withdrawn", "晚到 500 没有把交付改写成 failed/retry")
                check(SECRET not in json.dumps(detail, ensure_ascii=False), "交付详情/诊断记录中没有回显内容")
                check(not db_has_secret(db_path), "数据库所有表中均无该密钥（含 late_results/attempts）")
                worker.sender.close()
            finally:
                recv.stop()

        print("\n\033[1;32mAll retraction scenarios passed.\033[0m")
        print("CLI on this kind of database:  python -m webhook_delivery --db <file> --help")
        return 0
    except AssertionError as exc:
        print(f"\n\033[1;31mDEMO FAILED: {exc}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
