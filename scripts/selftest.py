#!/usr/bin/env python3
"""端到端自测（仅依赖标准库）：用临时目录拉起两个后端 + 一个代理并逐项核验。

覆盖：
  1. 普通写入落现役节点，备用节点无该写入（无重复投递）；
  2. 慢回执写入在途时切流：切流立即生效，新请求走新节点，
     在途写入仍由旧节点回执，且只存在于旧节点；
  3. 目标节点健康故障时切流被拦截（409），旧路由保持；
  4. 连续两次切流都不打断各自的在途写入（再次切流安全）；
  5. 回执超时：返回 504 + TIMEOUT_UNCERTAIN，不自动补投；
     事后向"原送达节点"查询可确认其实已落库并回执；
  6. 全量核对：两侧后端写入 ID 集合不相交，每条写入恰好出现一次；
  7. 暂停接流：新写入立即 503 拒绝（不落台账、不投后端），在途写入继续取回执，
     切流被锁定（409），全部确认后显示可备份，手动恢复后写入正常；
  8. 暂停与并发写入交错：受理边界清晰——每条写入要么完整受理（台账+后端齐全），
     要么干净拒绝（台账与后端都无痕迹），已受理的一条不丢；
  9. 待核写入的原节点失联（杀后端进程）：无法核实，绝不误报可备份；
     节点恢复后核实为已落库，状态更正 ACKED；
 10. 代理重启：暂停状态与待核记录仍有效（新写入仍拒、切流仍锁），
     值班员手动核实（确认 / 确知未受理）后才显示可备份，再手动恢复接流；
 11. 终态全量核对：ACKED 与后端记录一一对应，FAILED/NOT_ACCEPTED 两侧后端均无记录。

用法:
  python3 -I scripts/selftest.py [--python /path/to/venv/python]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS, FAIL = 0, 0
RESULTS: list[tuple[bool, str]] = []


def check(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        RESULTS.append((True, f"PASS  {name}" + (f"  ({detail})" if detail else "")))
        print(f"  \033[32mPASS\033[0m  {name} {detail}")
    else:
        FAIL += 1
        RESULTS.append((False, f"FAIL  {name}  {detail}"))
        print(f"  \033[31mFAIL\033[0m  {name} {detail}")


def http(method: str, url: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())
    except Exception as e:  # 连接被重置/拒接等（如代理被杀）：返回 0 供断言
        return 0, {"error": f"{type(e).__name__}: {e}"}


def wait_ready(url: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_for(fn, timeout: float = 15, interval: float = 0.15, desc: str = "condition"):
    """轮询直到 fn() 返回真值，返回该真值；超时抛 AssertionError。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = fn()
        if v:
            return v
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for {desc}")


def wait_status(proxy: str, write_id: str, expect: str, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        for w in s["writes"]:
            if w["id"] == write_id:
                if w["status"] == expect:
                    return w
        time.sleep(0.15)
    raise AssertionError(f"write {write_id} never reached status {expect}")


class SlowPost(threading.Thread):
    def __init__(self, proxy: str, delay: float, payload: str):
        super().__init__(daemon=True)
        self.proxy, self.delay, self.payload = proxy, delay, payload
        self.result: tuple[int, dict] | None = None
        self.write_id: str | None = None

    def run(self):
        self.result = http("POST", f"{self.proxy}/api/writes?delay={self.delay}",
                           {"payload": self.payload}, timeout=30)
        if self.result:
            self.write_id = self.result[1].get("id")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="cutover-selftest-"))
    logs = work / "logs"
    logs.mkdir()
    procs: list[subprocess.Popen] = []
    handles: dict[str, subprocess.Popen] = {}
    envs: dict[str, dict] = {}
    ports = {"proxy": 18000, "A": 18001, "B": 18002}
    proxy = f"http://127.0.0.1:{ports['proxy']}"

    def spawn(kind: str, app_dir: Path, env: dict, port: int) -> None:
        log = open(logs / f"{kind}.log", "ab")
        p = subprocess.Popen(
            [args.python, "-I", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--app-dir", str(app_dir)],
            env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append(p)
        handles[kind] = p
        envs[kind] = (app_dir, env, port)

    def respawn(kind: str) -> None:
        """按原参数重启一个进程（复用同一数据目录/端口）。"""
        app_dir, env, port = envs[kind]
        spawn(kind, app_dir, env, port)

    def stop_proc(kind: str, hard: bool = False) -> None:
        p = handles[kind]
        if p.poll() is None:
            p.kill() if hard else p.terminate()
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)

    try:
        print("== 启动两个后端 + 代理（读回执超时 2s，便于演练超时场景） ==")
        spawn("backend-a", ROOT / "backend",
              {"NODE_NAME": "A", "BACKEND_DB": str(work / "a.db")}, ports["A"])
        spawn("backend-b", ROOT / "backend",
              {"NODE_NAME": "B", "BACKEND_DB": str(work / "b.db")}, ports["B"])
        ok_a = wait_ready(f"http://127.0.0.1:{ports['A']}/healthz")
        ok_b = wait_ready(f"http://127.0.0.1:{ports['B']}/healthz")
        check(ok_a and ok_b, "两个后端启动并健康", f"A={ok_a} B={ok_b}")

        spawn("proxy", ROOT / "proxy", {
            "NODE_A_NAME": "A", "NODE_B_NAME": "B",
            "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
            "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
            "PROXY_DB": str(work / "proxy.db"),
            "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
        }, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理启动并提供控制台页面")

        _, st = http("GET", f"{proxy}/api/status")
        check(st["active"] == "A", "初始现役节点为 A", f"active={st['active']}")

        print("\n== 场景 1：普通写入只落现役节点 A ==")
        code, w0 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "base-write"})
        check(code == 201 and w0["status"] == "ACKED", "普通写入成功回执",
              f"id={w0.get('id','')[:8]} node={w0.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(any(r["id"] == w0["id"] for r in rec_a["records"]), "写入存在于 A 的 SQLite")
        check(not any(r["id"] == w0["id"] for r in rec_b["records"]), "B 上没有该写入（未重复投递）")

        print("\n== 场景 2：慢回执在途切流 A -> B，不中断、不重复 ==")
        slow1 = SlowPost(proxy, 1.2, "slow-during-cutover-1")
        slow1.start()
        # 等代理把 PENDING 行落库（请求已送达 A、回执未归）
        deadline = time.time() + 5
        pending_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["status"] == "PENDING":
                    pending_id = row["id"]
                    inflight = s["drainage"]["A"]["inflight_now"]
                    check(inflight >= 1, "切流前 A 显示在途写入", f"inflight_now={inflight}")
                    break
            if pending_id:
                break
            time.sleep(0.1)
        check(bool(pending_id), "慢写入已进入 PENDING（已送达、待回执）")

        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["ok"] and sw["active"] == "B", "在途切流 A -> B 成功")
        _, s2 = http("GET", f"{proxy}/api/status")
        check(s2["drainage"]["A"]["draining"] is True, "旧节点 A 进入排空调（DRAINING）")

        slow1.join(timeout=10)
        check(not slow1.is_alive() and slow1.result and slow1.result[0] == 201,
              "在途写入未被切流打断，仍正常拿到回执")
        check(slow1.write_id == pending_id and slow1.result[1]["node"] == "A",
              "回执仍来自原送达节点 A", f"node={slow1.result[1].get('node') if slow1.result else '?'}")
        row = wait_status(proxy, pending_id, "ACKED")
        check(row["target_node"] == "A", "代理记录中该写入始终绑定 A")

        code, w_new = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "after-switch"})
        check(code == 201 and w_new["node"] == "B", "切流后新请求立即走新节点 B",
              f"node={w_new.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(pending_id in ids_a and pending_id not in ids_b, "慢写入只在 A，未被投到 B")
        check(w_new["id"] in ids_b and w_new["id"] not in ids_a, "新写入只在 B")

        print("\n== 场景 3：目标节点探测失败 -> 保持旧路由 ==")
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": True})
        code, blocked = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 409 and blocked.get("blocked") and blocked.get("active") == "B",
              "A 故障时切向 A 被 409 拦截，现役仍为 B", blocked.get("reason", "")[:90])
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": False})
        code, sw2 = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw2["active"] == "A", "故障解除后切流成功，现役回到 A")

        print("\n== 场景 4：连续再次切流，各自在途写入都绑定原节点 ==")
        t1 = SlowPost(proxy, 1.2, "roundtrip-a")
        t1.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "第二次切流 A -> B 成功且不打断在途写入")
        t1.join(timeout=10)
        check(t1.result and t1.result[1]["node"] == "A", "切走前送达 A 的写入仍由 A 回执")

        t2 = SlowPost(proxy, 1.2, "roundtrip-b")
        t2.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200, "第三次切流 B -> A 成功")
        t2.join(timeout=10)
        check(t2.result and t2.result[1]["node"] == "B",
              "切走前送达 B 的写入仍由 B 回执（再次切流不打断它）")

        print("\n== 场景 5：回执超时 -> 结果不确定，不补投，可事后向原节点核实 ==")
        active_before = http("GET", f"{proxy}/api/status")[1]["active"]
        t0 = time.time()
        code, tout = http("POST", f"{proxy}/api/writes?delay=5",
                          {"payload": "timeout-case"}, timeout=15)
        elapsed = time.time() - t0
        check(code == 504 and tout.get("uncertain") and
              tout["status"] == "TIMEOUT_UNCERTAIN",
              f"读超时返回 504 + TIMEOUT_UNCERTAIN（{elapsed:.1f}s 触发）")
        timeout_id = tout["id"]
        timeout_node = tout["node"]
        _, s3 = http("GET", f"{proxy}/api/status")
        row = next(w for w in s3["writes"] if w["id"] == timeout_id)
        check(row["status"] == "TIMEOUT_UNCERTAIN", "超时状态已落库，页面可见结果不确定")
        _, rec_other = http("GET", f"{proxy}/api/nodes/"
                            f"{'B' if timeout_node == 'A' else 'A'}/records")
        check(not any(r["id"] == timeout_id for r in rec_other["records"]),
              "超时后代理没有向另一节点补投")

        print("   等待后端慢回执结束后，向原送达节点查询核实……")
        time.sleep(4.2)
        code, ref = http("GET", f"{proxy}/api/writes/{timeout_id}/refresh")
        check(code == 200 and ref["write"]["status"] == "ACKED"
              and ref["write"]["target_node"] == timeout_node,
              "向原节点查询后确认：写入其实已落库并回执，状态就地更正，未换节点")

        print("\n== 场景 6：全量核对，两侧后端写入不相交、每条恰好一次 ==")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "A/B 后端写入 ID 集合不相交（无任何重复投递）")
        _, s_all = http("GET", f"{proxy}/api/status")
        proxy_ids = {w["id"] for w in s_all["writes"]}
        check(proxy_ids == ids_a | ids_b,
              f"代理侧 {len(proxy_ids)} 条写入与后端记录一一对应",
              f"A={len(ids_a)} B={len(ids_b)}")

        # 直接打开后端 SQLite 核对持久化（后端各自保存写入的硬性要求）
        for node, db_path in (("A", work / "a.db"), ("B", work / "b.db")):
            con = sqlite3.connect(db_path)
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            con.close()
            check(n == len(ids_a if node == "A" else ids_b),
                  f"后端 {node} 的 SQLite 持久化写入数 = {n}")

        print("\n== 场景 7：暂停接流 —— 新写入即拒、在途续跑、切流锁定、静默后可备份 ==")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["pause"]["paused"] is False and s["backup"]["ready"] is False,
              "未暂停时：接流中，不显示可备份")

        slow = SlowPost(proxy, 1.6, "pause-inflight")
        slow.start()
        pid = None
        try:
            pid = wait_for(lambda: next(
                (w["id"] for w in http("GET", f"{proxy}/api/status")[1]["writes"]
                 if w["status"] == "PENDING"), None),
                timeout=8, desc="PENDING row of pause-inflight")
        except AssertionError:
            pass
        check(bool(pid), "慢写入进入 PENDING（暂停演练就绪）")

        code, pz = http("POST", f"{proxy}/api/pause", {"note": "备份演练"})
        check(code == 200 and pz.get("paused") is True, "暂停接流开启（持久）")

        code, rej = http("POST", f"{proxy}/api/writes?delay=0",
                         {"payload": "during-pause"})
        check(code == 503 and rej.get("status") == "INTAKE_PAUSED",
              "暂停期间新写入立即返回暂停状态（503 INTAKE_PAUSED）")
        rej_id = rej.get("id")
        _, s = http("GET", f"{proxy}/api/status")
        check(all(w["id"] != rej_id for w in s["writes"]),
              "被拒写入不落代理台账（writes 无记录）")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(not any(r["id"] == rej_id
                      for r in rec_a["records"] + rec_b["records"]),
              "被拒写入不投任何后端")

        active_now = s["active"]
        code, swb = http("POST", f"{proxy}/api/switch",
                         {"target": "B" if active_now == "A" else "A"})
        check(code == 409 and swb.get("blocked"),
              "暂停期间切流被禁止（409），现役节点不变")

        _, s = http("GET", f"{proxy}/api/status")
        blk_ids = [b["id"] for b in s["backup"]["blockers"]]
        check(s["backup"]["ready"] is False and pid in blk_ids,
              "在途写入阻塞静默：不显示可备份，且页面列出该在途写入")

        slow.join(timeout=10)
        check(slow.result and slow.result[0] == 201
              and slow.result[1].get("node") == active_now,
              "已受理的在途写入不受暂停影响，仍从原节点取回执")

        ok = False
        try:
            ok = bool(wait_for(
                lambda: http("GET", f"{proxy}/api/status")[1]["backup"]["ready"],
                timeout=10, desc="backup ready"))
        except AssertionError:
            pass
        check(ok, "全部确认后显示可备份（静默达成）")

        code, rs = http("POST", f"{proxy}/api/resume")
        check(code == 200 and rs.get("paused") is False, "值班员手动恢复接流")
        code, w = http("POST", f"{proxy}/api/writes?delay=0",
                       {"payload": "after-resume"})
        check(code == 201, "恢复后新写入正常受理")

        print("\n== 场景 8：暂停与并发写入交错 —— 受理边界清晰，已受理不丢 ==")
        batch1 = [SlowPost(proxy, 0.5, f"race-accepted-{i}") for i in range(6)]
        for t in batch1:
            t.start()
        ok6 = False
        try:
            ok6 = bool(wait_for(
                lambda: sum(1 for w in http("GET", f"{proxy}/api/status")[1]["writes"]
                            if w["status"] == "PENDING") >= 6,
                timeout=8, desc="6 PENDING in flight"))
        except AssertionError:
            pass
        check(ok6, "第一批 6 条并发写入全部受理在途")

        # 暂停与第二批并发写入同时发出：每条要么完整受理，要么干净拒绝
        batch2 = [SlowPost(proxy, 0, f"race-maybe-{i}") for i in range(8)]
        for t in batch2:
            t.start()
        code, pz = http("POST", f"{proxy}/api/pause", {"note": "并发边界演练"})
        check(code == 200 and pz.get("paused") is True, "并发写入中暂停成功")
        for t in batch1 + batch2:
            t.join(timeout=15)

        acc1 = [t for t in batch1 if t.result and t.result[0] == 201]
        check(len(acc1) == 6, "第一批已受理写入全部正常回执（暂停不丢已受理）",
              f"{len(acc1)}/6")
        acc2 = [t for t in batch2 if t.result and t.result[0] == 201]
        rej2 = [t for t in batch2 if t.result and t.result[0] == 503]
        check(len(acc2) + len(rej2) == 8,
              f"第二批每条要么完整受理要么干净拒绝（受理 {len(acc2)} / 拒绝 {len(rej2)}）")

        _, s = http("GET", f"{proxy}/api/status")
        ledger = {w["id"] for w in s["writes"]}
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        backend_ids = ({r["id"] for r in rec_a["records"]}
                       | {r["id"] for r in rec_b["records"]})
        check(all(t.write_id not in ledger and t.write_id not in backend_ids
                  for t in rej2),
              "被拒写入：台账与两个后端都无任何痕迹")
        check(all(t.write_id in ledger and t.write_id in backend_ids
                  for t in acc2),
              "交错中被受理的写入：台账与后端记录完整（受理边界清晰）")

        # 暂停确认后到达的写入必须全部被拒（确定性）
        batch3 = [SlowPost(proxy, 0, f"race-late-{i}") for i in range(4)]
        for t in batch3:
            t.start()
        for t in batch3:
            t.join(timeout=15)
        rej3 = [t for t in batch3 if t.result and t.result[0] == 503]
        _, s = http("GET", f"{proxy}/api/status")
        ledger = {w["id"] for w in s["writes"]}
        check(len(rej3) == 4
              and all(t.write_id not in ledger for t in rej3),
              "暂停生效后到达的写入全部被拒且不落台账", f"{len(rej3)}/4")
        code, _ = http("POST", f"{proxy}/api/resume")
        check(code == 200, "恢复接流")

        print("\n== 场景 9：待核写入节点失联 —— 绝不误报可备份；恢复后可核实 ==")
        _, s = http("GET", f"{proxy}/api/status")
        active9 = s["active"]
        code, tout = http("POST", f"{proxy}/api/writes?delay=5",
                          {"payload": "lost-node-case"}, timeout=15)
        check(code == 504 and tout.get("status") == "TIMEOUT_UNCERTAIN",
              "构造一条结果不确定写入（读超时）")
        tid, tnode = tout["id"], tout["node"]
        http("POST", f"{proxy}/api/pause", {"note": "失联演练"})
        # 直接杀掉该后端进程（断线），此时它的慢回执（5s）尚未完成
        stop_proc(f"backend-{tnode.lower()}")
        time.sleep(0.3)
        _, s = http("GET", f"{proxy}/api/status")
        blk = next((b for b in s["backup"]["blockers"] if b["id"] == tid), None)
        check(blk is not None and blk["node_reachable"] is False
              and s["backup"]["ready"] is False,
              "节点失联：待核写入阻塞静默，页面标记节点不可达，不显示可备份")
        code, ref = http("GET", f"{proxy}/api/writes/{tid}/refresh")
        check(ref.get("refreshed") is False
              and ref["write"]["status"] == "TIMEOUT_UNCERTAIN",
              "失联时核实失败，不确定状态被保留（绝不清除）")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["backup"]["ready"] is False, "失联期间任何时刻都不误报可备份")

        # 恢复该后端（同一数据卷/DB 文件）：写入早已持久化，重启收尾补齐回执记账
        respawn(f"backend-{tnode.lower()}")
        check(wait_ready(f"http://127.0.0.1:{ports[tnode]}/healthz"),
              f"后端 {tnode} 重启恢复（同一数据卷）")
        code, ref = http("GET", f"{proxy}/api/writes/{tid}/refresh?wait=1",
                         timeout=70)
        check(ref["write"]["status"] == "ACKED",
              "节点恢复后向原节点核实：写入确已落库，状态更正为 ACKED")
        ok = False
        try:
            ok = bool(wait_for(
                lambda: http("GET", f"{proxy}/api/status")[1]["backup"]["ready"],
                timeout=10, desc="ready after heal"))
        except AssertionError:
            pass
        check(ok, "全部确认后显示可备份")
        http("POST", f"{proxy}/api/resume")

        print("\n== 场景 10：代理重启 —— 暂停状态与待核记录仍有效，手动核实与恢复 ==")
        _, s = http("GET", f"{proxy}/api/status")
        active10 = s["active"]
        slow = SlowPost(proxy, 6, "restart-inflight")
        slow.start()
        rid = None
        try:
            rid = wait_for(lambda: next(
                (w["id"] for w in http("GET", f"{proxy}/api/status")[1]["writes"]
                 if w["status"] == "PENDING"), None),
                timeout=8, desc="PENDING before restart")
        except AssertionError:
            pass
        check(bool(rid), "重启前构造一条在途写入")
        # 确认后端已落库（该写入确已受理），再杀代理
        okb = False
        try:
            okb = bool(wait_for(
                lambda: any(r["id"] == rid for r in http(
                    "GET", f"http://127.0.0.1:{ports[active10]}/records")[1]
                    .get("records", [])),
                timeout=8, desc="write persisted on backend"))
        except AssertionError:
            pass
        check(okb, "在途写入已在原节点落库（确已受理）")
        http("POST", f"{proxy}/api/pause", {"note": "重启演练"})
        # SIGKILL 代理：模拟崩溃，在途 HTTP 连接中断，但台账与暂停状态已持久化
        stop_proc("proxy", hard=True)
        slow.join(timeout=10)
        check(slow.result and slow.result[0] == 0,
              "代理被杀：在途请求的客户端连接中断（预期现象）")
        # 代理停机期间，白盒插入一条"已超时但从未到达后端"的不确定记录
        # （模拟代理崩溃时请求已记账但未发出的窗口）
        synth_id = str(uuid.uuid4())
        con = sqlite3.connect(work / "proxy.db")
        con.execute(
            "INSERT INTO writes (id, target_node, payload, delay_hint, status, "
            "status_detail, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (synth_id, active10, "synthetic-never-arrived", 0,
             "TIMEOUT_UNCERTAIN", "simulated: request never reached backend",
             time.time(), time.time()))
        con.commit()
        con.close()

        respawn("proxy")
        check(wait_ready(proxy + "/"), "代理重启（同一 SQLite 数据卷）")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["pause"]["paused"] is True, "重启后暂停状态仍有效（未自动恢复）")
        blk_ids = [b["id"] for b in s["backup"]["blockers"]]
        check(rid in blk_ids and synth_id in blk_ids
              and s["backup"]["ready"] is False,
              "重启后待核记录仍有效：在途+不确定都继续阻塞静默")
        code, rej = http("POST", f"{proxy}/api/writes?delay=0",
                         {"payload": "post-restart"})
        check(code == 503 and rej.get("status") == "INTAKE_PAUSED",
              "重启后新写入仍被拒绝")
        code, swb = http("POST", f"{proxy}/api/switch",
                         {"target": "B" if active10 == "A" else "A"})
        check(code == 409, "重启后切流仍被锁定")

        # 手动核实：在途写入的后端慢回执（6s）用 wait=1 等它完成后确认
        code, ref = http("GET", f"{proxy}/api/writes/{rid}/refresh?wait=1",
                         timeout=70)
        check(ref["write"]["status"] == "ACKED",
              "重启后向原节点核实：在途写入最终回执，状态 ACKED（已受理不丢）")
        code, ref2 = http("GET", f"{proxy}/api/writes/{synth_id}/refresh")
        check(ref2["write"]["status"] == "NOT_ACCEPTED",
              "原节点可达且无记录：确知未受理（NOT_ACCEPTED），不再阻塞")
        ok = False
        try:
            ok = bool(wait_for(
                lambda: http("GET", f"{proxy}/api/status")[1]["backup"]["ready"],
                timeout=10, desc="ready after restart"))
        except AssertionError:
            pass
        check(ok, "全部确认/确知未受理后显示可备份")
        code, rs = http("POST", f"{proxy}/api/resume")
        check(code == 200 and rs.get("paused") is False,
              "值班员在网页手动恢复接流")
        code, w = http("POST", f"{proxy}/api/writes?delay=0",
                       {"payload": "post-restart-resume"})
        check(code == 201, "恢复后写入正常")

        print("\n== 场景 11：终态全量核对 ==")
        _, s = http("GET", f"{proxy}/api/status")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a = {r["id"] for r in rec_a["records"]}
        ids_b = {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "A/B 后端写入 ID 集合仍不相交")
        backend_ids = ids_a | ids_b
        acked = {w["id"] for w in s["writes"] if w["status"] == "ACKED"}
        check(acked == backend_ids, "所有 ACKED 写入与两侧后端记录一一对应",
              f"ACKED={len(acked)} 后端={len(backend_ids)}")
        ghost = [w["id"] for w in s["writes"]
                 if w["status"] in ("FAILED", "NOT_ACCEPTED")
                 and w["id"] in backend_ids]
        check(not ghost, "FAILED/NOT_ACCEPTED 写入在任何后端都无记录")
        left = [w["id"] for w in s["writes"]
                if w["status"] in ("PENDING", "TIMEOUT_UNCERTAIN")]
        check(not left, "测试结束时无残留待核写入")

    finally:
        print("\n== 清理进程 ==")
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        if FAIL:
            print(f"\n日志保留在: {logs}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n================ 自测结果: {PASS} 通过 / {FAIL} 失败 ================")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
