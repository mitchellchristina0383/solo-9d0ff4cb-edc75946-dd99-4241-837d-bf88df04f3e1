"""FastAPI 反向代理 + 写入切流控制台。

切流安全语义（本文件的所有代码路径都遵守）：
  1. 每个写入请求在发出前快照"当前现役节点"，并把该节点名持久化到 writes 行；
     之后无论怎么切流，这个请求只与该节点通信，绝不重投到另一节点。
  2. 切流只改 route 表，对已经发往旧节点、尚未回执的在途请求不做任何取消动作；
     回执仍从旧节点读取（也可事后用 /api/writes/{id}/refresh 向原节点查询）。
  3. 切流前先探测目标节点 /healthz，探测失败则保留旧路由并返回 409。
  4. 读超时（写入大概率已在后端落库，只是回执没回来）标记为 TIMEOUT_UNCERTAIN，
     返回 504 并在页面提示"结果不确定"，代理不做任何自动补投/重试。

备份窗口（暂停接流）语义：
  5. /api/pause 启动持久暂停（落 SQLite，代理重启后仍有效，只能 /api/resume 手动恢复）。
     暂停后新写入立即返回 503 INTAKE_PAUSED：不落代理台账、不投任何后端。
  6. 受理边界由 _admit_lock 临界区保证：暂停检查 + 绑定节点 + 台账落库原子完成；
     已出临界区的写入视为已受理，即使暂停随后生效也继续向原节点投递取回执，绝不丢失。
  7. 暂停期间禁止切流（409），避免备份窗口内路由变化。
  8. 是否可备份只看持久台账：所有 PENDING / TIMEOUT_UNCERTAIN 写入全部确认（ACKED）
     或确知未受理（NOT_ACCEPTED / FAILED）才显示可备份；待核写入的原节点失联时
     无法核实，状态保持原状，绝不误报可备份。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from . import db

NODE_A = os.environ.get("NODE_A_NAME", "A")
NODE_B = os.environ.get("NODE_B_NAME", "B")
URL_A = os.environ.get("BACKEND_A_URL", "http://127.0.0.1:8001")
URL_B = os.environ.get("BACKEND_B_URL", "http://127.0.0.1:8002")

# 连接失败 = 节点不可达、请求未送达；读超时 = 请求已发出、回执未归。
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "3.0"))
READ_TIMEOUT = float(os.environ.get("WRITER_READ_TIMEOUT", "30.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "2.5"))

# 暂停期间新写入的响应状态（仅响应用，绝不落库）。
INTAKE_PAUSED = "INTAKE_PAUSED"

# node -> 在途写入 id 集合（仅内存态，进程重启后以 DB 中 PENDING 行作为排空依据）。
_inflight: dict[str, set[str]] = {}
_inflight_lock = asyncio.Lock()

# 受理临界区锁：暂停检查 + 绑定节点 + 台账落库 + 在途登记 原子完成；
# 暂停/恢复与切流的提交也走这把锁，保证"暂停 vs 写入/切流"交错时边界明确。
_admit_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    await db.register_node(NODE_A, URL_A)
    await db.register_node(NODE_B, URL_B)
    _inflight.setdefault(NODE_A, set())
    _inflight.setdefault(NODE_B, set())
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT,
                              write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        # 显式关闭 httpx 默认的连接重试，保证"一次投递"语义。
        limits=httpx.Limits(max_connections=100),
    )
    pause = await db.get_pause()
    blocking = await db.list_blocking_writes()
    await db.add_event(
        "BOOT",
        f"proxy started, active={await db.get_active()}, "
        f"paused={pause['paused']}, blocking_writes={len(blocking)}"
        + ("; intake still PAUSED across restart, manual resume required"
           if pause["paused"] else ""),
    )
    try:
        yield
    finally:
        await app.state.http.aclose()


app = FastAPI(title="write-cutover-proxy", lifespan=lifespan)


# ---------------------------------------------------------------- 健康探测

async def probe_node(base_url: str) -> tuple[bool, str]:
    """返回 (是否健康, 说明)。探测本身绝不触发任何写入。"""
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.get(
            f"{base_url}/healthz", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return False, f"probe request failed: {type(exc).__name__}: {exc}"
    if resp.status_code == 200:
        return True, "healthy"
    return False, f"healthz returned HTTP {resp.status_code}: {resp.text[:200]}"


async def node_health_map() -> dict[str, dict[str, str]]:
    nodes = await db.list_nodes()
    results = await asyncio.gather(*(probe_node(n["base_url"]) for n in nodes))
    return {
        n["name"]: {"healthy": ok, "detail": detail, "base_url": n["base_url"]}
        for n, (ok, detail) in zip(nodes, results)
    }


# ---------------------------------------------------------------- 暂停接流（备份窗口）

@app.post("/api/pause")
async def pause_intake(request: Request) -> JSONResponse:
    """启动持久暂停：新写入立即 503 拒绝（不落台账、不投后端），切流锁定。

    状态落 SQLite，代理重启后仍然有效；已受理的在途写入不受影响，
    继续向原节点取回执。恢复只能由值班员在网页手动执行（/api/resume）。
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    note = str(body.get("note", "")) if isinstance(body, dict) else ""
    async with _admit_lock:
        prev = await db.get_pause()
        await db.set_paused(True, note or prev.get("note", ""))
    if not prev["paused"]:
        await db.add_event(
            "PAUSE_ON",
            f"intake paused by operator; note={note!r}; new writes rejected, "
            "switching locked, in-flight writes continue to their original node",
        )
    blocking = await db.list_blocking_writes()
    return JSONResponse(content={
        "ok": True, "paused": True, "already_paused": bool(prev["paused"]),
        "blocking_writes": len(blocking),
        "message": "intake paused (persisted); new writes get 503 INTAKE_PAUSED, "
                   "switching locked; in-flight writes continue",
    })


@app.post("/api/resume")
async def resume_intake() -> JSONResponse:
    """值班员手动恢复接流。系统绝不自动恢复（重启后仍保持暂停）。"""
    async with _admit_lock:
        prev = await db.get_pause()
        await db.set_paused(False, prev.get("note", ""))
    if prev["paused"]:
        await db.add_event("PAUSE_OFF", "intake resumed by operator")
    return JSONResponse(content={"ok": True, "paused": False,
                                 "was_paused": bool(prev["paused"]),
                                 "message": "intake resumed"})


# ---------------------------------------------------------------- 切流

def _paused_switch_409(active: str, target: str) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"ok": False, "active": active, "blocked": True, "paused": True,
                 "reason": "intake is paused (backup window); switching is "
                           "disabled until manual resume",
                 "message": "kept old route"},
    )


@app.post("/api/switch")
async def switch(request: Request) -> JSONResponse:
    body = await request.json()
    target = str(body.get("target", "")).strip()
    nodes = {n["name"]: n["base_url"] for n in await db.list_nodes()}
    if target not in nodes:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"unknown target node: {target!r}",
                     "known_nodes": sorted(nodes)},
        )

    active = await db.get_active()
    if target == active:
        return JSONResponse(content={"ok": True, "noop": True, "active": active,
                                     "message": "target is already active"})

    # 暂停接流（备份窗口）期间禁止切流：先快速检查，提交前再在临界区复查。
    if (await db.get_pause())["paused"]:
        await db.add_event(
            "SWITCH_BLOCKED",
            f"{active} -> {target} rejected: intake paused (backup window)",
        )
        return _paused_switch_409(active, target)

    healthy, detail = await probe_node(nodes[target])
    if not healthy:
        # 探测失败：保持旧路由，不动 route 表。
        await db.add_event(
            "SWITCH_BLOCKED",
            f"{active} -> {target} blocked, target unhealthy: {detail}",
        )
        return JSONResponse(
            status_code=409,
            content={"ok": False, "active": active, "blocked": True,
                     "reason": f"target {target} probe failed: {detail}",
                     "message": "kept old route"},
        )

    async with _admit_lock:
        # 复查：探测期间暂停可能已生效，暂停期间禁止切流。
        if (await db.get_pause())["paused"]:
            await db.add_event(
                "SWITCH_BLOCKED",
                f"{active} -> {target} rejected: intake paused during probe",
            )
            return _paused_switch_409(active, target)
        async with _inflight_lock:
            inflight_snapshot = {k: len(v) for k, v in _inflight.items()}
            await db.set_active(target)
    await db.add_event(
        "SWITCH_OK",
        f"{active} -> {target} ok; inflight at cutover={inflight_snapshot}; "
        "new requests now route to target, in-flight requests stay on old node",
    )
    return JSONResponse(content={
        "ok": True, "active": target, "previous": active,
        "inflight_at_cutover": inflight_snapshot,
        "message": "switch done; in-flight writes continue against their original node",
    })


# ---------------------------------------------------------------- 写入代理

async def _track(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).add(write_id)


async def _untrack(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).discard(write_id)


async def _is_inflight(node: str, write_id: str) -> bool:
    """该写入此刻是否仍在本进程内投递中（代理重启后必为 False）。"""
    async with _inflight_lock:
        return write_id in _inflight.get(node, set())


@app.post("/api/writes")
async def proxy_write(request: Request) -> JSONResponse:
    params = request.query_params
    delay = float(params.get("delay", "0"))
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {"payload": body}

    write_id = str(body.get("id") or uuid.uuid4())
    payload = str(body.get("payload", ""))

    # 受理边界（临界区）：暂停检查 + 快照现役节点 + 台账落库 + 在途登记 原子完成。
    # 出临界区即视为"已受理"：即使暂停随后生效，本请求也继续向原节点投递取回执；
    # 暂停生效后到达的写入在临界区内被立即拒绝——不落台账、不投后端。
    async with _admit_lock:
        pause = await db.get_pause()
        if pause["paused"]:
            return JSONResponse(
                status_code=503,
                content={"ok": False, "id": write_id, "status": INTAKE_PAUSED,
                         "paused": True, "recorded": False, "delivered": False,
                         "detail": "intake paused (backup window): write NOT "
                                   "accepted — no ledger row, no delivery to any node"},
            )
        active = await db.get_active()
        base_url = await db.node_url(active)
        body["id"] = write_id
        await db.insert_write(write_id, active, payload, delay)
        await _track(active, write_id)

    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.post(
            f"{base_url}/writes", params={"delay": delay}, json=body
        )
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout,
            httpx.WriteTimeout) as exc:
        # 连接层面失败：请求没有被后端受理，标记 FAILED，不向任何节点补投。
        detail = f"connect to {active} failed: {type(exc).__name__}: {exc}"
        await db.update_write_status(write_id, db.FAILED, detail)
        await db.add_event("WRITE_FAILED", f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.FAILED, "detail": detail})
    except httpx.ReadTimeout:
        # 读超时：请求已送达，后端可能已落库，回执未归。结果不确定，禁止补投。
        detail = (f"read timeout after {READ_TIMEOUT}s waiting ack from {active}; "
                  "write may already be persisted there, result UNCERTAIN; "
                  "no automatic re-delivery to another node")
        await db.update_write_status(write_id, db.TIMEOUT_UNCERTAIN, detail)
        await db.add_event("WRITE_TIMEOUT_UNCERTAIN",
                           f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=504,
                            content={"id": write_id, "node": active,
                                     "status": db.TIMEOUT_UNCERTAIN,
                                     "uncertain": True, "detail": detail})
    except httpx.HTTPError as exc:
        detail = f"unexpected transport error against {active}: {exc}"
        await db.update_write_status(write_id, db.FAILED, detail)
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.FAILED, "detail": detail})
    finally:
        await _untrack(active, write_id)

    if resp.status_code in (200, 201):
        try:
            ack = resp.json()
        except Exception:
            ack = {}
        await db.update_write_status(
            write_id, db.ACKED,
            f"acked by {active} (backend status={ack.get('status', 'acked')})",
            acked=True,
        )
        await db.add_event("WRITE_ACKED", f"{write_id} acked by {active}")
        return JSONResponse(status_code=201,
                            content={"id": write_id, "node": active,
                                     "status": db.ACKED, "backend": ack})

    # 后端明确拒绝（如故障态 500）：未受理，FAILED；同样不补投。
    detail = f"{active} rejected with HTTP {resp.status_code}: {resp.text[:300]}"
    await db.update_write_status(write_id, db.FAILED, detail)
    await db.add_event("WRITE_FAILED", f"{write_id} -> {active}: {detail}")
    return JSONResponse(
        status_code=502,
        content={"id": write_id, "node": active, "status": db.FAILED, "detail": detail},
    )


@app.get("/api/writes/{write_id}/refresh")
async def refresh_write(write_id: str, wait: bool = False) -> JSONResponse:
    """向"原送达节点"查询该写入的最新回执状态——切流/暂停/代理重启后仍找旧节点。

    核实结论只会朝"确定"方向推进：
      * 原节点确认已回执         -> ACKED（确认）；
      * 原节点可达且明确无此记录  -> NOT_ACCEPTED（确知未受理；本系统后端受理即落库，
                                    可达而无记录即从未受理，绝不补投）；
      * 原节点失联 / 记录仍在等回执 -> 保持原状态，继续阻塞备份静默，绝不误清。
    wait=true 时让原节点最多等待 60 秒回执完成再作答（用于慢回执收尾）。
    """
    row = await db.get_write(write_id)
    if row is None:
        return JSONResponse(status_code=404,
                            content={"error": f"unknown write {write_id}"})
    node = row["target_node"]
    base_url = await db.node_url(node)
    client: httpx.AsyncClient = app.state.http
    read_to = 65.0 if wait else 10.0
    try:
        resp = await client.get(
            f"{base_url}/writes/{write_id}",
            params={"wait": "true"} if wait else None,
            timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=read_to,
                                  write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        )
    except httpx.RequestError as exc:
        # 节点失联：无法核实，保持原状态（继续阻塞静默），绝不按"未受理"误清。
        detail = (f"original node {node} unreachable ({type(exc).__name__}); "
                  f"cannot verify, status kept {row['status']}")
        await db.update_write_status(write_id, row["status"],
                                     f"{row['status_detail']} | {detail}")
        refreshed = await db.get_write(write_id)
        return JSONResponse(status_code=200,
                            content={"refreshed": False, "node_reachable": False,
                                     "write": refreshed, "note": detail})

    if resp.status_code == 404:
        # 原节点可达且确认无此记录。本系统后端"受理即落库"，可达而无记录即确知未受理。
        # 例外：该写入此刻仍在本进程内投递中（代理未重启、请求未结束），不能抢下结论。
        live = await _is_inflight(node, write_id)
        if row["status"] == db.TIMEOUT_UNCERTAIN or (
            row["status"] == db.PENDING and not live
        ):
            detail = (f"original node {node} reachable and has no record of it; "
                      "confirmed NEVER accepted (no re-delivery)")
            await db.update_write_status(write_id, db.NOT_ACCEPTED, detail)
            await db.add_event(
                "WRITE_RESOLVED",
                f"{write_id} confirmed NOT accepted by original node {node}",
            )
            return JSONResponse(content={"refreshed": True,
                                         "write": await db.get_write(write_id),
                                         "note": detail})
        note = (f"original node {node} has no row yet; request still in flight "
                f"locally; keep status {row['status']} (do not re-deliver)")
        return JSONResponse(content={"refreshed": False, "write": row, "note": note})

    data = resp.json()
    if data.get("status") == "acked" and row["status"] != db.ACKED:
        await db.update_write_status(
            write_id, db.ACKED,
            f"acked confirmed later by original node {node}", acked=True
        )
        await db.add_event("WRITE_RESOLVED",
                           f"{write_id} confirmed acked by original node {node}")
    refreshed = await db.get_write(write_id)
    out: dict[str, Any] = {"refreshed": True, "write": refreshed,
                           "backend_view": data}
    if data.get("status") == "pending":
        out["note"] = (f"original node {node} HAS accepted it but the ack is "
                       "still pending; still blocking quiet — verify again later")
    return JSONResponse(content=out)


# ---------------------------------------------------------------- 状态/排空

@app.get("/api/status")
async def status() -> JSONResponse:
    active = await db.get_active()
    health = await node_health_map()
    writes = await db.list_writes()
    pause = await db.get_pause()

    async with _inflight_lock:
        inflight_counts = {k: len(v) for k, v in _inflight.items()}
        inflight_ids = {k: sorted(v) for k, v in _inflight.items()}

    # 排空视角：已切走的旧节点上仍未拿到回执的写入。
    per_node: dict[str, dict[str, Any]] = {}
    for w in writes:
        slot = per_node.setdefault(
            w["target_node"], {"total": 0, "pending": 0, "acked": 0,
                               "timeout_uncertain": 0, "failed": 0}
        )
        slot["total"] += 1
        slot[w["status"].lower()] = slot.get(w["status"].lower(), 0) + 1
    for name in health:
        per_node.setdefault(name, {"total": 0, "pending": 0, "acked": 0,
                                   "timeout_uncertain": 0, "failed": 0})
    for name, slot in per_node.items():
        slot["inflight_now"] = inflight_counts.get(name, 0)
        slot["inflight_ids"] = inflight_ids.get(name, [])
        slot["is_active"] = name == active
        slot["draining"] = name != active and (
            slot["inflight_now"] > 0 or slot["pending"] > 0
        )

    # 备份静默判定（只看持久台账，不看内存态）：
    # 暂停接流中 且 没有任何 PENDING / TIMEOUT_UNCERTAIN 写入，才显示可备份。
    # 待核写入的原节点失联时无法核实，保持阻塞，绝不误报。
    blockers = []
    for w in await db.list_blocking_writes():
        h = health.get(w["target_node"], {})
        blockers.append({
            "id": w["id"], "target_node": w["target_node"],
            "status": w["status"], "status_detail": w["status_detail"],
            "created_at": w["created_at"],
            "node_reachable": bool(h.get("healthy")),
        })
    backup = {
        "paused": pause["paused"],
        "ready": bool(pause["paused"]) and not blockers,
        "blocking_count": len(blockers),
        "unreachable_count": sum(1 for b in blockers if not b["node_reachable"]),
        "blockers": blockers,
    }

    return JSONResponse(content={
        "ts": time.time(),
        "active": active,
        "pause": pause,
        "backup": backup,
        "nodes": health,
        "drainage": per_node,
        "writes": writes,
        "events": await db.list_events(),
        "timeouts": {"connect": CONNECT_TIMEOUT, "read": READ_TIMEOUT,
                     "probe": PROBE_TIMEOUT},
    })


@app.get("/api/nodes/{name}/records")
async def node_records(name: str) -> JSONResponse:
    """透传某后端自己保存的写入记录。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    try:
        resp = await app.state.http.get(
            f"{base_url}/records", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    return JSONResponse(content=resp.json())


@app.post("/api/nodes/{name}/fault")
async def node_fault(name: str, request: Request) -> JSONResponse:
    """给某后端注入/解除健康故障（透传后端控制接口）。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    body = await request.json()
    try:
        resp = await app.state.http.post(
            f"{base_url}/control/fault", json={"on": bool(body.get("on", True))},
            timeout=httpx.Timeout(PROBE_TIMEOUT),
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    await db.add_event("FAULT_INJECTION",
                       f"node {name} fault on={bool(body.get('on', True))}")
    return JSONResponse(content=resp.json())


# ---------------------------------------------------------------- 控制台页面

@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)
