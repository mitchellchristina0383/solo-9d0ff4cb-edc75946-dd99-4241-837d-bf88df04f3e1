# 写入切流反向代理（FastAPI + SQLite）

值班工程师更新服务节点时，用本系统把写入流量从旧节点切到备用节点。核心保证：

- **不中断已送达的写入**：每个写入在投递前快照现役节点并持久化绑定，切流只改路由表，
  不取消在途请求；回执仍从**原送达节点**读取，连续多次切流也不打断它。
- **不重复投递**：代理对任何写入只发一次。连接失败标 `FAILED`、读超时标
  `TIMEOUT_UNCERTAIN`，都**不会**自动改投另一节点；后端两侧写入 ID 集合永不相交。
- **探测失败保持旧路由**：切流前先探目标节点 `/healthz`，失败返回 409，路由表不变。
- **超时显式不确定**：写入已在后端落库但回执超时（读超时）时返回 504 +
  `TIMEOUT_UNCERTAIN`，页面醒目提示，可事后点"向原节点查询"核实，由原节点就地更正状态。

## 备份窗口（暂停接流）

值班工程师备份代理及两台后端的数据卷前，先在控制台点"暂停接流"进入备份窗口：

- **持久暂停**：暂停状态落 SQLite（`pause` 表），代理重启后仍然有效，
  只能由值班员在网页点"恢复接流"手动解除，系统绝不自动恢复。
- **新写入即拒**：暂停后 `POST /api/writes` 立即返回 `503 INTAKE_PAUSED`——
  不落代理台账、不投任何后端。
- **受理边界明确**："暂停检查 + 绑定节点 + 台账落库"在同一把锁的临界区内原子完成。
  已出临界区的写入视为已受理，即使暂停随后生效也继续向原节点投递取回执，绝不丢失；
  暂停生效后到达的写入一律干净拒绝。
- **暂停期间禁止切流**：`POST /api/switch` 返回 409，路由表不变。
- **静默判定（可备份）**：网页列出所有阻止静默的写入（在途 `PENDING` +
  结果不确定 `TIMEOUT_UNCERTAIN`）。只有它们全部**确认**（`ACKED`）或
  **确知未受理**（`NOT_ACCEPTED`/`FAILED`）时才显示"已静默，可备份"。
  判定只看持久台账，不看内存态，代理重启后依然正确。
- **失联不误报**：待核写入的原节点不可达时无法核实，状态保持原状并继续阻塞，
  页面标红"节点失联"，绝不显示可备份。
- **核实路径**：点"向原节点查询"（或"核实全部待核写入"）向**原送达节点**核实：
  原节点确认已回执 → `ACKED`；原节点可达且明确无此记录 → `NOT_ACCEPTED`
  （本系统后端受理即落库，可达而无记录即从未受理，不会补投）；
  原节点仍慢回执中 → 保持阻塞稍后再查（`?wait=1` 可让原节点等回执完成再作答）。

## 目录结构

```
backend/            模拟写入后端（两个实例，各自 SQLite 保存收到的写入）
  app/main.py       慢回执 delay、健康故障注入 /control/fault、/healthz、/records
  Dockerfile
proxy/              FastAPI 反向代理 + 切流控制台
  app/main.py       代理、切流、暂停接流、备份静默判定、探测、在途/排空跟踪、故障注入透传
  app/db.py         SQLite 持久层（nodes / route / pause / writes / events）
  app/dashboard.html 操作网页（备份窗口、现役节点、排空状态、写入记录、事件日志）
  Dockerfile
docker-compose.yml  一键启动 proxy + backend-a + backend-b（含健康检查与数据卷）
scripts/selftest.py 纯标准库端到端自测（74 项断言）
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 控制台： http://localhost:8080/
```

- 后端各自把写入保存在卷 `backend-a-data` / `backend-b-data` 的 `/data/writes.db`；
  代理状态（含暂停状态与待核记录）在卷 `proxy-data` 的 `/data/proxy.db`，
  容器重建/重启后仍然有效。
- 想演练回执超时，可把 compose 里 `WRITER_READ_TIMEOUT` 调小（如 `2`）后重建。

## 不用 Docker 的本地运行（开发/自测）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r proxy/requirements.txt        # 后端依赖相同
mkdir -p .run/data
NODE_NAME=A BACKEND_DB=$PWD/.run/data/a.db \
  python -m uvicorn app.main:app --app-dir backend --port 8001 &
NODE_NAME=B BACKEND_DB=$PWD/.run/data/b.db \
  python -m uvicorn app.main:app --app-dir backend --port 8002 &
NODE_A_NAME=A NODE_B_NAME=B \
BACKEND_A_URL=http://127.0.0.1:8001 BACKEND_B_URL=http://127.0.0.1:8002 \
PROXY_DB=$PWD/.run/data/proxy.db \
  python -m uvicorn app.main:app --app-dir proxy --port 8080
```

## 网页操作演练

1. 慢回执延迟填 `10` 秒，点"投递写入"，在请求返回前立刻点另一节点的"切换流量"；
2. 观察：现役节点立即变更；旧节点卡片显示 **DRAINING 排空中** 与在途写入 ID；
   慢请求结束后仍由旧节点回执，写入只存在于旧节点；
3. 点某节点"注入健康故障"后再向它切流：收到 409 拦截提示，现役节点不变；
4. "拉取 A/B 的记录"可对比两个后端各自 SQLite 中保存的写入；
5. 超时写入显示红色 **TIMEOUT_UNCERTAIN**，点"向原节点查询"可核实真实落库情况；
6. **备份窗口演练**：慢回执写入在途时点"暂停接流"——新写入立即 503 被拒、
   切流按钮锁定；在途写入继续取回执；待核列表清空后显示绿色"已静默，可备份"；
   此时备份三个数据卷，完成后点"恢复接流"。暂停中重启代理容器，状态依旧。

## HTTP 接口（代理 8080）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/writes?delay=秒` | 投递写入（代理只投一次，绑定当前现役节点）；暂停时 503 `INTAKE_PAUSED` |
| POST | `/api/pause` | body `{"note":"..."}`；持久暂停接流，新写入即拒、切流锁定 |
| POST | `/api/resume` | 值班员手动恢复接流（系统绝不自动恢复） |
| POST | `/api/switch` | body `{"target":"B"}`；探测失败 409 保持旧路由；暂停期间 409 禁止切流 |
| GET  | `/api/status` | 现役节点、暂停状态、备份静默判定与待核列表、健康、排空、写入与事件 |
| GET  | `/api/writes/{id}/refresh?wait=1` | 向**原送达节点**查询回执并就地更新状态；`wait=1` 让原节点等慢回执完成（≤60s） |
| GET  | `/api/nodes/{name}/records` | 透传该后端自己保存的写入记录 |
| POST | `/api/nodes/{name}/fault` | 注入/解除健康故障 `{"on": true|false}` |

## 自测

```bash
python -I scripts/selftest.py            # 自动拉起 3 个进程，跑完即清理
```

覆盖：普通写入无重复、慢回执在途切流、探测失败保旧路由、连续再次切流不打断在途写入、
读超时 504 不确定且不补投（事后向原节点核实可转 ACKED）、暂停接流（即拒/在途续跑/
切流锁定/静默判定/手动恢复）、暂停与并发写入交错的受理边界、待核写入节点失联不误报、
代理重启后暂停与待核记录仍有效、两侧后端记录不相交且与代理一一对应。

## 关键环境变量

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `WRITER_READ_TIMEOUT` | 30 | 等回执读超时秒数，超时即结果不确定 |
| `CONNECT_TIMEOUT` | 3 | 连接超时，连接失败视为未送达 `FAILED` |
| `PROBE_TIMEOUT` | 2.5 | 切流健康探测超时 |
| `BACKEND_A_URL` / `BACKEND_B_URL` | 本地 8001/8002 | 两个后端地址 |
