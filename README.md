# 地震波形片段拼接服务（Waveform Assembly API）

把乱序到达的波形片段按 **台站 + UTC 分钟** 收集、校验、拼装，
仅对 **完整且无冲突** 的分钟生成不可变的发布记录，供震相识别程序取用。
单台站供数流（subscription feed）以固定起点 + 追加式变更日志的方式，
把每分钟的发布、撤回、重新发布持续分页投递给识别服务。

- 框架：FastAPI（自带 `/docs` Swagger UI、`/redoc`）
- 存储：SQLite（WAL 模式，单文件，挂在 `/data` 卷上，重启数据不变）
- 并发：所有写操作走 `BEGIN IMMEDIATE` 事务，上传、发布与创建订阅在同一已提交片段集合上串行执行

## 一、启动

### Docker Compose（推荐）

```bash
docker compose up --build
# API:    http://localhost:8000
# 文档:   http://localhost:8000/docs
# 健康:   http://localhost:8000/health
```

数据库保存在命名卷 `waveform-data`（容器内 `/data/waveform.db`）。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
WAVEFORM_DB_PATH=./data/waveform.db uvicorn app.main:app --reload
```

## 二、数据模型与规则

片段（fragment）：

| 字段 | 约束 |
| --- | --- |
| `station` | 非空字符串，台站代码 |
| `minute_utc` | 整分钟 UTC 时间（秒必须为 0），如 `2026-11-17T05:06:00Z` |
| `fragment_no` | 片段号，非负整数 |
| `revision` | 修订号，**正整数** |
| `start_sample` | 起始采样位，0–59 |
| `samples` | 非空整数数组；`start_sample + len(samples) ≤ 60`，越界拒绝（422） |

修订与重传规则（同一 `station + minute_utc + fragment_no`）：

- 修订号 **更高** → 接受并覆盖旧版（`status: "superseded"`）
- 修订号 **更低** → 拒绝（409），旧数据不动
- 同修订号、**同内容**（SHA-256 一致）→ 幂等重传，沿用原结果（`status: "unchanged"`），不产生新行
- 同修订号、**异文** → 拒绝（409），旧数据不动

拼装（assembly）：

- 每分钟固定 60 个 1 Hz 采样位
- 缺位在 `gaps` 中逐个列出；覆盖数见 `coverage`
- 重叠位数值不同即为冲突，`conflicts` 给出 **位置、涉及片段号、各自数值**；
  重叠但数值相同不算冲突
- `complete = 60 位全覆盖且无冲突`

发布（publication）：

- 只有完整且无冲突的分钟可发布；缺位或冲突时返回 409 并在 `detail` 中说明，
  **失败不留下任何半份记录**（单事务回滚）
- 发布记录冻结当时的 60 点波形 `wave`、每个片段的修订号与内容（`items`），
  以及片段集合指纹 `fragment_set_hash`
- 同一片段集合重复发布是幂等的，返回同一条记录
- 之后补传/更正只改变当前拼装；该分钟片段集合一旦变化，旧记录自动标 `stale=true`，
  仍可按 ID 读取，波形与修订保持冻结
- 每份记录只来自同一个已提交片段集合（事务快照保证）

## 三、HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/fragments` | 上传片段（JSON body） |
| GET | `/fragments?station=&minute_utc=` | 查看某分钟当前片段 |
| GET | `/assembly?station=&minute_utc=` | 查看拼装状态：覆盖率、缺口、冲突 |
| POST | `/publications?station=&minute_utc=` | 发布完整无冲突分钟 |
| GET | `/publications?station=&minute_utc=` | 发布记录列表（可按台站/分钟过滤） |
| GET | `/publications/latest?station=&minute_utc=` | 该分钟最新一条记录 |
| GET | `/publications/{id}` | 按 ID 读取（过期记录同样可读） |
| POST | `/subscriptions?station=` | 创建单台站供数流订阅，冻结当时未过期发布作为起点 |
| GET | `/subscriptions/{id}/feed?cursor=&page_size=` | 分页读取起点与后续撤回/重新发布事件 |

### 典型流程

```bash
MIN=2026-11-17T05:06:00Z
# 乱序上传两个片段，恰好铺满 60 位
curl -s -X POST localhost:8000/fragments -H 'content-type: application/json' -d '{
  "station":"STA1","minute_utc":"'"$MIN"'","fragment_no":2,"revision":1,
  "start_sample":30,"samples":'"$(/workspace/.venv/bin/python -c 'print(list(range(30,60)))')"'}'
curl -s -X POST localhost:8000/fragments -H 'content-type: application/json' -d '{
  "station":"STA1","minute_utc":"'"$MIN"'","fragment_no":1,"revision":1,
  "start_sample":0,"samples":'"$(/workspace/.venv/bin/python -c 'print(list(range(0,30)))')"'}'

# 检查拼装
curl -s "localhost:8000/assembly?station=STA1&minute_utc=$MIN"
# 发布
curl -s -X POST "localhost:8000/publications?station=STA1&minute_utc=$MIN"
```

## 四、单台站供数流（Subscription Feed）

震相识别服务需要持续拿到某台站“当前有效”的分钟波形，并在台网更正后撤回旧值。
供数流由**固定起点（snapshot）**和**追加式变更日志（live tail）**两部分组成。

### 数据保证

- **创建即固定**：`POST /subscriptions` 在一个 `BEGIN IMMEDIATE` 事务里冻结该台站
  当时所有未过期（`stale=false` 且片段集合指纹仍匹配）的发布，写入不可变的
  `subscription_snapshot`。翻页期间即使继续上传、发布，起点页内容也不会变化。
- **撤回先于替换**：片段补传或更正使发布过期时，业务事务先写入 `withdraw` 事件，
  重新发布成功后才写入含新发布号与冻结波形的 `upsert` 事件；两类事件与业务改动
  在同一事务提交，失败整体回滚，不留半份事件。
- **不漏报、不乱序**：订阅记录变更日志高水位 `max_event_id`。高水位之前的状态全部
  体现在起点里，之后的变更只可能出现在 live tail；`feed_events` 仅追加、全局 `id`
  单调，台站内严格按提交顺序投递，事件不丢失、不重复。
- **游标可重试、可重启**：游标是 `base64(phase|subscription_id|last_id)` 的不透明串，
  背后是只增不改的两张表。重试同一游标、或进程重启后读取，都返回同一页。
- **兼容旧库与原 API**：新表用 `CREATE TABLE IF NOT EXISTS` 增量建立，不改动原有三张
  表；旧 SQLite 卷可直接挂载启动，原有接口行为不变。

### 事件与分页

事件对象：

| 字段 | 说明 |
| --- | --- |
| `seq` | 订阅内单调递增的序号（起点行为快照行 id，live 行为日志 id） |
| `event_type` | `upsert`（投放/替换某分钟波形）或 `withdraw`（撤回某发布号） |
| `publication_id` | 关联的发布号；`withdraw` 不带 `publication` 载荷 |
| `publication` | `upsert` 时内嵌完整发布（冻结的 60 点 `wave`、`items` 等） |

分页响应含 `phase`（`snapshot` / `live`）、`events`、`next_cursor`、`has_more`。
起点页恒为 `snapshot`；起点读完后的下一页起为 `live`。live tail 暂无新事件时，
用返回的同一游标轮询会得到空事件、`has_more=false` 的稳定页。撤回对客户端是幂等的：
读到未知或已撤回的发布号直接忽略即可。

### 典型流程

```bash
# 1. 创建订阅，拿到起点首页游标
SUB=$(curl -s -X POST "localhost:8000/subscriptions?station=STA1")
SID=$(echo "$SUB" | python3 -c 'import json,sys; print(json.load(sys.stdin)["subscription_id"])')
CURSOR=$(echo "$SUB" | python3 -c 'import json,sys; print(json.load(sys.stdin)["next_cursor"])')

# 2. 反复翻页，先拿完固定起点，再沿 live tail 接收变更
while true; do
  PAGE=$(curl -s "localhost:8000/subscriptions/$SID/feed?cursor=$CURSOR&page_size=100")
  echo "$PAGE" | python3 -c 'import json,sys; [print(e["event_type"], e["publication_id"]) for e in json.load(sys.stdin)["events"]]'
  CURSOR=$(echo "$PAGE" | python3 -c 'import json,sys; print(json.load(sys.stdin)["next_cursor"])')
  echo "$PAGE" | python3 -c 'import json,sys; raise SystemExit(0 if json.load(sys.stdin)["has_more"] else 1)' || break
done
# 之后台网更正 -> 先收到 withdraw(旧发布号)，重新发布后收到 upsert(新发布号+新波形)
```

错误：游标格式非法或属于其他订阅返回 400；订阅不存在返回 404；
`page_size` 越界（1–500，默认 100）返回 422。

## 六、测试与自测

```bash
pip install pytest httpx
python -m pytest tests/ -v          # 43 个用例：校验/修订/拼装/发布/过期/重启/并发 + 供数流 21 例
python scripts/smoke.py             # 对运行中的服务做 47 项端到端检查（原 33 项 + 供数流 14 项）
```

并发用例会启动真实 uvicorn 进程，用多个线程同时上传并尝试发布，
断言中间态发布只可能干净失败（409），完成态恰好产生一条完整记录，无半份数据。
供数流并发用例进一步断言高竞争下撤回/替换严格配对、`seq` 全局有序。

## 七、目录结构

```
app/
  config.py      # 环境变量配置（WAVEFORM_DB_PATH）
  database.py    # SQLite 连接与建表（含 feed_events / subscriptions / snapshot）
  models.py      # Pydantic 请求/响应模型（含订阅与供数流分页模型）
  service.py     # 事务、拼装、发布过期、变更事件与订阅分页逻辑
  main.py        # FastAPI 路由（含 /subscriptions 与 /feed）
tests/test_api.py    # 原有 22 个接口用例
tests/test_feed.py   # 供数流 20 个用例（起点/分页/撤回顺序/重试/重启/兼容/并发）
scripts/smoke.py
Dockerfile
docker-compose.yml
requirements.txt
```
