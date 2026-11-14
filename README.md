# 湖仓水位观测表 Delta Lake 压实 + 版本归档 API

用 **FastAPI + PyArrow** 手工实现的 Delta Lake（读协议 v1 / 写协议 v2）小文件压实服务，
以 **双进程** 方式运行，面向一张无分区的水位观测表（非空字符串站号、UTC 秒与水位毫米
均为 int64）。压实把指定版本中的至少两份 Parquet 合成一份，保持原值、重复行、模式与
全部旧快照不变，并在外部写者持续提交的环境下用乐观并发控制（OCC）保证提交安全。

另提供 **版本归档**：运维员提交版号与归档名，服务把该版的活跃 Parquet、协议与表结构
物理复制到共享数据卷中的独立 Delta 归档（`archives/<name>`），供水位建模组离线复算。
归档原子发布、幂等重试、同名异版冲突即报错，且此后原表的增删与压实都不再影响它。

## 目录结构

```
.
├── app/
│   ├── delta.py            # Delta 协议：JSON 日志、快照回放、原子提交、压实/OCC
│   ├── archive.py          # 版本归档：复制、原子发布、幂等/冲突、按名查询
│   ├── external_writer.py  # 外部写者：dataChange=true 的追加与删除（独立进程）
│   ├── sample_data.py      # 样例表引导（4 个小文件，幂等、并发安全）
│   └── main.py             # FastAPI 路由、Swagger、错误码映射
├── scripts/
│   ├── _demo_common.py
│   ├── demo_readback.py            # 演示 1：压实并读回新旧版本
│   ├── demo_concurrent_append.py   # 演示 2：压实期间外部并发追加
│   ├── demo_concurrent_delete.py   # 演示 3：压实期间外部删除 -> 409 冲突
│   ├── demo_archive.py             # 演示 4：归档指定版本并验证独立性/幂等/冲突
│   ├── run_local_servers.sh        # 无 Docker 时本地双进程启动
│   └── run_all_demos.sh
├── tests/
│   ├── test_compaction.py  # 压实：端到端/并发/崩溃测试
│   └── test_archive.py     # 归档：内容/独立性/幂等/并发/崩溃/HTTP 测试
├── Dockerfile
├── docker-compose.yml              # api-1:8001、api-2:8002，共享一个数据卷
├── requirements.txt / requirements-dev.txt
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

两个副本共享命名卷 `delta-data` 中的同一张表，任一进程都可发起压实：

| 服务   | 地址              | Swagger UI                |
|--------|-------------------|---------------------------|
| api-1  | http://localhost:8001 | http://localhost:8001/docs |
| api-2  | http://localhost:8002 | http://localhost:8002/docs |

启动时自动创建样例表：版本 0 为 `protocol(1,2)+metaData`，版本 1–4 各追加一个
Parquet（共 24 行、4 个文件）。容器内表路径为 `/data/water_level`。

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/health` | 健康检查，返回最新版号 |
| POST | `/compact` | 压实：body `{"version": 4, "files": ["a.parquet", "b.parquet"]}`，`version` 省略时取最新版，`files` 至少 2 个 |
| GET  | `/table/versions` | 全部连续版号 |
| GET  | `/table/snapshot?version=N` | 快照信息（不传为最新，支持时间旅行） |
| GET  | `/table/stats` | 行数、模式、孤儿清理结果 |
| POST | `/archives` | 归档：body `{"name": "wl-v4", "version": 4}`，把该版活跃 Parquet、协议与表结构复制为独立 Delta 归档；同名同版幂等（200，`created=false`），同名异版 409 |
| GET  | `/archives` | 列出全部已发布归档（暂存目录永不出现在列表中） |
| GET  | `/archives/{name}` | 按名称查询归档的源表、源版号、行数与文件数 |

`POST /compact` 成功响应包含提交版号与文件数变化：

```json
{
  "committed_version": 5,
  "base_version": 4,
  "files_added": 1,
  "files_removed": 2,
  "output_file": "….parquet",
  "input_files": ["….parquet", "….parquet"],
  "rows": 12,
  "input_bytes": 2168,
  "output_bytes": 1131,
  "attempts": 1
}
```

错误码：400 请求非法（少于两文件、路径逃逸等）、404 版号或文件不存在、
409 并发冲突（所选文件已被他人移除）、503 表暂不可用。
归档接口：400 归档名非法、404 源版号或归档名不存在、409 同名异版冲突。

### 示例

```bash
curl -s localhost:8001/table/snapshot | jq .active_files
curl -s -X POST localhost:8001/compact -H 'content-type: application/json' \
  -d '{"files": ["<f1>.parquet", "<f2>.parquet"]}'
curl -s "localhost:8001/table/snapshot?version=4"   # 旧快照时间旅行
```

## 版本归档（离线复算）

水位建模组离线复算需要拿到**指定版本**的观测数据。`POST /archives` 把该版本
的活跃 Parquet、协议（reader=1 / writer=2）与表结构（schema、分区列等 metaData）
复制到共享数据卷中的独立 Delta 归档 `archives/<name>/`（自带从 0 连续编号的
`_delta_log`：v0 = protocol+metaData，v1 = 全部 add 与 `ARCHIVE` 提交信息，
源版号记录在 metaData 的 `configuration` 中）。

```bash
curl -s -X POST localhost:8001/archives -H 'content-type: application/json' \
  -d '{"name": "wl-baseline-v4", "version": 4}'      # 201 首次发布
curl -s localhost:8002/archives/wl-baseline-v4        # 按名查询源版号与行数
curl -s localhost:8001/archives                       # 归档列表
```

**归档不变量**

* **读回一致**：归档是逐字节的物理复制（不硬链接、不移动源文件），行与重复行、
  模式与原版完全一致；行数取自 Parquet 页脚，精确且包含重复行。
* **独立性**：归档有自己的日志与数据文件；此后原表的追加、删除、压实都不改变
  归档的文件集与内容。
* **原子发布**：归档先在隐藏暂存目录 `.tmp-*` 中组装（数据先复制、日志后写入、
  全程 fsync），最后一次 `os.rename` 原子现身。两个副本同名并发只有一方能
  rename 成功——只发布一份完整归档；复制中断或写盘失败只留下隐藏暂存目录，
  任何查询都读不到半成品，暂存目录由启动时或 `cleanup_archive_staging()` 按
  mtime 安全回收。
* **幂等与冲突**：同名同版重试直接返回已完成结果（200，`created=false`）或
  在未完成时继续完成；同名异版返回 409。归档名必须是单段安全路径
  （`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`）。

## 演示脚本

脚本同时走 HTTP 与本地表路径（外部写者以独立 OS 进程直接提交 Delta 日志）：

```bash
pip install -r requirements-dev.txt
bash scripts/run_local_servers.sh            # 终端 A：本地起两个 uvicorn
bash scripts/run_all_demos.sh                # 终端 B：依次跑四个演示
```

* **演示 1（读回新旧版）**：压实后新版行数/模式不变、文件数 -2+1；`?version=` 读回
  旧版，文件集与数据完全不变。
* **演示 2（并发追加）**：外部进程在压实进行中连续提交 6 个 `dataChange=true` 追加，
  压实仍成功（无关文件变化不构成冲突），日志保持 0 起连续。
* **演示 3（并发删除）**：外部进程删除所选文件之一，压实提交时复检发现文件失效，
  返回 409 且不复活已删数据；改用仍有效的文件重试即成功。
* **演示 4（版本归档）**：经 api-1 归档当前版本（201），同名同版重试幂等返回
  （200，`created=false`）；api-2 按名查询源版号与行数；原表压实后归档不变；
  同名异版返回 409。

## 协议与正确性保证

**表结构**：无分区；协议固定 reader=1、writer=2；无删除向量；JSON 日志从 0 连续
编号（`_delta_log/00000000000000000000.json` …），每行一个 JSON action。

**数据不变量**

* 压实仅做 `concat`，不去重、不过滤、不改序语义；模式逐字段校验，非空约束校验。
* 新文件先写 `_staging/` 再 **硬链接** 到唯一最终路径（纳秒时间戳+UUID，路径永不复用），
  半成品不会出现在任何快照中、不可查询。
* 被移除的物理 Parquet 不删除，旧快照（时间旅行）始终可读。
* 写盘失败/进程中途退出：已提交快照不受影响；`_staging` 与 `.tmp-*` 孤儿由
  `/table/stats` 或启动时的 `cleanup_staging()` 按 mtime 安全回收。

**并发控制（OCC）**

* 日志提交 = 唯一临时文件 + `os.link` 原子改名：两个进程不可能得到同一版号，
  已提交日志绝不被覆盖或截断。
* 压实在提交前重新加载最新快照：**所选文件全部仍有效才替换**；任一文件已被并发
  移除则报 409 并清理新产物，绝不复活已删数据。
* 仅无关文件的增删不影响压实；日志版号竞争时自动退避重试（上限 25 次）。
* 压实提交的 add/remove 均带 `"dataChange": false`（add 另带 `tags.compaction=true`，
  `commitInfo.operation=OPTIMIZE` 与指标），外部追加/删除为 `dataChange=true`。

## 测试

```bash
python -m pytest tests/ -q
```

`tests/test_compaction.py` 覆盖：连续日志与协议号、行/重复行/模式保持、时间旅行、
路径校验、跨进程同集/异集压实竞争、并发删除 409 与不复活、崩溃后快照不变、staging
孤儿清理、双进程并发建表、临时日志文件忽略，以及 HTTP 层全部状态码映射。

`tests/test_archive.py` 覆盖：归档行/重复行/模式/协议与源版一致、空版本与旧版本
（压实后）归档、原表增删压实后归档不变、同名同版幂等、同名异版 409、归档名校验、
双进程同名同版只发布一份、双进程同名异版唯一赢家、复制中断后无可读半成品且重试
可完成、暂存目录按龄清理，以及 HTTP 层 201/200/404/409/422 映射。
