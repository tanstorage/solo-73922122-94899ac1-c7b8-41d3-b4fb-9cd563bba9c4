# Avro 契约注册中心（Schema Contract Registry）

数据平台契约发布网页：每条数据链路登记**当前 writer 模式、当前 reader 模式
及全部已发布 writer 历史**，任何一端升级 Avro 模式前都必须通过兼容性裁决，
把无法解码的结构挡在数据链路之外。内置 **Avro OCF 脱敏下载**：把采集批次
交给外部分析前，按已提交契约清除客户标识字段。

技术栈：FastAPI + avro（Apache Avro Python 包）+ SQLite。

## 快速开始

```bash
docker compose up --build
```

启动两个**共用同一个 SQLite 库文件**的 Web 进程：

| 进程 | 地址 |
|------|------|
| web1 | http://localhost:8001 |
| web2 | http://localhost:8002 |

浏览器打开任一地址即可：创建链路、粘贴模式发布任一端、试算检查、
查询当前契约与全部 writer 历史。页面右上角显示当前响应的进程 ID；
在 web1 创建的链路刷新 web2 即可看到（共库）。数据落在 named volume
`contracts` 里，重启/重建容器后保留。

## 功能与规则

- **初建即校验**：创建链路时要求 reader 能读取初始 writer，否则 409 拒绝且不留数据。
- **发布 writer**：新 writer 必须能被**当前 reader** 读取。
- **发布 reader**：新 reader 必须能读取**全部历史 writer**（含当前 writer，
  当前 writer 即历史中最大 seq 的那一条）。
- **Avro 规则**：按规范 Schema Resolution 实现——原始类型提升
  （int→long/float/double、long→float/double、float→double、string↔bytes）、
  record 字段增删与**默认值**、**递归 record**（循环守卫）、union
  （writer 每个分支须可读；reader 任一分支可读即可，拒绝时报告最接近分支）、
  enum（符号覆盖 / default）、fixed（长度）、array/map 嵌套，
  以及 record/enum/fixed **别名**与字段**别名**（相对别名按 reader 命名空间补全）。
- **拒绝时显示冲突路径**：如 `$.v`、`$.payload.items[]`、`$.next|writer#1(Node).v`，
  并标注冲突来自哪一版历史 writer（`writer_seq`）。
- **失败保留旧契约**：任何拒绝都发生在事务外或回滚内，库中契约原样不动。
- **当前模式与历史一起提交**：发布 writer 时，历史 INSERT 与当前模式 UPDATE
  在同一事务内完成。

## OCF 脱敏下载

把采集批次交给外部分析前清除客户标识：

1. 选择链路，页面列出**当前 reader 中类型恰为 `["null","string"]`** 的顶层
   敏感字段供勾选（`GET /api/links/{name}/mask/fields`）；
2. 上传 **≤ 2 MiB、codec 为 null** 的 Avro OCF 文件；
3. 文件头 writer 模式须匹配**某版已发布模式**——按规范化 JSON 比较，
   **对象键序与空白不影响匹配**；
4. 服务端用**同一已提交版本**的 reader 与完整 writer 历史解码全文件，
   仅把选中字段置 null，其余字段的解码值、记录顺序与条数保持一致，
   再以 **reader 模式**写出新的 OCF（codec=null）；
5. 成功后页面显示**契约版本、记录数**并自动下载
   （响应头 `X-Contract-Version` / `X-Record-Count` / `X-Writer-Seq` /
   `X-Masked-Fields`）。

可靠性保证：

- **不交付部分结果**：模式未登记（`writer_not_registered`）、codec 非 null
  （`unsupported_codec`）、文件截断/损坏（`invalid_ocf`）、超过 2 MiB
  （413 `too_large`）或读写失败时，整个请求报错，不落任何部分内容——
  全部解码与写出都在内存中完成后才返回响应。
- **一致快照、不占写锁**：脱敏开始时一次性读取契约一致快照（WAL 下的
  只读事务），处理全程不持 SQLite 写锁；处理期间另一进程发布新契约时，
  本次仍使用快照中的原版本（可用 `demo_delay_ms` 表单字段放大该窗口演示）。

## 并发设计（重点）

兼容检查**不持有 SQLite 写锁**，但并发正确性由**提交时状态**保证：

1. **预检（无锁）**：读取当前快照做兼容性检查，失败直接 409；
   检查与查询走自动提交的只读 SELECT（WAL 模式，读写互不阻塞）。
2. **提交（短事务）**：`BEGIN IMMEDIATE` 取写锁后，**在事务内重新读取
   已提交状态并重新裁决**，通过才写入，否则 `ROLLBACK`。

因此两个并发发布即使各自预检都通过，也不可能都提交成功并留下不兼容
组合——后提交者会基于先提交者已落库的状态重新检查并被拒绝
（响应中 `stage: "commit"` 即表示在提交时裁决阶段被拒）。

### 可控演示：int 写端 / long 读端

链路 `demo-int-long`：writer=`int`、reader=`long`。同时发起：

- writer 改为 `long`（对当前 reader=long 预检通过）
- reader 改为 `int`（对当前历史=[int] 预检通过）

若两次提交都成功，将留下 writer=long / reader=int 的不兼容组合
（int 读端无法读 long 写端）。两个请求各带 `demo_delay_ms` 演示延迟
（延迟发生在**事务外**，仅用于放大并发窗口），提交时按当时状态裁决：
**恰好一个成功，另一个 409**，最终契约必然兼容。

两种方式体验：

1. **网页**：打开首页 →「并发演示」→ ① 创建/重置演示链路 → ② 并发提交两端修改。
2. **命令行**（两个请求分别打到两个进程）：

   ```bash
   demo/race_demo.sh            # 默认打 localhost:8001 / 8002
   DELAY_MS=1500 demo/race_demo.sh
   ```

   输出示例（某一次运行）：`writer→long` 返回 200，`reader→int` 返回 409：

   ```json
   {"detail": {"error": "incompatible", "stage": "commit",
     "message": "新 reader 无法读取全部历史 writer，已保留旧契约",
     "conflicts": [{"writer_seq": 2, "path": "$.v",
       "message": "writer 类型 \"long\" 无法被 reader 类型 \"int\" 读取"}]}}
   ```

   多跑几次可看到另一种胜负（reader→int 成功、writer→long 被 409），
   两种结局的最终状态都兼容。

## API 一览

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/info` | 进程 ID、库路径、链路数 |
| GET | `/api/links` | 链路列表（名称/版本/更新时间） |
| POST | `/api/links` | 创建链路 `{name, writer, reader}`（初建校验） |
| GET | `/api/links/{name}` | 当前 writer/reader + 全部 writer 历史 |
| PUT | `/api/links/{name}/writer` | 发布新 writer `{schema, demo_delay_ms?}` |
| PUT | `/api/links/{name}/reader` | 发布新 reader `{schema, demo_delay_ms?}` |
| POST | `/api/links/{name}/check` | 试算 `{end, schema}`，只检查不提交 |
| GET | `/api/links/{name}/mask/fields` | 当前 reader 中可脱敏字段（类型恰为 `["null","string"]` 的顶层字段） |
| POST | `/api/links/{name}/mask` | OCF 脱敏下载：multipart 表单 `file`（≤2 MiB、codec=null）、`fields`（JSON 数组）、`demo_delay_ms?` |
| DELETE | `/api/links/{name}` | 删除链路（演示重置用） |

- `schema` 接受 JSON 对象或 JSON 字符串；非法 JSON/非法 Avro 模式返回 400。
- 脱敏成功返回 `application/avro` 附件，元信息在 `X-Contract-Version` /
  `X-Record-Count` / `X-Writer-Seq` / `X-Masked-Fields` 响应头；
  失败返回 JSON `detail.error`（`writer_not_registered` / `unsupported_codec` /
  `invalid_ocf` / `too_large` / `field_not_maskable` / `write_failed`），
  不交付任何部分结果。
- 拒绝返回 409，`detail.conflicts[]` 含 `path` / `message` / `writer_seq`，
  `detail.stage` 为 `pre-check`（预检）或 `commit`（提交时裁决），
  `detail.state` 为库中当前（未受影响的）契约。
- 每个响应带 `X-Server-Id` 头标识处理进程。

## 存储结构（SQLite，WAL）

- `links(name, writer, reader, version, created_at, updated_at)`：当前契约。
- `writer_history(link, seq, schema, published_at)`：全部已发布 writer，
  `seq` 从 1 递增，最大 seq 即当前 writer（与 `links.writer` 同事务写入）。

## 本地开发与测试

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest httpx
pytest tests/ -q                     # 46 个用例：规则 / 历史 / 回滚 / 重启保留 / 并发裁决 / OCF 脱敏

# 本地起两个共库进程（不用 Docker）
DB_PATH=/tmp/contracts.db SERVER_ID=web1 uvicorn app.main:app --port 8001 &
DB_PATH=/tmp/contracts.db SERVER_ID=web2 uvicorn app.main:app --port 8002 &
```

## 目录结构

```
app/
  main.py            FastAPI 路由与错误映射（含 OCF 脱敏下载端点）
  compat.py          Avro writer→reader 兼容性检查（冲突路径）
  store.py           SQLite 存储与两阶段发布（预检无锁 / 提交时裁决）
  mask.py            OCF 脱敏：writer 匹配、reader 解码、置 null、reader 写出
  static/index.html  网页界面（建链路、发布、查询、并发演示、脱敏下载）
demo/race_demo.sh    跨进程并发演示脚本
tests/               pytest 用例
Dockerfile / docker-compose.yml / requirements.txt
```
