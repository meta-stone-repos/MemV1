# MemV：独立的 AML 接口层

只封装 HTTP 协议，不绑定任何记忆系统、LLM、Embedding 或向量数据库。
实现 `MemoryBackend`，把实例传给 `create_app(backend)` 即可更换记忆方法。

## 文件职责

| 文件 | 唯一职责 |
| --- | --- |
| `contracts.py` | 请求、消息与检索结果的数据结构和校验 |
| `backend.py` | 记忆后端基类，以及重试所需的稳定 ID / 进度上下文 |
| `aml_api.py` | HTTP 路由、鉴权、错误映射和结果格式化 |
| `receipts.py` | SQLite 请求去重和可恢复进度；不存记忆内容 |
| `request_scheduler.py` | 并发限额、同用户串行、排队超时 |
| `server.py` | 组装应用，只在这里选择后端 |

依赖方向：`server → HTTP 层 → MemoryBackend 契约`。
具体记忆算法依赖契约，HTTP 层不导入具体算法。测试用的简单存储只在 `tests/` 中。

## 1. 启动接口

需要 Python 3.11 或更新版本。在 6kd_6 上：

```bash
cd /data/bbj/MemV/aml
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m uvicorn server:app --host 0.0.0.0 --port 18083 --workers 1
```

或者用 Docker 后台运行，关闭 SSH 不影响进程：

```bash
cd /data/bbj/MemV/aml
docker compose up -d --build
docker compose logs -f aml
```

两种方式选择一种，不能同时占用 18083。停止 Docker 服务用 `docker compose stop aml`。

默认 `server.py` 是 `app = create_app(backend=None)`：

- `http://174.1.59.6:18083/docs` 可查看和调试协议。
- `POST /add`、`POST /search` 和 `GET /health` 返回 HTTP 503，明确说明尚未配置 backend。
- 不生成成功回执、不写入记忆、不伪造空检索结果。

Docker 健康检查只检查 HTTP 进程；业务是否可用以 `/health` 为准。

## 2. 接入自己的记忆方法

新建 `my_backend.py`，继承 `backend.MemoryBackend`。必须实现两个异步方法：

```python
from backend import AddContext, MemoryBackend
from contracts import AddRequest, MemoryRecord, SearchRequest

class MyBackend(MemoryBackend):
    backend_id = "my-method-v1"
    # 只有底层存储支持不同用户并发时才设为 True。
    supports_user_concurrency = True

    async def add(self, request: AddRequest, context: AddContext) -> None:
        # 在此调用你的记忆系统；持久化且可检索之后才能返回。
        raise NotImplementedError("请实现记忆写入")

    async def search(self, request: SearchRequest) -> list[MemoryRecord]:
        # 只返回 request.user_id 的记忆证据，不生成问题的最终答案。
        raise NotImplementedError("请实现记忆检索")
```

这是一份待实现的模板，不是可工作的记忆算法。需要连接数据库、加载 SDK 时，
再覆盖 `async def start(self)` / `async def close(self)`；HTTP 应用会管理其生命周期。
安装你所选系统需要的依赖，然后把 `server.py` 改成：

```python
from aml_api import create_app
from my_backend import MyBackend

backend = MyBackend()  # 初始化参数由你的类自行定义
app = create_app(backend=backend)
```

Docker 模式修改代码后执行 `docker compose restart aml`；新增依赖写进
`requirements.txt` 后执行 `docker compose up -d --build`。普通 Python 模式重启 uvicorn。

后端约定：

- `user_id` 是用户隔离边界，原样传给后端；`session_id` 只标记消息来源，不能限制跨会话检索。
- 消息顺序、正文、时间、搜索 `options` 原样传入，不截断正文。
- Add 可能在部分失败后重试。用 `context.message_id(index)` 作为稳定写入 ID，
  或使用存储自己的幂等机制。`context.timestamp(index)` 在重试时也保持稳定。
- 多步骤写入可以用 `context.phase(index)` / `context.checkpoint(index, phase)`。
  阶段编号由后端自定；只在持久化完成后提交进度，不需要使用固定三阶段。
- `search` 返回 `MemoryRecord` 或相同字段的字典。HTTP 层校验结果、按 ID 去重、
  按 `score` 降序并应用全局 `top_k`。不要在后端返回假成功。
- `backend_id` 应稳定；换算法或换底层存储后使用新标识和独立存储路径。
  旧请求回执不会阻止新方法写入，旧记忆也不会自动迁移。

## 3. 请求格式（保持原协议）

Add：`POST http://174.1.59.6:18083/add`

```json
{
  "request_id": "write-001",
  "user_id": "alice",
  "session_id": "conversation-1",
  "messages": [
    {"role": "user", "content": "I prefer aisle seats", "timestamp": 1704067200000}
  ]
}
```

成功响应：

```json
{"success": true, "request_id": "write-001", "user_id": "alice", "session_id": "conversation-1"}
```

Search：`POST http://174.1.59.6:18083/search`

```json
{"user_id": "alice", "query": "What seat do I prefer?", "top_k": 10}
```

响应示例：

```json
{"data": [{"id": "memory-001", "content": "I prefer aisle seats", "score": 0.9}]}
```

`timestamp` 可省略，填写时为 Unix 毫秒整数。消息 `role` 支持 `user`、`assistant`。
Search 可附带 `options: ["A", "B"]`；`score`、`created_at` 可省略。

## 4. 并发、重试与配置

默认至多 16 个请求执行、32 个请求在途（执行＋排队），同一用户的 Add/Search 按顺序执行。
**后端必须声明 `supports_user_concurrency = True` 才启用不同用户并行**；未声明时安全地串行。
当前调度为单进程，保持 `--workers 1`，不要用多个进程共享同一套回执和存储。

| 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `AML_STATE_DIR` | `./data`（Docker 中 `/app/data`） | 请求回执目录 |
| `AML_MAX_ACTIVE` | 16，不超过在途上限 | 执行并发 |
| `AML_MAX_INFLIGHT` | 32 | 执行与排队请求总上限 |
| `AML_QUEUE_TIMEOUT` | 1800 秒 | 排队等待上限 |
| `AML_REQUEST_TIMEOUT` | 1800 秒 | 获取执行名额之后的操作上限 |
| `AML_API_KEY` | 空 | 默认无鉴权；可选 Bearer / Token / X-Api-Key |

请求内容和成功响应格式保持不变。错误仍使用 `{"detail":{"reason":"..."}}`：

| HTTP 状态 | 含义 |
| --- | --- |
| 401 | 配置了密钥但凭证不匹配 |
| 409 | 同一个 backend 下的 `request_id` 被用于不同内容 |
| 422 | 请求格式不符合协议 |
| 429 | 在途请求已满；`Retry-After: 5` |
| 502 | 后端操作失败，成功回执不会写入 |
| 503 | 没有接入 backend 或尚未就绪 |
| 504 | 排队或执行超时 |

失败后应使用相同 `request_id` 和相同内容重试。接口不自动重试后端的整个操作，
模型重试等算法策略由具体 backend 决定。不同用户也不要复用同一个 `request_id`。

`data/requests.sqlite3` 的现有格式未改变。`legacy_receipt_backend_id` 仅是显式的旧版
回执导入选项，新后端无需设置。历史数据库保留在 `data/`，不会被新方法自动读取或删除。

## 测试

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m unittest discover -v
```

测试使用临时目录和测试 backend，不调用线上模型，也不改线上记忆数据。
