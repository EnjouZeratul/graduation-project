# 技术文档：地质灾害智能预警系统（FastAPI + LangGraph + Vue）

本文档面向“读代码/二次开发/部署运维”的同学，目标是把项目的核心模块、关键函数、批次（batch）机制、配置参数与协作关系讲清楚。

> 合规声明：本文档与项目代码仅用于学习、课程设计、科研和技术交流，禁止用于商业运营、数据转售或其他盈利场景。第三方站点/API 的调用需由使用者自行确认授权、ToS 与法律责任。

---

## 1. 技术栈与运行形态

**后端**
- FastAPI：HTTP API + WebSocket（见 `backend/app/main.py`、`backend/app/routes/`）
- LangGraph：多阶段工作流编排（见 `backend/app/agents/graph.py`）
- Celery：被动刷新（定时任务）+ 异步执行（见 `backend/app/celery_app.py`、`docker-compose.yml`）
- Redis：Celery broker/backend + WebSocket 推送桥接（Pub/Sub）（见 `backend/app/websocket_manager.py`）
- Redis：同时承载“采集缓存持久化”（WU key 池、active key、地区级天气缓存），容器重启后可复用（见 `backend/app/agents/data_sources.py`）
- PostgreSQL：持久化 `regions`、`warnings`（见 `backend/app/models.py`）
- httpx + BeautifulSoup（可选）：非官方站点爬虫与解析（见 `backend/app/agents/data_sources.py`、`backend/app/agents/scraper_parsers.py`）
- 高德天气（AMap Web 服务）：适配层数据源（见 `backend/app/agents/data_sources.py`、`backend/app/integrations/amap_weather.py`）
- Weather Underground 补充源（weather.com API，key 自动发现+缓存）：见 `backend/app/agents/data_sources.py`

**前端**
- Vue 3 + TypeScript（见 `frontend/src/App.vue`、`frontend/src/services/`）
- ECharts 地图展示（见 `frontend/src/components/MapView.vue`）

**部署/网关**
- Docker Compose：一键启动 `backend`、`frontend`、`postgres`、`redis`、`celery_worker`、`celery_beat`、`nginx`（见 `docker-compose.yml`）
- Nginx：反向代理前后端（见 `deploy/nginx.conf`）

### 1.1 仓库入口导航（简版）
面向“先定位再读代码”的入口说明：

- `backend/`：后端主工程（API、工作流、数据源、任务调度）。
- `frontend/`：前端主工程（地图、预警列表、详情、交互逻辑）。
- `deploy/`：部署/网关配置（Nginx 等）。
- `docs/`：说明文档（技术、产品、论文参考）。
- `tools/`：调试与辅助脚本（排错、映射生成等）。

- `.env`：运行参数与数据源开关。
- `docker-compose.yml`：本地开发/联调的统一编排入口。
- `readme.md`：项目总入口文档（启动、配置、接口、排错）。

- `tianqi.com的模式/`：天气网页快照样本目录（解析与 slug 映射调试）。
- `wunderground.com的模式/`：WU 页面快照样本目录（key 发现调试）。
- `国家气象数据API调用说明/`：国家气象 API 文档与站点数据样本。

---

## 2. 总体架构（数据流）

核心思想：**后端定期/手动触发“预警工作流”**，把每个地区的风险等级 + 原因 + 置信度 + 候选灾害写入数据库，并通过 WebSocket 增量推送到前端，前端地图与侧栏实时更新。

### 2.1 关键通道

1) **HTTP（前端 -> 后端）**
- 地区列表：`GET /api/regions`（见 `backend/app/routes/regions.py`）
- 主动刷新：`POST /api/warnings/trigger/async`（见 `backend/app/routes/warnings.py`）
- 中止刷新：`POST /api/warnings/trigger/abort`（见 `backend/app/routes/warnings.py`）
- 状态查询：`GET /api/warnings/trigger/status`
- 调试：`GET /api/warnings/debug/last-collection`、`POST /api/warnings/debug/reset-scraper-runtime`

2) **WebSocket（后端 -> 前端）**
- 前端连接：`/ws/warnings`（见 `backend/app/main.py`）
- 后端推送来源：Redis Pub/Sub `warnings_channel`（见 `backend/app/websocket_manager.py`）
- 后端发布者：工作流每个 batch commit 后 `publish(delta)`（见 `backend/app/routes/warnings.py`）

### 2.2 数据写入与前端呈现

- 每个地区的最新预警记录写入 `warnings` 表，最新风险等级同步写到 `regions.risk_level`。
- 前端页面加载时先 `GET /api/regions` 拉取“快照”。
- 刷新过程中后端按 batch 推送 delta，前端按 `region_code` 合并，不会把缺失字段覆盖成 0（见 `frontend/src/App.vue` 的 `mergeMeteorology()` + `applyRealtimeWarnings()`）。

---

## 3. 数据模型（DB）

见 `backend/app/models.py`

### 3.1 Region（地区）
- `code`：6 位行政区划码（唯一）
- `name`：地区名
- `risk_level`：当前风险等级（green/yellow/orange/red）
- `last_updated_at`：最近更新时间

### 3.2 Warning（预警记录）
- `region_id`：外键指向 Region
- `level`：green/yellow/orange/red
- `reason`：可解释原因（用户可见，已做中文化与降噪）
- `meteorology`：JSON 字符串，存储融合后的特征与元信息（置信度拆解、source_status、候选灾害等）
- `created_at`：时间戳
- `source`：来源标记（例如 `langgraph-hybrid`）

说明：
- “历史主动预警次数”目前是一个代理指标：来自 `warnings` 表中 yellow/orange/red 的计数（排除测试数据），见 `backend/app/agents/data_sources.py` 的 `count_historical_events()`。

---

## 4. 配置与模式（.env -> Settings）

见 `backend/app/core/config.py` 的 `Settings`

### 4.1 三态 API Key 逻辑
在 `backend/app/agents/data_sources.py` 中：
- `live`：填真实 key
- `simulate`：填 `simulate`（或 `simulated` 等）启用模拟数据
- `disabled`：占位符或空值视为禁用

对应函数：
- `_api_key_mode(value) -> live/simulate/disabled`

### 4.2 工作流关键参数（常用）
- `WORKFLOW_MAX_RUNTIME_SECONDS`：单次刷新最大运行时间（`Settings.workflow_max_runtime_seconds`）
- `WORKFLOW_MANUAL_REGION_LIMIT`：快速模式默认地区数（`Settings.workflow_manual_region_limit`）
- `COLLECTOR_MAX_CONCURRENCY`：数据采集并发上限（`Settings.collector_max_concurrency`）
- `ENABLE_LLM_REFINEMENT` / `LLM_REFINE_MAX_REGIONS` / `LLM_CONFIDENCE_THRESHOLD`：LLM 精修开关与阈值（见 `backend/app/agents/graph.py`）

### 4.3 爬虫关键参数（常用）
见 `Settings`：
- `UNOFFICIAL_SCRAPER_ENABLED`
- `SCRAPER_ALLOWED_DOMAINS`：白名单
- `SCRAPER_URL_TEMPLATE`：URL 模板（支持 `{tianqi_slug}`）
- `SCRAPER_REQUEST_INTERVAL_SECONDS`：全局节流
- `SCRAPER_MAX_PARALLEL_REQUESTS`：爬虫并发
- `SCRAPER_MAX_REQUESTS_PER_WINDOW`：30 分钟窗口预算（防止高频）
- `SCRAPER_CACHE_MINUTES`：缓存时间

安全策略：
- 政府域名阻断（`_is_government_domain()`）
- allowlist 必须命中（`_domain_allowed()`）
- URL collision 保护（`scraper_url_collision`，防止多个地区命中同一 URL 把站点打爆/串数据）

### 4.4 WU 补充源关键参数（常用）
见 `Settings`：
- `WU_ENABLED`：是否启用 `weather_wu_api` 补充源
- `WU_API_KEY`：手工 key（可选）
- `WU_KEY_DISCOVERY_ENABLED`：是否允许自动发现 key
- `WU_KEY_DISCOVERY_URL`：自动发现 key 的页面地址
- `WU_KEY_REFRESH_MINUTES`：key 缓存 TTL
- `WU_TIMEOUT_SECONDS` / `WU_MAX_RETRIES`：请求超时与重试
- `WU_LANGUAGE` / `WU_UNITS`：weather.com 查询参数（默认 `en-US`/`m`）

### 4.5 缓存持久化参数与行为（新增）
- 采集结果缓存 TTL：`SCRAPER_CACHE_MINUTES`
- WU key 刷新 TTL：`WU_KEY_REFRESH_MINUTES`
- Redis key：
  - `ghws:cache:wu:key_pool`
  - `ghws:cache:wu:active_key`
  - `ghws:cache:scraper:<source_name>:<region_code>`

行为说明：
- 读缓存顺序：内存缓存优先，未命中则回源 Redis。
- 写缓存：内存与 Redis 同步写入。
- 清理：`POST /api/warnings/debug/reset-scraper-runtime?clear_cache=true` 会清理内存与 Redis 缓存。

---

## 5. 主动刷新与被动刷新

### 5.1 主动刷新（前端按钮触发）
入口：`POST /api/warnings/trigger/async`（`backend/app/routes/warnings.py`）

模式：
- 快速模式：`fast_mode=true` + `region_limit`（默认取 `WORKFLOW_MANUAL_REGION_LIMIT`）
- 全量模式：`fast_mode=false`（不限制，刷新全量地区）

重要机制：
- **批次轮换**：快速模式不会永远刷新“前 N 个”，而是“固定包含高风险头部 + 其余按 request_id 轮换”，见 `backend/app/routes/warnings.py` 的 `_pick_regions_for_run()`。
- **中止**：`POST /api/warnings/trigger/abort`，在“当前 batch 结束后”停止，并保留已处理结果；已处理的 batch 已入库并已推送 delta。

### 5.2 被动刷新（Celery 定时）
入口：`backend/app/celery_app.py`
- `crontab(minute="*/30")` 每 30 分钟执行 `run_warning_workflow_task()`
- 实际调用：`run_and_persist_warning_workflow(force_llm=True)`

说明：
- 被动刷新与主动刷新走同一工作流/同一多源采集/同一爬虫限速参数。
- 默认全量地区（未传 `region_limit`）。

---

## 6. 批次（batch）机制：为什么存在、在哪里、怎么工作

批次的目的：
- **边跑边入库**：减少“全量跑完才出结果”的等待
- **边跑边推送**：前端逐步更新地图（delta）
- **中止更及时**：用户中止时最多等当前 batch 完成
- **超时也能保留已完成部分**：避免“超时全作废”

实现位置：`backend/app/routes/warnings.py` 的 `run_and_persist_warning_workflow()`

关键步骤：
1) 选定地区列表后，调用 `_group_by_prefix(region_inputs)`
   - 先按行政区划前两位（近似省级）分组
   - 再把大组切小 batch
   - batch 大小：`batch_size = max(15, min(40, COLLECTOR_MAX_CONCURRENCY*2))`

2) 每个 batch：
   - 调用 `run_warning_workflow(timestamp, regions=batch, force_llm=True)`（见 `backend/app/agents/graph.py`）
   - 将 batch 结果写入 DB：更新 `regions` + 插入 `warnings`
   - `commit()`
   - 向 Redis Pub/Sub 发布 delta：`publish("warnings_channel", delta_message)`

3) 运行态：
   - `WORKFLOW_LOCK_KEY`：避免重复触发并发刷新（Redis 分布式锁）
   - **heartbeat**：防止 uvicorn `--reload` 重启导致锁残留（僵尸锁自动清理）

---

## 7. LangGraph 工作流（节点与协作）

实现文件：`backend/app/agents/graph.py`

节点顺序（见 `build_graph()`）：
1) `coordinator_agent`：初始化 state
2) `data_collector_agent`：
   - 并发拉取 `DATA_SOURCES`（见 `backend/app/agents/data_sources.py`）
   - 记录 `source_status.success/errors`
   - 读取历史统计（`count_historical_events()`）
   - 拉取“上一次预警快照”用于变化检测（`_parse_latest_warning_snapshot()`）
3) `data_validation_agent`：缺失/异常处理、质量分与质量备注
4) `local_risk_agent`：本地风险分与基础置信度（省 token）
5) `neighbor_influence_agent`：邻区风险融合（`NEIGHBOR_INFLUENCE_WEIGHT`）
6) `llm_refinement_agent`：
   - 仅对“变化较大/低置信度/强制触发”的少量地区做轻量复核
   - 输出 JSON 并转为中文 `reason_append`
7) `decision_maker_agent`：
   - 产出最终 `level/reason/confidence`
   - 把 `hazard_candidates`、`confidence_breakdown`、`source_status` 等写进 `meteorology` JSON
   - 原因末尾追加“最可能灾害：…”

---

## 8. 数据源（多源融合）与爬虫映射

### 8.1 数据源注册
文件：`backend/app/agents/data_sources.py`
- `DATA_SOURCES` 当前包含：
  - `weather_cma`
  - `weather_amap`
  - `weather_wu_api`
  - `weather_openweather`
  - `geology_cgs`
  - `weather_scraper`
  - `geology_scraper`
- 每个源提供：
  - `fetch(region_code, region_name?)`
  - `normalize(raw)` 输出统一字段（例如 `rain_24h/rain_1h/slope/fault_distance` 等）
  - `reliability` 用于融合权重

新增（近期）：
- `WeatherUndergroundApiSource` 成功命中后会回写 active key 到 Redis，减少后续冷启动/重启的 key 探测开销。
- WU key 发现流程优先读取 Redis key_pool（未过期时不重复抓页面）。

### 8.2 融合规则（同 channel）
文件：`backend/app/agents/graph.py`
函数：`_merge_channel_payload(channel, payloads)`
- 数值字段：按 reliability 加权平均
- 非数值字段：保留“更可靠源”的首个值（避免低可靠爬虫覆盖模拟/官方元信息）

### 8.3 Tianqi slug 映射（重点）
文件：`backend/app/agents/data_sources.py`
- `backend/app/data/tianqi_slug_overrides.json`：本地 overrides（可手动修正歧义城市）
- `_resolve_tianqi_slug(region_name)`：解析 slug 的核心

经验规则：
- overrides 尽量使用 `regions.name` 的全称 key（例如 `中山市`），少用简称（例如 `中山`），避免歧义导致张冠李戴。
- 修改 overrides 后，建议调用 `POST /api/warnings/debug/reset-scraper-runtime?clear_cache=true` 让后端重新加载并清缓存。

### 8.4 URL collision（防串数据/防打爆）
文件：`backend/app/agents/data_sources.py`
- `_tianqi_url_owner` 记录 “canonical URL -> owner target_code”
- 如果不同地区解析到同一个 URL，会返回 `scraper_url_collision`，避免错误数据写入与重复轰炸站点。

---

## 9. 前端协作方式（快照 + WS delta 合并）

入口文件：`frontend/src/App.vue`

关键点：
- 初始快照：`fetchRegions()`（`frontend/src/services/api.ts`）
- WS：`connectWarningsWS()`（`frontend/src/services/ws.ts`）
- delta 合并：`applyRealtimeWarnings()` 按 `region_code` 定位地区并合并 `meteorology`
  - `mergeMeteorology(prev, incoming)` 会保留已有字段，避免“缺失覆盖成 0/全绿”
- 刷新后快照同步：`syncSnapshotAfterRefresh()` 采用“按地区合并”而非整包覆盖  
  - 若某地区本轮无新 `latest_warning`，保留前端旧状态  
  - 若快照时间戳比当前旧，也保留旧状态
- 手动按钮：
  - 主动刷新（快速模式默认）：`triggerWarningWorkflowAsync(true)`
  - 全量刷新：`triggerWarningWorkflowAsync(false)`
  - 中止：`abortWarningWorkflow()`
  - 随机模拟（不入库）：`debugRandomizeWarnings()` -> `POST /api/warnings/debug/randomize`

界面展示更新（近期）：
- 原因文本中会移除“最可能灾害：...”片段，避免与单独“可能灾害”字段重复展示。

地图：
- `frontend/src/components/MapView.vue` 使用 ECharts map，根据 `risk_level` 渐变着色并保留清晰边界。

---

## 10. 常用排错入口（文件与命令）

### 10.1 看最近一次采集结果与错误分布
- `GET /api/warnings/debug/last-collection`
- `GET /api/warnings/trigger/status`

### 10.2 重置爬虫运行态/缓存（尤其在更新 overrides 后）
- `POST /api/warnings/debug/reset-scraper-runtime?clear_cache=true`

### 10.4 验证 Redis 持久化是否生效（新增）
```powershell
# 触发一轮 20 区快速刷新后查看缓存
$wuCacheKeys = docker compose exec -T redis redis-cli --scan --pattern "ghws:cache:scraper:weather_wu_api:*"
"wu_cache_key_count=$($wuCacheKeys.Count)"
docker compose exec -T redis redis-cli GET ghws:cache:wu:key_pool
docker compose exec -T redis redis-cli GET ghws:cache:wu:active_key
```

### 10.3 PowerShell 脚本
- `tools/debug-scraper.ps1`：导出失败详情到 `failed-weather-scraper.json`
- `tools/build-tianqi-overrides.ps1`：从 HTML 快照生成 `tianqi_slug_overrides.json`

---

## 11. 二次开发指南（最常见的改动点）

### 11.1 接入官方 CMA/CGS API
文件：`backend/app/agents/data_sources.py`
- `CMAWeatherDataSource`（国家气象）是**站点观测接口**（按 `Station_Id_C` 查），需要先做行政区 `region_code -> Station_Id_C` 映射：
  - 站点表放置：`backend/app/data/China_SURF_Station.xlsx`
  - 生成映射：`backend/app/tools/build_cma_region_station_map.py`
  - 输出文件：`backend/app/data/cma_region_station_map.json`
  - 可选 overrides：`backend/app/data/cma_region_station_overrides.json`（少量特殊地区手工纠错）
- CMA 环境变量（见 `backend/app/core/config.py`）：
  - `CMA_USER_ID` / `CMA_PASSWORD`
  - `CMA_BASE_URL`（建议 `http://api.data.cma.cn:8090`）
  - `CMA_INTERFACE_ID`（默认 `getSurfEleByTimeRangeAndStaID`）
  - `CMA_DATA_CODE`（默认 `SURF_CHN_MUL_HOR_3H`）
  - `CMA_TIME_ZONE_OFFSET_HOURS`（默认 `8`）
- 在 `normalize()` 中把 CMA 的 `PRE_3h` 累加成 `rain_24h`（mm）；`rain_1h` 默认留空，避免不可靠换算。
- `CGSGeologyDataSource` 同理：在 `fetch()` 填充真实请求与鉴权，在 `normalize()` 统一成标准字段（`slope/fault_distance/...`）。

### 11.2 Weather Underground 补充源（weather.com API）
文件：`backend/app/agents/data_sources.py`
- 源名称：`weather_wu_api`（`channel=meteorology`）
- 数据来源：`/v3/wx/observations/current`（按地区经纬度 `geocode=lat,lon`）
- key 来源：
  - 手工配置：`WU_API_KEY`
  - 自动发现：`WU_KEY_DISCOVERY_URL` 页面中提取 `apiKey`
- 关键保护：
  - key 缓存 + TTL（`WU_KEY_REFRESH_MINUTES`）
  - key 失效（401/403）自动重取一次
  - 失败按非致命错误返回，不阻塞工作流
- 建议定位：仅作为补充源，不覆盖权威官方 mm 降雨主源

### 11.2 新增一个数据源
步骤：
1) 实现一个 `DataSource`（同文件）
2) `DATA_SOURCES[name] = source` 注册
3) 只要 `normalize()` 输出的字段被 `graph.py` 使用，就会自动参与融合与决策

### 11.3 调整评分/置信度/候选灾害规则
文件：`backend/app/agents/graph.py`
- `_risk_score_from_data()`、`local_risk_agent()`
- `_infer_hazard_candidates()`
- 置信度拆解：`confidence_breakdown` 结构

---

## 12. 约定与注意事项

- 用户可见内容尽量中文化；`reason_append` 也要求中文（见 `graph.py` 的 `_normalize_llm_reason_append_zh()`）。
- 爬虫只作为辅助，且必须遵守 allowlist 与政府域名阻断策略。
- 对全国全量抓取要保守配置并发/节流/预算；更推荐“快速模式批次轮换”多次覆盖全局。
