# 地质灾害预警系统（FastAPI + LangGraph + Vue）

## 1. 项目概述
本项目用于地质灾害风险预警，核心目标是：
- 融合气象、地质、历史信息做分区风险评估。
- 输出可解释预警结果（等级 + 原因 + 置信度 + 可能灾害候选）。
- 在地图端实时展示，并支持手动“主动刷新”。
- 在不明显增加 LLM 成本的前提下利用 LangGraph 做多阶段决策。


## 用途与合规声明（重要）
- 本项目仅用于学习、课程设计、科研与技术交流演示，不得用于商业运营、付费服务、数据转售或引流获利。
- 本项目涉及第三方站点/API 的调用示例仅用于工程实践说明；实际使用前请自行核验并严格遵守目标平台 ToS/robots/法律法规。
- 使用者应自行承担部署与使用行为的合规责任；作者与贡献者不对未经授权的数据抓取、商业使用或违规使用后果负责。
- 生产场景请优先接入已授权的官方数据源（如 CMA/CGS 等），并将非官方源降级为补充源。

## 技术文档
- `docs/TECHNICAL_DOC.md`：面向读代码/二次开发/运维的整体技术说明（模块、批次机制、关键函数与协作关系）。
- `docs/PRODUCT_DOC.md`：面向答辩/汇报的产品说明（目标、功能、需求与创新点）。

## 2. 核心能力（当前版本）
- 风险等级：`green / yellow / orange / red`（低风险到高风险）。
- 置信度：按地区独立计算，并在“地区详情”中展示依据与计算拆解。
- 邻区影响融合：通过邻区风险均值修正本地风险分，避免孤岛式判断。
- LLM 精修策略：优先本地计算，仅对“变化较大/低置信度/强制触发”地区做轻量复核。
- 手动刷新：前端“主动刷新”触发异步工作流，避免页面长时间阻塞。
- 手动刷新进度可视：运行中显示“本次计划/已处理/已运行秒数”，其中“已处理”按批次递增（常见为 15 的整数倍，取决于 batch 大小）。
- 30 分钟周期调度：后端定时拉取并计算；可手动提前触发。
- 数据源扩展：注册表模式，后续新增官方 API/第三方 API/爬虫无需改主图流程。
- 前端展示优化：同一地区“可能灾害”与原因文本中的“最可能灾害”去重，避免重复阅读。
- 快速刷新一致性：当本轮仅处理部分地区时，未处理地区保留上轮有效状态，不会被快照覆盖成绿色。

## 2.1 近期优化摘要（当前版本）
- 前端中文一致化与展示降噪：用户可见文案统一中文，移除重复与噪声片段。
- 主动刷新进度增强：支持实时显示“计划数/已处理数/运行秒数”。
- 快速刷新稳定性增强：部分刷新不再覆盖未处理地区旧状态。
- 中止刷新可保留结果：中止后已处理批次继续可见并可推送。
- WU 补充源工程化：支持 key 自动发现、缓存、失效回退。
- Redis 持久化缓存：WU key 与地区级缓存可跨 backend 重启复用。
- 多源融合可控：官方源优先、补充源兜底、缺失时沿用上轮有效观测。

## 3. 目录结构（核心）
```text
backend/app/
  agents/
    graph.py              # LangGraph 多智能体工作流
    data_sources.py       # 数据源注册、抓取、缓存、限速
    scraper_parsers.py    # 通用+站点定制解析器（含 tianqi）
    llm_provider.py       # LLM 提供商自动识别
  routes/
    warnings.py           # 预警接口、主动刷新、状态查询、AI问答
    regions.py
  core/config.py
  schemas.py
frontend/src/
  App.vue                 # 主界面（预警列表、地区详情、主动刷新、AI问答）
  components/MapView.vue  # 地图可视化
  services/api.ts
  services/ws.ts
```

### 3.1 项目根目录导航（有些可自行获取的内容已从目录移除）
说明：以下为项目根目录条目及用途。部分文件是调试产物，属于可选保留项。

- `backend/`：后端代码（API、LangGraph 工作流、数据源、Celery 任务）。
- `frontend/`：前端代码（Vue 页面、地图组件、API/WS 调用）。
- `deploy/`：部署与网关配置（如 Nginx）。
- `docs/`：技术文档、产品文档文档。
- `tools/`：排错脚本与辅助工具脚本。
- `tianqi.com的模式/`：天气网页快照样本（解析/映射调试用）。#可自行获取
- `wunderground.com的模式/`：WU 页面快照样本（key 发现与解析调试用）。#可自行获取
- `国家气象数据API调用说明/`：国家气象 API 资料目录（站点表、字段说明等）。#可自行获取
- `备份合集/`：历史备份目录（不参与当前运行链路）。#可自行创建

- `.env`：环境变量配置（数据源开关、API 参数、并发/限速、工作流参数）。
- `.env.example`：环境变量模板文件（发布仓库建议保留该文件）。
- `docker-compose.yml`：容器编排入口（backend/frontend/postgres/redis/celery/nginx）。
- `readme.md`：项目总说明（运行、配置、接口、排错、合规）。

- `debug-last-collection.json`：最近一次采集调试导出文件。
- `last-collection-raw.json`：采集结果原始导出文件。
- `failed-weather-scraper.json`：爬虫失败明细（常由调试脚本输出）。
- `scraper-errors.json`：爬虫错误聚合导出文件。
- `wug-live.html`：WU 页面抓取 HTML 样本（手动测试生成）。
- `wug-endpoints.txt`：从 WU 页面提取的接口端点记录。

### 3.2 环境变量文件
已提供 `.env.example` 作为模板，使用者可自行创建 `.env`。


## 4. 关键流程
1. `data_collector_agent`：并发收集多源数据，记录成功/失败状态。  
2. `data_validation_agent`：缺失值、异常值、质量分处理。  
3. `local_risk_agent`：本地风险与基础置信度计算（省 token）。  
4. `neighbor_influence_agent`：邻区影响融合。  
5. `llm_refinement_agent`：仅对必要地区做轻量 LLM 精修。  
6. `decision_maker_agent`：产出最终等级、原因、置信度、候选灾害。  

## 5. 启动
```bash
docker compose up -d --build
```

访问地址：
- 前端：`http://localhost`
- 健康检查：`http://localhost/health`

首次导入地区（若数据库为空）：
```bash
curl -X POST http://localhost/api/regions/seed
```

## 6. 主动刷新接口
- `POST /api/warnings/trigger/async`：异步触发（前端按钮使用）
- `GET /api/warnings/trigger/status`：查询运行状态
- `POST /api/warnings/trigger/abort`：请求中止（不会丢弃已处理结果；当前批次结束后停止）
- `POST /api/warnings/trigger/reset`：手动清理卡住的运行态（仅排障用）
- `POST /api/warnings/trigger`：同步触发（会等待全流程）
- `GET /api/warnings/debug/last-collection`：查看最近一次采集/判断结果
- `POST /api/warnings/debug/reset-scraper-runtime`：重置爬虫预算计数与冷却状态（可选清缓存）
- `POST /api/warnings/debug/randomize`：生成全量“随机模拟”快照并通过 WS 推送（不入库，不调用 LangGraph/LLM/API/爬虫）

说明：
- 后端有运行态锁与超时控制，避免重复并发触发长期卡死。
- 前端会轮询状态并同步快照；同时，后端会通过 WS 增量推送（delta）逐步更新地图与详情。
- 即使发生超时（`workflow_timeout_*`），已完成部分也会尽量写入数据库并推送（不会“全盘作废”）。
- 刷新结束后的快照同步采用“按地区合并”策略：若某地区在本轮未更新或快照时间戳更旧，则保留前端当前状态。
- 快速模式（`fast_mode=true`）会在每次触发时固定覆盖高风险地区，并对剩余地区做“批次轮换”（基于 request_id 的确定性偏移），避免永远只刷新前 100 个地区；多次快速刷新可逐步逼近全量覆盖，同时降低爬虫 403/封控风险。
- “随机模拟”（`debug/randomize`）用于前端演示与快速联调：点一次就立刻给全部地区生成一套新随机数据并推送到前端；不写入 `warnings` 表，因此不会污染“历史主动预警次数”等统计。

随机模拟手动触发示例：
```powershell
Invoke-RestMethod -Method Post "http://localhost/api/warnings/debug/randomize" | ConvertTo-Json -Depth 4
```

## 7. `.env` 关键项（建议）
文件位置：项目根目录（与 `docker-compose.yml` 同级）。  
首次使用：先复制 `.env.example` 为 `.env`，再填写本地配置与 API Key。  
```env
# 数据库/缓存
DATABASE_URL=postgresql+psycopg2://ghws_user:ghws_password@postgres:5432/ghws
REDIS_URL=redis://redis:6379/0

# LLM 自动识别顺序：custom -> deepseek -> openai -> qwen
LLM_PROVIDER=auto
DEEPSEEK_API_KEY=
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat
OPENAI_API_KEY=
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-4o-mini
QWEN_API_KEY=
QWEN_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
QWEN_MODEL=qwen-plus

# 官方 API（拿到后填真值；不填则回退到其他源）
CMA_API_KEY=                       # 可用于 simulate 模式开关（填 simulate 即启用模拟）
# 国家气象（CMA）站点观测接口：需要 userId/pwd（按站号查询）
CMA_USER_ID=
CMA_PASSWORD=
CMA_BASE_URL=http://api.data.cma.cn:8090
CMA_INTERFACE_ID=getSurfEleByTimeRangeAndStaID
CMA_DATA_CODE=SURF_CHN_MUL_HOR_3H
CMA_TIME_ZONE_OFFSET_HOURS=8
CGS_API_KEY=
CGS_BASE_URL=http://api.cgs.gov.cn
OPENWEATHER_API_KEY=
AMAP_API_KEY=
AMAP_BASE_URL=https://restapi.amap.com
# Weather Underground 补充源（weather.com API，自动发现 key）
WU_ENABLED=true
WU_API_KEY=
WU_API_BASE_URL=https://api.weather.com
WU_KEY_DISCOVERY_ENABLED=true
WU_KEY_DISCOVERY_URL=https://www.wunderground.com/weather/cn/hangzhou
WU_KEY_REFRESH_MINUTES=360
WU_TIMEOUT_SECONDS=8
WU_MAX_RETRIES=1
WU_LANGUAGE=en-US
WU_UNITS=m
WU_RELIABILITY=0.62

# 工作流策略
ENABLE_LLM_REFINEMENT=true
LLM_REFINE_MAX_REGIONS=20
LLM_CONFIDENCE_THRESHOLD=0.6
NEIGHBOR_INFLUENCE_WEIGHT=0.2
WORKFLOW_MAX_RUNTIME_SECONDS=3600

# 非官方爬虫（辅助）
UNOFFICIAL_SCRAPER_ENABLED=true
SCRAPER_ALLOWED_DOMAINS=["tianqi.com","weather.sina.com.cn","qweather.com"]
SCRAPER_USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...
SCRAPER_URL_TEMPLATE=https://www.tianqi.com/{tianqi_slug}/
SCRAPER_TIANQI_CITY_INDEX_URL=https://www.tianqi.com/chinacity.html
# 可选：手工修正少数城市 slug（用于解决歧义/站点变更导致的 qingyuan3 这类情况）
# 文件位置：backend/app/data/tianqi_slug_overrides.json
SCRAPER_CITY_LEVEL_ONLY=true
SCRAPER_REQUEST_INTERVAL_SECONDS=1.8
SCRAPER_MAX_PARALLEL_REQUESTS=2
SCRAPER_TIMEOUT_SECONDS=12
SCRAPER_MAX_RETRIES=1
SCRAPER_MAX_REQUESTS_PER_WINDOW=600
SCRAPER_CACHE_MINUTES=30
SCRAPER_PARSER_COOLDOWN_MINUTES=3

# 地质爬虫（可选）
GEOLOGY_SCRAPER_ENABLED=false
GEOLOGY_SCRAPER_URL_TEMPLATE=
```

## 8. API 申请后的接入方式（建议）
后续申请到气象/地质 API 后，可按以下步骤接入：
1. 在 `.env` 写入真实 Key 与 Base URL。  
2. 在 `backend/app/agents/data_sources.py` 对应数据源 `fetch()` 里补齐真实请求参数。  
3. 在 `normalize()` 统一输出字段（`rain_24h`、`rain_1h`、`slope`、`fault_distance` 等）。  
4. 对新字段只扩展 `graph.py` 中评分逻辑，不改主流程节点结构。  
5. 用 `GET /api/warnings/debug/last-collection` 校验 `source_status` 是否命中官方源。  

### 8.1 国家气象（CMA）站点数据：region_code -> Station_Id_C 映射（推荐做一次离线生成）
国家气象接口是“按站号（`Station_Id_C`）查询”，而本系统是“按行政区划（`region_code`）评估”。因此需要一层映射。

当前版本采用“方案2”：把站点表（`China_SURF_Station.xlsx`）离线生成一个 JSON 映射文件，运行时只读 JSON，避免运行时解析 Excel 引入复杂依赖/不确定性。

文件位置：
- 站点 Excel（默认不提交到仓库，首次使用请自行下载并放置）：`backend/app/data/China_SURF_Station.xlsx`
- 生成的映射：`backend/app/data/cma_region_station_map.json`
- 可选 overrides（手工修正少量特殊地区）：`backend/app/data/cma_region_station_overrides.json`

生成命令（在容器内执行，读取数据库 `regions` 坐标并计算最近台站）：
```powershell
# 1) 把“本机下载路径”的站点表复制到后端挂载目录（这样容器内可读）
Copy-Item "<本机下载路径>\\China_SURF_Station.xlsx" ".\\backend\\app\\data\\China_SURF_Station.xlsx" -Force

# 2) 生成映射 JSON（输出到 backend/app/data）
docker compose exec -T backend python -m app.tools.build_cma_region_station_map `
  --station-xlsx /app/app/data/China_SURF_Station.xlsx `
  --out /app/app/data/cma_region_station_map.json `
  --overrides /app/app/data/cma_region_station_overrides.json
```

说明：
- `regions` 表经纬度覆盖率为 100% 时，“最近台站”映射可直接计算，无需先补坐标。
- CMA 返回的是 `PRE_3h`（3 小时降水，mm），系统会在 `normalize()` 内累加近 24 小时得到 `rain_24h`；`rain_1h` 默认保留缺失（避免做不可靠换算），必要时可结合其它源估算并在 `data_quality_note` 标注。

### 8.2 Weather Underground 补充源（weather.com API）启用说明
当前版本支持 `weather_wu_api` 作为**补充源**参与主动/被动刷新，不替代官方主源：
- 按地区经纬度调用 `api.weather.com` 实况接口（`v3/wx/observations/current`）。
- 支持从 `WU_KEY_DISCOVERY_URL` 页面 payload 自动发现 `apiKey`，并做缓存与轮换。
- 遇到 key 失效（401/403）会尝试重取 key；失败时按“非致命错误”处理，不阻塞整体工作流。
- 缓存持久化：`WU key pool/active key` 与地区级 `weather_wu_api` 结果会写入 Redis，容器重启后可复用，降低冷启动外网请求压力与风控风险。

建议配置（已在 `.env` 示例中给出）：
- `WU_ENABLED=true`
- `WU_KEY_DISCOVERY_ENABLED=true`
- `WU_KEY_REFRESH_MINUTES=360`
- `WU_TIMEOUT_SECONDS=8`
- `WU_MAX_RETRIES=1`

说明：
- 该能力用于学习与工程验证，平台策略可能变化，稳定性不保证，严禁商业化依赖。
- `weather_wu_api` 的融合权重为补充级，官方 mm 降雨源优先。

### 8.3 Redis 持久化缓存（新增）
当前版本对“必要 + 建议”两类缓存做了持久化（Redis）：
- 必要：WU key 池与 active key
  - `ghws:cache:wu:key_pool`
  - `ghws:cache:wu:active_key`
- 建议：地区级采集缓存（含 `weather_wu_api`）
  - key 模式：`ghws:cache:scraper:<source_name>:<region_code>`

实现效果：
- 后端容器重启后，缓存仍可复用（不必每次重新发现 key/全量请求外网）。
- 仅在执行 `POST /api/warnings/debug/reset-scraper-runtime?clear_cache=true` 或清空 Redis 卷时清除。

快速验证命令（PowerShell）：
```powershell
# 1) 清缓存
Invoke-RestMethod -Method Post "http://localhost/api/warnings/debug/reset-scraper-runtime?clear_cache=true" | ConvertTo-Json -Depth 6
docker compose exec -T redis redis-cli --scan --pattern "ghws:cache:*"

# 2) 跑 20 区，生成缓存
Invoke-RestMethod -Method Post "http://localhost/api/warnings/trigger/async?fast_mode=true&region_limit=20" | Out-Null
do { Start-Sleep -Seconds 2; $status = Invoke-RestMethod "http://localhost/api/warnings/trigger/status" } while ($status.running)

# 3) 看缓存与 WU key
$wuCacheKeys = docker compose exec -T redis redis-cli --scan --pattern "ghws:cache:scraper:weather_wu_api:*"
"wu_cache_key_count=$($wuCacheKeys.Count)"
docker compose exec -T redis redis-cli GET ghws:cache:wu:key_pool
docker compose exec -T redis redis-cli GET ghws:cache:wu:active_key
```

## 9. 爬虫策略与合规说明  
- 本项目代码与文档仅用于学习用途，禁止商业化使用。
- 爬虫仅作辅助源，不替代官方源。  
- 已内置政府域名阻断逻辑（命中 `*.gov*` 会拒绝抓取）。  
- 必须通过 `SCRAPER_ALLOWED_DOMAINS` 白名单放行目标站点。  
- 已有全局限速、窗口预算、缓存与解析失败冷却，避免高频冲击目标站点。  
- 抓取使用连接复用与按域名退避（遇到 `403/429` 会自动冷却一段时间），降低持续失败与被封风险。  
- 对 `tianqi.com` 的城市拼音 slug：  
  - 优先从城市索引页生成映射，并**跳过** `/province/...` 这类省份索引链接，避免把 `province` 误当 slug（会导致 `.../province/` 404 或 slug 串错）。  
  - 当 `SCRAPER_CITY_LEVEL_ONLY=true` 时，对 `区/县/旗` 不做“拼音猜测”兜底，避免生成 `jinganqu` 这类往往无效的页面导致大量 403。  
  - 失败时只尝试保守的 URL 变体（不再追加 `/7/` 这类高概率触发 403 的路径）。  
- `tianqi_slug_overrides.json` 匹配策略：优先用地区名“正规化后缀剥离”结果匹配（如 `汕尾市 -> 汕尾`），再考虑重解码/启发式候选，避免误命中到其它区县条目导致“数据串地区”。  
- 已支持受控并发抓取（`SCRAPER_MAX_PARALLEL_REQUESTS`，建议从 `2` 开始），在不明显提高封禁风险的前提下提升吞吐。  
- 抓取前仍需自行确认目标站点 robots/ToS 和当地法律要求。  
- 若覆盖全国地区，`SCRAPER_MAX_REQUESTS_PER_WINDOW` 需大于单轮唯一抓取目标数（建议 `>= 600`）。  
- 建议配置浏览器风格 `SCRAPER_USER_AGENT`，避免部分站点直接拒绝机器人 UA。  
 
## 10. 如何确认爬虫是否生效  
1. 确认 `.env`：  
   - `UNOFFICIAL_SCRAPER_ENABLED=true`  
   - `SCRAPER_ALLOWED_DOMAINS` 包含目标域名  
   - `SCRAPER_URL_TEMPLATE` 正确  
2. 点前端“主动刷新”或调用异步触发接口。    
3. 调用 `GET /api/warnings/debug/last-collection`，重点看：  
   - `source_status.success.meteorology` 是否含 `weather_scraper`  
   - 若启用 WU 补充源，`source_status.success.meteorology` 也可能包含 `weather_wu_api`
   - `source_status.errors` 是否出现 `domain_not_allowed` / `tianqi_slug_not_found` 等  
4. 导出爬虫失败明细（便于排错）：在 PowerShell 运行 `tools/debug-scraper.ps1`，会生成 `failed-weather-scraper.json`。  
5. 如果频繁出现 `tianqi_slug_not_found` 或 URL 串错（例如 `.../province/`）：  
   - 先运行 `tools/build-tianqi-overrides.ps1` 从仓库内的快照页面生成 `backend/app/data/tianqi_slug_overrides.json`。  
   - 再调用 `POST /api/warnings/debug/reset-scraper-runtime?clear_cache=true`，让后端重新加载 overrides 并清理缓存后再测（当前版本无需重启容器）。  
6. 推荐“最小验证集”：先跑 20 个地区确认不全 0，再跑 100 个地区看失败分布，并在测试后打印具体问题 URL。  
```powershell
# 0) PowerShell 中文输出建议（避免乱码影响阅读）
chcp 65001 | Out-Null
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()

# 20 个地区
Invoke-RestMethod -Method Post "http://localhost/api/warnings/trigger/reset" | Out-Null
Invoke-RestMethod -Method Post "http://localhost/api/warnings/debug/reset-scraper-runtime?clear_cache=true" | Out-Null
Invoke-RestMethod -Method Post "http://localhost/api/warnings/trigger/async?fast_mode=true&region_limit=20" | Out-Null
do { Start-Sleep -Seconds 3; $status = Invoke-RestMethod "http://localhost/api/warnings/trigger/status" } while ($status.running)
$debug = Invoke-RestMethod "http://localhost/api/warnings/debug/last-collection"
"results_count=$($debug.results.Count)"

# 20 个地区：确认不是“全 0”
$total = $debug.results.Count
$nonZero = ($debug.results | Where-Object {
  ($_.meteorology.rain_24h -gt 0) -or
  ($_.meteorology.rain_1h -gt 0) -or
  ($_.meteorology.humidity -gt 0) -or
  ($_.meteorology.wind_speed -gt 0)
}).Count
"total=$total nonZero=$nonZero"

# 100 个地区
Invoke-RestMethod -Method Post "http://localhost/api/warnings/trigger/reset" | Out-Null
Invoke-RestMethod -Method Post "http://localhost/api/warnings/debug/reset-scraper-runtime?clear_cache=true" | Out-Null
Invoke-RestMethod -Method Post "http://localhost/api/warnings/trigger/async?fast_mode=true&region_limit=100" | Out-Null
do { Start-Sleep -Seconds 3; $status = Invoke-RestMethod "http://localhost/api/warnings/trigger/status" } while ($status.running)
$debug = Invoke-RestMethod "http://localhost/api/warnings/debug/last-collection"
$debug.results |
  ForEach-Object { $_.meteorology.source_status.errors.weather_scraper.message } |
  Group-Object | Sort-Object Count -Descending | Format-Table -Auto

# 100 个地区：打印 403 的完整 URL（用于判断是站点策略噪声还是 slug/URL 错配）
$debug.results |
  Where-Object { $_.meteorology.source_status.errors.weather_scraper.message -eq "http_status_403" } |
  Select-Object -First 60 region_name, region_code,
    @{n="url";e={$_.meteorology.source_status.errors.weather_scraper.url}} |
  Format-List

# 100 个地区：快速检查是否仍出现明显错误路径（理论上应为空）
$debug.results |
  ForEach-Object { $_.meteorology.source_status.errors.weather_scraper.url } |
  Where-Object { $_ -match "/province/|/7/" } |
  Select-Object -First 50

# 100 个地区：按 slug 聚合 403（快速识别“汕尾 -> maweiqu”这类误命中） 
$debug.results | 
  Where-Object { $_.meteorology.source_status.errors.weather_scraper.message -eq "http_status_403" } | 
  ForEach-Object { 
    $u = $_.meteorology.source_status.errors.weather_scraper.url 
    $slug = "" 
    try { $slug = ([uri]$u).Segments[1].TrimEnd("/") } catch {} 
    [pscustomobject]@{ code=$_.region_code; slug=$slug; url=$u } 
  } | 
  Group-Object slug | 
  Sort-Object Count -Descending | 
  Format-Table -Auto 

# 100 个地区：定位“拿到 HTML 但解析不到指标”的 URL
$debug.results |
  Where-Object { $_.meteorology.source_status.errors.weather_scraper.error -eq "html_parse_no_metrics" } |
  Select-Object region_name, region_code,
    @{n="url";e={$_.meteorology.source_status.errors.weather_scraper.url}} |
  Format-List
```   

## 11. 测试后快速排错（按顺序复制粘贴）
适用场景：刚跑完 `region_limit=20/100`，需要立刻确认问题类型与具体 URL（避免“看起来都绿/都 0”但实际上是抓取失败或映射错误）。

```powershell
# 建议先开 UTF-8 输出（避免中文乱码影响排错）
chcp 65001 | Out-Null
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()

$debug = Invoke-RestMethod "http://localhost/api/warnings/debug/last-collection"

# 0) 总览：各气象源“成功组合”分布（可快速判断当前主要依赖了哪个源）
$debug.results |
  ForEach-Object { ($_.meteorology.source_status.success.meteorology -join ",") } |
  Group-Object |
  Sort-Object Count -Descending |
  Format-Table -Auto

# 0.1) 总览：所有气象源错误分布（不只 weather_scraper / 不只 WU）
$debug.results |
  ForEach-Object {
    $_.meteorology.source_status.errors.PSObject.Properties |
      ForEach-Object { "$($_.Name):$($_.Value.message)" }
  } |
  Group-Object |
  Sort-Object Count -Descending |
  Format-Table -Auto

# 1) 错误分布（先看大头是什么）
$debug.results |
  ForEach-Object { $_.meteorology.source_status.errors.weather_scraper.message } |
  Group-Object |
  Sort-Object Count -Descending |
  Format-Table -Auto

# 2) 列出 403 的 URL（判断是站点临时限制，还是 URL/slug 错配）
$debug.results |
  Where-Object { $_.meteorology.source_status.errors.weather_scraper.message -eq "http_status_403" } |
  Select-Object -First 60 region_name, region_code,
    @{n="url";e={$_.meteorology.source_status.errors.weather_scraper.url}} |
  Format-List

# 3) 列出“拿到 HTML 但解析不到指标”的 URL（通常是页面结构变了，或拿到了非目标页面）
$debug.results |
  Where-Object { $_.meteorology.source_status.errors.weather_scraper.error -eq "html_parse_no_metrics" } |
  Select-Object region_name, region_code,
    @{n="url";e={$_.meteorology.source_status.errors.weather_scraper.url}} |
  Format-List

# 4) 可选：检查 WU 补充源错误分布（401/403/key 轮换）
$debug.results |
  ForEach-Object { $_.meteorology.source_status.errors.weather_wu_api.message } |
  Group-Object |
  Sort-Object Count -Descending |
  Format-Table -Auto
```

补充说明：
- 若出现“多地区反复请求同一 URL”，多半是抓取目标折叠（district -> city）或 slug 映射异常导致。当前版本已避免在 `city_code` 不存在时折叠；并增加了保险：当不同 `target_code` 映射到同一 `tianqi.com` 页面时会返回 `scraper_url_collision`，避免持续打同一个页面。
- 站点临时限制（403）通常会自动解除（几分钟到数小时不等），属于目标站点 WAF/风控的常见行为，不保证稳定；建议控制并发和频率，优先用缓存与城市级聚合减少请求数。

附：如需导出失败详情并长期对比：  
```powershell
.\tools\debug-scraper.ps1
Get-Content .\failed-weather-scraper.json -Encoding utf8 | Select-Object -First 60
```  

## 12. 清理测试预警（推荐，可选）
如果早期做过“杭州橙色预警”等测试，可能会影响“历史主动预警次数 X 次”等统计或页面展示。可调用接口按规则删除测试预警并回填各地区最近的非测试结果：

```powershell
Invoke-RestMethod -Method Post "http://localhost/api/warnings/cleanup-test-data" | ConvertTo-Json -Depth 6
```
  
## 13. 常见问题   
### Q0: Windows PowerShell 输出中文乱码
- 先执行：`chcp 65001`  
- 再执行：`[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()`  
- 读取本文档建议：`Get-Content readme.md -Encoding utf8`

### Q1: 主动刷新一直显示处理中
- 先查 `GET /api/warnings/trigger/status` 是否 `running=true`。  
- 若长时间不结束，检查 `last_error` 字段与后端日志。  
- 系统有超时收敛逻辑，超时后应自动回收运行态。  
- 若使用 `uvicorn --reload` 开发（本项目 docker compose 默认开启），后端热重载可能在任务运行中重启进程，导致 Redis 锁“残留”从而出现“已有任务在运行/已处理 0 个地区”的假象。当前版本已加入心跳与自动清理机制，一般会在下一次触发时自动修复；必要时可调用 `POST /api/warnings/trigger/reset` 强制清理。

### Q2: 地图风险和实时预警数量看起来不一致
- 实时弹窗列表只显示 `orange/red`。  
- 地图展示全部风险等级（含 `green/yellow`）。  

### Q3: 全部地区风险过低且数据相同
- 通常是数据源未命中（API key 无效、模板错误、域名未放行）导致回退值偏多。  
- 优先检查 `debug/last-collection` 的 `source_status`。  

### Q4: `weather_scraper_error` 大量出现 `http_status_403`    
- 先确认未走代理/VPN，且出口 IP 未被目标站点风控。  
- 如果已经出现“浏览器也上不去”的情况，通常是站点临时封控，需要等待自动解除（常见为几分钟到数小时）。  
- 为了降低再次被封风险，建议先用更保守配置：  
  - `SCRAPER_MAX_PARALLEL_REQUESTS=1`（先临时关闭并行）  
  - `SCRAPER_REQUEST_INTERVAL_SECONDS=3.0 ~ 4.0`  
  - `SCRAPER_MAX_RETRIES=1`（减少失败重试对站点的压力）  
- 调用 `POST /api/warnings/debug/reset-scraper-runtime?clear_cache=true` 清理预算与缓存后再测。    
- 若仍持续 403：   
  - 先排除 slug 映射/误命中问题（运行 `tools/build-tianqi-overrides.ps1`，并检查 403 的 URL 是否出现明显不匹配的 slug，例如 `汕尾 -> maweiqu` 这类）。   
  - 再考虑以官方气象 API 作为主源，爬虫仅做补充。    
说明：少量 `http_status_403` 属于站点策略噪声是可能的；系统会优先使用已缓存/其他数据源结果，避免因单点失败把整张地图覆盖成全绿。   
补充：若看到“一批不同地区反复请求同一个 URL（例如海南多个市县都变成同一个页面）”，通常是抓取目标被折叠到同一个 `target_code` 导致缓存/并发去重复用；当前版本已避免在“推导出的 city_code 不存在”时折叠，减少这种现象。  
 
### Q5: `docker compose up --build` 有时很快、有时卡很久 
- 首次构建或依赖更新时，Docker 需要重新下载并安装 Python/Node 依赖，网络波动会显著影响耗时。 
- 若只是代码变更且依赖未变，后续构建通常会命中缓存并明显加快。 
 
### Q6: `weather_scraper_error` 出现 `ConnectError` 
- 这通常表示容器内无法建立到目标站点的网络连接（DNS 解析失败、代理环境变量干扰、TLS 握手失败等），并非 HTTP 403。 
- 先看 `failed-weather-scraper.json` 里的 `message`（已包含异常类型与可能的底层原因）。 
- 若本机浏览器能访问但容器不行，优先检查：是否开启了代理/VPN、Docker/WSL 的网络与 DNS 配置、以及环境变量 `HTTP_PROXY/HTTPS_PROXY`（项目代码已默认忽略容器环境代理）。

### Q7: `tools/build-tianqi-overrides.ps1` 生成 0 条  
- 仓库里的快照页面 URL 可能被换行/空格打断（例如 `<https://www.tianqi.com/ \n wuyuan1/>`）。脚本已内置归一化逻辑；若仍为 0：请显式传入 `-InputPath` 指向快照文件，并确认该文件包含大量 `tianqi.com/<slug>/` 链接。  

### Q8: “历史主动预警次数 X 次”为什么会一直累计？
- 当前版本里，“历史主动预警次数”来自数据库 `warnings` 表：统计该地区近 N 年（默认 10 年）内的**系统预警记录次数**（仅 yellow/orange/red，且排除了测试预警）。因此在频繁测试/手动刷新、或启用定时任务时会逐步增长。  
- 后续接入官方历史灾情/地质灾害 API 后，可把该指标替换为“真实历史灾害事件次数/强度”。  
- 开发/测试阶段如果想清空累计，可参考下一节“清理测试数据”。  

## 14. 清理测试数据（破坏性，可选）
用途：
- 清空历史测试产生的预警记录，避免影响“历史事件”代理指标与后续判断。  
注意：
- 这是**破坏性操作**，会删除数据库中的预警历史（仅建议开发/演示环境使用）。  

### 14.1 清空所有预警（推荐：彻底重置）
```powershell
docker compose exec -T postgres psql -U ghws_user -d ghws -c "TRUNCATE TABLE warnings RESTART IDENTITY;"
```

### 14.2 仅删除“测试/演示/手动”相关预警（更保守）
```powershell
docker compose exec -T postgres psql -U ghws_user -d ghws -c "DELETE FROM warnings WHERE lower(coalesce(source,'')) LIKE '%test%' OR lower(coalesce(source,'')) LIKE '%mock%' OR lower(coalesce(source,'')) LIKE '%demo%' OR lower(coalesce(source,'')) LIKE '%manual%' OR lower(coalesce(source,'')) LIKE '%sample%' OR coalesce(reason,'') LIKE '%测试%' OR coalesce(reason,'') LIKE '%演示%' OR coalesce(reason,'') LIKE '%杭州橙色%';"
```

删除后可选验证（看剩余预警条数）：
```powershell
docker compose exec -T postgres psql -U ghws_user -d ghws -c "SELECT COUNT(*) FROM warnings;"
```

### 14.3 删除后立刻刷新运行态/缓存（避免读到旧快照）
```powershell
Invoke-RestMethod -Method Post "http://localhost/api/warnings/trigger/reset" | Out-Null
Invoke-RestMethod -Method Post "http://localhost/api/warnings/debug/reset-scraper-runtime?clear_cache=true" | Out-Null
```
