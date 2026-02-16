# 毕业设计论文参考：地质灾害智能预警系统（FastAPI + LangGraph + Vue）

本文档是一个“可直接套用并填充”的论文结构参考，面向本项目的实际实现。可将其作为目录 + 写作要点 + 关键图表清单 + 与代码文件的对应关系。

> 声明：本项目及本文档仅用于毕业设计/学习研究，不得用于商业用途。涉及第三方数据源或网页抓取时，应遵守平台条款、robots 协议与相关法律法规。

---

## 摘要（参考写法）

地质灾害预警需要同时考虑气象触发条件、地质易发性、历史风险压力与空间关联影响，且要求结果可解释、可更新、可扩展。本文设计并实现了一套基于 FastAPI、LangGraph 与 Vue 的地质灾害智能预警系统，通过多源数据融合与分阶段决策实现对全国地区的风险分区展示；系统采用批次（batch）增量入库与 WebSocket 推送机制，实现刷新过程中的实时更新；并通过“本地计算为主、LLM 轻量复核为辅”的策略降低大模型调用成本。实验与运行结果表明，该系统能够在数据缺失与源不稳定条件下维持稳定输出，并提供置信度拆解与候选灾害类型提示，为研判与展示提供辅助支持。

关键词：地质灾害预警；多源数据融合；LangGraph；FastAPI；WebSocket；置信度；成本控制

---

## 第 1 章 绪论

### 1.1 研究背景与意义
- 地质灾害特点：突发性、强破坏性、空间异质性。
- 传统预警痛点：单一阈值/单一数据源、解释困难、更新不及时、系统扩展成本高。
- 本课题意义：在工程可落地前提下，构建可解释、多源融合、可扩展、可视化的预警系统。

### 1.2 国内外研究现状（写作提示）
- 规则阈值法、统计学习法、物理机理模型、知识图谱与专家系统。
- 大模型在研判/摘要/问答中的应用与成本问题。
- 多源数据融合与数据质量评估方法。

### 1.3 研究内容与论文结构
- 系统总体方案与关键模块。
- 多源融合与工作流编排方法。
- 置信度计算与可解释输出。
- 增量推送与中止机制（工程实现）。
- 论文结构概述。

---

## 第 2 章 需求分析

### 2.1 业务需求
- 全国/多地区风险分区展示。
- 橙/红风险重点提示；低风险不打扰但可查询。
- 点击地区给出原因、置信度与候选灾害类型。
- 刷新机制：主动刷新、定时刷新、可中止、增量更新。

### 2.2 非功能需求
- 可用性：刷新过程可见进度与结果逐步更新。
- 可靠性：源缺失/失败不应导致全盘“变 0 / 变绿”。
- 成本：降低 LLM 调用频率与 tokens。
- 合规：爬虫白名单、政府域名阻断、限速与窗口预算。
- 可扩展：后续接入官方 API 时不需要大改动。

### 2.3 用例与原型（可放图）
- 用例图：浏览地图、查看详情、主动刷新/全量刷新/中止、AI 问答。
- 界面原型：地图 + 侧栏预警 + 地区详情面板。

---

## 第 3 章 系统总体设计

### 3.1 总体架构
- 前端：Vue + ECharts（地图渲染、交互、WS 监听、增量合并）。
- 后端：FastAPI（API/WS）、LangGraph（工作流）、Celery（定时任务与异步执行）。
- 存储：PostgreSQL（地区与预警记录），Redis（消息与锁）。

建议配图：
- 系统架构图（前端/后端/DB/Redis/Nginx/Celery）。
- 数据流图（触发刷新 -> 多源采集 -> 融合 -> 入库 -> WS 推送 -> 前端合并展示）。

### 3.2 模块划分与职责
- 数据源层：多源 fetch + normalize + 可靠度（`backend/app/agents/data_sources.py`）。
- 工作流层：数据采集、质量评估、本地风险、邻区影响、LLM 精修、决策输出（`backend/app/agents/graph.py`）。
- 业务接口层：主动刷新/状态/中止、调试接口、地区列表（`backend/app/routes/warnings.py`、`backend/app/routes/regions.py`）。
- 推送层：Redis Pub/Sub -> WebSocket 广播（`backend/app/websocket_manager.py`、`backend/app/main.py`）。
- 前端展示层：地图与列表、详情、问答（`frontend/src/App.vue`、`frontend/src/components/MapView.vue`）。

---

## 第 4 章 关键技术与方法

### 4.1 多源数据融合与质量评估
- 多源同时采集：官方气象/地质 API、爬虫辅助源、历史预警代理指标。
- 高德天气（AMap）补充：实况接口无 mm 降雨时，输出 `rain_24h_est/rain_1h_est` 并在质量备注中标注“降雨为估算”，仅在其他源缺失时提升为评分输入，避免覆盖真实 mm 数据。
- 统一字段规范：`rain_24h/rain_1h/soil_moisture/wind_speed/slope/fault_distance/...`
- 融合策略：数值按可靠度加权；非数值保留更可靠源的元信息。
- 质量评估：缺失/异常处理、质量分、质量备注与兜底策略。

### 4.2 分阶段决策工作流（LangGraph）
说明要点：
- 为什么用工作流：把“采集、清洗、评分、空间融合、LLM 复核、解释输出”拆成清晰可控的阶段。
- 节点设计：`data_collector`、`data_validation`、`local_risk`、`neighbor_influence`、`llm_refinement`、`decision_maker`。
- 变化检测：只对变化大的地区或低置信度地区进行 LLM 精修。

### 4.3 风险评分与预警等级
参考写法：
- 采用可解释的加权打分：降雨强度、短历时降雨、土壤含水、风速、坡度、断层距离、历史压力等。
- 预警等级映射：分段阈值（绿/黄/橙/红）。

### 4.4 置信度设计与可解释化
说明要点：
- 置信度不是单一概率，体现“数据质量、变化幅度、阈值距离、源覆盖、邻区修正、LLM 修正”等因素。
- 输出“置信度依据”与“计算方式拆解”，便于用户理解与复核。

### 4.5 实时更新：批次（batch）与 WebSocket 增量推送
- batch 的目的：边跑边出结果、可中止、超时保留已完成部分。
- 实现要点：每个 batch 完成就 commit 并 publish delta；前端按 `region_code` 合并避免缺失覆盖。
- 前端进度展示：展示“计划地区数/已处理地区数/运行秒数”，其中已处理数量按 batch 增长。

### 4.6 缓存持久化与稳定性增强（新增建议写点）
- 问题背景：容器重启会清空进程内缓存，导致冷启动时外网请求激增、风控概率上升。
- 方案：将 WU key 池、active key、地区级补充源缓存落到 Redis（带 TTL）。
- 效果：重启后仍可复用缓存，减少重复 key 探测与重复拉取。
- 对应实现：`backend/app/agents/data_sources.py`（Redis key 命名与读写逻辑）。

### 4.6 成本控制：本地优先 + LLM 轻量复核
- 大模型参与范围受控：只复核少量地区，且 max_tokens/temperature 受控。
- 强制刷新时可仅挑选最高风险/变化最大地区做复核。

### 4.7 合规爬虫与风险控制
- allowlist + 政府域名阻断 + 限速 + 预算窗口 + 缓存。
- URL collision 防护：避免多地区误映射同一页面导致串数据与封控。
- overrides 映射：对歧义地名使用“全称 key”避免简称误命中。

---

## 第 5 章 系统实现

本章建议用“模块实现 + 关键函数/参数”方式写清楚。

### 5.1 后端实现
- API 与路由：`backend/app/routes/warnings.py`、`backend/app/routes/regions.py`
- 工作流：`backend/app/agents/graph.py`
- 数据源：`backend/app/agents/data_sources.py`
- 推送：`backend/app/websocket_manager.py`
- 定时任务：`backend/app/celery_app.py`
- 配置：`backend/app/core/config.py`

### 5.2 前端实现
- 主页面：`frontend/src/App.vue`
- 地图：`frontend/src/components/MapView.vue`
- API/WS：`frontend/src/services/api.ts`、`frontend/src/services/ws.ts`

### 5.3 数据库与表结构
- `regions` 与 `warnings`（`backend/app/models.py`）
- 说明字段含义与“最新预警/历史预警”的关系。

建议配图：
- 关键页面截图（地图、详情、主动刷新状态、问答）。
- 关键 JSON 示例（某地区 meteorology 里包含的字段、source_status、confidence_breakdown、hazard_candidates）。

---

## 第 6 章 测试与评估

### 6.1 功能测试
- 主动刷新/全量刷新/中止是否正确。
- WS 增量推送是否生效。
- 缺失数据是否沿用上轮，避免变 0/全绿。
- overrides 更新后映射是否正确。
- 重启后缓存复用是否正确（验证 Redis 中 `ghws:cache:*` 键数量与 key_pool/active_key）。

### 6.2 性能与稳定性
- 刷新耗时：region_limit=20/100/全量。
- 403/封控概率与参数关系（间隔、并发、预算、缓存）。
- 超时策略：超时仍保留已处理结果。
- 冷启动与重启场景：对比“持久化前后”外网请求次数、任务耗时、403 比例。

### 6.3 成本评估
- LLM 精修地区数与 tokens 上限策略。
- “本地优先”的覆盖比例与效果。

---

## 第 7 章 总结与展望

### 7.1 工作总结
- 系统实现了多源融合、可解释预警、实时可视化、成本可控的工程化方案。

### 7.2 不足与改进方向
- 官方 API 接入后可提升权威性与覆盖字段。
- 邻区影响可以升级为更真实的空间邻接（边界相邻/流域/地形连通）。
- 数据变化检测可更精细，进一步降低 LLM 调用。

---

## 参考文献（写作提示）
- 地质灾害预警方法综述、降雨阈值研究、滑坡易发性评估。
- 多源数据融合与数据质量评估。
- LangGraph/Agent 工作流、WebSocket 实时系统。

---

## 附录 A：与代码文件的对应关系（可直接复制进论文）

- 工作流实现：`backend/app/agents/graph.py`
- 数据源与爬虫：`backend/app/agents/data_sources.py`
- 主动刷新/批次/推送：`backend/app/routes/warnings.py`
- WS 桥接：`backend/app/websocket_manager.py`、`backend/app/main.py`
- 定时刷新：`backend/app/celery_app.py`
- 前端核心：`frontend/src/App.vue`、`frontend/src/components/MapView.vue`
- 配置：`backend/app/core/config.py`、`.env`

---

## 附录 B：接入官方 API 后需要更新的文件清单（重要）

后续接入“官方气象/地质 API”后，建议同步更新以下文件与文档，保证一致性与可维护性：

### B.1 必改代码文件
1) `backend/app/agents/data_sources.py`
- `CMAWeatherDataSource.fetch()`：补齐真实请求、鉴权参数、错误码处理（注意 CMA 是站点接口，需要先完成 `region_code -> Station_Id_C` 映射）
- `CGSGeologyDataSource.fetch()`：同上
- `normalize()`：统一输出字段，确保 `graph.py` 的评分逻辑能直接使用

2) `backend/app/core/config.py`
- 补充/校正官方 API 的 `BASE_URL`、额外参数（如果需要）
- 明确 `.env` 示例字段

3) `backend/app/agents/graph.py`
- 若官方 API 提供新的强特征（如更精细的地质易发性、历史灾害事件），在 `_risk_score_from_data()` 与 `_infer_hazard_candidates()` 中扩展使用
- 在 UI 输出中遇到缺失字段时保持“缺失不展示”的策略

### B.2 可能需要更新的前端文件
1) `frontend/src/App.vue`
- 地区详情展示字段（新增/移除“永远缺失”的字段）
- 置信度依据文案（若引入新的质量项）

2) `frontend/src/services/api.ts`
- 如新增官方 API 状态检查/数据源状态接口，可补充类型与调用

### B.3 必更新文档文件
1) `readme.md`
- `.env` 示例：官方 key、base_url、是否启用爬虫的建议默认值
- “如何确认爬虫生效/官方源生效”的排错章节

2) `docs/TECHNICAL_DOC.md`
- 数据源层：新增字段、融合策略、错误码与重试策略
- 工作流层：评分逻辑/置信度拆解是否变化

3) `docs/PRODUCT_DOC.md`
- 产品能力描述：从“辅助/兜底”升级为“官方主源+辅助源”
- 合规说明：官方源优先后，爬虫默认策略可更保守

4) 本文件 `docs/THESIS_REFERENCE.md`
- 实验与评估章节：补充“官方 API 接入前后对比”
- 数据来源描述：从“模拟/爬虫”为主转为“官方权威数据”为主
