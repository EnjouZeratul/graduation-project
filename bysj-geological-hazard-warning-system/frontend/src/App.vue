<template>
  <div class="app-container">
    <header class="app-header">
      <h1>地质灾害智能预警系统</h1>
      <p class="subtitle">基于多智能体协同的地质灾害预警平台（FastAPI + LangGraph + Vue）</p>
    </header>

    <main class="app-main">
      <section class="map-section">
        <div class="manual-refresh-wrap">
          <button class="manual-refresh-btn" :disabled="refreshLoading" @click="handleManualRefresh">
            {{ refreshLoading ? "提交中..." : "主动刷新" }}
          </button>
          <button class="manual-refresh-btn secondary" :disabled="refreshLoading" @click="handleManualFullRefresh">
            {{ refreshLoading ? "提交中..." : "全量刷新" }}
                    </button>
          <button class="manual-refresh-btn tertiary" :disabled="refreshLoading || workflowRunning" @click="handleRandomSimulate">
            {{ refreshLoading ? "提交中..." : "随机模拟" }}
          </button>
          <button class="manual-refresh-btn danger" :disabled="abortLoading || !workflowRunning" @click="handleAbortRefresh">
            {{ abortLoading ? "处理中..." : "中止" }}
          </button>
          <span v-if="refreshHint" class="refresh-hint">{{ refreshHint }}</span>
        </div>
        <MapView
          v-if="regions.length > 0"
          :regions="regions"
          :selected-region="selectedRegion"
          @region-click="handleRegionClick"
        />
      </section>

      <aside class="sidebar">
        <div class="panel">
          <div class="panel-header">
            <h2>实时预警（自动弹出仅橙/红）</h2>
            <span class="update-time">最近更新时间：{{ formatDisplayTime(lastUpdateTime) }}</span>
          </div>
          <div class="panel-note">
            当前自动弹出地区：{{ autoPopupWarnings.length }} 个；地图展示全部风险等级。
          </div>
          <ul class="warning-list">
            <li v-for="item in autoPopupWarnings" :key="item.id" :class="['warning-item', `lvl-${item.level}`]">
              <div class="warning-main">
                <div class="warning-region">{{ item.region_name || "未知地区" }}</div>
                <div class="warning-level">{{ levelLabel(item.level) }}</div>
              </div>
              <div class="warning-meta">
                <span class="warning-confidence">置信度：{{ confidenceLabel(item.confidence) }}</span>
              </div>
              <div v-if="warningHazards(item).length > 0" class="warning-hazards">
                可能灾害：{{ warningHazards(item).join("、") }}
              </div>
              <div v-if="item.reason" class="warning-reason">{{ displayReason(item.reason) }}</div>
              <div class="warning-time">{{ formatDisplayTime(item.created_at) }}</div>
            </li>
            <li v-if="autoPopupWarnings.length === 0" class="warning-empty">
              当前无需要自动弹出的较高/高风险预警。
            </li>
          </ul>
        </div>

        <div class="panel detail-panel" v-if="selectedRegion">
          <h2>地区详情</h2>
          <p><strong>名称：</strong>{{ selectedRegion.name }}</p>
          <p>
            <strong>当前风险等级：</strong>
            {{ levelLabel(selectedRegion.latest_warning?.level || selectedRegion.risk_level) }}
          </p>
          <p>
            <strong>置信度：</strong>
            {{ confidenceLabel(selectedRegion.latest_warning?.confidence) }}
          </p>
          <p v-if="selectedRegion.latest_warning?.reason">
            <strong>最新预警原因：</strong>{{ displayReason(selectedRegion.latest_warning.reason) }}
          </p>
          <p v-if="selectedHazardCandidates.length > 0">
            <strong>可能灾害：</strong>{{ selectedHazardCandidates.join("、") }}
          </p>

          <div class="confidence-panel" v-if="selectedRegion.latest_warning">
            <h3>置信度依据</h3>
            <p v-if="selectedConfidenceReason">{{ selectedConfidenceReason }}</p>
            <p v-if="selectedConfidenceBreakdown.formula">
              <strong>计算方式：</strong>{{ selectedConfidenceBreakdown.formula }}
            </p>
            <p v-if="selectedConfidenceBreakdown.final_confidence !== undefined">
              <strong>最终置信度：</strong>{{ confidenceLabel(selectedConfidenceBreakdown.final_confidence) }}
            </p>
          </div>

          <div class="chat-panel">
            <h3>AI 问答</h3>
            <div class="chat-list">
              <div
                v-for="(item, index) in chatMessages"
                :key="`${item.role}-${index}`"
                :class="['chat-item', `chat-${item.role}`]"
              >
                <div class="chat-role">{{ item.role === "user" ? "你" : "AI" }}</div>
                <div class="chat-content">{{ item.content }}</div>
              </div>
              <div v-if="chatLoading" class="chat-loading">AI 正在思考...</div>
            </div>

            <div v-if="chatError" class="chat-error">{{ chatError }}</div>

            <div class="chat-input-row">
              <input
                v-model="chatInput"
                type="text"
                placeholder="针对当前地区提问"
                @keydown.enter.prevent="sendChatMessage"
              />
              <button :disabled="chatLoading || !chatInput.trim()" @click="sendChatMessage">
                {{ chatLoading ? "发送中..." : "发送" }}
              </button>
            </div>
          </div>
        </div>
      </aside>
    </main>
  </div>
</template>

<script lang="ts" setup>
import { computed, onMounted, ref, watch } from "vue";
import MapView from "./components/MapView.vue";
import {
  abortWarningWorkflow,
  chatWithRegionAI,
  debugRandomizeWarnings,
  fetchTriggerWorkflowStatus,
  fetchRegions,
  triggerWarningWorkflowAsync,
  type RegionChatHistoryItem,
  type RegionDetail,
  type WarningItem
} from "./services/api";
import { connectWarningsWS } from "./services/ws";

interface ChatMessage extends RegionChatHistoryItem {
  createdAt: string;
}

const regions = ref<RegionDetail[]>([]);
const selectedRegion = ref<RegionDetail | null>(null);
const lastUpdateTime = ref<string | null>(null);
const refreshLoading = ref(false);
const refreshHint = ref<string | null>(null);
const refreshWatchRunning = ref(false);
const workflowRunning = ref(false);
const abortLoading = ref(false);
const abortRequested = ref(false);

const chatMessages = ref<ChatMessage[]>([]);
const chatInput = ref("");
const chatLoading = ref(false);
const chatError = ref<string | null>(null);

const regionLatestWarnings = computed<WarningItem[]>(() => {
  const items: WarningItem[] = [];
  regions.value.forEach((region) => {
    const warning = region.latest_warning;
    if (!warning) return;
    items.push({
      ...warning,
      level: warning.level || region.risk_level,
      region_name: warning.region_name || region.name
    });
  });
  items.sort((a, b) => {
    const ta = new Date(a.created_at).getTime();
    const tb = new Date(b.created_at).getTime();
    return tb - ta;
  });
  return items;
});

const autoPopupWarnings = computed(() => {
  return regionLatestWarnings.value.filter((item) => item.level === "orange" || item.level === "red");
});

const selectedWarningMeta = computed<Record<string, any>>(() => {
  const raw = selectedRegion.value?.latest_warning?.meteorology;
  if (!raw) return {};
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch {
      return {};
    }
  }
  return {};
});

const selectedHazardCandidates = computed<string[]>(() => {
  const value = selectedWarningMeta.value?.hazard_candidates;
  const fromMeta = Array.isArray(value) ? value.map((v) => String(v)).filter(Boolean) : [];
  if (fromMeta.length > 0) return fromMeta;
  // Fallback: hazard candidates may only exist in `reason` when WS delta overwrote/omitted meteorology fields.
  return extractHazardCandidatesFromReason(String(selectedRegion.value?.latest_warning?.reason || ""));
});

const selectedConfidenceReason = computed<string>(() => {
  return String(selectedWarningMeta.value?.confidence_reason || "");
});

const selectedConfidenceBreakdown = computed<Record<string, any>>(() => {
  const value = selectedWarningMeta.value?.confidence_breakdown;
  return value && typeof value === "object" ? value : {};
});

function levelLabel(level: string) {
  switch (level) {
    case "green":
      return "低风险";
    case "yellow":
      return "较低风险";
    case "orange":
      return "较高风险";
    case "red":
      return "高风险";
    default:
      return level;
  }
}

function confidenceLabel(value: number | null | undefined) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) return "未知";
  const pct = Math.round(Number(value) * 1000) / 10;
  return pct % 1 === 0 ? `${pct.toFixed(0)}%` : `${pct.toFixed(1)}%`;
}

function parseBackendTime(value: string | null | undefined): Date | null {
  if (!value) return null;
  const text = String(value).trim();
  if (!text) return null;

  // Backend may return naive ISO timestamp; treat it as UTC to avoid local-time drift.
  const normalized = /([zZ]|[+-]\d{2}:\d{2})$/.test(text) ? text : `${text}Z`;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function formatDisplayTime(value: string | null | undefined) {
  const parsed = parseBackendTime(value);
  if (!parsed) return "暂无";
  return parsed.toLocaleString("zh-CN", { hour12: false });
}

function buildChatIntro(region: RegionDetail): string {
  const level = region.latest_warning?.level || region.risk_level || "green";
  const reason = region.latest_warning?.reason;
  const conf = confidenceLabel(region.latest_warning?.confidence);
  if (reason) {
    return `当前风险等级：${levelLabel(level)}，置信度：${conf}。最新预警原因：${reason}`;
  }
  return `当前风险等级：${levelLabel(level)}，置信度：${conf}。你可以询问该地区的预警情况。`;
}

function resetChatForRegion(region: RegionDetail) {
  chatMessages.value = [
    {
      role: "assistant",
      content: buildChatIntro(region),
      createdAt: new Date().toISOString()
    }
  ];
  chatInput.value = "";
  chatError.value = null;
}

function handleRegionClick(regionCode: string) {
  const region = regions.value.find((r) => r.code === regionCode);
  if (region) selectedRegion.value = region;
}

function extractHazardCandidatesFromReason(reason: string): string[] {
  const text = String(reason || "");
  const marker = "最可能灾害：";
  const idx = text.lastIndexOf(marker);
  if (idx < 0) return [];
  const tail = text.slice(idx + marker.length);
  // Stop at common separators to avoid capturing trailing explanation.
  const cut = tail.split(/；|。|\n|\r|（|\(|\[/)[0] || "";
  const items = cut
    .split(/[、,，\s]+/)
    .map((s) => s.trim())
    .filter(Boolean);
  return Array.from(new Set(items)).slice(0, 5);
}

function warningHazards(item: WarningItem): string[] {
  const fromReason = extractHazardCandidatesFromReason(String(item?.reason || ""));
  if (fromReason.length > 0) return fromReason;

  const met = safeParseJsonObject(item?.meteorology);
  const candidates = met?.hazard_candidates;
  return Array.isArray(candidates) ? candidates.map((v: any) => String(v)).filter(Boolean) : [];
}

function displayReason(reason: string | null | undefined): string {
  let text = String(reason || "").trim();
  if (!text) return "";

  // Hide noisy internal summaries from user-facing UI.
  text = text.replace(/；?存在\\d+个关键数据源错误/g, "");
  // Remove duplicated hazard summary because UI already shows hazard tags explicitly.
  text = text.replace(/；?最可能灾害：[^；。\\n\\r]*/g, "");
  // If LLM append is English-only (legacy data), drop it to keep UI Chinese.
  text = text.replace(/；?LLM复核：([^；。\\n\\r]*)/g, (_m, p1) => {
    const seg = String(p1 || "").trim();
    if (!seg) return "";
    const hasCjk = /[\\u4e00-\\u9fff]/.test(seg);
    if (hasCjk) return `；LLM复核：${seg}`;
    return "";
  });

  text = text.replace(/；{2,}/g, "；").replace(/^；+/, "").replace(/；+$/, "").trim();
  return text;
}

function safeParseJsonObject(value: any): Record<string, any> {
  if (!value) return {};
  if (typeof value === "object") return value as Record<string, any>;
  if (typeof value !== "string") return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? (parsed as Record<string, any>) : {};
  } catch {
    return {};
  }
}

function mergeMeteorology(prevRaw: any, incomingRaw: any): Record<string, any> {
  const prev = safeParseJsonObject(prevRaw);
  const incoming = safeParseJsonObject(incomingRaw);

  const merged: Record<string, any> = { ...prev, ...incoming };

  // Preserve nested objects (shallow merge) so delta payloads don't wipe existing detail.
  if (prev.source_status && typeof prev.source_status === "object") {
    merged.source_status = { ...(prev.source_status || {}), ...(incoming.source_status || {}) };
  }
  if (prev.confidence_breakdown && typeof prev.confidence_breakdown === "object") {
    merged.confidence_breakdown = { ...(prev.confidence_breakdown || {}), ...(incoming.confidence_breakdown || {}) };
  }

  // Preserve hazard candidates when delta does not include them.
  if (Array.isArray(prev.hazard_candidates) && !Array.isArray(incoming.hazard_candidates)) {
    merged.hazard_candidates = prev.hazard_candidates;
  }
  if (Array.isArray(prev.hazard_candidates) && Array.isArray(incoming.hazard_candidates) && incoming.hazard_candidates.length === 0) {
    merged.hazard_candidates = prev.hazard_candidates;
  }

  return merged;
}

function normalizeRealtimePayload(raw: any) {
  const timestamp = raw?.timestamp || new Date().toISOString();
  const results = Array.isArray(raw?.results) ? raw.results : [];
  return { timestamp, results };
}

function updateLastUpdateTimeFromRegions() {
  const latestTs = regions.value
    .map((region) => region.latest_warning?.created_at)
    .filter((v): v is string => Boolean(v))
    .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())[0];
  if (latestTs) {
    lastUpdateTime.value = latestTs;
  }
}

function applyRealtimeWarnings(raw: any) {
  const { timestamp, results } = normalizeRealtimePayload(raw);
  if (results.length === 0) return;

  lastUpdateTime.value = timestamp;

  const warningByCode = new Map<string, any>();
  results.forEach((item: any) => {
    if (item?.region_code) warningByCode.set(String(item.region_code), item);
  });

  regions.value = regions.value.map((region) => {
    const incoming = warningByCode.get(String(region.code));
    if (!incoming) return region;
    const mergedMeteorology = mergeMeteorology(region.latest_warning?.meteorology, incoming.meteorology || {});
    return {
      ...region,
      risk_level: incoming.level || region.risk_level,
      latest_warning: {
        id: region.latest_warning?.id ?? 0,
        region_id: incoming.region_id ?? region.id,
        level: incoming.level || region.latest_warning?.level || region.risk_level,
        reason: incoming.reason || region.latest_warning?.reason || null,
        confidence: incoming.confidence ?? incoming?.meteorology?.confidence ?? null,
        meteorology: JSON.stringify(mergedMeteorology || {}),
        created_at: timestamp,
        source: "langgraph-hybrid",
        region_name: incoming.region_name || region.name
      }
    };
  });

  if (selectedRegion.value) {
    const latest = regions.value.find((item) => item.code === selectedRegion.value?.code);
    if (latest) selectedRegion.value = latest;
  }
}

async function handleManualRefresh() {
  if (refreshLoading.value) return;
  refreshLoading.value = true;
  refreshHint.value = null;
  try {
    // Default to fast mode to reduce scraper pressure and lower 403 risk.
    const trigger = await triggerWarningWorkflowAsync(true);
    refreshHint.value = trigger.message || "已提交主动刷新任务。";

    if (!refreshWatchRunning.value) {
      void watchRefreshTask(trigger.request_id || null);
    }
  } catch {
    refreshHint.value = "主动刷新提交失败，请稍后重试。";
  } finally {
    refreshLoading.value = false;
  }
}

async function handleManualFullRefresh() {
  if (refreshLoading.value) return;
  refreshLoading.value = true;
  refreshHint.value = null;
  try {
    const trigger = await triggerWarningWorkflowAsync(false);
    refreshHint.value = trigger.message || "已提交全量刷新任务。";
    if (!refreshWatchRunning.value) {
      void watchRefreshTask(trigger.request_id || null);
    }
  } catch {
    refreshHint.value = "全量刷新提交失败，请稍后重试。";
  } finally {
    refreshLoading.value = false;
  }
}


async function handleRandomSimulate() {
  if (refreshLoading.value || workflowRunning.value) return;
  refreshLoading.value = true;
  refreshHint.value = null;
  try {
    const resp = await debugRandomizeWarnings();
    if (!resp.ok) {
      refreshHint.value = resp.message || '随机模拟失败。';
      return;
    }
    refreshHint.value = resp.message || '已推送随机模拟数据（不入库）。';
    applyRealtimeWarnings({ timestamp: resp.timestamp, results: resp.results });
  } catch {
    refreshHint.value = '随机模拟失败，请稍后重试。';
  } finally {
    refreshLoading.value = false;
  }
}
async function syncSnapshotAfterRefresh() {
  const regionRes = await fetchRegions();
  const prevByCode = new Map<string, RegionDetail>(regions.value.map((r) => [String(r.code), r]));
  regions.value = regionRes.items.map((incoming) => {
    const prev = prevByCode.get(String(incoming.code));
    if (!prev) return incoming;

    // Fast refresh may only process part of regions. Keep previous warning/risk for untouched regions.
    const incomingTs = parseBackendTime(incoming.latest_warning?.created_at)?.getTime() || 0;
    const prevTs = parseBackendTime(prev.latest_warning?.created_at)?.getTime() || 0;
    const hasIncomingWarning = Boolean(incoming.latest_warning);
    const hasPrevWarning = Boolean(prev.latest_warning);

    if (!hasIncomingWarning && hasPrevWarning) {
      return {
        ...incoming,
        risk_level: prev.risk_level || incoming.risk_level,
        latest_warning: prev.latest_warning
      };
    }

    if (hasIncomingWarning && hasPrevWarning && incomingTs < prevTs) {
      return {
        ...incoming,
        risk_level: prev.risk_level || incoming.risk_level,
        latest_warning: prev.latest_warning
      };
    }

    return incoming;
  });
  updateLastUpdateTimeFromRegions();
  if (selectedRegion.value) {
    const latest = regions.value.find((item) => item.code === selectedRegion.value?.code);
    if (latest) selectedRegion.value = latest;
  }
}

async function watchRefreshTask(requestId: string | null = null) {
  if (refreshWatchRunning.value) return;
  refreshWatchRunning.value = true;

  // Keep polling until the task really ends; avoid replacing progress hint with a generic message.
  const startedAt = Date.now();
  const maxPollingMs = 8 * 60 * 60 * 1000;
  try {
    while (Date.now() - startedAt < maxPollingMs) {
      await new Promise((resolve) => window.setTimeout(resolve, 2000));
      try {
        const status = await fetchTriggerWorkflowStatus();
        workflowRunning.value = Boolean(status.running);
        if (status.running) {
          const processed = Number(status.last_processed_regions || 0);
          const selected = Number(status.selected_regions || 0);
          const total = Number(status.total_regions || 0);
          const elapsed = Number(status.current_elapsed_seconds || 0);
          if (selected > 0) {
            const modeLabel = String(status.last_trigger || "").includes("manual_fast") ? "快速模式" : "全量模式";
            if (abortRequested.value) {
              refreshHint.value = `已请求中止，等待当前批次结束（${modeLabel} 本次计划 ${selected}/${total || selected} 个地区，已处理 ${processed} 个，已运行 ${elapsed}s）。`;
            } else {
              refreshHint.value = `主动刷新进行中：${modeLabel} 本次计划 ${selected}/${total || selected} 个地区，已处理 ${processed} 个，已运行 ${elapsed}s。`;
            }
          } else {
            refreshHint.value = `主动刷新进行中：已处理 ${processed} 个地区，已运行 ${elapsed}s。`;
          }
          if (requestId && status.current_request_id && status.current_request_id !== requestId) {
            refreshHint.value = "当前有其他主动刷新任务在执行，请稍后查看结果。";
          }
          continue;
        }

        if (status.last_error) {
          const err = String(status.last_error || "");
          const count = Number(status.last_processed_regions || 0);
          const selected = Number(status.selected_regions || 0);
          const total = Number(status.total_regions || 0);
          const modeLabel = String(status.last_trigger || "").includes("manual_fast") ? "快速模式" : "全量模式";
          if (err.startsWith("workflow_partial_timeout_after_")) {
            refreshHint.value =
              selected > 0
                ? `主动刷新达到时间上限（${modeLabel}），已处理 ${count} 个地区（本次计划 ${selected}/${total || selected}）。未处理地区将沿用上次数据。`
                : `主动刷新达到时间上限（${modeLabel}），已处理 ${count} 个地区。未处理地区将沿用上次数据。`;
          } else if (err === "manual_abort") {
            refreshHint.value =
              selected > 0
                ? `已中止主动刷新（${modeLabel}），已处理 ${count} 个地区（本次计划 ${selected}/${total || selected}）。已保留已处理结果，未处理地区沿用上次数据。`
                : `已中止主动刷新（${modeLabel}），已处理 ${count} 个地区。已保留已处理结果，未处理地区沿用上次数据。`;
          } else {
            refreshHint.value = `主动刷新结束，但出现异常：${err}`;
          }
        } else {
          const count = Number(status.last_processed_regions || 0);
          const selected = Number(status.selected_regions || 0);
          const total = Number(status.total_regions || 0);
          const modeLabel = String(status.last_trigger || "").includes("manual_fast") ? "快速模式" : "全量模式";
          refreshHint.value =
            selected > 0
              ? `主动刷新完成（${modeLabel}），已处理 ${count} 个地区（本次计划 ${selected}/${total || selected}）。`
              : `主动刷新完成，已处理 ${count} 个地区。`;
        }
        abortRequested.value = false;
        await syncSnapshotAfterRefresh();
        return;
      } catch {
        // ignore single poll failure and retry next cycle
      }
    }
    refreshHint.value = "主动刷新已运行较长时间，仍在后台执行中，页面会继续自动更新。";
  } finally {
    refreshWatchRunning.value = false;
    workflowRunning.value = false;
  }
}

async function handleAbortRefresh() {
  if (abortLoading.value) return;
  abortLoading.value = true;
  try {
    const resp = await abortWarningWorkflow();
    abortRequested.value = Boolean(resp.running);
    refreshHint.value = resp.message || "已请求中止。";
    if (!refreshWatchRunning.value) {
      void watchRefreshTask(resp.request_id || null);
    }
  } catch {
    refreshHint.value = "中止请求失败，请稍后重试。";
  } finally {
    abortLoading.value = false;
  }
}

async function sendChatMessage() {
  if (!selectedRegion.value || chatLoading.value) return;
  const question = chatInput.value.trim();
  if (!question) return;

  chatInput.value = "";
  chatError.value = null;
  chatMessages.value.push({ role: "user", content: question, createdAt: new Date().toISOString() });

  chatLoading.value = true;
  try {
    const history = chatMessages.value.slice(-8).map((item) => ({ role: item.role, content: item.content }));
    const result = await chatWithRegionAI({
      region_code: selectedRegion.value.code,
      question,
      history
    });
    chatMessages.value.push({
      role: "assistant",
      content: result.answer,
      createdAt: result.generated_at || new Date().toISOString()
    });
  } catch {
    chatError.value = "AI 服务暂时不可用。";
  } finally {
    chatLoading.value = false;
  }
}

watch(selectedRegion, (region) => {
  if (!region) {
    chatMessages.value = [];
    return;
  }
  resetChatForRegion(region);
});

onMounted(async () => {
  const regionRes = await fetchRegions();
  regions.value = regionRes.items;
  updateLastUpdateTimeFromRegions();

  connectWarningsWS((data) => {
    applyRealtimeWarnings(data);
  });

  // If a workflow is already running when the page loads, allow user to abort and keep status visible.
  try {
    const status = await fetchTriggerWorkflowStatus();
    workflowRunning.value = Boolean(status.running);
    if (status.running && !refreshWatchRunning.value) {
      void watchRefreshTask(status.current_request_id || null);
    }
  } catch {
    // ignore
  }
});
</script>

<style scoped>
.app-container {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  background:
    radial-gradient(1200px 600px at 8% -8%, rgba(90, 154, 181, 0.22), transparent 58%),
    radial-gradient(980px 520px at 92% 0%, rgba(222, 154, 118, 0.16), transparent 60%),
    linear-gradient(180deg, #0d1522 0%, #111a29 48%, #0b1420 100%);
  color: #e5e7eb;
}

.app-header {
  padding: 16px 32px;
  border-bottom: 1px solid rgba(148, 163, 184, 0.25);
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.app-header h1 {
  margin: 0;
  font-size: 22px;
}

.subtitle {
  margin: 0;
  font-size: 13px;
  color: #9ca3af;
}

.app-main {
  flex: 1;
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 16px;
  padding: 16px 24px 24px;
}

.map-section {
  position: relative;
  background:
    radial-gradient(760px 380px at 20% 0%, rgba(134, 173, 200, 0.18), transparent 62%),
    linear-gradient(155deg, #152437 0%, #1a2a40 54%, #132335 100%);
  border-radius: 16px;
  border: 1px solid rgba(165, 190, 210, 0.3);
  box-shadow: 0 16px 34px rgba(7, 13, 22, 0.45);
  padding: 12px;
}

.manual-refresh-wrap {
  position: absolute;
  left: 22px;
  top: 22px;
  z-index: 10;
  display: flex;
  align-items: center;
  gap: 8px;
}

.manual-refresh-btn {
  border: 1px solid rgba(16, 185, 129, 0.75);
  background: rgba(16, 185, 129, 0.22);
  color: #d1fae5;
  border-radius: 8px;
  padding: 8px 12px;
  font-size: 12px;
  cursor: pointer;
}

.manual-refresh-btn.secondary {
  border: 1px solid rgba(148, 163, 184, 0.6);
  background: rgba(148, 163, 184, 0.12);
  color: #e5e7eb;
}
.manual-refresh-btn.tertiary {
  border: 1px solid rgba(59, 130, 246, 0.7);
  background: rgba(59, 130, 246, 0.18);
  color: #dbeafe;
}


.manual-refresh-btn.danger {
  border: 1px solid rgba(244, 63, 94, 0.7);
  background: rgba(244, 63, 94, 0.14);
  color: #ffe4e6;
}

.manual-refresh-btn:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.refresh-hint {
  max-width: 360px;
  font-size: 12px;
  color: #cbd5e1;
  background: rgba(15, 23, 42, 0.78);
  border: 1px solid rgba(71, 85, 105, 0.9);
  border-radius: 8px;
  padding: 6px 8px;
}

.sidebar {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.panel {
  background: rgba(19, 30, 46, 0.92);
  border-radius: 16px;
  padding: 12px 14px;
  border: 1px solid rgba(129, 153, 173, 0.35);
  box-shadow: 0 12px 24px rgba(7, 13, 22, 0.36);
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 8px;
}

.panel-header h2 {
  margin: 0;
  font-size: 16px;
}

.update-time {
  font-size: 11px;
  color: #9ca3af;
}

.panel-note {
  margin-bottom: 8px;
  font-size: 12px;
  color: #9eb5c8;
}

.warning-list {
  list-style: none;
  margin: 0;
  padding: 0;
  max-height: 420px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.warning-item {
  padding: 6px 8px;
  border-radius: 10px;
  background: rgba(15, 23, 42, 0.9);
  border: 1px solid rgba(55, 65, 81, 0.9);
  font-size: 12px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.warning-main {
  display: flex;
  justify-content: space-between;
}

.warning-region {
  font-weight: 500;
}

.warning-level {
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 11px;
}

.warning-meta {
  font-size: 11px;
  color: #cbd5e1;
}

.warning-hazards {
  font-size: 11px;
  color: #dbeafe;
}

.warning-time {
  color: #9ca3af;
  font-size: 11px;
}

.warning-empty {
  padding: 8px;
  border-radius: 10px;
  border: 1px dashed rgba(148, 163, 184, 0.45);
  color: #9ca3af;
  font-size: 12px;
}

.warning-item.lvl-green {
  border-color: #8fc4a2;
}
.warning-item.lvl-green .warning-level {
  background: rgba(143, 196, 162, 0.2);
  color: #d9f2e4;
}
.warning-item.lvl-yellow {
  border-color: #d7c985;
}
.warning-item.lvl-yellow .warning-level {
  background: rgba(215, 201, 133, 0.2);
  color: #f4ebc4;
}
.warning-item.lvl-orange {
  border-color: #d99f74;
}
.warning-item.lvl-orange .warning-level {
  background: rgba(217, 159, 116, 0.22);
  color: #f4d8bf;
}
.warning-item.lvl-red {
  border-color: #ce8a86;
}
.warning-item.lvl-red .warning-level {
  background: rgba(206, 138, 134, 0.22);
  color: #f3d7d3;
}

.detail-panel h2 {
  margin-top: 0;
  margin-bottom: 8px;
  font-size: 15px;
}

.detail-panel p {
  margin: 4px 0;
  font-size: 13px;
}

.confidence-panel {
  margin-top: 10px;
  border-top: 1px solid rgba(148, 163, 184, 0.25);
  padding-top: 10px;
}

.confidence-panel h3 {
  margin: 0 0 6px;
  font-size: 14px;
}

.chat-panel {
  margin-top: 12px;
  border-top: 1px solid rgba(148, 163, 184, 0.25);
  padding-top: 10px;
}

.chat-panel h3 {
  margin: 0 0 8px;
  font-size: 14px;
}

.chat-list {
  max-height: 220px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding-right: 4px;
}

.chat-item {
  border-radius: 10px;
  padding: 8px;
  font-size: 12px;
  line-height: 1.4;
}

.chat-user {
  background: rgba(30, 41, 59, 0.95);
  border: 1px solid rgba(100, 116, 139, 0.7);
}

.chat-assistant {
  background: rgba(2, 132, 199, 0.16);
  border: 1px solid rgba(2, 132, 199, 0.45);
}

.chat-role {
  font-size: 11px;
  color: #93c5fd;
  margin-bottom: 4px;
}

.chat-content {
  color: #e5e7eb;
  white-space: pre-wrap;
}

.chat-loading {
  color: #9ca3af;
  font-size: 12px;
}

.chat-error {
  color: #fda4af;
  font-size: 12px;
  margin-top: 8px;
}

.chat-input-row {
  margin-top: 10px;
  display: flex;
  gap: 8px;
}

.chat-input-row input {
  flex: 1;
  border: 1px solid rgba(71, 85, 105, 0.9);
  background: rgba(2, 6, 23, 0.9);
  color: #e5e7eb;
  border-radius: 8px;
  padding: 8px;
  font-size: 12px;
}

.chat-input-row button {
  border: 1px solid rgba(2, 132, 199, 0.7);
  background: rgba(2, 132, 199, 0.2);
  color: #e5e7eb;
  border-radius: 8px;
  padding: 8px 12px;
  font-size: 12px;
  cursor: pointer;
}

.chat-input-row button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

@media (max-width: 960px) {
  .app-main {
    grid-template-columns: 1fr;
  }
}
</style>








