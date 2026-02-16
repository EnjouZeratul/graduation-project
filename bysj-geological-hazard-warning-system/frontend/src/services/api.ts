import axios from "axios";

const api = axios.create({
  baseURL: "/api"
});

export interface Region {
  id: number;
  name: string;
  code: string;
  risk_level: string;
  longitude?: number | null;
  latitude?: number | null;
}

export interface WarningItem {
  id: number;
  region_id: number;
  level: string;
  reason?: string | null;
  meteorology?: string | null;
  confidence?: number | null;
  created_at: string;
  source?: string | null;
  region_name?: string | null;
}

export interface RegionDetail extends Region {
  latest_warning?: WarningItem | null;
}

export interface TriggerWarningResponseItem {
  region_name: string;
  region_code: string;
  level: string;
  reason: string;
  confidence: number;
  meteorology: Record<string, any>;
}

export interface TriggerWarningResponse {
  timestamp: string;
  processed_regions: number;
  results: TriggerWarningResponseItem[];
}

export interface TriggerWarningAsyncResponse {
  accepted: boolean;
  running: boolean;
  message: string;
  started_at?: string | null;
  request_id?: string | null;
}

export interface TriggerWorkflowStatus {
  running: boolean;
  current_request_id?: string | null;
  current_started_at?: string | null;
  last_started_at?: string | null;
  last_finished_at?: string | null;
  last_error?: string | null;
  last_trigger?: string | null;
  last_processed_regions?: number;
  last_timestamp?: string | null;
  total_regions?: number;
  selected_regions?: number;
  current_elapsed_seconds?: number;
}

export interface AbortWorkflowResponse {
  ok: boolean;
  running: boolean;
  message: string;
  request_id?: string | null;
}

export interface CleanupTestWarningsResponse {
  deleted_warnings: number;
  affected_regions: number;
  message: string;
}

export interface RegionChatHistoryItem {
  role: "user" | "assistant";
  content: string;
}

export interface RegionChatRequest {
  region_code: string;
  question: string;
  history?: RegionChatHistoryItem[];
}

export interface RegionChatResponse {
  region_code: string;
  answer: string;
  risk_level?: string | null;
  generated_at: string;
}

export async function fetchRegions(): Promise<{ items: RegionDetail[]; total: number }> {
  const res = await api.get("/regions");
  return res.data;
}

export async function fetchWarnings(): Promise<{ items: WarningItem[]; total: number }> {
  const res = await api.get("/warnings");
  return res.data;
}

export async function triggerWarningWorkflow(): Promise<TriggerWarningResponse> {
  const res = await api.post("/warnings/trigger");
  return res.data;
}

export async function triggerWarningWorkflowAsync(fastMode = false): Promise<TriggerWarningAsyncResponse> {
  const res = await api.post(`/warnings/trigger/async?fast_mode=${fastMode ? "true" : "false"}`);
  return res.data;
}

export async function fetchTriggerWorkflowStatus(): Promise<TriggerWorkflowStatus> {
  const res = await api.get("/warnings/trigger/status");
  return res.data;
}

export async function abortWarningWorkflow(): Promise<AbortWorkflowResponse> {
  const res = await api.post("/warnings/trigger/abort");
  return res.data;
}

export async function cleanupTestWarnings(): Promise<CleanupTestWarningsResponse> {
  const res = await api.post("/warnings/cleanup-test-data");
  return res.data;
}

export async function chatWithRegionAI(payload: RegionChatRequest): Promise<RegionChatResponse> {
  const res = await api.post("/warnings/chat", payload);
  return res.data;
}

export interface DebugRandomizeResponse {
  ok: boolean;
  message: string;
  timestamp: string;
  total_regions: number;
  results: TriggerWarningResponseItem[];
}

export async function debugRandomizeWarnings(): Promise<DebugRandomizeResponse> {
  const res = await api.post("/warnings/debug/randomize");
  return res.data;
}
