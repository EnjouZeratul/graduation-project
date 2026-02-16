from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class RegionBase(BaseModel):
    id: int
    name: str
    code: str
    parent_code: Optional[str] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    risk_level: str
    last_updated_at: datetime

    model_config = {"from_attributes": True}


class WarningBase(BaseModel):
    id: int
    region_id: int
    level: str = Field(description="预警等级：green/yellow/orange/red")
    reason: Optional[str] = None
    meteorology: Optional[str] = Field(default=None, description="气象信息 JSON 字符串")
    confidence: Optional[float] = Field(default=None, description="置信度 0-1")
    created_at: datetime
    source: str
    region_name: Optional[str] = None

    model_config = {"from_attributes": True}


class RegionDetail(RegionBase):
    latest_warning: Optional[WarningBase] = None


class WarningListResponse(BaseModel):
    items: List[WarningBase]
    total: int


class RegionListResponse(BaseModel):
    items: List[RegionDetail]
    total: int


class JiusiWorkflowInput(BaseModel):
    timestamp: datetime
    regions: List[str]


class JiusiWarningResult(BaseModel):
    region_name: str
    region_code: str
    level: str
    reason: str
    confidence: float = Field(default=0.5, ge=0, le=1)
    meteorology: dict


class JiusiWorkflowOutput(BaseModel):
    timestamp: datetime
    results: List[JiusiWarningResult]


class TriggerWorkflowResponse(BaseModel):
    timestamp: datetime
    processed_regions: int
    results: List[JiusiWarningResult]


class TriggerWorkflowAsyncResponse(BaseModel):
    accepted: bool
    running: bool
    message: str
    started_at: Optional[datetime] = None
    request_id: Optional[str] = None


class AbortWorkflowResponse(BaseModel):
    ok: bool
    running: bool
    message: str
    request_id: Optional[str] = None


class WorkflowStatusResponse(BaseModel):
    running: bool
    current_request_id: Optional[str] = None
    current_started_at: Optional[datetime] = None
    last_started_at: Optional[datetime] = None
    last_finished_at: Optional[datetime] = None
    last_error: Optional[str] = None
    last_trigger: Optional[str] = None
    last_processed_regions: int = 0
    last_timestamp: Optional[datetime] = None
    total_regions: int = 0
    selected_regions: int = 0
    current_elapsed_seconds: int = 0


class CleanupTestWarningsResponse(BaseModel):
    deleted_warnings: int
    affected_regions: int
    message: str


class DebugRandomizeResponse(BaseModel):
    ok: bool
    message: str
    timestamp: str
    total_regions: int
    results: List[dict]


class RegionChatHistoryItem(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=800)


class RegionChatRequest(BaseModel):
    region_code: str = Field(min_length=1, max_length=16)
    question: str = Field(min_length=1, max_length=500)
    history: List[RegionChatHistoryItem] = Field(default_factory=list)


class RegionChatResponse(BaseModel):
    region_code: str
    answer: str
    risk_level: Optional[str] = None
    generated_at: datetime

