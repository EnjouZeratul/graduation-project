from __future__ import annotations

import json

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import get_db_fastapi as get_db
from app.models import Region, Warning
from app.schemas import RegionDetail, RegionListResponse, WarningBase
from app.warning_filters import is_test_warning

router = APIRouter(prefix="/api/regions", tags=["regions"])


def _extract_confidence(meteorology: str | None) -> float | None:
    if not meteorology:
        return None
    try:
        parsed = json.loads(meteorology)
    except Exception:
        return None
    value = parsed.get("confidence")
    try:
        if value is None:
            return None
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return None


@router.get("", response_model=RegionListResponse)
def list_regions(db: Session = Depends(get_db)) -> RegionListResponse:
    regions = db.execute(select(Region).order_by(Region.id.asc())).scalars().all()
    items = []

    for region in regions:
        warning_candidates = (
            db.execute(
                select(Warning)
                .where(Warning.region_id == region.id)
                .order_by(Warning.created_at.desc())
                .limit(20)
            )
            .scalars()
            .all()
        )
        latest_warning = next(
            (
                w
                for w in warning_candidates
                if not is_test_warning(source=w.source, reason=w.reason)
            ),
            None,
        )

        region_data = RegionDetail.model_validate(region)
        if latest_warning:
            warning = WarningBase.model_validate(latest_warning)
            warning.region_name = region.name
            warning.confidence = _extract_confidence(latest_warning.meteorology)
            region_data.latest_warning = warning
        items.append(region_data)

    total = db.execute(select(func.count(Region.id))).scalar_one()
    return RegionListResponse(items=items, total=total)


@router.get("/{region_id}", response_model=RegionDetail)
def get_region_detail(region_id: int, db: Session = Depends(get_db)) -> RegionDetail:
    region: Region | None = db.get(Region, region_id)
    if region is None:
        raise HTTPException(status_code=404, detail="Region not found")

    warning_candidates = (
        db.execute(
            select(Warning)
            .where(Warning.region_id == region.id)
            .order_by(Warning.created_at.desc())
            .limit(20)
        )
        .scalars()
        .all()
    )
    latest_warning = next(
        (
            w
            for w in warning_candidates
            if not is_test_warning(source=w.source, reason=w.reason)
        ),
        None,
    )

    data = RegionDetail.model_validate(region)
    if latest_warning:
        warning = WarningBase.model_validate(latest_warning)
        warning.region_name = region.name
        warning.confidence = _extract_confidence(latest_warning.meteorology)
        data.latest_warning = warning
    return data


@router.post("/seed", response_model=dict)
async def seed_regions(db: Session = Depends(get_db)) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get("https://geo.datav.aliyun.com/areas_v3/bound/100000_full_city.json")
        geojson = response.json()

    count = 0
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        adcode = str(props.get("adcode", ""))
        name = props.get("name", "")

        if adcode and name and len(adcode) == 6:
            existing = db.execute(select(Region).where(Region.code == adcode)).scalar_one_or_none()
            if not existing:
                center = props.get("center", [0, 0])
                db.add(
                    Region(
                        name=name,
                        code=adcode,
                        risk_level="green",
                        longitude=center[0] if len(center) > 0 else None,
                        latitude=center[1] if len(center) > 1 else None,
                    )
                )
                count += 1

    db.commit()
    return {"message": f"成功导入 {count} 个新地区"}
