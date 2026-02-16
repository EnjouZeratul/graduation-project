from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from app.db import Base


class Region(Base):
    __tablename__ = "regions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, index=True)
    code = Column(String(6), unique=True, index=True, nullable=False)
    parent_code = Column(String(6), index=True, nullable=True)


    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)

    risk_level = Column(String(16), nullable=False, default="green")
    last_updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    warnings = relationship("Warning", back_populates="region")


class Warning(Base):
    __tablename__ = "warnings"

    id = Column(Integer, primary_key=True, index=True)
    region_id = Column(Integer, ForeignKey("regions.id"), nullable=False, index=True)

    level = Column(String(16), nullable=False)  # green / yellow / orange / red
    reason = Column(Text, nullable=True)
    meteorology = Column(Text, nullable=True)  # JSON string of weather data

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    source = Column(String(64), nullable=False, default="jiusi")

    region = relationship("Region", back_populates="warnings")

