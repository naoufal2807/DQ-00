# app/models.py
import uuid
from datetime import datetime

from sqlalchemy import (
    Column,
    String,
    Integer,
    BigInteger,
    Float,
    ForeignKey,
    DateTime,
    JSON,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def gen_uuid() -> str:
    return str(uuid.uuid4())


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True, default=gen_uuid)
    user_id = Column(String, nullable=True)  # later: FK to users table
    name = Column(String, nullable=False)
    stored_path = Column(String, nullable=False)
    row_count = Column(BigInteger, nullable=True)
    column_count = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    columns = relationship(
        "DatasetColumn",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    profiles = relationship(
        "ColumnProfile",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )


class DatasetColumn(Base):
    __tablename__ = "dataset_columns"

    id = Column(String, primary_key=True, default=gen_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    name = Column(String, nullable=False)
    dtype = Column(String, nullable=False)
    position = Column(Integer, nullable=False)
    role = Column(String, nullable=True)  # e.g. "id", "target", "timestamp"
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    dataset = relationship("Dataset", back_populates="columns")


class ColumnProfile(Base):
    __tablename__ = "column_profiles"

    id = Column(String, primary_key=True, default=gen_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    column_name = Column(String, nullable=False)

    completeness = Column(Float, nullable=False)
    non_null_count = Column(BigInteger, nullable=False)
    null_count = Column(BigInteger, nullable=False)
    distinct_count = Column(BigInteger, nullable=False)
    uniqueness = Column(Float, nullable=False)

    metric_payload = Column(JSON, nullable=False)  # flexible stats payload
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    dataset = relationship("Dataset", back_populates="profiles")
