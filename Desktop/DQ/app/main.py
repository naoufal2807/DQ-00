# app/main.py
import os
import shutil
from typing import Dict, Any

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.config import DATA_RAW_DIR, DATA_PARQUET_DIR
from app.db import get_db
from app.llm import llm_chat
from app.models import Dataset, DatasetColumn, ColumnProfile
from app.dq_profiler import profile_dataset

app = FastAPI(title="DQ Backend", version="0.1.0")

# basic CORS for local dev (relax later if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(DATA_RAW_DIR, exist_ok=True)
os.makedirs(DATA_PARQUET_DIR, exist_ok=True)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/datasets/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    # TODO: later use real user auth
    user_id = None

    if not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded")

    filename = file.filename
    ext = os.path.splitext(filename)[1].lower()

    if ext not in [".csv", ".parquet", ".pq"]:
        raise HTTPException(
            status_code=400,
            detail="Only CSV and Parquet files are supported for now",
        )

    # Save raw file
    raw_path = os.path.join(DATA_RAW_DIR, filename)
    with open(raw_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # For now: if parquet, move to parquet dir; else keep CSV
    if ext in [".parquet", ".pq"]:
        stored_path = os.path.join(DATA_PARQUET_DIR, filename)
        shutil.move(raw_path, stored_path)
    else:
        stored_path = raw_path

    # Profile using Polars
    try:
        profile = profile_dataset(stored_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Profiling failed: {e}")

    dataset = Dataset(
        user_id=user_id,
        name=filename,
        stored_path=stored_path,
        row_count=profile["row_count"],
        column_count=profile["column_count"],
    )
    db.add(dataset)
    db.flush()  # to get dataset.id

    # Insert columns + profiles
    for col_name, col_profile in profile["columns"].items():
        col = DatasetColumn(
            dataset_id=dataset.id,
            name=col_name,
            dtype=col_profile["dtype"],
            position=col_profile["position"],
            role=None,
        )
        db.add(col)

        payload: Dict[str, Any] = {
            "sample_values": col_profile.get("sample_values"),
            "numeric_stats": col_profile.get("numeric_stats"),
            "categorical_stats": col_profile.get("categorical_stats"),
            "datetime_stats": col_profile.get("datetime_stats"),
        }

        prof = ColumnProfile(
            dataset_id=dataset.id,
            column_name=col_name,
            completeness=col_profile["completeness"],
            non_null_count=col_profile["non_null_count"],
            null_count=col_profile["null_count"],
            distinct_count=col_profile["distinct_count"],
            uniqueness=col_profile["uniqueness"],
            metric_payload=payload,
        )
        db.add(prof)

    db.commit()

    return {
        "dataset_id": dataset.id,
        "name": dataset.name,
        "row_count": dataset.row_count,
        "column_count": dataset.column_count,
    }


@app.get("/datasets/{dataset_id}/profile")
def get_dataset_profile(
    dataset_id: str,
    db: Session = Depends(get_db),
):
    dataset: Dataset | None = (
        db.query(Dataset)
        .filter(Dataset.id == dataset_id)
        .first()
    )
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    profiles = (
        db.query(ColumnProfile)
        .filter(ColumnProfile.dataset_id == dataset_id)
        .all()
    )

    result = {
        "dataset_id": dataset.id,
        "name": dataset.name,
        "stored_path": dataset.stored_path,
        "row_count": dataset.row_count,
        "column_count": dataset.column_count,
        "columns": {},
    }

    for prof in profiles:
        base = {
            "completeness": prof.completeness,
            "non_null_count": prof.non_null_count,
            "null_count": prof.null_count,
            "distinct_count": prof.distinct_count,
            "uniqueness": prof.uniqueness,
        }
        # merge in metric_payload (sample_values, stats)
        if prof.metric_payload:
            base.update(prof.metric_payload)
        result["columns"][prof.column_name] = base

    return result


@app.post("/chat")
def chat(payload: Dict[str, Any]):
    """
    Simple generic chat endpoint to test OpenRouter wiring.
    Body:
    {
        "message":"Hello from DQ!"
    }
    """
    user_message = payload.get("message")
    if not user_message:
        raise HTTPException(status_code=400, detail="Field 'message' is required")
    
    reply = llm_chat(
        [
            {
                "role": "system",
                "content":"You are DQ, a friendly AI assistant integrated into a data quality platform"
                
            },
            {
                "role": "user", "content": user_message
            },
            
        ]
    )
    return {"reply": reply}

@app.post("/datasets/{dataset_id}/insights")
def dataset_insights(
    dataset_id: str,
    db: Session = Depends(get_db),
):
    """
    Use the stored data quality profile + OpenRouter to generate
    human-readable insights on the dataset.
    """
    dataset: Dataset | None = (
        db.query(Dataset)
        .filter(Dataset.id == dataset_id)
        .first()
    )
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    profiles = (
        db.query(ColumnProfile)
        .filter(ColumnProfile.dataset_id == dataset_id)
        .all()
    )

    # Build a compact dict of profiles to send to the model
    profile_dict: Dict[str, Any] = {}
    for p in profiles:
        base = {
            "completeness": p.completeness,
            "non_null_count": p.non_null_count,
            "null_count": p.null_count,
            "distinct_count": p.distinct_count,
            "uniqueness": p.uniqueness,
        }
        if p.metric_payload:
            base.update(p.metric_payload)
        profile_dict[p.column_name] = base

    prompt = f"""
You are an expert in data quality and analytics. You are helping a user understand
the quality of a dataset.

Dataset name: {dataset.name}
Row count: {dataset.row_count}

Here are the column profiles (JSON-like):
{profile_dict}

Please do the following:
1) List the top 5 most important data quality issues you see.
2) For each issue, briefly explain why it matters.
3) Suggest concrete actions or transformations to improve the dataset.
4) If the dataset looks generally healthy, mention that too.

Be concise but clear. Use bullet points.
"""

    reply = llm_chat(
        [
            {
                "role": "system",
                "content": "You are DQ, an AI assistant specialized in data quality diagnostics and remediation.",
            },
            {"role": "user", "content": prompt},
        ]
    )

    return {
        "dataset_id": dataset_id,
        "name": dataset.name,
        "insights": reply,
    }
