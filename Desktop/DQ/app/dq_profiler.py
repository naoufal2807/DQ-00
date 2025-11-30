# app/dq_profiler.py
import os
from typing import Any, Dict, List

import polars as pl

from app.config import MAX_ROWS_PROFILE, MAX_TOP_K


# ---- simple dtype helpers (no polars.datatypes helpers) ----

NUMERIC_DTYPES = {
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64",
}

def _is_numeric_dtype(dtype: pl.DataType) -> bool:
    return str(dtype) in NUMERIC_DTYPES


def _is_datetime_dtype(dtype: pl.DataType) -> bool:
    s = str(dtype)
    # e.g. "Date", "Datetime[μs]", "Datetime[ns]" etc.
    return s == "Date" or s.startswith("Datetime")


def _is_categorical_dtype(dtype: pl.DataType) -> bool:
    s = str(dtype)
    return s in ("Utf8", "Categorical")


# ---- loading & profiling helpers ----

def _load_lazy(path: str) -> pl.LazyFrame:
    lower = path.lower()
    if lower.endswith((".parquet", ".pq")):
        return pl.scan_parquet(path)
    if lower.endswith(".csv"):
        return pl.scan_csv(path, infer_schema_length=10_000)
    raise ValueError(f"Unsupported file type for {path}")


def _profile_numeric(series: pl.Series) -> Dict[str, Any]:
    if series.len() == 0:
        return {
            "min": None,
            "max": None,
            "mean": None,
            "std": None,
            "p25": None,
            "p50": None,
            "p75": None,
        }

    return {
        "min": float(series.min()),
        "max": float(series.max()),
        "mean": float(series.mean()),
        "std": float(series.std()) if series.len() > 1 else 0.0,
        "p25": float(series.quantile(0.25, interpolation="nearest")),
        "p50": float(series.quantile(0.50, interpolation="nearest")),
        "p75": float(series.quantile(0.75, interpolation="nearest")),
    }


def _profile_categorical(series: pl.Series, top_k: int = MAX_TOP_K) -> Dict[str, Any]:
    if series.len() == 0:
        return {"top_k": []}

    vc = (
        series
        .value_counts()
        .sort("count", descending=True)
        .head(top_k)
    )

    top_values = [
        {"value": row["values"], "count": int(row["count"])}
        for row in vc.iter_rows(named=True)
    ]
    return {"top_k": top_values}


def _sample_values(series: pl.Series, max_samples: int = 5) -> List[Any]:
    return (
        series
        .drop_nulls()
        .unique()
        .head(max_samples)
        .to_list()
    )


def profile_dataset(path: str) -> Dict[str, Any]:
    """
    Profile a dataset with Polars.

    Returns JSON-serializable dict:
    {
      "dataset_name": ...,
      "path": ...,
      "row_count": ...,
      "column_count": ...,
      "columns": {
        "<col_name>": { ... metrics ... }
      }
    }
    """
    lf = _load_lazy(path)
    lf_limited = lf.head(MAX_ROWS_PROFILE)

    row_count = lf_limited.select(pl.len()).collect().item()
    df = lf_limited.collect(streaming=True)

    dataset_profile: Dict[str, Any] = {
        "dataset_name": os.path.basename(path),
        "path": path,
        "row_count": int(row_count),
        "column_count": len(df.columns),
        "columns": {},
    }

    for idx, col in enumerate(df.columns):
        s = df[col]
        dtype = s.dtype

        non_null_count = row_count - s.null_count()
        null_count = s.null_count()
        completeness = float(non_null_count / row_count) if row_count > 0 else 0.0
        distinct_count = int(s.n_unique())
        uniqueness = (
            float(distinct_count / non_null_count)
            if non_null_count > 0 else 0.0
        )

        col_profile: Dict[str, Any] = {
            "position": idx,
            "dtype": str(dtype),
            "non_null_count": int(non_null_count),
            "null_count": int(null_count),
            "completeness": completeness,
            "distinct_count": distinct_count,
            "uniqueness": uniqueness,
            "sample_values": _sample_values(s),
        }

        if _is_numeric_dtype(dtype):
            col_profile["numeric_stats"] = _profile_numeric(s)
        elif _is_datetime_dtype(dtype):
            if non_null_count > 0:
                col_profile["datetime_stats"] = {
                    "min": str(s.min()),
                    "max": str(s.max()),
                }
            else:
                col_profile["datetime_stats"] = {"min": None, "max": None}
        elif _is_categorical_dtype(dtype):
            col_profile["categorical_stats"] = _profile_categorical(s)

        dataset_profile["columns"][col] = col_profile

    return dataset_profile
