# app/config.py
import os

# where to store uploaded / processed data inside the container

DATA_RAW_DIR = os.getenv("DQ_DATA_RAW_DIR", "data/raw")
DATA_PARQUET_DIR = os.getenv("DQ_DATA_PARQUET_DIR", "data/parquet")

# Profilings limits 
MAX_ROWS_PROFILE = int(os.getenv("DQ_MAX_ROWS_PROFILE", "100000"))
MAX_TOP_K = int(os.getenv("DQ_MAX_TOP_K", "5"))

# Database URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://dq_user:dq_password@localhost:5432/dq_db",
)