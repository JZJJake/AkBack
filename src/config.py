import os

# Base paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Create directories if they don't exist
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Data settings
COLUMN_MAPPING = {
    "日期": "date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "股票代码": "symbol",
    # Optional mappings for other common fields if present
    "振幅": "amplitude",
    "涨跌幅": "pct_chg",
    "涨跌额": "chg",
    "换手率": "turnover"
}

# Concurrency settings
MAX_WORKERS = 4  # Adjust based on system capabilities and API limits

# Trading Constants
COMMISSION_RATE = 0.0003  # 0.03%
MIN_COMMISSION = 5.0      # 5 RMB
STAMP_DUTY = 0.0005       # 0.05% (charged only on sells)
