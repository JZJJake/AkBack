import os
import pandas as pd
import requests
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime, timedelta
from src.config import PROCESSED_DIR, LOG_DIR

# Setup logging
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "data_enhancer.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

API_BASE_URL = "http://localhost:8080"
MAX_WORKERS = 4
REQUEST_DELAY = 1.0  # Seconds

class DataEnhancer:
    def __init__(self, data_dir=PROCESSED_DIR):
        self.data_dir = data_dir

    def get_all_stock_files(self):
        """Returns list of parquet files in data directory."""
        return [f for f in os.listdir(self.data_dir) if f.endswith(".parquet")]

    def fetch_tick_data(self, symbol, date_str):
        """
        Fetch tick data from tdx-api for a specific date.

        Args:
            symbol (str): Stock code (e.g., '000001')
            date_str (str): YYYYMMDD

        Returns:
            dict: {'outvol': int, 'invol': int} or None
        """
        url = f"{API_BASE_URL}/api/minute-trade-all"
        params = {"code": symbol, "date": date_str}

        try:
            time.sleep(REQUEST_DELAY) # Rate limiting
            response = requests.get(url, params=params, timeout=10)

            if response.status_code != 200:
                logging.warning(f"API Error {symbol} {date_str}: Status {response.status_code}")
                return None

            data = response.json()
            if data.get("code") != 0 or "data" not in data or "List" not in data["data"]:
                # Often means no data for this date (weekend/holiday or not listed)
                return None

            ticks = data["data"]["List"]
            if not ticks:
                return {'outvol': 0, 'invol': 0}

            # Process ticks
            # Status: 0=Buy (Out), 1=Sell (In), 2=Neutral
            outvol = 0
            invol = 0

            for tick in ticks:
                vol = tick.get("Volume", 0)
                status = tick.get("Status", 2)

                if status == 0:
                    outvol += vol
                elif status == 1:
                    invol += vol

            return {'outvol': outvol, 'invol': invol}

        except Exception as e:
            logging.error(f"Exception fetching {symbol} {date_str}: {e}")
            return None

    def enhance_stock(self, filename, days_limit=30):
        """
        Enhance a single stock file with OUTVOL/INVOL data.

        Args:
            filename (str): '000001.parquet'
            days_limit (int): Max number of recent days to enhance (to save time).
                              Set to None for full history.
        """
        symbol = filename.replace(".parquet", "")
        file_path = os.path.join(self.data_dir, filename)

        try:
            df = pd.read_parquet(file_path)

            # Ensure columns exist
            if 'outvol' not in df.columns:
                df['outvol'] = pd.NA
            if 'invol' not in df.columns:
                df['invol'] = pd.NA

            # Determine rows to update
            # Identify rows where 'outvol' is missing (NaN)
            # Also filter by days_limit if provided

            # Find indices where enhancement is needed
            missing_mask = df['outvol'].isna() | df['invol'].isna()

            if days_limit:
                # Only consider the last N days
                cutoff_date = df['date'].max() - timedelta(days=days_limit)
                date_mask = df['date'] > cutoff_date
                target_indices = df[missing_mask & date_mask].index
            else:
                target_indices = df[missing_mask].index

            if len(target_indices) == 0:
                return symbol, "Up to date"

            updated_count = 0

            # Iterate through rows needing update
            # Note: Modifying DataFrame in loop is slow, but safe for logic here.
            # Using index to update.
            for idx in target_indices:
                date = df.at[idx, 'date']
                date_str = date.strftime("%Y%m%d")

                tick_metrics = self.fetch_tick_data(symbol, date_str)

                if tick_metrics:
                    df.at[idx, 'outvol'] = tick_metrics['outvol']
                    df.at[idx, 'invol'] = tick_metrics['invol']
                    updated_count += 1
                else:
                    # Mark as 0 or leave NaN?
                    # If API returns valid response but empty list -> 0.
                    # If API fails/no data -> Leave NaN to retry later?
                    # Or assume 0 if market closed?
                    # Strategy logic needs numerical values.
                    # If we leave NaN, strategy will filter it out.
                    pass

            if updated_count > 0:
                # Save back to parquet
                # Convert object columns to numeric if needed (Int64 allows NaNs)
                df['outvol'] = pd.to_numeric(df['outvol'], errors='coerce')
                df['invol'] = pd.to_numeric(df['invol'], errors='coerce')
                df.to_parquet(file_path, compression='snappy', index=False)
                return symbol, f"Updated {updated_count} days"
            else:
                return symbol, "No data fetched"

        except Exception as e:
            msg = f"Enhancement failed: {e}"
            logging.error(f"{symbol}: {msg}")
            return symbol, msg

    def run_all(self, days_limit=30):
        """Run enhancement for all stocks."""
        files = self.get_all_stock_files()
        print(f"Starting Data Enhancement for {len(files)} stocks (limit={days_limit} days)...")
        print("Warning: Full history enhancement is slow; considering recent days only.")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {executor.submit(self.enhance_stock, f, days_limit): f for f in files}

            with tqdm(total=len(files), desc="Enhancing Data") as pbar:
                for future in as_completed(future_to_file):
                    symbol, status = future.result()
                    # Optional: pbar.set_postfix_str(f"{symbol}: {status}")
                    pbar.update(1)

if __name__ == "__main__":
    enhancer = DataEnhancer()
    enhancer.run_all(days_limit=5) # Default short run for testing
