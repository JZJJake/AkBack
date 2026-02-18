import akshare as ak
import pandas as pd
import os
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime
from src.config import PROCESSED_DIR, LOG_DIR, COLUMN_MAPPING, MAX_WORKERS

# Setup logging
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "data_error.log"),
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class DataLoader:
    def __init__(self, data_dir=PROCESSED_DIR):
        self.data_dir = data_dir

    def get_all_stocks(self):
        """
        Get list of all A-share stocks.
        Returns a DataFrame with columns: ['symbol', 'name']
        """
        try:
            df = ak.stock_zh_a_spot_em()
            # Select relevant columns and rename
            # akshare usually returns: 序号, 代码, 名称, ...
            df = df[["代码", "名称"]].rename(columns={"代码": "symbol", "名称": "name"})
            return df
        except Exception as e:
            print(f"Error fetching stock list: {e}")
            logging.error(f"Error fetching stock list: {e}")
            return pd.DataFrame(columns=["symbol", "name"])

    def download_history(self, symbol, start_date, end_date):
        """
        Download historical data for a specific stock.
        start_date, end_date: strings in 'YYYYMMDD' format.
        Returns a DataFrame or None if failed.
        """
        try:
            # Random delay for rate limiting
            time.sleep(random.uniform(0.1, 0.5))

            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )

            if df is None or df.empty:
                return None

            # Rename columns
            df.rename(columns=COLUMN_MAPPING, inplace=True)

            # Keep only mapped columns + extra if any, but ensure we have at least date
            if "date" not in df.columns:
                 # Attempt to find date column if mapping failed or unexpected format
                 # Usually it is "日期" which is mapped to "date"
                 logging.error(f"{symbol}: 'date' column missing after renaming.")
                 return None

            # Convert to numeric
            numeric_cols = ["open", "close", "high", "low", "volume", "amount"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Drop rows with NaN in essential columns
            essential_cols = ["open", "close", "high", "low", "volume"]
            df.dropna(subset=[c for c in essential_cols if c in df.columns], inplace=True)

            # Ensure date is string or datetime?
            # User requirement: "numeric columns are stored as float or int"
            # Date is usually string 'YYYY-MM-DD' from akshare.
            # Convert to standard datetime or keep as string?
            # Parquet handles datetime well. Let's convert to datetime.
            df['date'] = pd.to_datetime(df['date'])

            return df

        except Exception as e:
            logging.error(f"Error downloading {symbol}: {e}")
            return None

    def load_stock_data(self, symbol):
        """
        Load stock data from local Parquet file.
        """
        file_path = os.path.join(self.data_dir, f"{symbol}.parquet")
        if not os.path.exists(file_path):
            return None
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
            return None

    def sync_stock(self, symbol):
        """
        Sync data for a single stock.
        Returns tuple (symbol, status_message)
        """
        file_path = os.path.join(self.data_dir, f"{symbol}.parquet")
        start_date = "19900101"
        end_date = datetime.now().strftime("%Y%m%d")

        existing_df = None

        if os.path.exists(file_path):
            try:
                existing_df = pd.read_parquet(file_path)
                if not existing_df.empty:
                    last_date = existing_df["date"].max()
                    # Start from next day
                    start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
            except Exception as e:
                logging.error(f"Error reading existing file for {symbol}, re-downloading all: {e}")

        # If start_date > end_date, it means we are up to date
        if start_date > end_date:
            return symbol, "Up to date"

        new_data = self.download_history(symbol, start_date, end_date)

        if new_data is None or new_data.empty:
             return symbol, "No new data"

        if existing_df is not None:
            # Concatenate
            combined_df = pd.concat([existing_df, new_data])
            # Drop duplicates by date
            combined_df.drop_duplicates(subset=["date"], keep="last", inplace=True)
            # Sort
            combined_df.sort_values("date", inplace=True)
        else:
            combined_df = new_data

        # Save to parquet
        try:
            combined_df.to_parquet(file_path, compression="snappy", index=False)
            return symbol, "Updated"
        except Exception as e:
            msg = f"Save failed: {e}"
            logging.error(f"{symbol}: {msg}")
            return symbol, msg

    def sync_all_data(self, max_workers=MAX_WORKERS):
        """
        Sync all stocks using concurrency.
        """
        stocks = self.get_all_stocks()
        if stocks.empty:
            print("No stocks found.")
            return

        symbols = stocks["symbol"].tolist()
        print(f"Starting sync for {len(symbols)} stocks with {max_workers} workers...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_symbol = {executor.submit(self.sync_stock, symbol): symbol for symbol in symbols}

            # Progress bar
            with tqdm(total=len(symbols), desc="Syncing Stocks") as pbar:
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        _, status = future.result()
                        # Optional: pbar.set_postfix_str(f"{symbol}: {status}")
                    except Exception as e:
                        logging.error(f"Unhandled exception for {symbol}: {e}")
                    finally:
                        pbar.update(1)

if __name__ == "__main__":
    # Test run
    loader = DataLoader()
    # print(loader.get_all_stocks().head())
    # loader.sync_stock("000001")
