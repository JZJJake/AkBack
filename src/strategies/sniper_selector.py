from src.strategies.selector import BaseSelector
import pandas as pd
import numpy as np
import akshare as ak
import logging
from src.config import PROCESSED_DIR
import os

class SniperSelector(BaseSelector):
    """
    Sniper Strategy Selector.

    Filters:
    - Market Cap < 35 Billion
    - Not ST
    - Price > 2

    Indicators:
    - ZDP = MA((OUTVOL - INVOL) / Volume, 3)
    - MAZDP = MA(ZDP, 6)
    - KDJ (9,3,3)

    Signal:
    - OK = A3 AND ZDPP AND (ZDP < -4)
    - OK2 = A3 AND ZDPP2 AND (ZDP < -4)
    - BUY = (OK AND KDJJ) OR (OK2 AND J > Ref(J,1))
    """
    def __init__(self, data_loader):
        self.data_loader = data_loader
        self.total_shares_map = {} # {symbol: total_shares}
        self.st_stocks = set()
        self._initialize_stock_info()

    def _initialize_stock_info(self):
        """Fetch total shares and ST status once."""
        try:
            print("Fetching stock info for market cap calculation...")
            # ak.stock_zh_a_spot_em() returns real-time data including total shares
            # Columns: 代码, 名称, 最新价, 总市值, 流通市值, ...
            # We need "总股本" (Total Shares).
            # Note: akshare columns might vary. Let's check typical output.
            # Usually: "代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅", "最高", "最低", "今开", "昨收", "量比", "换手率", "市盈率-动态", "市净率", "总市值", "流通市值", "涨速", "5分钟涨跌", "60日涨跌幅", "年初至今涨跌幅"
            # It seems "总股本" might not be directly in spot_em default columns, but "总市值" is.
            # Wait, user said: "On initialization, call ak.stock_zh_a_spot_em() once to get the latest total_shares... Map it: {symbol: shares}."
            # Actually, "总市值" (Total Market Cap) is Price * Total Shares.
            # So Total Shares = Total Market Cap / Price.
            # Or we can just use "总市值" directly if we trust the API's current value.
            # But we need historical market cap: Close * Fixed_Shares.
            # So we need Shares.

            df = ak.stock_zh_a_spot_em()

            # Filter columns
            # Rename for convenience
            # '总市值': 'total_mv' (in Yuan? or 100M? Usually Yuan)
            # '最新价': 'price'
            # '名称': 'name'
            # '代码': 'symbol'

            if '总市值' in df.columns and '最新价' in df.columns:
                # Calculate shares
                # Avoid division by zero
                df['shares'] = df.apply(lambda row: row['总市值'] / row['最新价'] if row['最新价'] > 0 else 0, axis=1)

                # Store in map
                self.total_shares_map = dict(zip(df['代码'], df['shares']))

            # ST stocks
            # Check if name contains 'ST'
            st_df = df[df['名称'].str.contains('ST', case=False, na=False)]
            self.st_stocks = set(st_df['代码'].tolist())

            print(f"Initialized {len(self.total_shares_map)} stocks. Found {len(self.st_stocks)} ST stocks.")

        except Exception as e:
            logging.error(f"Failed to fetch stock info: {e}")
            # Fallback?

    def _calculate_indicators(self, df):
        """
        Calculate technical indicators for a single stock dataframe.
        Returns df with added columns.
        """
        # Ensure sufficient data
        if len(df) < 30:
            return df

        # 1. ZDP
        # RAW_ZDP = (OUTVOL - INVOL) / Volume
        # Need to ensure outvol/invol are numeric and volume != 0
        # If outvol/invol are NaN, result is NaN.

        # Handle missing enhanced data
        if 'outvol' not in df.columns or 'invol' not in df.columns:
             df['zdp'] = np.nan
             df['mazdp'] = np.nan
        else:
            # Fill NaN with 0? Or propagate?
            # If we don't have tick data, ZDP cannot be calculated.
            # User strategy relies on ZDP. So these rows will be invalid.
            outvol = pd.to_numeric(df['outvol'], errors='coerce')
            invol = pd.to_numeric(df['invol'], errors='coerce')
            volume = pd.to_numeric(df['volume'], errors='coerce')

            # Avoid division by zero
            # RAW_ZDP is often multiplied by 100? User formula: (OUT-IN)/Vol.
            # This is a ratio -1 to 1.
            # But later: (ZDP < -4). This implies scaling?
            # Or maybe Volume is in Hands and OUT/IN are in Hands.
            # User formula: ZDP = MA(RAW, 3).
            # Condition: ZDP < -4.
            # A ratio of -1 to 1 can never be < -4.
            # IMPLIED: RAW_ZDP = (OUT - IN) / Vol * 100?
            # "RAW_ZDP = (df['outvol'] - df['invol']) / df['volume']" (User prompt).
            # "ZDP < -4" (User prompt).
            # This is mathematically impossible unless ZDP is scaled.
            # Standard "Capital Flow" indicators often scale by 100.
            # I will assume * 100.

            raw_zdp = (outvol - invol) / volume * 100.0

            df['zdp'] = raw_zdp.rolling(window=3).mean()
            df['mazdp'] = df['zdp'].rolling(window=6).mean()

        # 2. MA20 & Vol MA20
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma20_ref1'] = df['ma20'].shift(1)
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()

        # 3. KDJ (9,3,3)
        low_9 = df['low'].rolling(window=9).min()
        high_9 = df['high'].rolling(window=9).max()
        rsv = (df['close'] - low_9) / (high_9 - low_9) * 100

        # EMA calculation for K, D
        # Pandas ewm adjust=False is standard for technical indicators?
        # User said: K = EMA(RSV, alpha=1/3) which is equivalent to SMA(RSV, 3, 1)?
        # Actually standard KDJ uses SMA (Complicated Chinese version: SMA(X, N, M) = (M*X + (N-M)*Y')/N).
        # Which is equivalent to EMA with alpha = M/N.
        # Here N=3, M=1. Alpha = 1/3.
        # df.ewm(alpha=1/3, adjust=False) matches this.

        df['k'] = rsv.ewm(alpha=1/3, adjust=False).mean()
        df['d'] = df['k'].ewm(alpha=1/3, adjust=False).mean()
        df['j'] = 3 * df['k'] - 2 * df['d']

        df['j_ref1'] = df['j'].shift(1)
        df['j_ref2'] = df['j'].shift(2)

        return df

    def select(self, date):
        """
        Select a stock for the given date.
        """
        # Convert date to timestamp for comparison
        target_date = pd.to_datetime(date)
        date_str = target_date.strftime("%Y-%m-%d")

        # We need to iterate candidates.
        # To save time, we can iterate files in data/processed
        # But loading 5000 files is slow.
        # In a real "Runner", this `select` is called daily.
        # Loading 5000 files daily is prohibitive (5000 * 365 reads).
        # Optimization:
        # 1. Pre-load everything? (Memory intensive).
        # 2. Daily Loop in Runner implies we are simulating day-by-day.
        #    In reality, we only need to scan stocks that "might" be valid.

        # For this implementation, I will iterate all files because I have no choice without a database.
        # But I will try to be fast: read only necessary columns?
        # `fastparquet` / `pyarrow` can read columns.
        # But we need history for indicators.

        # Let's limit the universe for testing/performance if needed.
        # Or just iterate.

        best_stock = None
        best_zdp = -9999.0

        # Get list of symbols (from files)
        # Assuming DataEnhancer has run and files exist.
        files = [f for f in os.listdir(PROCESSED_DIR) if f.endswith(".parquet")]

        # Shuffle files to avoid bias? No, we want deterministic.
        files.sort()

        for f in files:
            symbol = f.replace(".parquet", "")

            # 1. Filter: Not ST
            if symbol in self.st_stocks:
                continue

            # 2. Load Data (Recent history only? Need at least 30 days before date)
            # We can't easily seek in parquet by date without reading index.
            # Read full file (cached by OS hopefully).
            try:
                # Read only needed columns to speed up?
                # cols = ['date', 'open', 'close', 'high', 'low', 'volume', 'outvol', 'invol']
                df = pd.read_parquet(os.path.join(PROCESSED_DIR, f))
            except:
                continue

            if df.empty:
                continue

            # Filter by date <= target_date
            # And we need enough history for indicators (20 days MA + buffer).
            # So slicing:
            df = df[df['date'] <= target_date]
            if df.empty or df.iloc[-1]['date'] != target_date:
                # Stock not trading on target_date
                continue

            # 3. Filter: Price > 2
            current_close = df.iloc[-1]['close']
            if current_close <= 2.0:
                continue

            # 4. Filter: Market Cap < 35 Billion
            # Cap = Close * Shares
            shares = self.total_shares_map.get(symbol, 0)
            market_cap = current_close * shares # in Yuan
            if market_cap > 35_000_000_000: # 35 Billion
                continue

            # 5. Calculate Indicators
            # We only need the last row's signals, but indicators require history.
            # Slice last 40 rows
            df_slice = df.tail(40).copy()
            df_slice = self._calculate_indicators(df_slice)

            # Get latest row (Date T)
            # Wait, `select(date)` logic:
            # "User: Logic: selector.select(date) simulates a decision made Pre-Market on Date T.
            # Constraint: The internal logic of the selector must ONLY use data up to date - 1."
            #
            # BUT: "Execution: Trades occur at Open of Date T."
            # If I use data up to T-1, I am deciding on T morning based on T-1 Close.
            #
            # My code: `df = df[df['date'] <= target_date]` includes Date T.
            # If I use Date T data, I am using Close(T) to decide to buy at Open(T) -> Lookahead Bias!
            #
            # CORRECTION:
            # `target_date` passed to `select` is the "Action Date" (Today).
            # We must use data up to `target_date - 1 day`.
            #
            # Let's adjust slicing.
            # Find the row for `target_date`. We can't use it.
            # We need the row BEFORE `target_date`.

            # Actually, `runner` calls `select(date)`.
            # If we are standing at Morning of `date`, we only know `date-1`.
            # So I should filter `df['date'] < target_date`.

            mask = df['date'] < target_date
            if not mask.any():
                continue

            # Take the valid history
            df_history = df[mask].copy()

            # Need at least 30 days
            if len(df_history) < 30:
                continue

            # Calculate indicators on this history
            # Only need last few rows
            df_calc = df_history.tail(40).copy()
            df_calc = self._calculate_indicators(df_calc)

            # Get the last row (T-1)
            row = df_calc.iloc[-1]

            # Check Indicators
            # A3: (Close > Open) AND (MA20 > Ref(MA20, 1)) AND (Close > MA20) AND (Vol > MA_Vol_20)
            a3 = (row['close'] > row['open']) and \
                 (row['ma20'] > row['ma20_ref1']) and \
                 (row['close'] > row['ma20']) and \
                 (row['volume'] > row['vol_ma20'])

            if not a3:
                continue

            # ZDP Logic
            zdp = row['zdp']
            mazdp = row['mazdp']

            if pd.isna(zdp) or pd.isna(mazdp):
                continue

            # ZDPP: (ZDP > MAZDP) AND (ZDP < 0)
            zdpp = (zdp > mazdp) and (zdp < 0)

            # ZDPP2: CROSS(ZDP, MAZDP) AND (ZDP < 0)
            # Cross: ZDP(T-1) > MAZDP(T-1) AND ZDP(T-2) < MAZDP(T-2)
            prev_row = df_calc.iloc[-2]
            zdp_prev = prev_row['zdp']
            mazdp_prev = prev_row['mazdp']

            cross = (zdp > mazdp) and (zdp_prev < mazdp_prev)
            zdpp2 = cross and (zdp < 0)

            # Final Signal
            # OK = A3 AND ZDPP AND (ZDP < -4)
            # OK2 = A3 AND ZDPP2 AND (ZDP < -4)

            cond_zdp_low = (zdp < -4)

            ok = a3 and zdpp and cond_zdp_low
            ok2 = a3 and zdpp2 and cond_zdp_low

            if not (ok or ok2):
                continue

            # BUY = (OK AND KDJJ) OR (OK2 AND J > Ref(J,1))

            # KDJJ: (Ref(J,1) <= 30) AND (J > Ref(J,1)) AND (Ref(J,1) < Ref(J,2))
            # Note: Ref(J,1) here means J(T-2). Current row is T-1.
            # So:
            # J(T-1) > J(T-2)
            # J(T-2) <= 30
            # J(T-2) < J(T-3)

            j = row['j']
            j_1 = row['j_ref1'] # T-2
            j_2 = row['j_ref2'] # T-3

            kdjj = (j_1 <= 30) and (j > j_1) and (j_1 < j_2)

            buy_signal = (ok and kdjj) or (ok2 and (j > j_1))

            if buy_signal:
                # We found a candidate.
                # User says: "return one stock".
                # Strategy: Pick the one with strongest signal?
                # Maybe lowest ZDP (most oversold but turning)?
                # Or just return first found?
                # "Output: The stock symbol to hold today".
                # I'll try to find the "best" one if multiple trigger.
                # Criteria: Lowest ZDP (deepest dip)? Or J turnaround strength?
                # Let's use ZDP. The lower the ZDP (below -4), the more extreme the flow divergence was.
                # Actually, ZDP < -4 means "Out < In" (Net Sell).
                # Wait, (Out - In) / Vol.
                # If ZDP < 0, In > Out. Selling pressure.
                # Strategy buys when Selling Pressure is high but trend reversing?
                # "ZDPP (Trend): (ZDP > MAZDP) and (ZDP < 0)" -> Selling pressure decreasing.

                # I will prefer the one with the *highest* ZDP among candidates (closest to crossing 0?).
                # Or simply the first one.
                # Let's stick to returning the first valid one for speed,
                # or store and sort.
                # Given iteration over 5000 stocks is slow, storing valid ones and picking best is better.

                # Let's maximize ZDP (closest to recovery).
                if zdp > best_zdp:
                    best_zdp = zdp
                    best_stock = symbol

        return best_stock
