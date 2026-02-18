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
        self.signals = {} # {date: [symbol1, symbol2]}
        self._initialize_stock_info()
        self._precalculate_signals()

    def _precalculate_signals(self):
        """Pre-calculate daily buy signals for all stocks to optimize backtest speed."""
        print("Pre-calculating signals for all stocks...")
        files = [f for f in os.listdir(PROCESSED_DIR) if f.endswith(".parquet")]

        for f in files:
            symbol = f.replace(".parquet", "")

            # 1. Filter: Not ST
            if symbol in self.st_stocks:
                continue

            try:
                # Read necessary columns
                df = pd.read_parquet(os.path.join(PROCESSED_DIR, f))
            except:
                continue

            if df.empty or len(df) < 30:
                continue

            # 2. Filter: Market Cap < 35 Billion (Dynamic based on Price)
            # We can only check this row-by-row or vectorized.
            # 3. Filter: Price > 2

            # Calculate Indicators
            df = self._calculate_indicators(df)

            # Vectorized Conditions
            # A3
            cond_a3 = (df['close'] > df['open']) & \
                      (df['ma20'] > df['ma20_ref1']) & \
                      (df['close'] > df['ma20']) & \
                      (df['volume'] > df['vol_ma20'])

            # ZDPP
            cond_zdpp = (df['zdp'] > df['mazdp']) & (df['zdp'] < 0)

            # ZDPP2 (Cross)
            # Cross: ZDP(T) > MAZDP(T) AND ZDP(T-1) < MAZDP(T-1)
            # We need shift.
            zdp_prev = df['zdp'].shift(1)
            mazdp_prev = df['mazdp'].shift(1)
            cond_cross = (df['zdp'] > df['mazdp']) & (zdp_prev < mazdp_prev)
            cond_zdpp2 = cond_cross & (df['zdp'] < 0)

            # ZDP Low
            cond_zdp_low = (df['zdp'] < -4)

            # OK / OK2
            cond_ok = cond_a3 & cond_zdpp & cond_zdp_low
            cond_ok2 = cond_a3 & cond_zdpp2 & cond_zdp_low

            # KDJJ
            # J(T) > J(T-1)
            # J(T-1) <= 30
            # J(T-1) < J(T-2)
            j_1 = df['j_ref1']
            j_2 = df['j_ref2']
            cond_kdjj = (j_1 <= 30) & (df['j'] > j_1) & (j_1 < j_2)

            # Final Buy Signal
            # BUY = (OK AND KDJJ) OR (OK2 AND J > Ref(J,1))
            cond_buy = (cond_ok & cond_kdjj) | (cond_ok2 & (df['j'] > j_1))

            # Price > 2 and Cap < 35B
            shares = self.total_shares_map.get(symbol, 0)
            # Market Cap = Close * Shares
            cond_cap = (df['close'] * shares) < 35_000_000_000
            cond_price = df['close'] > 2.0

            final_mask = cond_buy & cond_cap & cond_price

            # Extract dates where signal is True
            # Adjust date for "Execution at T+1".
            # The signal is generated on Date T (Close).
            # The Runner calls select(Date T+1).
            # So if Signal is True on T, we want select(T+1) to return this stock.
            # But wait, Runner logic:
            # "Select(T) uses info from T-1".
            # If I signal on Date T, I can buy on T+1.
            # Runner loop: For date D: select(D).
            # select(D) should look at signals generated on D-1.

            valid_dates = df.loc[final_mask, 'date']

            for date in valid_dates:
                # Store signal for Next Trading Day?
                # Or store for current date and let Selector logic handle "T-1"?
                # In `select(date)`, I previously filtered `df['date'] < date`.
                # So `select(date)` looks for signal on `date-1` (or latest available).

                # To make lookup O(1):
                # We map `date + 1 day` -> symbol?
                # But trading days are not continuous.
                # Safer: Store signal on `date`.
                # In `select(target_date)`, we look for signal on `target_date - 1`?
                # No, we look for signal on the "latest trading day before target_date".
                # But that's hard with a simple dict.

                # Compromise:
                # Store {date: [ (symbol, zdp) ]}
                # When select(date) is called, we check if `date` has a signal?
                # No, `select(date)` is called at Morning of `date`.
                # Signal is from Close of `date-1`.
                # If we iterate calendar days, `date-1` might be Sunday. Signal was Friday.
                # So we need to find "Signal Date < date".

                # Simpler approach for Backtest Loop (which iterates contiguous dates):
                # Just store signal at `date` (Signal Date).
                # `select(target_date)`: find max(d) < target_date in signals?
                # This is still potentially slow if many keys.

                # Let's just store {SignalDate: [List of Candidates]}
                # And in `select(target_date)`, we iterate `range(target_date-5, target_date)`?
                # Or just assume daily iteration and carry forward?

                # Actually, `SniperSelector.select(date)` logic was:
                # 1. Filter `df['date'] < target_date`.
                # 2. Get last row.
                # This implies finding the *most recent* trading day.

                # Let's stick to the previous logic but Pre-Calculate.
                # We can store a sorted list of (Date, Symbol, ZDP) tuples.
                # [(D1, Sym1, Z1), (D1, Sym2, Z2), (D2, ...)]
                # Then we can query efficiently?

                # Even better:
                # `self.daily_candidates = { date: [(symbol, zdp)] }`
                # But this requires knowing which date corresponds to "Next Open".
                # Since we don't know the trading calendar easily, let's map Signal Date -> Candidates.
                # And in `select(date)`, we check `date - 1`?
                # If `date` is Tuesday, `date-1` is Monday. OK.
                # If `date` is Monday, `date-1` is Sunday. No signal.
                # We need `date-3` (Friday).

                # Since the Runner loops every day:
                # If today is Monday, `select(Monday)` called.
                # We look for signals from Sunday? No. Saturday? No. Friday? Yes.
                #
                # If I simply store signals by their Date.
                # `select(today)` can look back 1-10 days for the *latest* signal date.

                dt_key = date # Timestamp
                zdp_val = df.loc[df['date'] == date, 'zdp'].values[0]

                if dt_key not in self.signals:
                    self.signals[dt_key] = []
                self.signals[dt_key].append((symbol, zdp_val))

        print(f"Pre-calculation complete. Signals found on {len(self.signals)} days.")

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
        Uses pre-calculated signals to find the best candidate from the most recent trading day.
        """
        target_date = pd.to_datetime(date)

        # Look back up to 10 days to find the most recent trading day with signals
        # This handles weekends/holidays where target_date (e.g. Monday) should use Friday's signal.

        for i in range(1, 11):
            lookback_date = target_date - pd.Timedelta(days=i)
            if lookback_date in self.signals:
                candidates = self.signals[lookback_date]
                if candidates:
                    # Pick best candidate based on highest ZDP
                    # Candidates is list of (symbol, zdp)
                    best_candidate = max(candidates, key=lambda x: x[1])
                    return best_candidate[0]

        return None
