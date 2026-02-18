import pandas as pd
from src.backtest.account import Account
import logging
from tqdm import tqdm

class PortfolioRunner:
    """
    Backtest runner for portfolio rotation strategies.
    Iterates through calendar days and executes trades based on Selector logic.
    """
    def __init__(self, data_loader, initial_capital=100000.0):
        self.data_loader = data_loader
        self.initial_capital = initial_capital
        self.account = None
        self.trades = []
        self.daily_curves = []

        # Cache for currently held stock data
        self.current_stock_data = None
        self.current_symbol = None

    def _get_stock_data(self, symbol):
        """Helper to load stock data."""
        return self.data_loader.load_stock_data(symbol)

    def _get_price_for_date(self, df, date, price_col='open'):
        """Get price for a specific date from dataframe."""
        if df is None or df.empty:
            return None
        # Assuming df is sorted by date
        # Use simple lookup
        # df['date'] should be datetime
        row = df[df['date'] == date]
        if row.empty:
            return None
        return row.iloc[0][price_col]

    def run(self, selector, start_date, end_date):
        """
        Run the backtest.

        Args:
            selector: BaseSelector instance.
            start_date (str): 'YYYY-MM-DD'
            end_date (str): 'YYYY-MM-DD'

        Returns:
            dict: {'trades': pd.DataFrame, 'curve': pd.DataFrame}
        """
        self.account = Account(self.initial_capital)
        self.trades = []
        self.daily_curves = []
        self.current_stock_data = None
        self.current_symbol = None

        dates = pd.date_range(start=start_date, end=end_date)

        # Iterate through dates
        for date in tqdm(dates, desc="Running Backtest"):
            # 1. Check Data Availability for Current Holding (if any)
            # If we hold a stock but today is not a trading day for it, skip.
            is_trading_day = True
            current_open_price = None
            current_close_price = None

            if self.current_symbol:
                if self.current_stock_data is None:
                     # Should not happen if logic is correct
                     logging.error(f"Holding {self.current_symbol} but no data cached.")
                     is_trading_day = False
                else:
                    # Check if date exists in data
                    row = self.current_stock_data[self.current_stock_data['date'] == date]
                    if row.empty:
                        is_trading_day = False
                    else:
                        current_open_price = row.iloc[0]['open']
                        current_close_price = row.iloc[0]['close']

            if self.current_symbol and not is_trading_day:
                # Holding stock, but market closed (holiday/weekend).
                # Skip trading. Valuation stays same as previous day.
                # Just record previous day's curve again? Or skip recording?
                # Standard practice: Record curve with same value to maintain time series.
                if self.daily_curves:
                    last_curve = self.daily_curves[-1].copy()
                    last_curve['date'] = date
                    self.daily_curves.append(last_curve)
                continue

            # 2. Settle T+1 (Unlock shares from yesterday)
            self.account.settle()

            # 3. Selection
            target_symbol = selector.select(date)

            # 4. Execution Logic
            # If current holding != target:
            if self.current_symbol != target_symbol:

                # A. Sell Current (if any)
                if self.current_symbol:
                    # Sell at Open
                    if current_open_price:
                        sell_qty = self.account.positions[self.current_symbol]['sellable']
                        if sell_qty > 0:
                            success, msg, details = self.account.sell(self.current_symbol, current_open_price, sell_qty)
                            if success:
                                details['date'] = date
                                self.trades.append(details)
                                # Clear holding
                                self.current_symbol = None
                                self.current_stock_data = None
                            else:
                                logging.warning(f"Failed to sell {self.current_symbol} on {date}: {msg}")
                        else:
                             # T+1 restriction prevents selling.
                             # If we bought yesterday, we can't sell today.
                             # This means rotation fails for today. Keep holding.
                             logging.info(f"Cannot sell {self.current_symbol} on {date} due to T+1.")
                             target_symbol = self.current_symbol # Force hold

                # B. Buy Target (if any and valid)
                if target_symbol and target_symbol != self.current_symbol:
                    # Load data for target
                    target_data = self._get_stock_data(target_symbol)

                    if target_data is not None and not target_data.empty:
                        # Check if target trades today
                        target_row = target_data[target_data['date'] == date]

                        if not target_row.empty:
                            target_open = target_row.iloc[0]['open']
                            # Buy Max (pct=1.0)
                            # Using cash released from sell (T+0 for buying)
                            max_qty = self.account.get_max_buyable(target_open)

                            if max_qty > 0:
                                success, msg, details = self.account.buy(target_symbol, target_open, max_qty)
                                if success:
                                    details['date'] = date
                                    self.trades.append(details)
                                    self.current_symbol = target_symbol
                                    self.current_stock_data = target_data
                                else:
                                    logging.warning(f"Failed to buy {target_symbol}: {msg}")
                        else:
                            logging.info(f"Target {target_symbol} not trading on {date}. Cannot buy.")
                    else:
                        logging.warning(f"No data found for target {target_symbol}.")

            # 5. Valuation
            # Update market value using Close price
            current_prices = {}
            if self.current_symbol:
                # We need Close price for valuation
                # If we just bought it, we have data.
                # If we held it, we have data.
                if self.current_stock_data is not None:
                     row = self.current_stock_data[self.current_stock_data['date'] == date]
                     if not row.empty:
                         current_prices[self.current_symbol] = row.iloc[0]['close']

            self.account.update_market_value(current_prices)

            self.daily_curves.append({
                "date": date,
                "total_assets": self.account.total_assets,
                "cash": self.account.cash,
                "holdings_symbol": self.current_symbol,
                "holdings_value": self.account.total_assets - self.account.cash
            })

        return {
            "trades": pd.DataFrame(self.trades) if self.trades else pd.DataFrame(),
            "curve": pd.DataFrame(self.daily_curves) if self.daily_curves else pd.DataFrame()
        }
