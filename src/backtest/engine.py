import pandas as pd
from src.backtest.account import Account
from src.config import COMMISSION_RATE
import logging

class Order:
    """
    Order object to specify trading actions.
    """
    def __init__(self, action, type='market', quantity=None, pct=None, price=None):
        self.action = action.upper()  # 'BUY' or 'SELL'
        self.type = type.lower()      # 'market' or 'limit'
        self.quantity = quantity      # Number of shares (e.g., 100)
        self.pct = pct                # Percentage of portfolio (0.0 to 1.0)
        self.price = price            # Limit price (if type='limit')

    def __repr__(self):
        qty_str = f"qty={self.quantity}" if self.quantity else f"pct={self.pct}"
        return f"Order({self.action}, {self.type}, {qty_str}, price={self.price})"


class BacktestEngine:
    """
    Event-driven Backtest Engine for single stock.
    """
    def __init__(self, data_loader, strategy, initial_capital=100000.0):
        self.data_loader = data_loader
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.account = None
        self.trades = []
        self.daily_curves = [] # List of {date, total_assets}

    def run_single_stock(self, symbol, start_date, end_date):
        """
        Run backtest for a single stock.
        """
        # Load Data
        df = self.data_loader.load_stock_data(symbol)
        if df is None or df.empty:
            logging.error(f"No data found for {symbol}")
            return None

        # Filter by date range
        # Assume 'date' column is datetime
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)].copy()
        df.sort_values('date', inplace=True)
        df.reset_index(drop=True, inplace=True)

        if df.empty:
            logging.warning(f"No data in range {start_date}-{end_date} for {symbol}")
            return None

        # Initialize Account & Strategy context
        self.account = Account(self.initial_capital)
        self.strategy.account = self.account
        self.strategy.symbol = symbol

        self.trades = []
        self.daily_curves = []

        pending_order = None # Order generated at Close(T), executed at Open(T+1)

        # Iterate Day by Day
        for i, row in df.iterrows():
            date = row['date']
            open_price = row['open']
            close_price = row['close']

            # --- Market Open (T) ---
            # Execute Pending Order from T-1
            if pending_order:
                self._execute_order(pending_order, symbol, date, open_price)
                pending_order = None

            # --- Market Close (T) ---
            # Update Market Value
            # Note: We use Close price for daily valuation
            self.account.update_market_value({symbol: close_price})
            self.daily_curves.append({
                "date": date,
                "total_assets": self.account.total_assets,
                "cash": self.account.cash,
                "holdings": self.account.positions.get(symbol, {}).get("total", 0)
            })

            # Call Strategy
            # Strategy sees today's Close
            order = self.strategy.on_bar(row)

            if order:
                # Store for execution at Next Open
                pending_order = order

            # Settle T+1 (Unlock shares bought today for selling tomorrow)
            self.account.settle()

        return {
            "trades": pd.DataFrame(self.trades),
            "curve": pd.DataFrame(self.daily_curves)
        }

    def _execute_order(self, order, symbol, date, execution_price):
        """
        Execute an order at the given price.
        """
        if order.type == 'limit':
            # Check if limit price is reachable
            # Simple assumption: if Buy Limit >= Open, fill at Limit?
            # Or fill at Open if better?
            # Standard backtest rule:
            # Buy Limit: if Open < Limit, fill at Open. If Open > Limit, check Low < Limit.
            # But we only have Open here (Close-to-Open model).
            # To simplify: Limit orders executed at Open if Open price satisfies condition.
            if order.action == 'BUY' and execution_price > order.price:
                return # Cannot fill
            if order.action == 'SELL' and execution_price < order.price:
                return # Cannot fill
            # If fillable, usually fill at execution_price (market gap) or limit price?
            # Conservative: Fill at execution_price
            pass

        quantity = 0

        # Calculate Quantity
        if order.action == 'BUY':
            if order.pct:
                # Buy % of current CASH? Or Total Assets?
                # Usually "Buy 100% of cash" means "Invest all available cash".
                available_cash = self.account.cash * order.pct
                # Max buyable shares
                quantity = self.account.get_max_buyable(execution_price)
                # But we might only want to use specific amount of cash
                # If pct < 1.0, we scale down?
                # get_max_buyable uses ALL cash.
                # Let's use `buy` method's check logic or calculate manually.

                # Correct logic for pct:
                target_cash = self.account.cash * order.pct
                # Calculate qty for target_cash
                # Q = target_cash / (P * (1+comm))
                # But Account.get_max_buyable logic is better encapsulated.
                # Let's temporarily set cash? No.
                # Just calculate:
                # We can't easily access the logic inside account without duplicating it.
                # Let's just use `get_max_buyable` (which assumes 100% cash) and scale it?
                # No, because of fixed costs.

                # Let's assume order.pct=1.0 for "All-in" is the main use case.
                if order.pct >= 0.99:
                    quantity = self.account.get_max_buyable(execution_price)
                else:
                    # Approximation for < 100%
                    quantity = int((available_cash) / (execution_price * (1 + COMMISSION_RATE))) // 100 * 100

            elif order.quantity:
                quantity = order.quantity

            if quantity > 0:
                success, msg, details = self.account.buy(symbol, execution_price, quantity)
                if success:
                    details['date'] = date
                    self.trades.append(details)
                else:
                    # logging.info(f"Buy failed: {msg}")
                    pass

        elif order.action == 'SELL':
            # Determine available quantity
            current_pos = self.account.positions.get(symbol, {}).get("sellable", 0)

            if order.pct:
                quantity = int(current_pos * order.pct)
                # Round to nearest 100? Sell orders don't strictly require 100 lots if clearing position?
                # A-share rule: Must sell in 100 lots unless selling *remainder*.
                # If selling 100%, sell all.
                if order.pct >= 0.99:
                    quantity = current_pos
                else:
                    quantity = (quantity // 100) * 100
            elif order.quantity:
                quantity = order.quantity

            if quantity > 0:
                success, msg, details = self.account.sell(symbol, execution_price, quantity)
                if success:
                    details['date'] = date
                    self.trades.append(details)
                else:
                    # logging.info(f"Sell failed: {msg}")
                    pass
