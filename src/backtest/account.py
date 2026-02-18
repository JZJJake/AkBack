from src.config import COMMISSION_RATE, MIN_COMMISSION, STAMP_DUTY
import logging

class Account:
    def __init__(self, initial_capital=100000.0):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {}  # {symbol: {"total": 0, "sellable": 0}}
        self.total_assets = initial_capital
        self.history = []  # List of daily asset values

    def _get_commission(self, trade_amount):
        """Calculate commission with minimum floor."""
        comm = trade_amount * COMMISSION_RATE
        return max(comm, MIN_COMMISSION)

    def _get_stamp_duty(self, trade_amount):
        """Calculate stamp duty (only on sells)."""
        return trade_amount * STAMP_DUTY

    def can_buy(self, price, quantity):
        """Check if sufficient cash exists for the trade."""
        cost = price * quantity
        comm = self._get_commission(cost)
        total_cost = cost + comm
        return self.cash >= total_cost

    def buy(self, symbol, price, quantity):
        """
        Execute a buy order.
        Returns (success, message, trade_details)
        """
        if quantity <= 0:
            return False, "Quantity must be positive", None

        trade_amount = price * quantity
        commission = self._get_commission(trade_amount)
        total_cost = trade_amount + commission

        if self.cash < total_cost:
            return False, f"Insufficient cash: {self.cash:.2f} < {total_cost:.2f}", None

        # Update cash
        self.cash -= total_cost

        # Update positions
        if symbol not in self.positions:
            self.positions[symbol] = {"total": 0, "sellable": 0}

        self.positions[symbol]["total"] += quantity
        # Sellable does NOT increase immediately (T+1)

        trade_details = {
            "symbol": symbol,
            "action": "BUY",
            "price": price,
            "quantity": quantity,
            "commission": commission,
            "stamp_duty": 0.0,
            "total_cost": total_cost
        }
        return True, "Success", trade_details

    def sell(self, symbol, price, quantity):
        """
        Execute a sell order.
        Returns (success, message, trade_details)
        """
        if quantity <= 0:
            return False, "Quantity must be positive", None

        if symbol not in self.positions:
            return False, "Symbol not in portfolio", None

        available_qty = self.positions[symbol]["sellable"]
        if available_qty < quantity:
            return False, f"Insufficient sellable quantity: {available_qty} < {quantity}", None

        trade_amount = price * quantity
        commission = self._get_commission(trade_amount)
        stamp_duty = self._get_stamp_duty(trade_amount)
        net_revenue = trade_amount - commission - stamp_duty

        # Update cash
        self.cash += net_revenue

        # Update positions
        self.positions[symbol]["total"] -= quantity
        self.positions[symbol]["sellable"] -= quantity

        # Clean up empty position
        if self.positions[symbol]["total"] == 0:
            del self.positions[symbol]

        trade_details = {
            "symbol": symbol,
            "action": "SELL",
            "price": price,
            "quantity": quantity,
            "commission": commission,
            "stamp_duty": stamp_duty,
            "net_revenue": net_revenue
        }
        return True, "Success", trade_details

    def update_market_value(self, current_prices):
        """
        Update total assets based on current market prices.
        current_prices: dict {symbol: price}
        """
        market_value = 0.0
        for symbol, pos in self.positions.items():
            price = current_prices.get(symbol, 0.0)
            market_value += pos["total"] * price

        self.total_assets = self.cash + market_value
        return self.total_assets

    def settle(self):
        """
        End of day settlement: update T+1 sellable quantities.
        """
        for symbol in self.positions:
            self.positions[symbol]["sellable"] = self.positions[symbol]["total"]

    def get_max_buyable(self, price):
        """
        Calculate max shares buyable with current cash (considering fees).
        """
        if price <= 0:
            return 0

        # Estimate with a safe margin first
        # cash = qty * price * (1 + comm_rate) -> approx qty = cash / (price * 1.0003)
        # But wait, commission has a minimum.

        # Simple iterative or direct formula:
        # Cost = Q * P + max(Q * P * rate, min_comm)

        # Case 1: Commission is above min
        # Cash >= Q * P * (1 + rate)
        # Q <= Cash / (P * (1 + rate))

        # Case 2: Commission is min
        # Cash >= Q * P + min_comm
        # Q <= (Cash - min_comm) / P

        q1 = int(self.cash / (price * (1 + COMMISSION_RATE)))
        q2 = int((self.cash - MIN_COMMISSION) / price)

        max_q = min(q1, q2)
        if max_q < 0: return 0

        # Floor to nearest 100
        return (max_q // 100) * 100
