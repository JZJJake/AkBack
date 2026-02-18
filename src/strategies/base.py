class Strategy:
    """
    Abstract Base Strategy for Backtesting.
    """
    def __init__(self):
        """
        Initialize strategy.
        Note: The engine will set self.account and self.symbol before running.
        """
        self.account = None
        self.symbol = None

    def on_bar(self, bar):
        """
        Called on every bar (daily candle).

        Args:
            bar: Dictionary or Series with 'date', 'open', 'high', 'low', 'close', 'volume'

        Returns:
            Order object or None
        """
        raise NotImplementedError("Strategy must implement on_bar()")
