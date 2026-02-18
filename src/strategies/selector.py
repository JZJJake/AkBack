from abc import ABC, abstractmethod

class BaseSelector(ABC):
    """
    Abstract Base Class for Stock Selectors.
    """
    @abstractmethod
    def select(self, date) -> str | None:
        """
        Selects a stock to hold for the given date.

        Args:
            date (str or pd.Timestamp): The date for which the selection is made.

        Returns:
            str: The stock symbol to hold (e.g., "000001").
            None: Hold cash.
        """
        pass
