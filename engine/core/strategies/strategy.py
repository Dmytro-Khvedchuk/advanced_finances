from abc import ABC, abstractmethod


class Strategy(ABC):
    """
    Abstract base class for a single trading strategy.
    """

    def __init__(self):
        pass

    @abstractmethod
    def generate_order(self, market_data: dict):
        """
        Should return an Order if the strategy wants to trade,
        or None if no signal.
        """
        pass
