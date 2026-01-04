"""This package is responsible for the backtesting."""

from src.app.backtest.strategy_context import StrategyContext
from src.app.backtest.walk_foward_evaluation import run_walk_forward_recommendation


__all__ = ["run_walk_forward_recommendation", "StrategyContext"]
