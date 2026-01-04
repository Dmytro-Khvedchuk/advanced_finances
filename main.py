"""The start of the program."""
from datetime import datetime, UTC

import polars as pl
from dotenv import load_dotenv

from src.app.backtest import run_walk_forward_recommendation
from src.app.backtest.evaluation import aggregate_confusion_matrices, classification_metrics
from src.app.data.api.domain.value_objects import BinanceKlineTimeframes
from src.app.data.api.market import MarketData
from src.app.data.api.scrapers import ForexFactoryScraper
from src.app.ml.features.indicator_strategies import IS1MomentumVol, IS2MeanReversionML, IS3VolatilityRegimeML
from src.app.ml.features.news_strategies import NS1SurpriseBias
from src.app.ml.recommendation import RecommendationEngine
from src.app.system.logs import get_logger, setup_logging


START_DATE: datetime = datetime(2025, 7, 1, tzinfo=UTC)
END_DATE: datetime = datetime(2026, 1, 1, tzinfo=UTC)
SYMBOL: str = "XRPUSDT"
TIMEFRAME: BinanceKlineTimeframes = BinanceKlineTimeframes.FIFTEEN_MINUTES
HORIZON_BARS: int = 4

logger = get_logger(__name__)


def main() -> None:
    """Main program loop."""
    setup_logging() 
    load_dotenv()

    config: dict = {
        "START_DATE": START_DATE,
        "END_DATE": END_DATE,
        "SYMBOL": SYMBOL,
        "TIMEFRAME": TIMEFRAME,
        "HORIZON_BARS": HORIZON_BARS
    }

    # news fetcher
    ff_scraper: ForexFactoryScraper = ForexFactoryScraper()
    forex_news: pl.DataFrame = ff_scraper.get_data(7, 2025, 6)  # noqa: F841

    # klines fetcher
    market_data: MarketData = MarketData()
    klines: pl.DataFrame = market_data.get_klines(  # noqa: F841
        START_DATE, END_DATE, SYMBOL, TIMEFRAME
    )

    strategies = {
        "is1": IS1MomentumVol(),
        "is2": IS2MeanReversionML(),
        "is3": IS3VolatilityRegimeML(),
        "news_ns1": NS1SurpriseBias(),
    }

    # recommender
    recommender = RecommendationEngine(
        strategy_weights={
            "is1": 0.25,
            "is2": 0.25,
            "is3": 0.25,
            "news_ns1": 0.25,
        },
        decision_threshold=0.4,
    )

    results = run_walk_forward_recommendation(
        klines,
        strategies=strategies,
        recommender=recommender,
        news_df=forex_news,
        horizon_bars=HORIZON_BARS,
    )

    # --- monthly output ---
    for r in results:
        logger.info(
            r["month"],
            r["metrics"],
            r.get("weights"),
        )

    # --- ALL-TIME metrics ---
    all_cms = [r["confusion"] for r in results]

    if all_cms:
        global_cm = aggregate_confusion_matrices(all_cms)
        global_metrics = classification_metrics(global_cm)

        logger.info(f"Confusion matrix: {global_cm}")
        logger.info("Metrics:")
        for k, v in global_metrics.items():
            logger.info(f"{k}: {v:.4f}")

    logger.info(f"{config=}")


if __name__ == "__main__":
    main()
