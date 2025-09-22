import numpy as np
import polars as pl
from sklearn.linear_model import LinearRegression

# === Symbol metrics ===

# Total Trades (count of executed trades) >
# Win Rate (%) (profitable trades ÷ total trades) >
# Profit Factor (gross profit ÷ gross loss) >
# Average Trade Return (%) >
# Max Drawdown (%) (largest peak-to-trough loss) >
# Sharpe Ratio (risk-adjusted return) >
# Sortino Ratio (downside risk-adjusted return) >
# Exposure (%) (time in market vs. total backtest period) >
# Commission & Slippage Costs (total deducted) >
# Annualized Return (%) (CAGR for that symbol) >


class MetricsGenerator:
    def __init__(
        self,
        equity_history,
        trade_history,
        order_history,
        current_positions,
        initial_balance,
    ):
        self.equity_history = equity_history
        self.general_equity_history = self.equity_history["General"]
        self.trade_history = trade_history
        self.order_history = order_history
        self.current_positions = current_positions
        self.initial_balance = initial_balance
        self.final_balance = self.general_equity_history[list(self.general_equity_history.keys())[-1]]

    def generate_general_metrics(self):
        metrics = {}

        # Total Net Profit
        net_profit, net_profit_pct = self._get_total_net_profit()
        metrics.update({"Total Net Profit ($)": net_profit})
        metrics.update({"Total Net Profit (%)": net_profit_pct})

        # Annualized Return
        annualized_return = self._get_annualized_return()
        metrics.update({"Annualized Return (CAGR) (%)": annualized_return})

        # Volatility
        daily_volatility, annual_volatility = self._get_volatility()
        metrics.update({"Volatility 1D": daily_volatility})
        metrics.update({"Volatility 1Y": annual_volatility})

        # Sharpe Ratio
        (
            monthly_sharpe_ratio,
            annual_sharpe_ratio,
            monthly_sortino_ratio,
            annual_sortino_ratio,
        ) = self._get_sharpe_sortino_ratios()
        metrics.update({"Sharpe Ratio 1M": monthly_sharpe_ratio})
        metrics.update({"Sharpe Ratio 1Y": annual_sharpe_ratio})
        metrics.update({"Sortino Ratio 1M": monthly_sortino_ratio})
        metrics.update({"Sortino Ratio 1Y": annual_sortino_ratio})

        # Max Drawdown
        max_drawdown_pct, max_drwadown_volume = self._get_max_drawdown()
        metrics.update({"Max Drawdown (%)": max_drawdown_pct})
        metrics.update({"Max Drawdown ($)": max_drwadown_volume})

        # Calmar Ratio
        calmar_ratio = annualized_return / max_drawdown_pct
        metrics.update({"Calmar Ratio": calmar_ratio})

        # Value at Risk
        var_95 = self._get_historical_var()
        metrics.update({"Value At Risk 95% (%)": var_95})

        equity_curve_stability = self._get_equity_curve_stability()
        metrics.update({"Equity Curve Stability R²": equity_curve_stability})

        portfolio_turnover = self._get_portfolio_turnover()
        metrics.update({"Portfolio Turnover": portfolio_turnover})

        return metrics

    def _get_portfolio_turnover(self):
        df = self.trade_history.select(
            [
                pl.col("entry_time").alias("timestamp"),
                pl.col("volume").alias("trade_value"),
            ]
        ).sort("timestamp")

        total_traded = df["trade_value"].sum()
        avg_portfolio_value = df["trade_value"].mean()

        if avg_portfolio_value == 0:
            return np.nan

        turnover_pct = (total_traded / avg_portfolio_value) * 100
        return turnover_pct

    def _get_equity_curve_stability(self):
        df = pl.DataFrame(
            {
                "timestamp": list(self.general_equity_history.keys()),
                "equity": list(self.general_equity_history.values()),
            }
        ).sort("timestamp")
        X = df["timestamp"].to_numpy().reshape(-1, 1)
        y = df["equity"].to_numpy()
        if len(y) < 2:
            return np.nan
        model = LinearRegression()
        model.fit(X, y)
        r_squared = model.score(X, y)

        return r_squared

    def _get_historical_var(self):
        df = pl.DataFrame(
            {
                "timestamp": list(self.general_equity_history.keys()),
                "equity": list(self.general_equity_history.values()),
            }
        )
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ms")))
        daily_df = df.group_by_dynamic(
            index_column="timestamp", every="1d", closed="right", by=None
        ).agg(pl.col("equity").last().alias("equity"))
        daily_df = daily_df.sort("timestamp")
        returns = daily_df.select("equity").drop_nulls().to_numpy().flatten()
        confidence_level = 0.95
        returns = np.asarray(returns)
        var_percentile = 100 * (1 - confidence_level)
        var = np.percentile(returns, var_percentile)
        return var

    def _get_max_drawdown(self):
        df = (
            pl.DataFrame(
                {
                    "timestamp": list(self.general_equity_history.keys()),
                    "equity": list(self.general_equity_history.values()),
                }
            )
            .with_columns(pl.col("timestamp").cast(pl.Datetime("ms")))
            .sort("timestamp")
        )
        df = df.with_columns(pl.col("equity").cum_max().alias("running_max"))
        df = df.with_columns(
            ((pl.col("equity") - pl.col("running_max"))).alias("drawdown_dollar"),
            (
                (pl.col("equity") - pl.col("running_max")) / pl.col("running_max") * 100
            ).alias("drawdown_pct"),
        )
        max_drawdown_row = df.select(
            [
                pl.col("drawdown_pct").min().alias("max_drawdown_pct"),
                pl.col("drawdown_dollar").min().alias("max_drawdown_dollar"),
            ]
        ).to_dict(as_series=False)

        return (
            max_drawdown_row["max_drawdown_pct"][0],
            max_drawdown_row["max_drawdown_dollar"][0],
        )

    def _get_sharpe_sortino_ratios(self):
        df = pl.DataFrame(
            {
                "timestamp": list(self.general_equity_history.keys()),
                "equity": list(self.general_equity_history.values()),
            }
        )
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime("ms")))
        monthly_df = df.group_by_dynamic(
            index_column="timestamp", every="1mo", closed="right", by=None
        ).agg(pl.col("equity").last().alias("equity"))
        monthly_df = monthly_df.sort("timestamp")
        monthly_df = monthly_df.with_columns(
            (pl.col("equity").pct_change().alias("monthly_return"))
        )
        monthly_returns = (
            monthly_df.select("monthly_return").drop_nulls().to_numpy().flatten()
        )
        monthly_sharpe = np.mean(monthly_returns) / np.std(monthly_returns)
        annualized_sharpe = monthly_sharpe * np.sqrt(12)

        monthly_sortino, annualized_sortino = self._get_sortino_ratio(monthly_returns)

        return monthly_sharpe, annualized_sharpe, monthly_sortino, annualized_sortino

    def _get_sortino_ratio(self, returns):
        returns = np.array(returns)
        excess_returns = returns - 0 / 12
        downside_returns = excess_returns[excess_returns < 0]
        downside_std = (
            np.std(downside_returns, ddof=1) if len(downside_returns) > 0 else np.nan
        )
        mean_excess_return = np.mean(excess_returns)
        sortino = mean_excess_return / downside_std if downside_std != 0 else np.nan
        annualized_sortino = sortino * np.sqrt(12)

        return sortino, annualized_sortino

    def _get_volatility(self):
        returns = self.trade_history["pnl"].to_numpy()
        daily_volatility = np.std(returns, ddof=1)
        annual_volatility = daily_volatility * np.sqrt(365)

        return daily_volatility, annual_volatility

    def _get_annualized_return(self):
        time_start = min(self.general_equity_history.keys())
        time_end = max(self.general_equity_history.keys())
        years = (time_end - time_start) / (60 * 60 * 24 * 365 * 1000)
        annualized_return = (
            (self.final_balance / self.initial_balance) ** (1 / years) - 1
        ) * 100

        return annualized_return

    def _get_total_net_profit(self):
        net_profit = self.final_balance - self.initial_balance
        net_profit_pct = net_profit / self.initial_balance * 100

        return net_profit, net_profit_pct
