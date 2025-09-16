from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource
from engine.apps.backtest.analytics.metrics import MetricsGenerator


class ReportGenerator:
    def __init__(self, portfolio):
        self.portfolio = portfolio

    def generate_general_metrics(self):
        (
            equity_history,
            trade_history,
            order_history,
            current_positions,
            initial_balance,
        ) = self.portfolio.get_metrics()

        metrics_generator = MetricsGenerator(
            equity_history,
            trade_history,
            order_history,
            current_positions,
            initial_balance,
        )

        metrics = metrics_generator.generate_general_metrics()

        print(" === GENERAL METRICS ===")

        for title, value in metrics.items():
            print(f"{title}: {value:.2f}")

        print(" === END ===")

        self._generate_general_chart(equity_history=equity_history)

    def _generate_general_chart(self, equity_history):
        title = "General Equity Chart"
        x_label = "Time"
        y_label = "Equity"

        sorted_items = sorted(equity_history.items())
        x, y = zip(*sorted_items)

        source = ColumnDataSource(data={"x": x, "y": y})

        p = figure(
            title=title,
            x_axis_label=x_label,
            y_axis_label=y_label,
            width=1600,
            height=800,
        )
        p.line("x", "y", source=source, line_width=2, color="navy")

        show(p)
