from bokeh.palettes import Category10
from bokeh.plotting import figure, output_file, show
from bokeh.models import ColumnDataSource, DatetimeTickFormatter
from engine.apps.backtest.analytics.metrics import MetricsGenerator
from copy import deepcopy


class ReportGenerator:
    def __init__(self, portfolio):
        self.portfolio = portfolio

    def _generate_symbol_pnl_chart(self, equity_history):
        equity_history = deepcopy(equity_history)
        equity_history.pop("General", None)

        p = figure(
            title="Symbol-wise PnL",
            x_axis_label="Time",
            y_axis_label="PnL",
            x_axis_type="datetime",
            width=1600,
            height=800,
        )

        colors = Category10[10]

        for i, (symbol, data) in enumerate(equity_history.items()):
            sorted_items = sorted(data.items())
            x, y = zip(*sorted_items)

            source = ColumnDataSource(data={"x": x, "y": y})
            p.line(
                "x",
                "y",
                source=source,
                line_width=2,
                color=colors[i % len(colors)],
                legend_label=symbol,
            )

        p.legend.location = "top_left"
        p.legend.click_policy = "hide"
        p.xaxis.formatter = DatetimeTickFormatter(
            days="%d %b", months="%b %Y", years="%Y"
        )
        output_file("symbol_pnl.html")

        show(p)

    def generate_symbol_metrics(self):
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

        metrics = metrics_generator.generate_symbolwise_metrics()

        for symbol, symbol_metrics in metrics.items():
            print()
            print(f" === {symbol} ===")
            for title, value in symbol_metrics.items():
                if isinstance(value, float):
                    print(f"{title}: {value:.2f}")
                else:
                    print(f"{title}: {value}")

            print(" === END ===")

        self._generate_symbol_pnl_chart(equity_history=equity_history)

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

        data = equity_history["General"]

        sorted_items = sorted(data.items())
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
        output_file("general_chart.html")

        show(p)
