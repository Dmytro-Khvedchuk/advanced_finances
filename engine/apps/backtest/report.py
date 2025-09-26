from bokeh.palettes import Category10
from bokeh.plotting import figure, output_file, show
from bokeh.models import ColumnDataSource, DatetimeTickFormatter
from engine.apps.backtest.analytics.metrics import MetricsGenerator
from copy import deepcopy
from utils.logger.logger import LoggerWrapper, log_execution

from bokeh.io import export_png
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from copy import deepcopy
from bokeh.palettes import Category10
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, DatetimeTickFormatter
import matplotlib.pyplot as plt
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.utils import ImageReader
from io import BytesIO


class ReportGenerator:
    def __init__(self, portfolio, log_level: int = 10):
        self.logger = LoggerWrapper(name="Report Generator Module", level=log_level)
        self.portfolio = portfolio
        self.log_level = log_level
        self.general_metrics = None
        self.symbol_wise_metrics = None

    @log_execution
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

    @log_execution
    def generate_symbol_metrics(self):
        (
            equity_history,
            trade_history,
            order_history,
            current_positions,
            initial_balance,
        ) = self.portfolio.get_metrics()

        metrics_generator = MetricsGenerator(
            equity_history=equity_history,
            trade_history=trade_history,
            order_history=order_history,
            current_positions=current_positions,
            initial_balance=initial_balance,
            log_level=self.log_level,
        )

        self.symbol_wise_metrics = metrics_generator.generate_symbolwise_metrics()

        for symbol, symbol_metrics in self.symbol_wise_metrics.items():
            print()
            print(f" === {symbol} ===")
            for title, value in symbol_metrics.items():
                if isinstance(value, float):
                    print(f"{title}: {value:.2f}")
                else:
                    print(f"{title}: {value}")

            print(" === END ===")

        self._generate_symbol_pnl_chart(equity_history=equity_history)

    @log_execution
    def generate_general_metrics(self):
        (
            equity_history,
            trade_history,
            order_history,
            current_positions,
            initial_balance,
        ) = self.portfolio.get_metrics()

        metrics_generator = MetricsGenerator(
            equity_history=equity_history,
            trade_history=trade_history,
            order_history=order_history,
            current_positions=current_positions,
            initial_balance=initial_balance,
            log_level=self.log_level,
        )

        self.general_metrics = metrics_generator.generate_general_metrics()

        print(" === GENERAL METRICS ===")

        for title, value in self.general_metrics.items():
            print(f"{title}: {value:.2f}")

        print(" === END ===")

        self._generate_general_chart(equity_history=equity_history)

    @log_execution
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

    def generate_pdf_report(
        self, strategy_name: str, output_file_path="strategy_report.pdf"
    ):
        print("🚀 Starting PDF report generation")

        # --------------------------
        # 0. Get metrics
        # --------------------------
        try:
            (
                equity_history,
                trade_history,
                order_history,
                current_positions,
                initial_balance,
            ) = self.portfolio.get_metrics()
            general_metrics = self.general_metrics
            symbol_metrics = self.symbol_wise_metrics
            print("✅ Metrics retrieved")
        except Exception as e:
            print(f"⚠️ Failed to retrieve metrics: {e}")
            return

        # --------------------------
        # 1. Generate charts as in-memory images
        # --------------------------
        chart_images = {}

        try:
            general_data = equity_history.get("General", {})
            if general_data:
                x, y = zip(*sorted(general_data.items()))
                plt.figure(figsize=(16, 8))
                plt.plot(x, y, color="navy", linewidth=2)
                plt.title("General Equity")
                plt.xlabel("Time")
                plt.ylabel("Equity")
                buf = BytesIO()
                plt.savefig(buf, format="png")
                buf.seek(0)
                chart_images["general"] = buf
                plt.close()
                print("✅ General equity chart created")
        except Exception as e:
            print(f"⚠️ Failed to create general equity chart: {e}")

        try:
            symbol_data = deepcopy(equity_history)
            symbol_data.pop("General", None)
            if symbol_data:
                colors = plt.cm.tab10.colors
                plt.figure(figsize=(16, 8))
                for i, (symbol, data) in enumerate(symbol_data.items()):
                    x_sym, y_sym = zip(*sorted(data.items()))
                    plt.plot(
                        x_sym,
                        y_sym,
                        color=colors[i % len(colors)],
                        label=symbol,
                        linewidth=2,
                    )
                plt.title("Symbol-wise PnL")
                plt.xlabel("Time")
                plt.ylabel("PnL")
                plt.legend()
                buf = BytesIO()
                plt.savefig(buf, format="png")
                buf.seek(0)
                chart_images["symbol"] = buf
                plt.close()
                print("✅ Symbol PnL chart created")
        except Exception as e:
            print(f"⚠️ Failed to create symbol PnL chart: {e}")

        # --------------------------
        # 2. Identify best/worst performers
        # --------------------------
        try:
            best_pnl = max(
                symbol_metrics.items(), key=lambda x: x[1].get("Total PnL", 0)
            )
            worst_pnl = min(
                symbol_metrics.items(), key=lambda x: x[1].get("Total PnL", 0)
            )
            print("✅ Best/Worst performers identified")
        except Exception as e:
            print(f"⚠️ Failed to calculate best/worst performers: {e}")
            return

        # --------------------------
        # 3. Generate PDF
        # --------------------------
        try:
            c = canvas.Canvas(output_file_path, pagesize=letter)
            width, height = letter
            print("✅ PDF canvas created")
        except Exception as e:
            print(f"⚠️ Failed to create PDF canvas: {e}")
            return

        # --------------------------
        # Page 1: Title + general metrics
        # --------------------------
        y_pos = height - 50
        c.setFont("Helvetica-Bold", 18)
        c.drawString(50, y_pos, f"BACKTEST REPORT FOR STRATEGY: {strategy_name}")
        y_pos -= 40

        # General metrics
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y_pos, "General Metrics:")
        y_pos -= 20
        c.setFont("Helvetica", 12)
        for k, v in general_metrics.items():
            try:
                c.drawString(50, y_pos, f"{k}: {float(v):.2f}")
            except (ValueError, TypeError):
                c.drawString(50, y_pos, f"{k}: {v}")
            y_pos -= 15
        y_pos -= 10

        # Best/Worst performers
        c.setFont("Helvetica-Bold", 14)
        c.drawString(
            50,
            y_pos,
            f"Best PnL: {best_pnl[0]} ({best_pnl[1].get('Total PnL', 0):.2f}) | "
            f"Worst PnL: {worst_pnl[0]} ({worst_pnl[1].get('Total PnL', 0):.2f})",
        )

        c.showPage()  # ------------------- Page 2: Charts -------------------
        y_pos = height - 50
        for key in ["general", "symbol"]:
            if key in chart_images:
                img = ImageReader(chart_images[key])
                c.drawImage(img, 50, y_pos - 250, width=500, height=250)
                y_pos -= 280
            else:
                c.setFont("Helvetica-Oblique", 12)
                c.drawString(50, y_pos, f"{key.capitalize()} chart not available")
                y_pos -= 20

        c.showPage()  # ------------------- Page 3: Symbol-wise metrics -------------------
        y_pos = height - 50
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, y_pos, "Symbol-wise Metrics")
        y_pos -= 30

        for symbol, metrics in symbol_metrics.items():
            c.setFont("Helvetica-Bold", 12)
            c.drawString(50, y_pos, f"{symbol}:")
            y_pos -= 15
            c.setFont("Helvetica", 12)
            for k, v in metrics.items():
                try:
                    c.drawString(70, y_pos, f"{k}: {float(v):.2f}")
                except (ValueError, TypeError):
                    c.drawString(70, y_pos, f"{k}: {v}")
                y_pos -= 15
            y_pos -= 10
            if y_pos < 100:  # Start a new page if near bottom
                c.showPage()
                y_pos = height - 50

        c.save()
        print(f"✅ PDF report generated: {output_file_path}")
