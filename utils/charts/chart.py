from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter
from bokeh.models import FixedTicker
from collections import defaultdict
from math import floor
from typing import Iterable, Tuple
from utils.charts.candle_chart import plot_candles_bokeh_pl


class Chart:
    def __init__(self):
        pass

    def chart_picker(self):
        pass

    def chart_candles(self, data):
        plot_candles_bokeh_pl(data)


# temporarily deprecated
class OrderbookChart:
    def __init__(self, width=1280, height=720, title="Order Book Depth"):
        self.bid_src = ColumnDataSource(dict(price=[], qty=[], abs_qty=[]))
        self.ask_src = ColumnDataSource(dict(price=[], qty=[], abs_qty=[]))
        self.lower = None
        self.upper = None

        tools = "pan,wheel_zoom,reset,save,hover"
        p = figure(
            title=title,
            width=width,
            height=height,
            x_axis_label="Quantity",
            y_axis_label="Price",
            tools=tools,
            toolbar_location="right",
            active_scroll="wheel_zoom",
        )
        p.yaxis.formatter = NumeralTickFormatter(format="0,0.00")
        p.xaxis.formatter = NumeralTickFormatter(format="0,0")
        p.grid.grid_line_alpha = 0.3
        p.outline_line_alpha = 0.6

        p.hbar(
            y="price",
            right="qty",
            height=0.8,
            source=self.bid_src,
            fill_alpha=0.7,
            fill_color="#2ca02c",
            line_color="#1b5e20",
            line_width=1.5,
            legend_label="Bids",
        )
        p.hbar(
            y="price",
            right="qty",
            height=0.8,
            source=self.ask_src,
            fill_alpha=0.7,
            fill_color="#d62728",
            line_color="#7f0000",
            line_width=1.5,
            legend_label="Asks",
        )

        hover = p.select_one(HoverTool)
        hover.tooltips = [("Price", "@price{0,0.00}"), ("Qty", "@abs_qty{0,0.######}")]
        p.legend.location = "top_left"
        p.legend.click_policy = "hide"

        self.figure = p

    def set_data_from_lists(self, bids_list, asks_list, top_levels=25):
        # lists like [['price','qty'], ...] as strings or numbers
        bids = [(float(p), float(q)) for p, q in bids_list]
        asks = [(float(p), float(q)) for p, q in asks_list]

        # 1) Bin to $1 buckets and aggregate quantities per bucket
        bid_bins = defaultdict(float)
        ask_bins = defaultdict(float)

        for p, q in bids:
            bucket = floor(p)  # $[n, n+1) bin
            bid_bins[bucket] += q

        for p, q in asks:
            bucket = floor(p)
            ask_bins[bucket] += q

        # 2) Turn into sorted lists (price axis goes down for bids, up for asks)
        bid_items = sorted(bid_bins.items(), key=lambda x: x[0], reverse=True)[
            :top_levels
        ]
        ask_items = sorted(ask_bins.items(), key=lambda x: x[0])[:top_levels]

        # Use bucket centers (n + 0.5) for nicer spacing; height < 1 so bars don't touch
        bid_prices = [b + 0.5 for b, _ in bid_items]
        ask_prices = [b + 0.5 for b, _ in ask_items]
        bid_qtys = [-q for _, q in bid_items]  # negative to extend left
        ask_qtys = [q for _, q in ask_items]  # positive to extend right
        bid_abs = [abs(q) for q in bid_qtys]
        ask_abs = [abs(q) for q in ask_qtys]

        self.bid_src.data = {"price": bid_prices, "qty": bid_qtys, "abs_qty": bid_abs}
        self.ask_src.data = {"price": ask_prices, "qty": ask_qtys, "abs_qty": ask_abs}

        # 3) Set ranges & 1-dollar tick grid
        all_buckets = []
        if bid_items:
            all_buckets += [b for b, _ in bid_items]
        if ask_items:
            all_buckets += [b for b, _ in ask_items]

        if all_buckets:
            min_b = min(all_buckets)
            max_b = max(all_buckets)
            # y shows bucket centers, so pad by 0.5 to include full bar heights
            self.figure.y_range.start = (min_b + 0.5) - 0.6
            self.figure.y_range.end = (max_b + 0.5) + 0.6
            # 1-dollar ticks
            ticks = list(range(min_b, max_b + 1))
            self.figure.yaxis.ticker = FixedTicker(ticks=[t + 0.5 for t in ticks])

        # x-range based on aggregated bars
        left_min = min(bid_qtys, default=0)
        right_max = max(ask_qtys, default=0)
        self.figure.x_range.start = min(left_min, 0) * 1.05
        self.figure.x_range.end = max(right_max, 0) * 1.05

    def set_windowed_data(
        self,
        bids_list,
        asks_list,
        *,
        lower: float,
        upper: float,
        bucket_size: float = 1.0,
    ):
        """Render bids/asks in a fixed price window with identical price buckets."""
        self.lower, self.upper = lower, upper

        bids = [(float(p), float(q)) for p, q in bids_list]
        asks = [(float(p), float(q)) for p, q in asks_list]

        prices, bid_qtys, ask_qtys = _bin_orderbook_to_window(
            bids, asks, lower=lower, upper=upper, bucket_size=bucket_size
        )

        bid_abs = [abs(q) for q in bid_qtys]
        ask_abs = [abs(q) for q in ask_qtys]

        self.bid_src.data = {"price": prices, "qty": bid_qtys, "abs_qty": bid_abs}
        self.ask_src.data = {"price": prices, "qty": ask_qtys, "abs_qty": ask_abs}

        # Y range and ticks fixed to the window
        if prices:
            # buckets are centered; pad slightly so bars are fully visible
            self.figure.y_range.start = lower + (bucket_size / 2.0) - 0.6
            self.figure.y_range.end = upper - (bucket_size / 2.0) + 0.6

            # $1 ticks (or bucket_size) aligned to centers
            start_tick = floor(lower / bucket_size)
            end_tick = floor((upper - 1e-9) / bucket_size)
            ticks = [
                t * bucket_size + (bucket_size / 2.0)
                for t in range(start_tick, end_tick + 1)
            ]
            self.figure.yaxis.ticker = FixedTicker(ticks=ticks)

        # X range autoscale to content (symmetric padding)
        left_min = min(bid_qtys, default=0.0)
        right_max = max(ask_qtys, default=0.0)
        self.figure.x_range.start = min(left_min, 0) * 1.05
        self.figure.x_range.end = max(right_max, 0) * 1.05


def _bin_orderbook_to_window(
    bids: Iterable[Tuple[float, float]],
    asks: Iterable[Tuple[float, float]],
    lower: float,
    upper: float,
    bucket_size: float = 1.0,
):
    """
    Bin bids/asks into identical price buckets in [lower, upper).
    Returns (prices, bid_qtys, ask_qtys) with equal lengths.
    """
    if lower >= upper:
        return [], [], []

    # normalize window to bucket indexes
    start_bucket = floor(lower / bucket_size)
    end_bucket = floor((upper - 1e-9) / bucket_size)  # inclusive last bucket

    bid_bins = defaultdict(float)
    ask_bins = defaultdict(float)

    for p, q in bids:
        if lower <= p < upper:
            b = floor(p / bucket_size)
            bid_bins[b] += q
    for p, q in asks:
        if lower <= p < upper:
            b = floor(p / bucket_size)
            ask_bins[b] += q

    buckets = list(range(start_bucket, end_bucket + 1))
    prices = [(b * bucket_size) + (bucket_size / 2.0) for b in buckets]
    bid_qty = [-(bid_bins.get(b, 0.0)) for b in buckets]  # negative → left
    ask_qty = [+(ask_bins.get(b, 0.0)) for b in buckets]  # positive → right
    return prices, bid_qty, ask_qty
