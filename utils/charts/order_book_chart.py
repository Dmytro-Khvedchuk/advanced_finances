# temporarily deprecated
from bokeh.plotting import figure, show, output_file
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter, Span, Div
from bokeh.layouts import column
from typing import Iterable, Tuple


def _can_fill_notional_at_best(
    levels: list[Tuple[float, float]], dollars: float
) -> bool:
    """Check if the first level alone can fill the notional."""
    if not levels:
        return False
    price, qty = levels[0]
    return (price * qty) >= dollars


def _can_fill_notional_across_book(
    levels: list[Tuple[float, float]], dollars: float
) -> bool:
    """Check if sum(price_i * qty_i) across levels can fill the notional."""
    need = dollars
    for price, qty in levels:
        take_notional = price * qty
        if take_notional >= need:
            return True
        need -= take_notional
    return False


def make_orderbook_bar_from_lists(
    bids: Iterable[Iterable[str]],
    asks: Iterable[Iterable[str]],
    *,
    title: str = "Order Book Depth — BTCUSDT",
    width: int = 1280,
    height: int = 720,
    top_levels: int = 25,
):
    # --- prepare & sort ---
    bid_pairs = [(float(p), float(q)) for p, q in bids]
    ask_pairs = [(float(p), float(q)) for p, q in asks]

    # IMPORTANT: bids sorted DESC, asks ASC
    bid_pairs = sorted(bid_pairs, key=lambda x: x[0], reverse=True)[:top_levels]
    ask_pairs = sorted(ask_pairs, key=lambda x: x[0])[:top_levels]

    # best prices / mid / spread
    best_bid = max((p for p, _ in bid_pairs), default=float("nan"))
    best_ask = min((p for p, _ in ask_pairs), default=float("nan"))
    mid = (best_bid + best_ask) / 2 if (bid_pairs and ask_pairs) else float("nan")
    spread = (best_ask - best_bid) if (bid_pairs and ask_pairs) else float("nan")

    # sources
    bid_src = ColumnDataSource(
        dict(
            price=[p for p, _ in bid_pairs],
            qty=[-q for _, q in bid_pairs],  # negative → left
            abs_qty=[q for _, q in bid_pairs],
        )
    )
    ask_src = ColumnDataSource(
        dict(
            price=[p for p, _ in ask_pairs],
            qty=[q for _, q in ask_pairs],  # positive → right
            abs_qty=[q for _, q in ask_pairs],
        )
    )

    # figure
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

    # bars (keep outlines strong)
    p.hbar(
        y="price",
        right="qty",
        height=0.8,
        source=bid_src,
        fill_alpha=0.7,
        fill_color="#2ca02c",
        line_color="#0b3d0b",
        line_width=2,
        legend_label="Bids",
    )
    p.hbar(
        y="price",
        right="qty",
        height=0.8,
        source=ask_src,
        fill_alpha=0.7,
        fill_color="#d62728",
        line_color="#5c0000",
        line_width=2,
        legend_label="Asks",
    )

    # axes ranges
    all_prices = bid_src.data["price"] + ask_src.data["price"]
    if all_prices:
        ymin, ymax = min(all_prices), max(all_prices)
        pad = (ymax - ymin) * 0.02 if ymax > ymin else 1.0
        p.y_range.start = ymin - pad
        p.y_range.end = ymax + pad
        left_min = min(bid_src.data["qty"], default=0)
        right_max = max(ask_src.data["qty"], default=0)
        p.x_range.start = min(left_min, 0) * 1.05
        p.x_range.end = max(right_max, 0) * 1.05

    # hover
    hover: HoverTool = p.select_one(HoverTool)
    hover.tooltips = [("Price", "@price{0,0.00}"), ("Qty", "@abs_qty{0,0.######}")]
    hover.mode = "mouse"
    p.legend.location = "top_left"
    p.legend.click_policy = "hide"

    # --- HIGH-VIS best bid/ask lines + labels ---
    if bid_pairs:
        p.add_layout(
            Span(
                location=best_bid,
                dimension="width",
                line_color="#0b3d0b",
                line_width=3,
                line_alpha=1.0,
            )
        )
    if ask_pairs:
        p.add_layout(
            Span(
                location=best_ask,
                dimension="width",
                line_color="#5c0000",
                line_width=3,
                line_alpha=1.0,
            )
        )

    # place labels after ranges are set, so we know x-range
    if all_prices:
        xr = p.x_range.end - p.x_range.start
        x_left = p.x_range.start + xr * 0.01
        x_right = p.x_range.end - xr * 0.20  # keep text inside frame

        if bid_pairs:
            p.add_layout(
                Label(
                    x=x_left,
                    y=best_bid,
                    text=f"Best Bid {best_bid:,.2f}",
                    text_color="#0b3d0b",
                    text_font_style="bold",
                    text_alpha=0.95,
                    x_offset=0,
                    y_offset=0,
                )
            )
        if ask_pairs:
            p.add_layout(
                Label(
                    x=x_right,
                    y=best_ask,
                    text=f"Best Ask {best_ask:,.2f}",
                    text_color="#5c0000",
                    text_font_style="bold",
                    text_alpha=0.95,
                    x_offset=0,
                    y_offset=0,
                )
            )

    # header info
    hdr = Div(
        text=(
            (
                f"<b>Best Bid:</b> {best_bid:,.2f} &nbsp; "
                f"<b>Best Ask:</b> {best_ask:,.2f} &nbsp; "
                f"<b>Mid:</b> {mid:,.2f} &nbsp; "
                f"<b>Spread:</b> {spread:,.2f}"
            )
            if (bid_pairs and ask_pairs)
            else "<i>No data</i>"
        ),
        width=width,
    )

    output_file("orderbook_depth_1280x720.html", title="Order Book Depth 1280x720")
    show(column(hdr, p))
