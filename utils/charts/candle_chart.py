from bokeh.io import show, output_file
from bokeh.models import NumeralTickFormatter
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure
from numpy import arange, full_like, int64
from polars import DataFrame


def plot_candles_bokeh_pl(
    df: DataFrame,
    open_col: str = "open",
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    title: str = "Candlestick Chart",
):
    """
    Plots a standard barchart from OHLC data

    :param open_col: Name of the Open column
    :type open_col: str
    :param high_col: Name of the High column
    :type high_col: str
    :param low_col: Name of the Low column
    :type low_col: str
    :param close_col: Name of the Close column
    :type close_col: str
    :param title: Title of the plot
    :type title: str
    """
    required = [open_col, high_col, low_col, close_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {missing}")

    if df.height == 0:
        raise ValueError("Empty DataFrame.")

    bar_numbers = arange(len(df), dtype=int64)

    width_units = 1.0

    o = df.get_column(open_col).to_numpy()
    h = df.get_column(high_col).to_numpy()
    l = df.get_column(low_col).to_numpy()
    c = df.get_column(close_col).to_numpy()

    up = c >= o
    down = ~up

    common = dict(
        x=bar_numbers,
        width=full_like(bar_numbers, width_units),
        open=o,
        high=h,
        low=l,
        close=c,
    )
    inc_src = ColumnDataSource({k: v[up] for k, v in common.items()})
    dec_src = ColumnDataSource({k: v[down] for k, v in common.items()})
    wick_src = ColumnDataSource(dict(x=bar_numbers, high=h, low=l))

    p = figure(
        x_axis_type="linear",
        title=title,
        sizing_mode="stretch_width",
        height=1200,
        toolbar_location="right",
    )
    p.outline_line_color = None
    p.toolbar.autohide = True
    p.xaxis.axis_label = "Bar Number"

    p.segment(x0="x", y0="high", x1="x", y1="low", source=wick_src, line_width=1)
    p.yaxis.formatter = NumeralTickFormatter(format="0,0.00")
    p.vbar(
        x="x",
        width="width",
        top="close",
        bottom="open",
        source=inc_src,
        fill_color="#26a69a",
        line_color="#26a69a",
    )
    p.vbar(
        x="x",
        width="width",
        top="open",
        bottom="close",
        source=dec_src,
        fill_color="#ef5350",
        line_color="#ef5350",
    )

    hover = HoverTool(
        tooltips=[
            ("Bar #", "@x"),
            ("Open", "@open{0,0.0000}"),
            ("High", "@high{0,0.0000}"),
            ("Low", "@low{0,0.0000}"),
            ("Close", "@close{0,0.0000}"),
        ],
        mode="vline",
    )
    p.add_tools(hover)

    try:
        show(p)
    except ModuleNotFoundError:
        output_file("candles.html", title=title)
        show(p)
