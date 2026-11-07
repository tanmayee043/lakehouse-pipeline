"""Crypto page: does Project 1's live capture agree with the exchange's official record?"""

import altair as alt
import pandas as pd
import streamlit as st

import queries

# Reference data-viz palette. Candle direction also shows in the legend and tooltip, not colour alone.
UP_COLOR = "#2a78d6"  # categorical slot 1, blue
DOWN_COLOR = "#e34948"  # slot 8, red
VOLUME_COLOR = "#2a78d6"
ANOMALY_COLORS = {"Price": "#2a78d6", "Volume": "#eb6834", "Both": "#1baf7a"}  # slots 1-3, fixed order

STATUS_LABELS = {
    "complete": "✅ Complete",
    "gaps_filled": "🩹 Gaps filled",
    "downtime_filled": "🔌 Downtime filled",
    "disagrees": "⚠️ Disagrees",
    "not_captured": "⏸️ Not captured",
    "stream_not_extracted": "⏳ Not copied yet",
    "awaiting_official_record": "⏳ Awaiting official record",
}
GAP_STATUS_LABELS = {
    "filled": "✅ Filled",
    "partly_filled": "🩹 Partly filled (rest is on a day not fetched yet)",
    "awaiting_official_record": "⏳ Waiting for that day's record",
}
STATUS_HELP = (
    "Complete: every trade matched. Gaps filled: Project 1 lost trades while running. Downtime filled: "
    "it was switched off for part of the day. Both are filled from the official record. Disagrees: a "
    "captured price or quantity differs from the record. Not captured: Project 1 didn't run that day."
)


@st.cache_data(ttl=600, show_spinner="Loading reconciliation…")
def reconciliation() -> pd.DataFrame:
    return queries.load_reconciliation()


@st.cache_data(ttl=600, show_spinner="Loading prices…")
def ohlcv(symbol: str) -> pd.DataFrame:
    return queries.load_ohlcv(symbol)


@st.cache_data(ttl=600)
def symbols() -> list[str]:
    return queries.load_symbols()


@st.cache_data(ttl=600, show_spinner="Loading gaps…")
def gaps() -> pd.DataFrame:
    return queries.load_gaps()


def day_axis() -> alt.X:
    """One band per UTC day, so daily bars line up with a single label each."""
    return alt.X(
        "yearmonthdate(trade_date):O", title=None, axis=alt.Axis(formatType="time", format="%b %d", labelAngle=0)
    )


def price_text(price: float) -> str:
    """Two decimals for prices like BTC's; four for small ones like XRP's."""
    return f"{price:,.2f}" if price >= 100 else f"{price:,.4f}"


def no_crypto_data(error: Exception) -> None:
    st.info(
        "No crypto data yet. In Dagster, materialize **daily_crypto_pipeline** for a day (or backfill a range), "
        f"then refresh this page.\n\nDetails: {error}",
        icon="ℹ️",
    )
    st.stop()


def candle_chart(df: pd.DataFrame) -> alt.LayerChart:
    data = df.assign(direction=["Up" if c >= o else "Down" for o, c in zip(df.open_price, df.close_price, strict=True)])
    color = alt.Color(
        "direction:N",
        scale=alt.Scale(domain=["Up", "Down"], range=[UP_COLOR, DOWN_COLOR]),
        legend=alt.Legend(title=None, orient="top", direction="horizontal"),
    )
    x = day_axis()
    tooltip = [
        alt.Tooltip("trade_date:T", title="Day (UTC)", format="%a %b %d"),
        alt.Tooltip("open_price:Q", title="Open", format=",.8~f"),
        alt.Tooltip("high_price:Q", title="High", format=",.8~f"),
        alt.Tooltip("low_price:Q", title="Low", format=",.8~f"),
        alt.Tooltip("close_price:Q", title="Close", format=",.8~f"),
        alt.Tooltip("vwap:Q", title="VWAP", format=",.8~f"),
        alt.Tooltip("trade_count:Q", title="Trades", format=","),
        alt.Tooltip("direction:N", title="Direction"),
    ]
    base = alt.Chart(data).encode(x=x, color=color, tooltip=tooltip)
    wick = base.mark_rule(strokeWidth=1.5).encode(
        y=alt.Y("low_price:Q", title="USDT", scale=alt.Scale(zero=False)), y2="high_price:Q"
    )
    body = base.mark_bar(width=alt.RelativeBandSize(0.6)).encode(y="open_price:Q", y2="close_price:Q")
    return alt.LayerChart(layer=[wick, body], title="Daily price (official record)", height=280)


def volume_chart(df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_bar(color=VOLUME_COLOR, width=alt.RelativeBandSize(0.6), cornerRadiusEnd=2)
        .encode(
            x=day_axis(),
            y=alt.Y("quote_volume:Q", title="USDT", axis=alt.Axis(format="~s")),
            tooltip=[
                alt.Tooltip("trade_date:T", title="Day (UTC)", format="%a %b %d"),
                alt.Tooltip("quote_volume:Q", title="Volume (USDT)", format=",.0f"),
                alt.Tooltip("trade_count:Q", title="Trades", format=","),
            ],
        )
        .properties(title="Daily volume", height=200)
    )


def anomaly_chart(df: pd.DataFrame) -> alt.Chart:
    long = df.melt(
        id_vars=["trade_date"],
        value_vars=["price_anomalies", "volume_anomalies", "both_anomalies"],
        var_name="type",
        value_name="count",
    )
    long["type"] = long["type"].map(
        {"price_anomalies": "Price", "volume_anomalies": "Volume", "both_anomalies": "Both"}
    )
    return (
        alt.Chart(long)
        .mark_bar(width=alt.RelativeBandSize(0.6), stroke="white", strokeWidth=1)
        .encode(
            x=day_axis(),
            y=alt.Y("count:Q", title="Anomalies", axis=alt.Axis(tickMinStep=1)),
            color=alt.Color(
                "type:N",
                scale=alt.Scale(domain=list(ANOMALY_COLORS), range=list(ANOMALY_COLORS.values())),
                legend=alt.Legend(title=None, orient="top", direction="horizontal"),
            ),
            order=alt.Order("type:N"),
            tooltip=[
                alt.Tooltip("trade_date:T", title="Day (UTC)", format="%a %b %d"),
                alt.Tooltip("type:N", title="Type"),
                alt.Tooltip("count:Q", title="Anomalies"),
            ],
        )
        .properties(title="Anomalies Project 1 flagged live", height=200)
    )


# ---------- page ----------

st.title("Crypto")
st.caption(
    "Project 1's live capture of Binance.US trades, checked against the exchange's official record. "
    "Days are UTC; the lakehouse only reads Project 1, never changes it."
)

try:
    recon = reconciliation()
except queries.WarehouseUnavailable as e:
    no_crypto_data(e)

if recon.empty:
    st.info("The crypto tables exist but hold no days yet. Run **daily_crypto_pipeline** in Dagster.", icon="ℹ️")
    st.stop()

recon_tab, market_tab, gaps_tab = st.tabs(["Reconciliation", "Market", "Gaps"])

# ----- Reconciliation -----
with recon_tab:
    captured = recon[recon["captured_trades"] > 0]
    in_sessions = captured["official_in_sessions"].sum()
    lost = int(captured["missed_while_running"].sum())
    completeness = (in_sessions - lost) / in_sessions if in_sessions else None
    disagreements = int((recon["price_mismatches"] + recon["quantity_mismatches"] + recon["stream_only_trades"]).sum())

    # Headline numbers first, context second: two rows stay readable on narrow screens.
    k1, k2, k3 = st.columns(3)
    k1.metric(
        "Completeness",
        f"{completeness:.2%}" if completeness is not None else "—",
        help="Share of official trades Project 1 captured while it was running (inside its capture sessions).",
    )
    k2.metric(
        "Lost while running",
        f"{'⚠️ ' if lost else '✅ '}{lost:,}",
        help="Trades Project 1 missed inside a capture session: real capture losses. Filled from the record.",
    )
    k4, k5, _ = st.columns(3)
    k4.metric(
        "Downtime filled",
        f"{int(captured['missed_during_downtime'].sum()):,}",
        help="Trades from stretches when Project 1 was switched off (no capture for over 5 minutes), "
        "filled from the official record.",
    )
    k3.metric(
        "Disagreements",
        f"{'⚠️ ' if disagreements else '✅ '}{disagreements:,}",
        help="Captured trades whose price or quantity differs from the official record, or that it doesn't have.",
    )
    k5.metric("Days captured", f"{captured['trade_date'].nunique()} / {recon['trade_date'].nunique()}")

    st.dataframe(
        recon.assign(
            status=recon["status"].map(STATUS_LABELS).fillna(recon["status"]),
            completeness_pct=[f"{v:.2f}%" if pd.notna(v) else "—" for v in recon["completeness_pct"]],
        )[
            [
                "trade_date",
                "symbol",
                "status",
                "completeness_pct",
                "day_coverage_pct",
                "official_trades",
                "captured_trades",
                "capture_sessions",
                "missed_while_running",
                "missed_during_downtime",
                "price_mismatches",
                "quantity_mismatches",
            ]
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "trade_date": st.column_config.DateColumn("Day (UTC)", format="ddd MMM D"),
            "symbol": "Symbol",
            "status": st.column_config.TextColumn("Status", help=STATUS_HELP),
            "completeness_pct": st.column_config.TextColumn(
                "Completeness", help="Inside Project 1's capture sessions that day; — when it didn't run"
            ),
            "day_coverage_pct": st.column_config.NumberColumn(
                "Day coverage", format="%.1f%%", help="Over the whole UTC day (low when Project 1 wasn't running)"
            ),
            "official_trades": st.column_config.NumberColumn("Official", format="%d"),
            "captured_trades": st.column_config.NumberColumn("Captured", format="%d"),
            "capture_sessions": st.column_config.NumberColumn("Sessions", format="%d"),
            "missed_while_running": st.column_config.NumberColumn("Lost running", format="%d"),
            "missed_during_downtime": st.column_config.NumberColumn("Downtime", format="%d"),
            "price_mismatches": st.column_config.NumberColumn("Price diffs", format="%d"),
            "quantity_mismatches": st.column_config.NumberColumn("Qty diffs", format="%d"),
        },
    )

# ----- Market -----
with market_tab:
    try:
        symbol_list = symbols()
    except queries.WarehouseUnavailable as e:
        no_crypto_data(e)
    symbol = st.selectbox("Symbol", symbol_list) or symbol_list[0]
    df = ohlcv(symbol)

    latest = df.iloc[-1]
    day = f"{pd.Timestamp(latest['trade_date']):%b %d}"
    # Two per row: full prices like 112,957.19 don't fit four across on narrow screens.
    m1, m2 = st.columns(2)
    m1.metric(f"Close · {day}", price_text(latest["close_price"]), help="USDT")
    m2.metric("Trades", f"{int(latest['trade_count']):,}")
    m3, m4 = st.columns(2)
    m3.metric("High", price_text(latest["high_price"]), help="USDT")
    m4.metric("Low", price_text(latest["low_price"]), help="USDT")

    st.altair_chart(candle_chart(df), width="stretch")
    left, right = st.columns(2)
    left.altair_chart(volume_chart(df), width="stretch")
    if df["captured_trades"].notna().any():
        right.altair_chart(anomaly_chart(df[df["captured_trades"].notna()]), width="stretch")
    else:
        right.info("Project 1 didn't capture this symbol on any of these days, so there are no live anomaly flags.")

# ----- Gaps -----
with gaps_tab:
    try:
        gap_df = gaps()
    except queries.WarehouseUnavailable as e:
        no_crypto_data(e)
    if gap_df.empty:
        st.success("Project 1 hasn't reported any gaps.", icon="✅")
    else:
        filled = int(gap_df["filled_by_lakehouse"].sum())
        st.caption(
            f"{filled} of {len(gap_df)} gaps filled from the official record. Project 1 still lists them as open: "
            "the lakehouse reads Project 1 but never writes back."
        )
        st.dataframe(
            gap_df.assign(
                lakehouse=gap_df["fill_status"].map(GAP_STATUS_LABELS).fillna(gap_df["fill_status"]),
                project1=["Resolved" if pd.notna(r) else "Open" for r in gap_df["resolved_in_project1_at_utc"]],
            )[
                [
                    "symbol",
                    "trade_date",
                    "first_missing_id",
                    "last_missing_id",
                    "missing_count",
                    "lakehouse",
                    "project1",
                ]
            ],
            hide_index=True,
            width="stretch",
            column_config={
                "symbol": "Symbol",
                "trade_date": st.column_config.DateColumn("Day (UTC)", format="ddd MMM D"),
                "first_missing_id": st.column_config.NumberColumn("First missing ID", format="%d"),
                "last_missing_id": st.column_config.NumberColumn("Last missing ID", format="%d"),
                "missing_count": st.column_config.NumberColumn("Trades", format="%d"),
                "lakehouse": "Lakehouse",
                "project1": "In Project 1",
            },
        )
