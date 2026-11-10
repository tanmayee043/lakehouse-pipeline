"""Crypto page: does the Crypto project's live capture agree with the exchange's official record?"""

from datetime import datetime, timedelta, timezone

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
# Service level objectives: same targets as the dbt vars in dbt_project.yml (docs/slo.md).
SLO_WINDOW_DAYS = 30
SLO_COMPLETENESS_PCT = 99.9
SLO_RECONCILED_BY_UTC_HOUR = 3

STATUS_HELP = (
    "Complete: every trade matched. Gaps filled: the Crypto project lost trades while running. Downtime filled: "
    "it was switched off for part of the day. Both are filled from the official record. Disagrees: a "
    "captured price or quantity differs from the record. Not captured: the Crypto project didn't run that day."
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


def slo_row(recon: pd.DataFrame) -> None:
    """The three crypto SLOs from docs/slo.md, each met or missed with an icon and a label."""
    now = datetime.now(timezone.utc)
    yesterday = now.date() - timedelta(days=1)
    newest = pd.Timestamp(recon["trade_date"].max()).date()
    recent = recon[pd.to_datetime(recon["trade_date"]).dt.date >= now.date() - timedelta(days=SLO_WINDOW_DAYS)]
    below = recent[recent["completeness_pct"] < SLO_COMPLETENESS_PCT]
    disagreements = recent["price_mismatches"] + recent["quantity_mismatches"] + recent["stream_only_trades"]
    disagreeing = recent[disagreements > 0]

    due = now.hour >= SLO_RECONCILED_BY_UTC_HOUR
    checks = [
        (
            "Reconciled through yesterday",
            newest >= yesterday or not due,
            f"newest day {newest:%b %d}" + ("" if due else f" (due {SLO_RECONCILED_BY_UTC_HOUR:02d}:00 UTC)"),
        ),
        (
            f"Completeness ≥ {SLO_COMPLETENESS_PCT}%",
            below.empty,
            f"last {SLO_WINDOW_DAYS} days" if below.empty else f"{len(below)} symbol-days below",
        ),
        (
            "No disagreements",
            disagreeing.empty,
            f"last {SLO_WINDOW_DAYS} days" if disagreeing.empty else f"{len(disagreeing)} symbol-days disagree",
        ),
    ]
    st.caption("Service level objectives")
    for column, (name, met, detail) in zip(st.columns(3), checks, strict=True):
        column.markdown(f"{'✅' if met else '⚠️'} **{name}** · {'Met' if met else 'Missed'}")
        column.caption(detail)


# Published Iceberg tables, their row keys, and how they're labelled.
TRAVEL_TABLES = {
    "fct_reconciliation_daily": ("Reconciliation", ["symbol", "trade_date"]),
    "fct_ohlcv_daily": ("Daily prices", ["symbol", "trade_date"]),
    "fct_anomalies_daily": ("Anomalies", ["symbol", "trade_date"]),
    "fct_trade_gaps": ("Gaps", ["symbol", "first_missing_id"]),
}


@st.cache_data(ttl=600, show_spinner="Loading snapshots…")
def snapshot_list(name: str) -> list[tuple[int, str, int]]:
    return [(s.snapshot_id, f"{s.committed_at:%b %d, %H:%M} UTC", s.rows) for s in queries.load_snapshots(name)]


@st.cache_data(show_spinner="Reading snapshot…")  # a snapshot never changes, so no expiry needed
def snapshot_rows(name: str, snapshot_id: int) -> pd.DataFrame:
    return queries.load_snapshot(name, snapshot_id)


def compare(then: pd.DataFrame, now: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    """Rows added, removed or changed between two versions of a table, matched on its key."""
    both = then.merge(now, on=key, how="outer", suffixes=(" then", " now"), indicator=True)
    values = [c for c in then.columns if c not in key]
    changed_cols = []
    for _, row in both.iterrows():
        if row["_merge"] != "both":
            changed_cols.append("")
            continue
        diff = [c for c in values if not (pd.isna(row[f"{c} then"]) and pd.isna(row[f"{c} now"]))
                and row[f"{c} then"] != row[f"{c} now"]]  # fmt: skip
        changed_cols.append(", ".join(diff))
    both["change"] = [
        {"left_only": "Removed", "right_only": "Added"}.get(m, "Changed" if cols else "")
        for m, cols in zip(both["_merge"], changed_cols, strict=True)
    ]
    both["columns changed"] = changed_cols
    return both[both["change"] != ""][[*key, "change", "columns changed"]]


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
        .properties(title="Anomalies the Crypto project flagged live", height=200)
    )


# ---------- page ----------

st.title("Crypto")
st.caption(
    "The Crypto project's live capture of Binance.US trades, checked against the exchange's official record. "
    "Days are UTC; the lakehouse only reads the Crypto project, never changes it."
)

try:
    recon = reconciliation()
except queries.WarehouseUnavailable as e:
    no_crypto_data(e)

if recon.empty:
    st.info("The crypto tables exist but hold no days yet. Run **daily_crypto_pipeline** in Dagster.", icon="ℹ️")
    st.stop()

recon_tab, market_tab, gaps_tab, travel_tab = st.tabs(["Reconciliation", "Market", "Gaps", "Time travel"])

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
        help="Share of official trades the Crypto project captured while it was running (inside its capture sessions).",
    )
    k2.metric(
        "Lost while running",
        f"{'⚠️ ' if lost else '✅ '}{lost:,}",
        help="Trades the Crypto project missed inside a capture session: real capture losses. Filled from the record.",
    )
    k4, k5, _ = st.columns(3)
    k4.metric(
        "Downtime filled",
        f"{int(captured['missed_during_downtime'].sum()):,}",
        help="Trades from stretches when the Crypto project was switched off (no capture for over 5 minutes), "
        "filled from the official record.",
    )
    k3.metric(
        "Disagreements",
        f"{'⚠️ ' if disagreements else '✅ '}{disagreements:,}",
        help="Captured trades whose price or quantity differs from the official record, or that it doesn't have.",
    )
    k5.metric("Days captured", f"{captured['trade_date'].nunique()} / {recon['trade_date'].nunique()}")

    slo_row(recon)

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
                "Completeness", help="Inside the Crypto project's capture sessions that day; — when it didn't run"
            ),
            "day_coverage_pct": st.column_config.NumberColumn(
                "Day coverage",
                format="%.1f%%",
                help="Over the whole UTC day (low when the Crypto project wasn't running)",
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
        right.info(
            "The Crypto project didn't capture this symbol on any of these days, so there are no live anomaly flags."
        )

# ----- Gaps -----
with gaps_tab:
    try:
        gap_df = gaps()
    except queries.WarehouseUnavailable as e:
        no_crypto_data(e)
    if gap_df.empty:
        st.success("The Crypto project hasn't reported any gaps.", icon="✅")
    else:
        filled = int(gap_df["filled_by_lakehouse"].sum())
        st.caption(
            f"{filled} of {len(gap_df)} gaps filled from the official record. "
            "The Crypto project still lists them as open: the lakehouse reads it but never writes back."
        )
        st.dataframe(
            gap_df.assign(
                lakehouse=gap_df["fill_status"].map(GAP_STATUS_LABELS).fillna(gap_df["fill_status"]),
                crypto_project=[
                    "Resolved" if pd.notna(r) else "Open" for r in gap_df["resolved_in_crypto_project_at_utc"]
                ],
            )[
                [
                    "symbol",
                    "trade_date",
                    "first_missing_id",
                    "last_missing_id",
                    "missing_count",
                    "lakehouse",
                    "crypto_project",
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
                "crypto_project": "Crypto project",
            },
        )

# ----- Time travel -----
with travel_tab:
    st.caption(
        "Each pipeline run that changes a table adds an Iceberg snapshot. Pick one to see the table as it was then, "
        "and what has changed since."
    )
    name = (
        st.selectbox("Table", list(TRAVEL_TABLES), format_func=lambda n: TRAVEL_TABLES[n][0])
        or "fct_reconciliation_daily"
    )
    try:
        versions = snapshot_list(name)
    except queries.WarehouseUnavailable as e:
        st.info(str(e), icon="ℹ️")
        st.stop()

    labels = {sid: f"{when} · {rows:,} rows" for sid, when, rows in versions}
    # Default to the previous version, so there's something to compare with when there is one.
    default = 1 if len(versions) > 1 else 0
    chosen = st.selectbox("As of", list(labels), index=default, format_func=lambda sid: labels[sid])
    chosen = chosen if chosen is not None else versions[default][0]

    then = snapshot_rows(name, chosen)
    now = snapshot_rows(name, versions[0][0])
    changes = compare(then, now, TRAVEL_TABLES[name][1])

    t1, t2, t3 = st.columns(3)
    t1.metric("Snapshots kept", f"{len(versions)}", help="Snapshots older than 90 days are expired.")
    t2.metric("Rows then → now", f"{len(then):,} → {len(now):,}")
    t3.metric("Rows changed since", f"{len(changes):,}")

    if chosen == versions[0][0]:
        st.caption("This is the current version.")
    elif changes.empty:
        st.success("Nothing has changed since this snapshot.", icon="✅")
    else:
        st.subheader("Changed since then")
        st.dataframe(changes, hide_index=True, width="stretch")

    st.subheader("The table as of then")
    st.dataframe(then, hide_index=True, width="stretch")
