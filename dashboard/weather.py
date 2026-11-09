"""Weather page: forecasts, city comparison and pipeline health from the weather marts."""

from datetime import datetime, timezone
from itertools import pairwise

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

import queries

# Chart colors: reference data-viz palette, one series per chart (no dual axes).
TEMPERATURE_COLOR = "#eb6834"  # categorical slot 2, orange
RAIN_COLOR = "#2a78d6"  # slot 1, blue
WIND_COLOR = "#1baf7a"  # slot 3, aqua
MUTED_INK = "#898781"

# Diverging temperature scale for the map: cold blue -> neutral gray at 15 °C -> hot red.
TEMPERATURE_STOPS = [
    (-15, (24, 79, 149)),
    (0, (109, 167, 236)),
    (15, (195, 194, 183)),
    (27, (230, 103, 103)),
    (40, (176, 42, 42)),
]

FRESH_HOURS = 26  # the freshness SLO (docs/slo.md): a daily job that ran on time, plus slack
STALE_HOURS = 50  # missed one run

DAY_LABELS = {0: "Today", 1: "Tomorrow"}

# ---------- data (cached; the pipeline refreshes at most daily) ----------


@st.cache_data(ttl=600, show_spinner="Loading cities…")
def cities() -> pd.DataFrame:
    return queries.load_cities()


@st.cache_data(ttl=600, show_spinner="Loading forecast…")
def hourly(city_id: str) -> pd.DataFrame:
    return queries.load_hourly(city_id)


@st.cache_data(ttl=600, show_spinner="Loading daily summaries…")
def daily() -> pd.DataFrame:
    return queries.load_daily()


@st.cache_data(ttl=600, show_spinner="Checking freshness…")
def health() -> pd.DataFrame:
    return queries.load_health()


# ---------- helpers ----------


def day_label(offset: int, date: pd.Timestamp) -> str:
    name = DAY_LABELS.get(offset, date.strftime("%A"))
    return f"{name} · {date.strftime('%b %d')}"


def temperature_rgb(temp_c: float) -> list[int]:
    """Linear interpolation between TEMPERATURE_STOPS, clamped at both ends."""
    if temp_c <= TEMPERATURE_STOPS[0][0]:
        return list(TEMPERATURE_STOPS[0][1])
    for (t0, c0), (t1, c1) in pairwise(TEMPERATURE_STOPS):
        if temp_c <= t1:
            f = (temp_c - t0) / (t1 - t0)
            return [round(a + (b - a) * f) for a, b in zip(c0, c1, strict=True)]
    return list(TEMPERATURE_STOPS[-1][1])


def hourly_chart(
    df: pd.DataFrame,
    field: str,
    title: str,
    unit: str,
    color: str,
    now: pd.Timestamp,
    *,
    bars: bool = False,
    height: int = 220,
) -> alt.LayerChart:
    """One measure over the 7-day window, with a hover crosshair and a "now" marker."""
    # Same time range on every chart so they line up vertically.
    domain = [df["forecast_hour_local"].min().isoformat(), df["forecast_hour_local"].max().isoformat()]
    x_axis = alt.Axis(format="%a %d", tickCount="day", labelAngle=0, grid=False)
    x = alt.X("forecast_hour_local:T", title=None, scale=alt.Scale(domain=domain), axis=x_axis)
    # Bars need a zero baseline; lines don't, and temperature reads better without one.
    y = alt.Y(f"{field}:Q", title=unit, axis=alt.Axis(tickCount=4), scale=alt.Scale(zero=bars))
    base = alt.Chart(df).encode(x=x)

    if bars:
        # Open-Meteo reports precipitation for the preceding hour, so each bar spans
        # exactly that hour: (t - 1h, t].
        bar_df = df[df[field] > 0].assign(hour_start=lambda d: d["forecast_hour_local"] - pd.Timedelta(hours=1))
        marks = (
            alt.Chart(bar_df)
            .mark_bar(color=color, clip=True)
            .encode(
                x=alt.X("hour_start:T", title=None, scale=alt.Scale(domain=domain), axis=x_axis),
                x2="forecast_hour_local:T",
                y=y,
                y2=alt.datum(0),
            )
        )
    else:
        marks = base.mark_line(color=color, strokeWidth=2, interpolate="monotone").encode(y=y)

    hover = alt.selection_point(
        fields=["forecast_hour_local"], nearest=True, on="pointerover", clear="pointerout", empty=False
    )
    crosshair = (
        base.mark_rule(color=MUTED_INK, strokeWidth=1)
        .encode(
            opacity=alt.condition(hover, alt.value(0.8), alt.value(0)),
            tooltip=[
                alt.Tooltip("forecast_hour_local:T", title="Local time", format="%a %b %d, %H:%M"),
                alt.Tooltip(f"{field}:Q", title=f"{title} ({unit})", format=".1f"),
            ],
        )
        .add_params(hover)
    )
    layers = [marks, crosshair]
    if not bars:
        layers.append(
            base.mark_circle(color=color, size=64, stroke="white", strokeWidth=2).encode(
                y=y, opacity=alt.condition(hover, alt.value(1), alt.value(0))
            )
        )

    if df["forecast_hour_local"].min() <= now <= df["forecast_hour_local"].max():
        now_df = pd.DataFrame({"forecast_hour_local": [now], "label": ["Now"]})
        now_rule = alt.Chart(now_df).encode(x="forecast_hour_local:T")
        layers += [
            now_rule.mark_rule(color=MUTED_INK, strokeDash=[4, 4]),
            now_rule.mark_text(align="left", dx=4, baseline="top", color=MUTED_INK, fontSize=11).encode(
                text="label:N", y=alt.value(2)
            ),
        ]

    return alt.LayerChart(layer=layers, title=title, height=height)


def stop_with(error: Exception) -> None:
    st.error(f"**Can't load the warehouse.** {error}", icon="⚠️")
    st.code("python ingestion/fetch_weather.py\ncd dbt_project\ndbt build --profiles-dir .", language="bash")
    st.stop()


# ---------- page ----------

try:
    city_df = cities()
except queries.WarehouseUnavailable as e:
    stop_with(e)

if city_df.empty:
    st.info("No cities have forecast data yet. Run the pipeline to fetch the first forecasts.", icon="ℹ️")
    st.stop()

last_fetch = city_df["last_fetched_at_utc"].max()
st.title("Weather")
st.caption(
    f"7-day hourly forecasts for {len(city_df)} cities from Open-Meteo · "
    f"last fetched {last_fetch:%b %d, %H:%M} UTC · all times are each city's local time"
)

forecast_tab, compare_tab, health_tab = st.tabs(["City forecast", "Compare cities", "Pipeline health"])

# ----- City forecast -----
with forecast_tab:
    labels = {row.city_id: f"{row.city_name}, {row.country}" for row in city_df.itertuples()}
    default_city = "london" if "london" in labels else city_df["city_id"].iloc[0]
    city_id = (
        st.selectbox(
            "City",
            options=list(labels),
            index=list(labels).index(default_city),
            format_func=lambda c: labels[c],
            help="Type to search all cities.",
        )
        or default_city
    )
    city = city_df.set_index("city_id").loc[city_id]
    now = pd.Timestamp(city["local_now"]).floor("h")

    try:
        hourly_df = hourly(city_id)
        daily_df = daily()
    except queries.WarehouseUnavailable as e:
        stop_with(e)

    city_daily = daily_df[daily_df["city_id"] == city_id]
    upcoming = city_daily[city_daily["day_offset"] >= 0]
    today = city_daily[city_daily["day_offset"] == 0]
    current = hourly_df[hourly_df["forecast_hour_local"] == now]

    st.caption(f"{city['timezone']} · local time {pd.Timestamp(city['local_now']):%a %b %d, %H:%M}")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Now", f"{current['temperature_c'].iloc[0]:.1f} °C" if not current.empty else "—")
    m2.metric(
        "Today's high / low",
        f"{today['temperature_max_c'].iloc[0]:.0f}° / {today['temperature_min_c'].iloc[0]:.0f}°"
        if not today.empty
        else "—",
    )
    m3.metric("Rain, next 7 days", f"{upcoming['precipitation_total_mm'].sum():.1f} mm" if not upcoming.empty else "—")
    m4.metric(
        "Peak wind, next 7 days", f"{upcoming['wind_speed_max_kmh'].max():.0f} km/h" if not upcoming.empty else "—"
    )
    if current.empty:
        st.warning(
            "The stored forecast doesn't cover the current hour, so the data is out of date. "
            "Check the Pipeline health tab.",
            icon="⚠️",
        )

    if hourly_df.empty:
        st.info("No hourly forecast stored for this city yet.", icon="ℹ️")
    else:
        st.altair_chart(
            hourly_chart(hourly_df, "temperature_c", "Temperature", "°C", TEMPERATURE_COLOR, now, height=260),
            use_container_width=True,
        )
        left, right = st.columns(2)
        left.altair_chart(
            hourly_chart(hourly_df, "precipitation_mm", "Precipitation", "mm", RAIN_COLOR, now, bars=True),
            use_container_width=True,
        )
        right.altair_chart(
            hourly_chart(hourly_df, "wind_speed_kmh", "Wind speed", "km/h", WIND_COLOR, now),
            use_container_width=True,
        )

    st.subheader("Daily summary")
    table = city_daily.assign(
        day=[day_label(o, d) for o, d in zip(city_daily["day_offset"], city_daily["forecast_date_local"], strict=True)]
    )
    st.dataframe(
        table[
            [
                "day",
                "temperature_min_c",
                "temperature_max_c",
                "temperature_avg_c",
                "precipitation_total_mm",
                "rainy_hours",
                "wind_speed_max_kmh",
            ]
        ],
        hide_index=True,
        use_container_width=True,
        column_config={
            "day": "Day",
            "temperature_min_c": st.column_config.NumberColumn("Low", format="%.1f °C"),
            "temperature_max_c": st.column_config.NumberColumn("High", format="%.1f °C"),
            "temperature_avg_c": st.column_config.NumberColumn("Average", format="%.1f °C"),
            "precipitation_total_mm": st.column_config.NumberColumn("Rain", format="%.1f mm"),
            "rainy_hours": st.column_config.NumberColumn("Rainy hours", format="%d h"),
            "wind_speed_max_kmh": st.column_config.NumberColumn("Peak wind", format="%.0f km/h"),
        },
    )

# ----- Compare cities -----
with compare_tab:
    try:
        daily_df = daily()
    except queries.WarehouseUnavailable as e:
        stop_with(e)

    offsets = sorted(o for o in daily_df["day_offset"].unique() if o >= 0)
    if not offsets:
        st.info("No upcoming days in the stored forecasts. Run the pipeline to refresh them.", icon="ℹ️")
    else:
        offset = st.segmented_control(
            "Day (each city's local calendar)",
            options=offsets,
            default=offsets[0],
            format_func=lambda o: DAY_LABELS.get(o, f"+{o} days"),
        )
        offset = offsets[0] if offset is None else offset
        day_df = daily_df[daily_df["day_offset"] == offset].merge(city_df, on="city_id")

        day_df = day_df.assign(
            fill=[temperature_rgb(t) + [220] for t in day_df["temperature_max_c"]],
            high_text=day_df["temperature_max_c"].map("{:.1f}".format),
            rain_text=day_df["precipitation_total_mm"].map("{:.1f}".format),
        )
        st.pydeck_chart(
            pdk.Deck(
                layers=[
                    pdk.Layer(
                        "ScatterplotLayer",
                        data=day_df,
                        get_position=["longitude", "latitude"],
                        get_fill_color="fill",
                        get_line_color=[255, 255, 255],
                        line_width_min_pixels=1.5,
                        stroked=True,
                        radius_min_pixels=6,
                        radius_max_pixels=14,
                        get_radius=60000,
                        pickable=True,
                    )
                ],
                initial_view_state=pdk.ViewState(latitude=20, longitude=10, zoom=0.6),
                # No scroll-wheel zoom, so scrolling the page never gets trapped by the map.
                views=[pdk.View(type="MapView", controller={"scrollZoom": False})],
                # Match the basemap to the viewer's light/dark theme (free Carto tiles, no token).
                map_style="dark" if st.context.theme.type == "dark" else "light",
                tooltip={
                    "html": "<b>{city_name}</b>, {country}<br/>High {high_text} °C · Rain {rain_text} mm",
                    "style": {"fontSize": "13px"},
                },
            ),
            height=380,
        )
        st.html(
            """
            <div style="display:flex;align-items:center;gap:8px;font-size:12px;color:#898781">
              <span>Daily high:</span><span>−15 °C</span>
              <span style="flex:0 1 220px;height:8px;border-radius:4px;background:linear-gradient(90deg,
                rgb(24,79,149),rgb(109,167,236) 27%,rgb(195,194,183) 55%,rgb(230,103,103) 76%,rgb(176,42,42))"></span>
              <span>40 °C</span>
            </div>
            """
        )

        st.dataframe(
            day_df.sort_values("temperature_max_c", ascending=False)[
                [
                    "city_name",
                    "country",
                    "forecast_date_local",
                    "temperature_max_c",
                    "temperature_min_c",
                    "precipitation_total_mm",
                    "wind_speed_max_kmh",
                ]
            ],
            hide_index=True,
            use_container_width=True,
            column_config={
                "city_name": "City",
                "country": "Country",
                "forecast_date_local": st.column_config.DateColumn("Local date", format="ddd MMM D"),
                "temperature_max_c": st.column_config.NumberColumn("High", format="%.1f °C"),
                "temperature_min_c": st.column_config.NumberColumn("Low", format="%.1f °C"),
                "precipitation_total_mm": st.column_config.NumberColumn("Rain", format="%.1f mm"),
                "wind_speed_max_kmh": st.column_config.NumberColumn("Peak wind", format="%.0f km/h"),
            },
        )

# ----- Pipeline health -----
with health_tab:
    try:
        health_df = health()
        daily_df = daily()
    except queries.WarehouseUnavailable as e:
        stop_with(e)

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    age_hours = (now_utc - last_fetch).total_seconds() / 3600
    if age_hours <= FRESH_HOURS:
        st.success(f"**Fresh** · last fetch {age_hours:.0f} h ago · SLO met (within {FRESH_HOURS} h)", icon="✅")
    elif age_hours <= STALE_HOURS:
        st.warning(
            f"**Stale** · last fetch {age_hours:.0f} h ago, so at least one daily run was missed · SLO missed",
            icon="⚠️",
        )
    else:
        st.error(f"**Out of date** · last fetch {age_hours / 24:.1f} days ago · SLO missed", icon="⛔")

    complete = daily_df["is_complete_day"].mean() if not daily_df.empty else 0
    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Cities with forecasts", f"{(health_df['hourly_rows'] > 0).sum()} / {len(health_df)}")
    h2.metric("Hourly rows", f"{health_df['hourly_rows'].sum():,}")
    h3.metric("Complete local days", f"{complete:.0%}")
    h4.metric(
        "Forecast reaches", f"{health_df['forecast_ends_utc'].min():%b %d} UTC", help="Earliest end across cities"
    )

    st.caption(
        "Run history, retries and every dbt test result live in the Dagster UI (`dagster dev`, then open http://localhost:3000)."
    )
    st.dataframe(
        health_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "city_name": "City",
            "country": "Country",
            "last_fetched_at_utc": st.column_config.DatetimeColumn("Last fetched (UTC)", format="MMM D, HH:mm"),
            "forecast_ends_utc": st.column_config.DatetimeColumn("Forecast ends (UTC)", format="MMM D, HH:mm"),
            "hourly_rows": st.column_config.NumberColumn("Hourly rows", format="%d"),
            "days_covered": st.column_config.NumberColumn("Local days", format="%d"),
        },
    )
