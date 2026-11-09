"""End-to-end: the real dashboard, served by Streamlit, driven in a real browser (Playwright).

Runs against a warehouse built from the sample data, so every expected number is known:
the planted faults give 11 trades lost while running, 178 downtime trades, 1 disagreement.

    pytest -m e2e          (needs a Playwright browser: playwright install chromium)
"""

import os
import socket
import subprocess
import sys
import time
import urllib.request
from collections.abc import Iterator

import pytest
from playwright.sync_api import Page, expect, sync_playwright

from tests.conftest import PROJECT_ROOT, Warehouse

pytestmark = pytest.mark.e2e


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def dashboard_url(warehouse: Warehouse) -> Iterator[str]:
    port = free_port()
    env = {**os.environ, "LAKEHOUSE_DUCKDB_PATH": str(warehouse.db_path)}
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "dashboard/app.py",
            "--server.port",
            str(port),
            "--server.headless",
            "true",
            "--browser.gatherUsageStats",
            "false",
        ],  # fmt: skip
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with urllib.request.urlopen(f"{url}/_stcore/health", timeout=2):
                    break
            except OSError:
                if time.monotonic() > deadline or server.poll() is not None:
                    raise RuntimeError("Streamlit didn't start") from None
                time.sleep(0.5)
        yield url
    finally:
        server.terminate()
        server.wait(timeout=20)


@pytest.fixture
def page(dashboard_url: str) -> Iterator[Page]:
    """A fresh browser page that fails the test on any console error or uncaught page error."""
    errors: list[str] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto(dashboard_url)
        yield page
        browser.close()
    assert errors == [], f"browser errors: {errors}"


def open_crypto(page: Page) -> None:
    page.get_by_role("link", name="Crypto").click()
    expect(page.get_by_role("heading", name="Crypto")).to_be_visible(timeout=30_000)


def test_weather_page_switches_city(page: Page) -> None:
    expect(page.get_by_role("heading", name="Weather")).to_be_visible(timeout=30_000)
    expect(page.get_by_text("Europe/London")).to_be_visible()

    page.get_by_role("combobox").first.click()
    page.get_by_role("option", name="Montréal, Canada").click()
    expect(page.get_by_text("America/Toronto")).to_be_visible(timeout=15_000)


def test_reconciliation_shows_the_planted_faults(page: Page) -> None:
    open_crypto(page)
    tiles = page.get_by_test_id("stMetric")
    expect(tiles.filter(has_text="Completeness")).to_contain_text("99.89%")
    expect(tiles.filter(has_text="Lost while running")).to_contain_text("11")
    expect(tiles.filter(has_text="Downtime filled")).to_contain_text("178")
    expect(tiles.filter(has_text="Disagreements")).to_contain_text("1")
    expect(page.get_by_text("Service level objectives")).to_be_visible()


def test_market_and_gaps_tabs(page: Page) -> None:
    open_crypto(page)
    page.get_by_role("tab", name="Market").click()
    expect(page.get_by_text("Daily price (official record)")).to_be_visible(timeout=15_000)

    page.get_by_role("tab", name="Gaps").click()
    expect(page.get_by_text("3 of 3 gaps filled from the official record.")).to_be_visible(timeout=15_000)


@pytest.mark.parametrize("link", ["Weather", "Crypto"])
def test_no_sideways_scroll_on_a_phone(dashboard_url: str, link: str) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 375, "height": 812})
        page.goto(dashboard_url)
        expect(page.get_by_role("heading", name="Weather")).to_be_visible(timeout=30_000)
        if link == "Crypto":
            page.get_by_test_id("stExpandSidebarButton").click()
            open_crypto(page)
        page.wait_for_timeout(1500)
        overflow = page.evaluate("document.documentElement.scrollWidth - document.documentElement.clientWidth")
        browser.close()
    assert overflow == 0
