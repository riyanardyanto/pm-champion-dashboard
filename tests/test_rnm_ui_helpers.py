import datetime

from src.utils.rnm_ui_helpers import (
    build_coldata,
    compute_period_dates,
    format_report_table,
    format_top_parts,
)


def test_build_coldata():
    cols = ["DATE", "Order description", "Amount in IDR", "Activity Description"]
    out = build_coldata(cols)
    assert isinstance(out, list)
    assert out[0]["text"] == "DATE"
    assert out[1]["text"] == "Order description"
    # wide columns recognized
    assert out[1]["width"] == 300
    assert out[3]["width"] == 300


def test_compute_period_dates_weekly():
    start, end = compute_period_dates("weekly", "Week 1", 2025)
    assert start <= end
    # week 1 of 2025 should be in early January (verify type)
    assert isinstance(start, datetime.date)


def test_compute_period_dates_monthly():
    start, end = compute_period_dates("monthly", "12 - December", 2025)
    assert start.month == 12
    assert end.month == 12


def test_format_report_table_zero_net():
    s = format_report_table(1000, 0)
    assert "R&M Cost" in s
    assert "Net Prod" in s
    assert "N/A" in s


def test_format_top_parts():
    rows = [
        {
            "Amount in IDR": 1000,
            "Material description": "mat1",
            "Order description": "od1",
        },
        {
            "Amount in IDR": 2000,
            "Material description": "mat2",
            "Order description": "od2",
        },
    ]
    out = format_top_parts(rows, n=2)
    assert "> 1,000 IDR | mat1" in out
    assert "- od2" in out
