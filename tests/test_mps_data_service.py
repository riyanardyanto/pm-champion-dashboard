import sys
from pathlib import Path
import datetime

# Ensure src package is importable for tests
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import polars as pl
from services import mps_data_service as svc


def test_parse_date_value_various_inputs():
    assert svc.parse_date_value(None) is None

    d = datetime.date(2024, 12, 31)
    assert svc.parse_date_value(d) == d

    dt = datetime.datetime(2024, 12, 31, 13, 45)
    assert svc.parse_date_value(dt) == d

    assert svc.parse_date_value("2024-12-31") == d
    assert svc.parse_date_value("31/12/2024") == d
    assert svc.parse_date_value("31-12-2024") == d
    assert svc.parse_date_value("2024-12-31 00:00:00") == d


def test_filter_data_by_date_string_and_shift():
    # Create a small DataFrame with mixed DATE representations
    df = pl.DataFrame(
        {
            "DATE": [
                "2025-01-02",
                datetime.datetime(2025, 1, 2),
                datetime.date(2025, 1, 3),
            ],
            "Shift": [1, 1, 2],
            "Owner": ["A", "B", "C"],
            "Equipment": ["1. EQ", "2. EQ", "3. EQ"],
            "Activity Description": ["a", "b", "c"],
        }
    )

    # Use the filter helper with a string date — should be coerced by parse_date_value
    filtered = svc.filter_data_mps_by_date_and_shift(df, "2025-01-02", 1)
    # Expect two rows (both date representations of 2025-01-02 with Shift=1)
    assert filtered.height == 2
    assert all(r["Shift"] == 1 for r in filtered.to_dicts())


def test_convert_date_values_list_and_series():
    vals = [
        "2024-01-01",
        datetime.datetime(2024, 1, 1),
        datetime.date(2024, 1, 2),
        None,
        "",
    ]
    converted = svc.convert_date_values(vals)
    assert converted[0] == datetime.date(2024, 1, 1)
    assert converted[1] == datetime.date(2024, 1, 1)
    assert converted[2] == datetime.date(2024, 1, 2)
    assert converted[3] is None
    assert converted[4] is None
