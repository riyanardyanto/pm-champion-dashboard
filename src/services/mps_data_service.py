import polars as pl
from ..utils.helpers import safe_read_excel
import datetime
from typing import Any, Iterable, List, Optional


def parse_date_value(v: Any) -> Optional[datetime.date]:
    """Normalize a single value to a python.date or return None.

    Accepts native date/datetime objects or string-like values commonly
    produced by Excel/CSV exports. Tries ISO parsing first then a set of
    common formats. Returns None for empty/invalid inputs.
    """
    if v is None:
        return None
    # Already a date (but not datetime)
    if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
        return v
    # If datetime, take the date part
    if isinstance(v, datetime.datetime):
        return v.date()

    # Try parsing strings into a date
    try:
        s = str(v).strip()
        if not s:
            return None

        # Try ISO first (YYYY-MM-DD)
        try:
            return datetime.date.fromisoformat(s)
        except Exception:
            pass

        # Try a few common formats seen in Excel/CSV exports
        for fmt in (
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.datetime.strptime(s, fmt).date()
            except Exception:
                continue

        # If none matched, return None so later logic can skip/raise a clear error
        return None
    except Exception:
        return None


def convert_date_values(vals: Iterable[Any]) -> List[Optional[datetime.date]]:
    """Convert an iterable of DATE-like values into a list of python.date or None.

    This is a small convenience wrapper around `parse_date_value` for callers that
    need to convert entire columns/series/lists without reimplementing the loop.
    """
    return [parse_date_value(v) for v in vals]


def read_excel_with_dynamic_header(
    excel_path: str,
    sheet: str = "Tracking",
    header_token: str = "DATE",
    fallback_header_row: int = 0,
) -> pl.DataFrame:
    """Read an Excel sheet and detect a header row dynamically.

    Behavior mirrors the previous script: read the full sheet into a DataFrame,
    try to locate a row where the first column equals `header_token` and use
    that row for column names (data starts on the next row). If the token
    is already present in the DataFrame columns, the DataFrame is returned
    unchanged.

    Args:
        excel_path: path to the excel file
        sheet: sheet name to read
        header_token: token to look for in the first column to detect header
        full_csv_path: optional path to write the full raw frame (for debugging)
        output_csv_path: optional path to write the resulting parsed frame
        fallback_header_row: index to use if header row isn't found
        write_debug_csvs: whether to write the CSV debug files

    Returns:
        pl.DataFrame: properly labeled dataframe (data rows only)
    """

    # Use full inference so polars doesn't incorrectly fall back to string dtype
    # because of mixed content within the first N rows.
    full_df = safe_read_excel(excel_path, sheet_name=sheet, infer_schema_length=None)

    # If token already in columns (case-insensitive), no reheader needed
    cols_up = [str(c).upper() for c in full_df.columns]
    if header_token.upper() in cols_up:
        return full_df

    # locate header row where the first column equals the header_token
    header_row_idx = None
    for i in range(full_df.height):
        try:
            first_val = full_df[i, 0]
        except Exception:
            # fallback to row method
            row = full_df.row(i)
            first_val = row[0] if row else None

        if isinstance(first_val, str) and first_val.upper() == header_token.upper():
            header_row_idx = i
            break

    if header_row_idx is None:
        header_row_idx = fallback_header_row

    # build normalized header list from that row
    raw_header = list(full_df.row(header_row_idx))
    seen: dict[str, int] = {}
    header: list[str] = []
    for h in raw_header:
        if h is None:
            h = ""
        elif isinstance(h, (bytes, bytearray)):
            try:
                h = h.decode()
            except Exception:
                h = str(h)
        elif isinstance(h, (pl.Series,)):
            h = str(h)
        elif hasattr(h, "isoformat") and not isinstance(h, str):
            try:
                h = h.isoformat()
            except Exception:
                h = str(h)
        else:
            h = str(h)

        if h in seen:
            seen[h] += 1
            header.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 1
            header.append(h)

    df = full_df.slice(header_row_idx + 1)
    df.columns = header

    return df


def read_data_mps(
    path: str | None = None, sheet_name: str | None = None
) -> "pl.DataFrame":
    # Read the entire sheet
    df: pl.DataFrame = read_excel_with_dynamic_header(path, sheet=sheet_name)

    # remove rows where DATE column is null
    df = df.filter(pl.col("DATE").is_not_null())

    # Clean Owner column similar to the original logic (avoid Expr.apply)
    if "Owner" in df.columns:
        owner_vals = df["Owner"].to_list()

        def _clean_owner(s):
            if s is None:
                return s
            try:
                st = str(s)
            except Exception:
                return s
            return st.split("shift")[0].split("Shift")[0].strip().capitalize()

        owners_clean = [_clean_owner(v) for v in owner_vals]
        df = df.with_columns(pl.Series("Owner", owners_clean))

    # Select only relevant columns
    df = df.select(["DATE", "Shift", "Owner", "Equipment", "Activity Description"])

    # Convert DATE column to date format (YYYY-MM-DD)
    # The sheet may contain python dates, datetimes or strings — normalize them to python.date.
    date_vals = df["DATE"].to_list()

    # Use the exported convenience helper to convert the column values
    converted = convert_date_values(date_vals)
    df = df.with_columns(pl.Series("DATE", converted))

    # Convert Shift column to int (if present)
    if "Shift" in df.columns:
        df = df.with_columns(pl.col("Shift").cast(pl.Int32).alias("Shift"))

    # Cast numeric runtime/plan columns to float for consistent downstream handling
    for num_col in ("Plan exe (min)", "Actual (min)"):
        if num_col in df.columns:
            # cast conservatively to Float64 to preserve decimals where present
            df = df.with_columns(pl.col(num_col).cast(pl.Float64).alias(num_col))

    # Cast obvious ints
    if "Year" in df.columns:
        df = df.with_columns(pl.col("Year").cast(pl.Int32).alias("Year"))
    if "No. DH" in df.columns:
        # sometimes this is empty or mixed; cast non-destructively
        df = df.with_columns(pl.col("No. DH").cast(pl.Int32).alias("No. DH"))

    # Sort by DATE column, and then by Shift if needed
    df = df.sort(by=["DATE", "Shift"], nulls_last=True, descending=False)

    # split Equipment column to get the part after the dot and trim spaces
    if "Equipment" in df.columns:
        equipment_vals = df["Equipment"].to_list()

        def _clean_equipment(s):
            if s is None:
                return s
            s_str = str(s)
            if "." in s_str:
                parts = s_str.split(".")
                # return part after first dot
                return parts[1].strip()
            return s_str.strip()

        equipments_clean = [_clean_equipment(v) for v in equipment_vals]
        df = df.with_columns(pl.Series("Equipment", equipments_clean))

    df.write_csv("x test.csv", include_header=True)
    return df.clone()


def filter_data_mps_by_year(df: "pl.DataFrame", year: int) -> "pl.DataFrame":
    # Filter the DataFrame to include only rows where the year of the DATE column matches year
    df_filtered = df.filter(pl.col("DATE").dt.year() == year)

    # Sort by DATE column, and then by Shift if needed
    df_filtered = df_filtered.sort(
        by=["DATE", "Shift"], nulls_last=True, descending=False
    )

    return df_filtered.clone()


def filter_data_mps_by_weeknumber(
    df: "pl.DataFrame", week_number: int, year: int
) -> "pl.DataFrame":
    # Filter the DataFrame to include only rows where the week number of the DATE column matches week_number
    df_filtered = df.filter(pl.col("DATE").dt.year() == year)
    df_filtered = df_filtered.filter(pl.col("DATE").dt.week() == week_number)

    # Sort by DATE column, and then by Shift if needed
    df_filtered = df_filtered.sort(
        by=["DATE", "Shift"], nulls_last=True, descending=False
    )

    return df_filtered.clone()


def filter_data_mps_by_date_and_shift(
    df: "pl.DataFrame", target_date: "pl.datetime.date", target_shift: int
) -> "pl.DataFrame":
    # Filter the DataFrame to include only rows where the DATE column matches target_date
    # and the Shift column matches target_shift
    # Coerce target_date to a native python date if it's a string or datetime
    if not isinstance(target_date, datetime.date):
        parsed = parse_date_value(target_date)
        # Keep previous behaviour: when parse fails, leave target_date alone so
        # downstream polars comparisons will raise a clear error (instead of
        # silently changing to None)
        if parsed is not None:
            target_date = parsed

    df_filtered = df.filter(
        (pl.col("DATE").dt.date() == target_date) & (pl.col("Shift") == target_shift)
    )

    # Sort by DATE column, and then by Shift if needed
    df_filtered = df_filtered.sort(
        by=["DATE", "Shift"], nulls_last=True, descending=False
    )

    return df_filtered.clone()


def create_report_text(df: "pl.DataFrame") -> str:
    rows = df.to_dicts()
    if not rows:
        return ""

    first = rows[0]
    # Format date safely
    date_val = first.get("DATE")
    try:
        # If it's a datetime-like object
        date_str = date_val.strftime("%Y-%m-%d")
    except Exception:
        date_str = str(date_val).split(" ")[0]

    # Handle shift which may be float, int, or missing
    shift_val = first.get("Shift", "")
    shift_text = ""
    try:
        if shift_val is None:
            shift_text = ""
        elif isinstance(shift_val, float):
            # If float is integral (e.g. 2.0) show as integer, otherwise show raw
            shift_text = (
                str(int(shift_val)) if shift_val.is_integer() else str(shift_val)
            )
        else:
            shift_text = str(int(shift_val))
    except Exception:
        shift_text = str(shift_val)

    report_header = f"📅 *MPS {date_str}, Shift {shift_text}*\n"

    for row in rows:
        owner = str(row.get("Owner", "")).strip().capitalize()
        equipment = row.get("Equipment", "")
        equipment_s = "" if equipment is None else str(equipment)
        equipment_name = (
            equipment_s.split(".")[1].strip()
            if "." in equipment_s
            else equipment_s.strip()
        )
        activity = row.get("Activity Description", "") or ""
        report_header += f"*{owner}*\n" f"> {equipment_name}\n" f"- {activity}\n\n"
    return report_header
