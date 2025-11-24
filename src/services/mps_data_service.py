import polars as pl
import datetime


schema = {
    "Grup": pl.String,
    "No. DH": pl.String,
    "Training Needed": pl.String,
    "Actual (min)": pl.String,
    "Comments": pl.String,
}


def read_data_mps(path: str | None = None) -> "pl.DataFrame":
    # read the excel file use polars
    df = pl.read_excel(
        path,
        sheet_name="Sheet3",
        schema_overrides=schema,
    )

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
    if isinstance(target_date, str):
        try:
            target_date = datetime.date.fromisoformat(target_date)
        except Exception:
            try:
                # fallback to full datetime isoparse then convert to date
                target_date = datetime.datetime.fromisoformat(target_date).date()
            except Exception:
                # as a last resort, leave as-is and let polars raise a clear error
                pass
    elif isinstance(target_date, datetime.datetime):
        target_date = target_date.date()

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
