import polars as pl
from ..utils.helpers import safe_read_csv
from tabulate import tabulate
from datetime import datetime


def read_data_dh(path: str = None) -> pl.DataFrame:
    # read the csv file use polars
    # Force polars to examine the entire file when inferring dtypes. This helps
    # avoid warnings like "Could not determine dtype for column N" when
    # columns contain mixed types beyond the default sample range.
    # Ask polars to try parsing dates during CSV read and infer schema from whole file
    df = safe_read_csv(path, infer_schema_length=None, try_parse_dates=True)
    df = df[
        [
            "NUMBER",
            "STATUS",
            "WORK CENTER TYPE",
            "DEFECT TYPES",
            "DEFECT COUNTERMEASURES",
            "PRIORITY",
            "DESCRIPTION",
            "FOUND DURING",
            "INSPECTION CATEGORIES",
            "REPORTED AT",
        ]
    ]

    return df.clone()


def get_data_dh_metrics(df: pl.DataFrame):
    # Precompute commonly used masks and grouped counts to avoid repeated filters
    component_categories = [
        "Fasteners",
        "MechanicalDrives&Transmissions",
        "Pneumatic/SteamSystems",
        "GluingSystems",
        "ElectromechanicSystems",
    ]

    # component-level dataframe and aggregated counts
    df_component = df.filter(
        pl.col("INSPECTION CATEGORIES").is_in(component_categories)
    )
    dh_component_found = (
        df_component.group_by("WORK CENTER TYPE")
        .agg(pl.count().alias("COUNT"))
        .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
        .select(["WORK CENTER", "COUNT"])
    )

    data_dh_component = {}
    data_dh_component["FOUND"] = (
        int(dh_component_found["COUNT"].sum()) if dh_component_found.height > 0 else 0
    )
    for row in dh_component_found.to_dicts():
        data_dh_component[f"  - {row['WORK CENTER']}"] = row["COUNT"]
    data_dh_component["FIX"] = int(
        df_component.filter(pl.col("STATUS") == "CLOSED").height
    )

    # overall counts grouped by work center (reuse)
    dh_found_found = (
        df.group_by("WORK CENTER TYPE")
        .agg(pl.count().alias("COUNT"))
        .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
        .select(["WORK CENTER", "COUNT"])
    )
    data_dh_found = {}
    data_dh_found["FOUND"] = (
        int(dh_found_found["COUNT"].sum()) if dh_found_found.height > 0 else 0
    )
    for row in dh_found_found.to_dicts():
        data_dh_found[f"  - {row['WORK CENTER']}"] = row["COUNT"]

    dh_found_high = df.filter(pl.col("PRIORITY") == "HIGH")
    data_dh_found["HIGH"] = int(dh_found_high.height)

    # DH OPEN
    dh_open_found = (
        df.filter(pl.col("STATUS") == "OPEN")
        .group_by("WORK CENTER TYPE")
        .agg(pl.count().alias("COUNT"))
        .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
        .select(["WORK CENTER", "COUNT"])
    )
    data_dh_open = {
        "DH OPEN": int(dh_open_found["COUNT"].sum()) if dh_open_found.height > 0 else 0
    }

    # percent of HIGH with countermeasures
    total_dh_high = int(dh_found_high.height)
    dh_high_with_cm = int(
        dh_found_high.filter(pl.col("DEFECT COUNTERMEASURES") != "").height
    )
    data_dh_open["% HIGH CM"] = 100 * (
        dh_high_with_cm / total_dh_high if total_dh_high != 0 else 0
    )

    # filter dataframe for column "DEFECT TYPES" containing "SOURCE_OF_CONTAMINATION"
    dh_soc_found = df.filter(
        pl.col("DEFECT TYPES").str.contains("SOURCE_OF_CONTAMINATION")
    )
    data_dh_soc = {}
    data_dh_soc["FOUND"] = int(dh_soc_found.height) if dh_soc_found.height > 0 else 0
    data_dh_soc["FIX"] = (
        int(dh_soc_found.filter(pl.col("STATUS") == "CLOSED").height)
        if dh_soc_found.height > 0
        else 0
    )

    all_data = {
        "DH COMPONENT": data_dh_component,
        "DH FOUND": data_dh_found,
        "DH OPEN": data_dh_open,
        "DH SOC": data_dh_soc,
    }
    return all_data


def get_data_dh_detail_open(df: pl.DataFrame):
    # Filter DH column 'STATUS' = 'OPEN'
    dh_open_df = df.filter(df["STATUS"] == "OPEN")

    dh_detail = {}

    # Get values in column 'DESCRIPTION' for all DH OPEN
    dh_open_details = []
    # Extract required columns once and loop over native Python dicts
    for row in dh_open_df.select(
        ["NUMBER", "WORK CENTER TYPE", "DESCRIPTION"]
    ).to_dicts():
        num = row.get("NUMBER")
        wct = str(row.get("WORK CENTER TYPE", "")).split()
        wc_type = (wct[0][0] if wct and len(wct[0]) > 0 else "") + (
            wct[1] if len(wct) > 1 else ""
        )
        description = row.get("DESCRIPTION")
        dh_open_details.append(f"{num} | {wc_type} | {description}")

    dh_detail["DH OPEN"] = dh_open_details

    return dh_detail


def get_data_dh_detail_high(df: pl.DataFrame):
    # Filter DH column 'PRIORITY' = 'HIGH'
    dh_high_df = df.filter(pl.col("PRIORITY") == "HIGH")

    dh_detail = {}

    # Get values in column 'DESCRIPTION' for all DH HIGH
    dh_high_details = []
    for row in dh_high_df.select(
        [
            "NUMBER",
            "WORK CENTER TYPE",
            "DESCRIPTION",
            "STATUS",
            "DEFECT COUNTERMEASURES",
        ]
    ).to_dicts():
        num = row.get("NUMBER")
        wct = str(row.get("WORK CENTER TYPE", "")).split()
        wc_type = (wct[0][0] if wct and len(wct[0]) > 0 else "") + (
            wct[1] if len(wct) > 1 else ""
        )
        description = row.get("DESCRIPTION")
        detail = f"{num} | {wc_type} | {description}"
        status = row.get("STATUS")
        countermeasures = row.get("DEFECT COUNTERMEASURES")
        dh_high_details.append([detail, status, countermeasures])

    dh_detail["DH HIGH"] = dh_high_details

    return dh_detail


def create_dh_report_text(df: pl.DataFrame) -> str:
    # Determine date range for the report (YYYY-MM-DD)
    reported_vals = df.filter(pl.col("REPORTED AT").is_not_null())[
        "REPORTED AT"
    ].to_list()

    def _parse_date_token(tok: str):
        if not tok:
            return None
        # try iso first (fast for YYYY-MM-DD)
        try:
            return datetime.fromisoformat(tok).date()
        except Exception:
            pass
        # try a handful of common formats
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(tok, fmt).date()
            except Exception:
                continue
        return None

    dates = []
    for v in reported_vals:
        if v is None:
            continue
        # handle already-parsed dates or datetimes
        if hasattr(v, "date") and not isinstance(v, str):
            try:
                dates.append(v.date() if hasattr(v, "date") else v)
                continue
            except Exception:
                pass
        tok = str(v).strip().split(" ")[0]
        parsed = _parse_date_token(tok)
        if parsed:
            dates.append(parsed)

    if dates:
        date_min = min(dates).isoformat()
        date_max = max(dates).isoformat()
        period = f"DH Report for {date_min} to {date_max}"
    else:
        period = "DH Report"

    data_dh_metrics = get_data_dh_metrics(df)

    # Build a compact tabulated section using precomputed dicts
    tabulate_report = ""
    for section, data in data_dh_metrics.items():
        tabulate_report += f"*{section}:*\n"
        table = [(key, value) for key, value in data.items()]
        # tabulate once per section, then wrap for output
        table_str = tabulate(table, headers=["Metric   ", "Value"], tablefmt="psql")
        tabulate_report += f"`{table_str.replace('\n', '`\n`')}`\n\n"

    # Details: open and high
    data_dh_detail_open = get_data_dh_detail_open(df)
    dh_open_report = "*DH OPEN Details:*\n"
    for detail in data_dh_detail_open.get("DH OPEN", []):
        dh_open_report += f"> {detail}\n"

    data_dh_detail_high = get_data_dh_detail_high(df)
    dh_high_report = "*DH HIGH Details:*\n"
    for item in data_dh_detail_high.get("DH HIGH", []):
        # item is [detail, status, countermeasures]
        if not item:
            continue
        detail = item[0]
        status = item[1] if len(item) > 1 else ""
        countermeasures = item[2] if len(item) > 2 else ""
        dh_high_report += (
            f"> {detail} \n- Status: {status} \n- Countermeasures: {countermeasures}\n"
        )

    full_report = f"{period}\n\n{tabulate_report}{dh_open_report}\n{dh_high_report}"
    return full_report
