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


# def create_dh_report_text(df: pl.DataFrame) -> str:
#     # build link_up from unique work center types
#     link_up = ""
#     list_lu = df.select(pl.col("WORK CENTER TYPE").unique()).to_series().to_list()
#     for item in list_lu:
#         link_up = f"{link_up},  {item}"

#         # determine period (min / max date part from `REPORTED AT`) using Python extraction
#         # Ensure `REPORTED AT` is a datetime column where possible; leave strings alone
#         if "REPORTED AT" in df.columns:
#             try:
#                 df = df.with_columns(
#                     pl.col("REPORTED AT").cast(pl.Datetime).alias("REPORTED AT")
#                 )
#             except Exception:
#                 # Best-effort: keep the column as-is (string) if casting fails
#                 pass

#         # determine period (min / max date part from `REPORTED AT`) using Python extraction
#         reported_vals = df.filter(pl.col("REPORTED AT").is_not_null())[
#             "REPORTED AT"
#         ].to_list()
#     date_parts = [
#         str(v).split(" ")[0]
#         for v in reported_vals
#         if v is not None and str(v).strip() != ""
#     ]
#     if date_parts:
#         date_min = min(date_parts)
#         date_max = max(date_parts)
#         period = f"*DH from {date_min} to {date_max}*"
#     else:
#         period = "*DH*"

#     # helper to produce grouped detail table (work center -> count)
#     def get_detail_DH(pldf: pl.DataFrame, value_name: str):
#         DH_detail = (
#             pldf.group_by("WORK CENTER TYPE")
#             .agg(pl.count().alias(value_name))
#             .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
#             .select(["WORK CENTER", value_name])
#         )
#         rows = DH_detail.to_dicts()
#         return f"`{str(tabulate(rows, headers='keys', tablefmt='psql', showindex=False)).replace('\n', '`\n`')}`"

#     # DH FOUND (total)
#     df_total = df
#     total_found = df_total.height
#     detail_found = get_detail_DH(df_total, " FOUND")

#     # DH FOUND DURING CIL
#     df_cil = df.filter(pl.col("FOUND DURING").is_in(["CIL"]))
#     total_found_cil = df_cil.height
#     detail_found_cil = get_detail_DH(df_cil, "   CIL")

#     # DH FIX (CLOSED)
#     df_close = df.filter(pl.col("STATUS").is_in(["CLOSED"]))
#     total_close = df_close.height
#     detail_close = get_detail_DH(df_close, "CLOSED")

#     # DH SOC (SOURCE_OF_CONTAMINATION)
#     df_soc_found = df_total.filter(
#         pl.col("DEFECT TYPES").fill_null("").str.contains("SOURCE_OF_CONTAMINATION")
#     )
#     df_soc_fix = df_close.filter(
#         pl.col("DEFECT TYPES").fill_null("").str.contains("SOURCE_OF_CONTAMINATION")
#     )

#     soc_list = [("FOUND", df_soc_found.height), ("FIX", df_soc_fix.height)]
#     detail_soc = f"`{str(tabulate(soc_list, headers=['STATUS     ', ' COUNT'], tablefmt='psql', showindex=False)).replace('\n', '`\n`')}`"

#     # DH OPEN (list)
#     df_open = df.filter(pl.col("STATUS").is_in(["OPEN"]))
#     open_rows = df_open.select(["NUMBER", "DESCRIPTION", "PRIORITY"]).rows()
#     data_open = [f"{i+1}. {r[0]}: {r[1]}\n" for i, r in enumerate(open_rows)]
#     str_open = "".join(data_open)

#     # DH HIGH (priority)
#     df_high = df.filter(pl.col("PRIORITY").is_in(["HIGH"]))
#     high_rows = df_high.select(
#         ["NUMBER", "DESCRIPTION", "STATUS", "DEFECT COUNTERMEASURES"]
#     ).rows()
#     data_high = [
#         f"{i+1}. {r[0]}: {r[1]}\n- Status : {r[2]}\n- CM      : {r[3]}\n\n"
#         for i, r in enumerate(high_rows)
#     ]
#     str_high = "".join(data_high)

#     return (
#         f"{period}\n\n*DH FOUND DURING CIL*: {total_found_cil}\n{detail_found_cil}\n\n"
#         f"*DH FOUND*: {total_found}\n{detail_found}\n\n*DH FIX (CLOSED)*: {total_close}\n{detail_close}\n\n"
#         f"*DH SOC*: \n{detail_soc}\n\n*DH OPEN*: {len(data_open)}\n{str_open}\n*DH HIGH*: {len(data_high)}\n{str_high}"
#     )


def get_data_dh_metrics(df: pl.DataFrame):
    # DH Component Level
    df_component = df.filter(
        df["INSPECTION CATEGORIES"].is_in(
            [
                "Fasteners",
                "MechanicalDrives&Transmissions",
                "Pneumatic/SteamSystems",
                "GluingSystems",
                "ElectromechanicSystems",
            ]
        )
    )
    # DH Component Level grouped by Work Center Type
    dh_component_found = (
        df_component.group_by("WORK CENTER TYPE")
        .agg(pl.len().alias("COUNT"))
        .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
        .select(["WORK CENTER", "COUNT"])
    )
    data_dh_component = {}
    data_dh_component["FOUND"] = dh_component_found["COUNT"].sum()
    for wc_type in dh_component_found["WORK CENTER"]:
        data_dh_component[f"  - {wc_type}"] = dh_component_found.filter(
            dh_component_found["WORK CENTER"] == wc_type
        )["COUNT"][0]
    data_dh_component["FIX"] = df_component.filter(df_component["STATUS"] == "CLOSED")[
        "STATUS"
    ].count()

    # DH Found Level grouped by Work Center Type
    dh_found_found = (
        df.group_by("WORK CENTER TYPE")
        .agg(pl.len().alias("COUNT"))
        .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
        .select(["WORK CENTER", "COUNT"])
    )
    data_dh_found = {}
    data_dh_found["FOUND"] = dh_found_found["COUNT"].sum()
    for wc_type in dh_found_found["WORK CENTER"]:
        data_dh_found[f"  - {wc_type}"] = dh_found_found.filter(
            dh_found_found["WORK CENTER"] == wc_type
        )["COUNT"][0]

    dh_found_high = df.filter(pl.col("PRIORITY") == "HIGH")
    data_dh_found["HIGH"] = dh_found_high["PRIORITY"].count()

    # DH Open Level grouped by Work Center Type
    dh_open_found = (
        df.filter(df["STATUS"] == "OPEN")
        .group_by("WORK CENTER TYPE")
        .agg(pl.len().alias("COUNT"))
        .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
        .select(["WORK CENTER", "COUNT"])
    )
    data_dh_open = {}
    data_dh_open["DH OPEN"] = dh_open_found["COUNT"].sum()

    # Precentage of DH Open Level grouped by Work Center Type with Countermeasures
    total_dh_high = df.filter(pl.col("PRIORITY") == "HIGH").height
    dh_high_with_cm = (
        df.filter(pl.col("PRIORITY") == "HIGH")
        .filter(pl.col("DEFECT COUNTERMEASURES") != "")
        .height
    )
    data_dh_open["% HIGH CM"] = 100 * (
        dh_high_with_cm / total_dh_high if total_dh_high != 0 else 0
    )
    # filter rows where 'SOURCE_OF_CONTAMINATION' is in column 'DEFECT TYPES'
    dh_soc_found = (
        df.filter(pl.col("DEFECT TYPES").str.contains("SOURCE_OF_CONTAMINATION"))
        .group_by("WORK CENTER TYPE")
        .agg(pl.len().alias("COUNT"))
        .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
        .select(["WORK CENTER", "COUNT"])
    )
    data_dh_soc = {}
    for wc_type in dh_soc_found["WORK CENTER"]:
        data_dh_soc[wc_type] = dh_soc_found.filter(
            dh_soc_found["WORK CENTER"] == wc_type
        )["COUNT"][0]

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
    for i in range(len(dh_open_df)):
        num = dh_open_df["NUMBER"][i]
        wc_type = (
            str(dh_open_df["WORK CENTER TYPE"][i]).split(" ")[0][0]
            + str(dh_open_df["WORK CENTER TYPE"][i]).split(" ")[1]
        )
        description = dh_open_df["DESCRIPTION"][i]
        dh_open_details.append(f"{num} | {wc_type} | {description}")

    dh_detail["DH OPEN"] = dh_open_details

    return dh_detail


def get_data_dh_detail_high(df: pl.DataFrame):
    # Filter DH column 'PRIORITY' = 'HIGH'
    dh_high_df = df.filter(pl.col("PRIORITY") == "HIGH")

    dh_detail = {}

    # Get values in column 'DESCRIPTION' for all DH HIGH
    dh_high_details = []
    for i in range(len(dh_high_df)):
        num = dh_high_df["NUMBER"][i]
        wc_type = (
            str(dh_high_df["WORK CENTER TYPE"][i]).split(" ")[0][0]
            + str(dh_high_df["WORK CENTER TYPE"][i]).split(" ")[1]
        )
        description = dh_high_df["DESCRIPTION"][i]

        detail = f"{num} | {wc_type} | {description}"
        status = dh_high_df["STATUS"][i]
        countermeasures = dh_high_df["DEFECT COUNTERMEASURES"][i]

        data = [detail, status, countermeasures]
        dh_high_details.append(data)

    dh_detail["DH HIGH"] = dh_high_details

    return dh_detail


def create_dh_report_text(df: pl.DataFrame) -> str:
    # date min and max from 'REPORTED AT' column as format YYYY-MM-DD
    reported_vals = df.filter(pl.col("REPORTED AT").is_not_null())[
        "REPORTED AT"
    ].to_list()
    date_objs = []  # list of datetime objects
    for v in reported_vals:
        if v is None:
            continue
        s = str(v).strip().split(" ")[0]
        if not s:
            continue
        parsed = None
        # try common formats (MM/DD/YYYY, MM-DD-YYYY, ISO, etc.)
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                parsed = datetime.strptime(s, fmt).date()
                break
            except ValueError:
                continue
        if parsed is None:
            # fallback to fromisoformat if possible
            try:
                parsed = datetime.fromisoformat(s).date()
            except Exception:
                continue
        date_objs.append(parsed)

    if date_objs:
        date_min = min(date_objs).isoformat()
        date_max = max(date_objs).isoformat()
        period = f"DH Report for {date_min} to {date_max}"
    else:
        period = "DH Report"
    print(period)

    data_dh_metrics = get_data_dh_metrics(df)

    tabulate_report = ""
    for section, data in data_dh_metrics.items():
        tabulate_report += f"*{section}:*\n"
        table = [(key, value) for key, value in data.items()]
        tabulate_report += f'`{tabulate(
            table, headers=["Metric   ", "Value"], tablefmt="psql"
        ).replace("\n", "`\n`")}`\n\n'
    # print(tabulate_report)

    data_dh_detail_open = get_data_dh_detail_open(df)
    dh_open_report = "*DH OPEN Details:*\n"
    for detail in data_dh_detail_open["DH OPEN"]:
        dh_open_report += f"> {detail}\n"
    # print(dh_open_report)

    data_dh_detail_high = get_data_dh_detail_high(df)
    dh_high_report = "*DH HIGH Details:*\n"
    for detail, status, countermeasures in data_dh_detail_high["DH HIGH"]:
        dh_high_report += (
            f"> {detail} \n- Status: {status} \n- Countermeasures: {countermeasures}\n"
        )
    # print(dh_high_report)

    full_report = f"{period}\n\n{tabulate_report}{dh_open_report}\n{dh_high_report}"
    return full_report
