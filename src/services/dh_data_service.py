import polars as pl
from tabulate import tabulate


def read_data_dh(path: str = None) -> pl.DataFrame:
    # read the csv file use polars
    df = pl.read_csv(path)
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
            "REPORTED AT",
        ]
    ]

    return df.clone()


def create_dh_report_text(df: pl.DataFrame) -> str:
    # build link_up from unique work center types
    link_up = ""
    list_lu = df.select(pl.col("WORK CENTER TYPE").unique()).to_series().to_list()
    for item in list_lu:
        link_up = f"{link_up},  {item}"

    # determine period (min / max date part from `REPORTED AT`) using Python extraction
    reported_vals = df.filter(pl.col("REPORTED AT").is_not_null())[
        "REPORTED AT"
    ].to_list()
    date_parts = [
        str(v).split(" ")[0]
        for v in reported_vals
        if v is not None and str(v).strip() != ""
    ]
    if date_parts:
        date_min = min(date_parts)
        date_max = max(date_parts)
        period = f"*DH from {date_min} to {date_max}*"
    else:
        period = "*DH*"

    # helper to produce grouped detail table (work center -> count)
    def get_detail_DH(pldf: pl.DataFrame, value_name: str):
        DH_detail = (
            pldf.group_by("WORK CENTER TYPE")
            .agg(pl.count().alias(value_name))
            .with_columns(pl.col("WORK CENTER TYPE").alias("WORK CENTER"))
            .select(["WORK CENTER", value_name])
        )
        rows = DH_detail.to_dicts()
        return f"`{str(tabulate(rows, headers='keys', tablefmt='psql', showindex=False)).replace('\n', '`\n`')}`"

    # DH FOUND (total)
    df_total = df
    total_found = df_total.height
    detail_found = get_detail_DH(df_total, " FOUND")

    # DH FOUND DURING CIL
    df_cil = df.filter(pl.col("FOUND DURING").is_in(["CIL"]))
    total_found_cil = df_cil.height
    detail_found_cil = get_detail_DH(df_cil, "   CIL")

    # DH FIX (CLOSED)
    df_close = df.filter(pl.col("STATUS").is_in(["CLOSED"]))
    total_close = df_close.height
    detail_close = get_detail_DH(df_close, "CLOSED")

    # DH SOC (SOURCE_OF_CONTAMINATION)
    df_soc_found = df_total.filter(
        pl.col("DEFECT TYPES").fill_null("").str.contains("SOURCE_OF_CONTAMINATION")
    )
    df_soc_fix = df_close.filter(
        pl.col("DEFECT TYPES").fill_null("").str.contains("SOURCE_OF_CONTAMINATION")
    )

    soc_list = [("FOUND", df_soc_found.height), ("FIX", df_soc_fix.height)]
    detail_soc = f"`{str(tabulate(soc_list, headers=['STATUS     ', ' COUNT'], tablefmt='psql', showindex=False)).replace('\n', '`\n`')}`"

    # DH OPEN (list)
    df_open = df.filter(pl.col("STATUS").is_in(["OPEN"]))
    open_rows = df_open.select(["NUMBER", "DESCRIPTION", "PRIORITY"]).rows()
    data_open = [f"{i+1}. {r[0]}: {r[1]}\n" for i, r in enumerate(open_rows)]
    str_open = "".join(data_open)

    # DH HIGH (priority)
    df_high = df.filter(pl.col("PRIORITY").is_in(["HIGH"]))
    high_rows = df_high.select(
        ["NUMBER", "DESCRIPTION", "STATUS", "DEFECT COUNTERMEASURES"]
    ).rows()
    data_high = [
        f"{i+1}. {r[0]}: {r[1]}\n- Status : {r[2]}\n- CM      : {r[3]}\n\n"
        for i, r in enumerate(high_rows)
    ]
    str_high = "".join(data_high)

    return (
        f"{period}\n\n*DH FOUND DURING CIL*: {total_found_cil}\n{detail_found_cil}\n\n"
        f"*DH FOUND*: {total_found}\n{detail_found}\n\n*DH FIX (CLOSED)*: {total_close}\n{detail_close}\n\n"
        f"*DH SOC*: \n{detail_soc}\n\n*DH OPEN*: {len(data_open)}\n{str_open}\n*DH HIGH*: {len(data_high)}\n{str_high}"
    )
