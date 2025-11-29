import httpx
from httpx_ntlm import HttpNtlmAuth
import polars as pl

from src.utils.spa_processor import scrape_data_spa

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
}


# fetch RNM data from given URL (html response) with NTLM auth
async def fetch_rnm_data(url, username, password, verify_ssl=False):
    # import here so a runtime without httpx/httpx_ntlm can still import this
    # module (and use parse_rnm_tables for local HTML parsing, for example)

    async with httpx.AsyncClient(
        auth=HttpNtlmAuth(username, password), headers=HEADERS, verify=verify_ssl
    ) as client:
        response = await client.get(url)
        if response.status_code == 200:
            return response.text
        else:
            raise httpx.HTTPStatusError(
                f"Failed to fetch data: {response.status_code}",
                request=response.request,
                response=response,
            )


async def get_spa_net_production(url, username, password, verify_ssl=False):
    """Fetch RTM SPA HTML via NTLM then parse into cleaned DataFrames.

    Returns a list of polars DataFrames (same as scrape_data_spa).
    """
    response = await fetch_rnm_data(
        url=url, username=username, password=password, verify_ssl=verify_ssl
    )
    dfs = scrape_data_spa(html=response)
    for df in dfs:
        if "Time range" in df.row(0):
            net_product = float(str(df.row(5)[11]).split()[2])
            unit = str(df.row(5)[11]).split()[3].strip(",")

    if unit == "k":
        net_product = float(net_product) * 1000
    elif unit == "Mio":
        net_product = float(net_product) * 1000000
    return net_product


def read_sap_consumption_data(file_path: str):
    """Read SAP consumption data from a local excel file and parse into DataFrames.

    Returns a polars DataFrames.
    """
    df = pl.read_excel(file_path, schema_overrides={"Recipient": pl.Utf8})

    df = df.select(
        [
            "Posting date / document",
            "Description equip.",
            "Order type",
            "Amount in local currency",
            "Material description",
            "Order description",
        ]
    )

    df.columns = [
        "Posting date",
        "Description equip.",
        "Order type",
        "Amount in IDR",
        "Material description",
        "Order description",
    ]

    return df


def aggregate_sap_consumption_data(
    df: pl.DataFrame, group_by: str = None
) -> pl.DataFrame:

    if group_by is None:
        return df
    elif group_by not in ["weekly", "monthly"]:
        raise ValueError("group_by must be either 'weekly', 'monthly', or None")

    # Add period column based on group_by
    period_col = "Weeknum" if group_by == "weekly" else "Month"
    period_expr = (
        pl.col("Posting date").dt.week()
        if group_by == "weekly"
        else pl.col("Posting date").dt.month()
    )

    df_grouped = (
        df.with_columns(period_expr.alias(period_col))
        .group_by([period_col])
        .agg(pl.col("Amount in local currency").sum().alias("Total Amount"))
        .sort([period_col])
    )

    return df_grouped
