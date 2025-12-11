import polars as pl
import httpx
from bs4 import BeautifulSoup
from typing import List, Union
from tabulate import tabulate
from datetime import datetime


def get_spa_url(
    env: str,
    link_up: str,
    segment_date_min: str,  # format: 'YYYY-MM-DD'
    segment_date_max: str,  # format: 'YYYY-MM-DD'
    shift: str = "",
    functional_location: str = "PACK",
) -> str:
    if env.lower() == "production":
        return get_url_period_loss_tree(
            link_up, segment_date_min, segment_date_max, shift, functional_location
        )

    elif env.lower() == "development":
        start_date = datetime.strptime(segment_date_min, "%Y-%m-%d")
        end_date = datetime.strptime(segment_date_max, "%Y-%m-%d")
        delta = end_date - start_date
        if delta.days > 7:
            month = start_date.month
            return f"http://127.0.0.1:5500/assets/spa/{month}.html"
        else:
            return "http://127.0.0.1:5500/assets/spa/1.html"


def get_url_period_loss_tree(
    link_up: str,
    segment_date_min: str,
    segment_date_max: str,
    shift: str = "",
    functional_location: str = "PACK",
) -> str:
    from urllib.parse import urlencode
    from src.utils.app_config import get_base_url

    line_prefix = "PMID-SE-CP-L0" if link_up == "17" else "ID01-SE-CP-L0"
    params = {
        "table": "SPA_NormPeriodLossTree",
        "act": "query",
        "submit1": "Search",
        "db_Line": f"{line_prefix}{link_up}",
        "db_FunctionalLocation": f"{line_prefix}{link_up}-{functional_location}",
        "db_SegmentDateMin": segment_date_min,
        "db_ShiftStart": shift,
        "db_SegmentDateMax": segment_date_max,
        "db_ShiftEnd": shift,
        "db_Normalize": 0,
        "db_PeriodTime": 10080,
        "s_PeriodTime": "",
        "db_LongStopDetails": 3,
        "db_ReasonCNT": 30,
        "db_ReasonSort": "stop count",
        "db_Language": "OEM",
        "db_LineFailureAnalysis": "x",
    }

    base_url = get_base_url()
    return base_url + urlencode(params, doseq=True)


def _normalize_to_dataframe(
    df_or_list: Union[pl.DataFrame, List[pl.DataFrame]],
) -> pl.DataFrame:
    """Helper to normalize input to a single DataFrame."""
    if isinstance(df_or_list, list):
        if not df_or_list:
            return pl.DataFrame()
        if len(df_or_list) == 1:
            return df_or_list[0]
        return pl.concat(df_or_list)
    return df_or_list


def scrape_tables_to_polars_numeric_headers(
    url: str = None, html: str = None
) -> List[pl.DataFrame]:
    """
    Scraping semua <table> dari URL atau HTML string,
    lalu mengubahnya menjadi list of Polars DataFrame
    dengan header kolom = 0, 1, 2, 3 ...
    Contoh: kolom pertama → 0, kolom kedua → 1, dst.
    """
    # Ambil HTML
    if html is None:
        if url is None:
            raise ValueError("Harus memberikan url atau html!")
        response = httpx.get(url, timeout=30)
        response.raise_for_status()
        # httpx.Response doesn't provide `apparent_encoding` like `requests` does.
        # Prefer httpx's `charset_encoding` and fall back to chardet when available.
        enc = getattr(response, "charset_encoding", None)
        if not enc:
            try:
                import chardet  # type: ignore

                enc = chardet.detect(response.content or b"").get("encoding")
            except Exception:
                enc = None
        if enc:
            response.encoding = enc
        html_content = response.text
    else:
        html_content = html

    soup = BeautifulSoup(html_content, "html.parser")
    tables = soup.find_all("table")

    if not tables:
        return []

    list_dfs = []

    for table in tables:
        # Extract data using list comprehension for efficiency
        data = [
            [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
            for row in table.find_all("tr")
        ]
        # Filter out empty rows
        data = [row for row in data if row and not all(c == "" for c in row)]

        if not data:
            continue

        # Determine max columns
        max_cols = max(len(row) for row in data)

        # Normalize rows to same length using list comprehension
        normalized_data = [
            (
                row + [""] * (max_cols - len(row))
                if len(row) < max_cols
                else row[:max_cols]
            )
            for row in data
        ]

        # Create headers
        headers = [str(j) for j in range(max_cols)]

        # Create Polars DataFrame
        df = pl.DataFrame(normalized_data, schema=headers, orient="row")
        list_dfs.append(df)

    return list_dfs


def get_relevant_tables(url: str = None, html: str = None) -> pl.DataFrame:
    """
    Fungsi tambahan untuk mendapatkan tabel yang relevan berdasarkan kriteria tertentu.
    Misalnya, hanya mengembalikan tabel dengan jumlah baris lebih dari 20.
    """
    all_tables = scrape_tables_to_polars_numeric_headers(url=url, html=html)
    # Collect matching tables into a real list (don't return a generator) and
    # ensure we return a Polars DataFrame (concat multiple tables) or an
    # empty DataFrame when none were found.
    relevant = [t for t in all_tables if t.height > 20]

    if not relevant:
        # return an empty DataFrame so callers can safely use .filter()
        return pl.DataFrame()

    if len(relevant) == 1:
        return relevant[0]
    else:
        return pl.concat(relevant)


# hapus baris dari dataframe jika value pada kolom 1 sama dengan value pada baris sebelumnya, dan value pada kolom 3 sama dengan null
def remove_duplicate_rows(df: Union[pl.DataFrame, List[pl.DataFrame]]) -> pl.DataFrame:
    """
    Remove rows where the value in column '1' is the same as the previous row's
    column '1' AND column '3' is null/empty for that row.

    Accepts a single Polars DataFrame or a list of DataFrames (which will be
    concatenated). Returns a Polars DataFrame.
    """

    # Normalize input to a single DataFrame
    df = _normalize_to_dataframe(df)

    if not isinstance(df, pl.DataFrame):
        raise TypeError(
            "remove_duplicate_rows expects a Polars DataFrame or a list of DataFrames"
        )

    if df.is_empty():
        return df

    # Columns in scraped tables are named as numeric strings '0','1',...
    col1_name = "1"
    col3_name = "3"

    # If either column is not present, nothing to remove
    if col1_name not in df.columns or col3_name not in df.columns:
        return df

    # Prepare columns for comparison: cast to Utf8 and normalize nulls to empty string
    col1 = df[col1_name].fill_null("").cast(pl.Utf8)
    prev_col1 = col1.shift(1)

    col3 = df[col3_name]
    # Some Polars versions don't have .str.strip; check for null or empty string
    col3_is_null_or_empty = col3.is_null() | (col3.fill_null("").cast(pl.Utf8) == "")

    # Mark rows to drop: current col1 equals previous col1 AND col3 is null/empty
    # Ensure comparisons with shifted values don't produce nulls (treat as False)
    equality_with_prev = (col1 == prev_col1).fill_null(False)
    to_drop = equality_with_prev & col3_is_null_or_empty

    # Keep rows where to_drop is False
    keep_mask = ~to_drop

    return df.filter(keep_mask)


# split dataframe berdasarkan row index pada kolom (6 atau 7 atau 8 atau 9) yang mempunyai nilai 'i'
def split_dataframe(
    df_or_tables: Union[pl.DataFrame, List[pl.DataFrame]],
    split_columns: List[Union[int, str]] | None,
    split_value: str,
) -> List[pl.DataFrame]:
    """
    Split a Polars DataFrame into a list of DataFrames using rows that contain
    `split_value` in any of `split_columns` as separators (each separator row
    starts a new chunk and is included at the top of that chunk).

    Parameters:
    - df_or_tables: a single Polars DataFrame (or list, which will be concatenated).
    - split_columns: list of column identifiers (ints for indices or strings for names).
      If None or no provided names match, defaults to columns '6','7','8','9' when available.
    - split_value: the value to look for (string comparison)

    Returns a list of Polars DataFrames (chunks). If no separator is found, returns
    a single-element list containing the original DataFrame (or concatenated DF).
    """
    # Normalize input to a single DataFrame
    df = _normalize_to_dataframe(df_or_tables)

    if df.is_empty():
        return []

    # Resolve split columns: accept ints (indices) or strings (names)
    available_cols = [str(c) for c in df.columns]

    cols_to_check: List[str] = []
    if split_columns:
        for c in split_columns:
            # int index -> convert to header name (headers are numeric strings like '0','1',...)
            if isinstance(c, int):
                name = str(c)
            else:
                name = str(c)
            if name in available_cols:
                cols_to_check.append(name)

    # Fallback: use columns '6','7','8','9' if none of the user-specified columns matched
    if not cols_to_check:
        for idx in (6, 7, 8, 9):
            name = str(idx)
            if name in available_cols:
                cols_to_check.append(name)

    if not cols_to_check:
        # Nothing to check against — return original DF as single chunk
        return [df]

    # Build boolean mask where any of the selected columns equals split_value
    mask = None
    for col in cols_to_check:
        # Cast values to string and compare (handle nulls)
        col_cmp = df[col].fill_null("").cast(pl.Utf8) == str(split_value)
        mask = col_cmp if mask is None else (mask | col_cmp)

    # Get separator indices efficiently
    separator_indices = mask.arg_true().to_list()

    if not separator_indices:
        return [df]

    chunks: List[pl.DataFrame] = []
    for i, start_idx in enumerate(separator_indices):
        end_idx = (
            separator_indices[i + 1] if i + 1 < len(separator_indices) else df.height
        )
        length = end_idx - start_idx
        if length <= 0:
            continue
        chunks.append(df.slice(start_idx, length))

    return chunks


def scrape_data_spa(url: str = None, html: str = None) -> List[pl.DataFrame]:
    """
    Scrape tables from the given URL or HTML string and return a list of cleaned
    Polars DataFrames after removing duplicate rows.
    """

    table = get_relevant_tables(url=url, html=html)
    cleaned_tables = remove_duplicate_rows(table)
    dfs = split_dataframe(cleaned_tables, [6, 7, 8, 9], "i")

    for i, df in enumerate(dfs):
        # row(...) returns a tuple so direct assignment to df.row(0)[1] fails
        # Build the new value safely and update the dataframe using polars
        if df.height == 0:
            continue

        # Protect against missing column indexes
        try:
            split_src = str(df.row(0)[2])
        except Exception:
            split_src = ""

        new_val = (
            str(df.row(0)[1]).split(split_src)[0] if split_src else str(df.row(0)[1])
        )

        # Use `with_columns` to replace the value for row 0 in column '1'
        dfs[i] = df.with_columns(
            pl.when(pl.arange(0, df.height) == 0)
            .then(pl.lit(new_val))
            .otherwise(pl.col("1"))
            .alias("1")
        )
    return dfs


# ==================== CONTOH PENGGUNAAN ====================

if __name__ == "__main__":
    # Contoh URL yang banyak tabelnya
    url = "http://127.0.0.1:5501/assets/spa/response.html"

    tables = get_relevant_tables(url=url)

    cleaned_tables = remove_duplicate_rows(tables)

    # dfs = split_dataframe(cleaned_tables, [6, 7, 8, 9], "i")
    dfs = scrape_data_spa(url=url)

    # Tampilkan preview
    # for idx, df in enumerate(tables):
    #     print(f"\n=== TABEL {idx+1} ({df.shape[0]} × {df.shape[1]}) ===")
    #     print(df.head(5))

    with open("spa1_output.txt", "w", encoding="utf-8") as f:
        for idx, df in enumerate(dfs):
            f.write(f"\n=== TABEL {idx+1} ({df.shape[0]} × {df.shape[1]}) ===\n")
            f.write(tabulate(df.to_dicts(), tablefmt="psql", headers="keys") + "\n\n")
