import datetime
from typing import List, Dict, Tuple
from tabulate import tabulate
from PIL import Image
import qrcode


def build_coldata(columns: List[str]) -> List[Dict]:
    """Build a Tableview coldata structure from column names.

    Rules: "Order description" and "Activity Description" -> wider + stretch
    """
    out = []
    for col in columns:
        if col in ("Order description", "Activity Description"):
            out.append({"text": col, "stretch": True, "width": 300})
        else:
            out.append({"text": col, "stretch": False, "width": 100})
    return out


def compute_period_dates(
    period: str, period_detail: str, year_value: int
) -> Tuple[datetime.date, datetime.date]:
    """Return (start_date, end_date) for given period selection.

    period: "weekly" or "monthly"
    period_detail: e.g. "Week 46" or "03 - March"
    year_value: int year
    """
    period = (period or "").lower()
    if period == "weekly":
        weeknum = int(period_detail.split()[1])
        start_date = datetime.date.fromisocalendar(year_value, weeknum, 1)
        end_date = start_date + datetime.timedelta(days=6)
        return start_date, end_date
    elif period == "monthly":
        month = int(period_detail.split(" - ")[0])
        start_date = datetime.date(year_value, month, 1)
        # get last day of month
        if month == 12:
            end_date = datetime.date(year_value + 1, 1, 1) - datetime.timedelta(days=1)
        else:
            end_date = datetime.date(year_value, month + 1, 1) - datetime.timedelta(
                days=1
            )
        return start_date, end_date
    else:
        raise ValueError("period must be 'weekly' or 'monthly'")


def format_report_table(rnm_cost: float, net_product: float) -> str:
    """Return a nicely formatted markdown-style table string for the report.

    Handles missing/zero net_product gracefully.
    """

    def fmt(x):
        try:
            return f"{x:,}"
        except Exception:
            return str(x)

    rate = "N/A"
    try:
        if float(net_product) > 0:
            rate = f"{rnm_cost / float(net_product):.4f}"
    except Exception:
        rate = "N/A"

    txt = [
        ["R&M Cost", fmt(rnm_cost), "IDR"],
        ["Net Prod", fmt(int(net_product) if net_product else 0), "stk"],
        ["R&M Rate", rate, "IDR/stk"],
    ]
    return f'`{tabulate(txt, headers=["Metric", "Value", "Unit"], tablefmt="psql").replace("\n", "`\n`")}`'


def format_top_parts(rowdicts: List[Dict], n: int = 5) -> str:
    """Return the textual 'Top Part Consumption' chunk based on row dictionaries.

    Expects keys like 'Amount in IDR', 'Material description', 'Order description'.
    """

    def fmt_num(x):
        try:
            return f"{int(x):,}"
        except Exception:
            try:
                return f"{float(x):,}"
            except Exception:
                return str(x)

    out = ""
    for row in rowdicts[:n]:
        amt = row.get("Amount in IDR", 0)
        material = row.get("Material description", "")
        order_desc = row.get("Order description", "")
        out += f"> {fmt_num(amt)} IDR | {material} \n- {order_desc}\n\n"
    return out


def make_qr_image(text: str, size: Tuple[int, int] = (400, 400)) -> Image.Image:
    """Create a QR PIL Image from text (kept testable)."""
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_Q,
        box_size=6,
        border=2,
    )
    qr.add_data(text)
    qr.make(fit=True)
    primary_color = "#000000"
    img = qr.make_image(fill_color=primary_color, back_color="orange")
    return img.resize(size)
