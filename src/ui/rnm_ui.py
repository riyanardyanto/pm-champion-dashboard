import asyncio
from tkinter.filedialog import askopenfilename
from async_tkinter_loop import async_handler
import logging
from tkinter import messagebox
import ttkbootstrap as ttk
import datetime
import polars as pl
from ttkbootstrap.tableview import Tableview
from PIL import ImageTk
from src.utils.app_config import read_config

from src.services.rnm_data_service import (
    get_spa_net_production,
    read_sap_consumption_data,
)
from src.utils.spa_processor import get_spa_url
from src.utils.rnm_helpers import sanitize_linkup
from src.utils.rnm_ui_helpers import (
    build_coldata,
    compute_period_dates,
    format_report_table,
    format_top_parts,
    make_qr_image,
)

# from tkinter.filedialog import askopenfilename
# from src.services.dh_data_service import create_dh_report_text, read_data_dh

# import qrcode
# from PIL import ImageTk


class RnMSidebar(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent, width=120)

        # Set label text color to blue (biru)
        label = ttk.Label(
            self,
            text="R&M",
            font=("Segoe UI", 34, "bold"),
            foreground="white",
            bootstyle="primary",
            # background="#24c647",
            justify="center",
        )
        label.pack(side="top", padx=(10, 10), pady=(2, 7))

        ttk.Separator(self, orient="horizontal").pack(
            side="top", fill="x", padx=5, pady=5
        )

        self.linkup = ttk.Combobox(self, width=14)
        self.linkup.pack(side="top", padx=10, pady=5)

        self.year = ttk.Combobox(
            self, values=[str(i) for i in range(2020, 2031)], width=14
        )
        self.year.pack(side="top", padx=10, pady=5)
        self.year.set(str(datetime.date.today().year))
        self.year.bind("<<ComboboxSelected>>", self.update_period_detail)

        self.period = ttk.Combobox(self, values=["Weekly", "Monthly"], width=14)
        self.period.pack(side="top", padx=10, pady=5)
        self.period.set("Weekly")
        self.period.bind("<<ComboboxSelected>>", self.update_period_detail)

        self.period_detail = ttk.Combobox(self, width=14)
        self.period_detail.pack(side="top", padx=10, pady=5)

        if self.period.get() == "Weekly":
            self.period_detail.configure(values=[f"Week {i}" for i in range(1, 53)])
            self.period_detail.set(
                f"Week {datetime.date.fromisocalendar(datetime.date.today().year, datetime.date.today().isocalendar()[1], 1).isocalendar()[1]}"
            )
        else:
            self.period_detail.configure(
                values=[
                    f"{i:02d} - {datetime.date(datetime.date.today().year, i, 1).strftime('%B')}"
                    for i in range(1, 13)
                ]
            )
            self.period_detail.set(
                f"{datetime.date.today().month:02d} - {datetime.date(datetime.date.today().year, datetime.date.today().month, 1).strftime('%B')}"
            )

        # self.weeknum = ttk.Combobox(
        #     self, values=[f"Week {i}" for i in range(1, 53)], width=14
        # )
        # self.weeknum.pack(side="top", padx=10, pady=5)
        # # set default week number to current week number
        # self.weeknum.set(
        #     f"Week {datetime.date.fromisocalendar(datetime.date.today().year, datetime.date.today().isocalendar()[1], 1).isocalendar()[1]}"
        # )
        # # konfiurasi event handler tanggal jika weeknum diubah
        # self.weeknum.bind("<<ComboboxSelected>>", self.update_weekdate)

        # weekdate_values = []
        # for i in range(7):
        #     date = datetime.date.fromisocalendar(
        #         int(self.year.get()), int(self.weeknum.get().split()[1]), 1
        #     )
        #     weekdate_values.append(date)
        # self.weekdate = ttk.Combobox(
        #     self,
        #     values=weekdate_values,
        #     width=14,
        # )
        # self.weekdate.pack(side="top", padx=10, pady=5)
        # self.weekdate.set(weekdate_values[0])

        # self.shift = ttk.Combobox(
        #     self, values=["Shift 1", "Shift 2", "Shift 3"], width=14
        # )
        # self.shift.pack(side="top", padx=10, pady=5)
        # self.shift.set("Shift 1")

        ttk.Separator(self, orient="horizontal").pack(
            side="top", fill="x", padx=5, pady=5
        )

        self.button = ttk.Button(self, text="Get Data", bootstyle="primary", width=15)
        self.button.pack(side="top", padx=10, pady=5)

    def update_period_detail(self, event=None) -> None:
        if self.period.get() == "Weekly":
            self.period_detail.configure(values=[f"Week {i}" for i in range(1, 53)])
            self.period_detail.set(
                f"Week {datetime.date.fromisocalendar(datetime.date.today().year, datetime.date.today().isocalendar()[1], 1).isocalendar()[1]}"
            )
        else:
            self.period_detail.configure(
                values=[
                    f"{i:02d} - {datetime.date(datetime.date.today().year, i, 1).strftime('%B')}"
                    for i in range(1, 13)
                ]
            )
            self.period_detail.set(
                f"{datetime.date.today().month:02d} - {datetime.date(datetime.date.today().year, datetime.date.today().month, 1).strftime('%B')}"
            )


class RnMPage(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent)

        # self.mps_df = read_data_mps()
        # filtered_df = filter_data_mps_by_weeknumber(self.mps_df, 46)

        coldata = []
        column_name = ["DATE", "Shift", "Owner", "Equipment", "Activity Description"]
        for col in column_name:
            if col == "Activity Description":
                coldata.append({"text": col, "stretch": True, "width": 300})
            else:
                coldata.append({"text": col, "stretch": False, "width": 100})

        rowdata = []

        self.mps_table = Tableview(
            self,
            coldata=coldata,
            rowdata=rowdata,
            autofit=True,
            height=8,
        )
        self.mps_table.pack(side="top", padx=10, pady=5, fill="both", expand=True)

        report_frame = ttk.Frame(self)
        report_frame.pack(side="top", fill="both", expand=True, padx=5, pady=5)

        self.report_text = ttk.Text(
            report_frame, wrap="word", width=50, font=("consolas", 10)
        )
        self.report_text.pack(side="left", padx=5, pady=5, fill="both", expand=True)

        self.report_text.insert("1.0", "")

        self.qr_code_label = ttk.Label(report_frame, text="QR Code Placeholder")
        self.qr_code_label.pack(side="right", padx=5, pady=5)


class RnMUI(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent)

        self.app_cfg = read_config()
        self.cfg_rnm = read_config(section="RNM")

        self.rnm_sidebar = RnMSidebar(self)
        self.rnm_sidebar.pack(side="left", fill="y", expand=False)
        self.rnm_sidebar.linkup.configure(values=self.cfg_rnm.link_up)
        self.rnm_sidebar.linkup.set(self.cfg_rnm.link_up[0])

        self.rnm_sidebar.button.configure(command=self.on_get_data_rnm)

        ttk.Separator(self, orient="vertical").pack(
            side="left", fill="y", padx=5, pady=0
        )
        self.rnm_page = RnMPage(self)
        self.rnm_page.pack(side="left", fill="both", expand=True)

    @async_handler
    async def on_get_data_rnm(self) -> None:
        # Open file dialog to select SAP consumption excel file
        filepath = askopenfilename(
            # initialdir="/",  # Sets the initial directory
            title="Select MPS csv file",
            filetypes=(("Excel Files", "*.xlsx"), ("All Files", "*.*")),
        )
        if not filepath:
            # user cancelled dialog - do nothing
            return

        try:
            df = read_sap_consumption_data(filepath)
        except Exception as exc:  # keep exceptions friendly for UI
            logging.exception("Failed to read SAP consumption file")
            messagebox.showerror("Error", f"Failed to open file: {exc}")
            return

        # remove rows with null in "Posting date" and ensure we have data
        if "Posting date" in df.columns:
            df = df.filter(pl.col("Posting date").is_not_null())
        else:
            messagebox.showerror(
                "Error", "Selected file does not contain 'Posting date' column"
            )
            return

        if df.is_empty():
            messagebox.showinfo(
                "No data", "No records found for selected file / filters"
            )
            return

        # sort df by "Amount in IDR" descending
        df = df.sort("Amount in IDR", descending=True)

        # filter df based on sidebar selections
        # Cache UI values locally (avoid repeated .get())
        period = (self.rnm_sidebar.period.get() or "").lower()
        period_detail = self.rnm_sidebar.period_detail.get() or ""
        year_value = self.rnm_sidebar.year.get() or str(datetime.date.today().year)
        linkup_value = self.rnm_sidebar.linkup.get() or ""

        if period == "weekly":
            try:
                weeknum = int(period_detail.split()[1])
                period_value = period_detail
            except Exception:
                messagebox.showerror("Error", "Invalid week selected")
                return

            df = df.filter(
                (pl.col("Posting date").dt.week() == weeknum)
                & (pl.col("Posting date").dt.year() == int(year_value))
            )
            start_date, end_date = compute_period_dates(
                "weekly", period_detail, int(year_value)
            )
            end_date = start_date + datetime.timedelta(days=6)

        elif period == "monthly":
            try:
                month = int(period_detail.split(" - ")[0])
                period_value = period_detail.split(" - ")[1]
            except Exception:
                messagebox.showerror("Error", "Invalid month selected")
                return
            df = df.filter(
                (pl.col("Posting date").dt.month() == month)
                & (pl.col("Posting date").dt.year() == int(year_value))
            )
            start_date, end_date = compute_period_dates(
                "monthly", period_detail, int(year_value)
            )
            # compute_period_dates already sets end_date appropriately

        # df = aggregate_sap_consumption_data(df, period, period_detail)

        # Update tableview (only show top N rows quickly)
        column_name = df.columns
        coldata = build_coldata(column_name)

        rowdicts = df.head(10).to_dicts()
        rowdata = [list(row.values()) for row in rowdicts]
        self.rnm_page.mps_table.build_table_data(coldata=coldata, rowdata=rowdata)

        # Update report text
        # === Section RNM Cost Calculation ===
        period_col = "Weeknum" if period == "weekly" else "Month"
        period_expr = (
            pl.col("Posting date").dt.week()
            if period == "weekly"
            else pl.col("Posting date").dt.month()
        )

        df_grouped = (
            df.with_columns(period_expr.alias(period_col))
            .group_by([period_col])
            .agg(pl.col("Amount in IDR").sum().alias("R&M Cost"))
            .sort([period_col])
        )

        # Extract rnm_cost safely (handle empty group)
        if df_grouped.is_empty():
            rnm_cost = 0.0
        else:
            # safer retrieval by column name if available
            try:
                rnm_cost = float(df_grouped[0, "R&M Cost"])
            except Exception:
                # fallback to row/col indexing
                rnm_cost = float(df_grouped.row(0)[1])

        # === Section Fetch SPA Data for Net Production ===
        # sanitize linkup value - remove leading 'LU' only
        sanitized_linkup = sanitize_linkup(linkup_value)

        url = get_spa_url(
            self.app_cfg.environment,
            sanitized_linkup,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

        # Fetch SPA net production safely
        try:
            net_product = await get_spa_net_production(
                url,
                self.app_cfg.username,
                self.app_cfg.password,
                verify_ssl=self.app_cfg.verify_ssl,
            )
        except Exception as exc:
            logging.exception("Failed to fetch SPA net production")
            messagebox.showwarning(
                "SPA Error", f"Failed to fetch SPA net production: {exc}"
            )
            net_product = 0.0

        # === Section Report Text ===
        report_table = format_report_table(rnm_cost, net_product)

        # Top part consumption (moved to helper for clarity & tests)
        top_part_txt = format_top_parts(rowdicts, n=5)

        # rnm_cost_df = aggregate_sap_consumption_data(df, period)
        self.rnm_page.report_text.delete("1.0", "end")
        self.rnm_page.report_text.insert(
            "end", f"*R&M Report for {period_value} {year_value}*\n\n"
        )
        self.rnm_page.report_text.insert("end", report_table)
        self.rnm_page.report_text.insert("end", "\n\n*Top Part Consumption:*\n")
        self.rnm_page.report_text.insert("end", top_part_txt)

        # Generate QR (PIL image) in background thread, create ImageTk on main thread
        qr_img = await asyncio.to_thread(
            make_qr_image, self.rnm_page.report_text.get("1.0", "end")
        )
        qr_img_tk = ImageTk.PhotoImage(qr_img)

        self.rnm_page.qr_code_label.configure(image=qr_img_tk, text="")
        self.rnm_page.qr_code_label.image = qr_img_tk
        self.rnm_page._qr_image = qr_img_tk
