import datetime
import ttkbootstrap as ttk
from ttkbootstrap.tableview import Tableview

from src.services.mps_data_service import (
    create_report_text,
    filter_data_mps_by_date_and_shift,
    filter_data_mps_by_weeknumber,
    read_data_mps,
)
from src.utils.app_config import read_config
import qrcode
from PIL import ImageTk
import asyncio
from async_tkinter_loop import async_handler


class MPSSidebar(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent, width=120)

        # Set label text color to blue (biru)
        label = ttk.Label(
            self,
            text="MPS",
            font=("Segoe UI", 36, "bold"),
            foreground="white",
            bootstyle="primary",
            # background="#24c647",
            justify="center",
        )
        label.pack(side="top", padx=(10, 10), pady=(0, 5))

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
        self.year.bind("<<ComboboxSelected>>", self.update_weekdate)

        self.weeknum = ttk.Combobox(
            self, values=[f"Week {i}" for i in range(1, 53)], width=14
        )
        self.weeknum.pack(side="top", padx=10, pady=5)
        # set default week number to current week number
        self.weeknum.set(
            f"Week {datetime.date.fromisocalendar(datetime.date.today().year, datetime.date.today().isocalendar()[1], 1).isocalendar()[1]}"
        )
        # konfiurasi event handler tanggal jika weeknum diubah
        self.weeknum.bind("<<ComboboxSelected>>", self.update_weekdate)

        weekdate_values = []
        for i in range(7):
            date = datetime.date.fromisocalendar(
                int(self.year.get()), int(self.weeknum.get().split()[1]), 1
            )
            weekdate_values.append(date)
        self.weekdate = ttk.Combobox(
            self,
            values=weekdate_values,
            width=14,
        )
        self.weekdate.pack(side="top", padx=10, pady=5)
        self.weekdate.set(weekdate_values[0])

        self.shift = ttk.Combobox(
            self, values=["Shift 1", "Shift 2", "Shift 3"], width=14
        )
        self.shift.pack(side="top", padx=10, pady=5)
        self.shift.set("Shift 1")

        ttk.Separator(self, orient="horizontal").pack(
            side="top", fill="x", padx=5, pady=5
        )

        self.button = ttk.Button(self, text="Get Data", bootstyle="primary", width=15)
        self.button.pack(side="top", padx=10, pady=5)

    def update_weekdate(self, event=None) -> None:
        weekdate_values = []
        for i in range(7):
            date = datetime.date.fromisocalendar(
                int(self.year.get()), int(self.weeknum.get().split()[1]), i + 1
            )
            weekdate_values.append(date)
        self.weekdate["values"] = weekdate_values
        self.weekdate.set(weekdate_values[0])


class MPSPage(ttk.Frame):

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


class MPSUI(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent)
        self.cfg = read_config(section="MPS")

        self.mps_sidebar = MPSSidebar(self)
        self.mps_sidebar.pack(side="left", fill="y", expand=False)
        self.mps_sidebar.linkup.configure(values=self.cfg.link_up)
        self.mps_sidebar.linkup.set(self.cfg.link_up[0])
        self.mps_sidebar.button.configure(command=self.on_get_data_mps)

        ttk.Separator(self, orient="vertical").pack(
            side="left", fill="y", padx=5, pady=0
        )
        self.mps_page = MPSPage(self)
        self.mps_page.pack(side="left", fill="both", expand=True)

    @async_handler
    async def on_get_data_mps(self) -> None:
        try:
            # Implement the logic to handle the "Get Data" button click
            # Prefer the path configured under the `MPS` section in config.ini.
            cfg = read_config(section="MPS")
            path = cfg.file_path[cfg.link_up.index(self.mps_sidebar.linkup.get())]

            # Read data in background thread
            self.mps_df = await asyncio.to_thread(read_data_mps, path)

            selected_year = int(self.mps_sidebar.year.get())
            selected_week = self.mps_sidebar.weeknum.get()
            week_number = int(selected_week.split(" ")[1])
            selected_day = self.mps_sidebar.weekdate.get()
            selected_shift = self.mps_sidebar.shift.get()

            # Prepare filtered table data in background thread
            filtered_df_by_week = await asyncio.to_thread(
                filter_data_mps_by_weeknumber, self.mps_df, week_number, selected_year
            )

            rowdata = await asyncio.to_thread(
                lambda df: [list(row.values()) for row in df.to_dicts()],
                filtered_df_by_week,
            )

            coldata = await asyncio.to_thread(
                lambda df: [
                    (
                        {"text": col, "stretch": True, "width": 300}
                        if col == "Activity Description"
                        else {"text": col, "stretch": False, "width": 100}
                    )
                    for col in df.columns
                ],
                filtered_df_by_week,
            )

            # Update GUI table on main thread
            self.mps_page.mps_table.build_table_data(coldata=coldata, rowdata=rowdata)

            # Prepare report data in background thread
            report_df = await asyncio.to_thread(
                filter_data_mps_by_date_and_shift,
                self.mps_df,
                selected_day,
                int(selected_shift.split(" ")[1]),
            )
            report_text = await asyncio.to_thread(create_report_text, report_df)

            # Update report widget on main thread
            self.mps_page.report_text.delete("1.0", "end")
            self.mps_page.report_text.insert("1.0", report_text)

            # Generate QR (PIL image) in background thread, convert to ImageTk in main thread
            def _make_qr_image(text: str):
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
                return img.resize((400, 400))

            qr_img = await asyncio.to_thread(_make_qr_image, report_text)
            qr_img_tk = ImageTk.PhotoImage(qr_img)

            self.mps_page.qr_code_label.configure(image=qr_img_tk, text="")
            self.mps_page.qr_code_label.image = qr_img_tk
            self.mps_page._qr_image = qr_img_tk
        except Exception as e:
            import traceback

            error_msg = f"Terjadi error:\n{e}\n\n{traceback.format_exc()}"
            self.mps_page.report_text.delete("1.0", "end")
            self.mps_page.report_text.insert("1.0", error_msg)
            self.mps_page.qr_code_label.configure(image="", text="Error")
            self.mps_page.qr_code_label.image = None
            self.mps_page._qr_image = None
