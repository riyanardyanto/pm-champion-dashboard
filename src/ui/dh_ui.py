import ttkbootstrap as ttk
from ttkbootstrap.tableview import Tableview
from tkinter.filedialog import askopenfilename
from src.services.dh_data_service import create_dh_report_text, read_data_dh

import qrcode
from PIL import ImageTk
import asyncio
from async_tkinter_loop import async_handler


class DHSidebar(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent, width=120)

        # Set label text color to blue (biru)
        label = ttk.Label(
            self,
            text="DH",
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

        self.button = ttk.Button(self, text="Get Data", bootstyle="primary", width=15)
        self.button.pack(side="top", padx=10, pady=5)


class DHPage(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent)

        # self.dh_df = read_data_dh()
        # filtered_df = filter_data_dh_by_weeknumber(self.dh_df, 46)

        coldata = []
        column_name = [
            "NUMBER",
            "STATUS",
            "WORK CENTER TYPE",
            "PRIORITY",
            "DESCRIPTION",
        ]
        for col in column_name:
            if col == "DESCRIPTION":
                coldata.append({"text": col, "stretch": True, "width": 300})
            else:
                coldata.append({"text": col, "stretch": False, "width": 100})

        rowdata = []

        self.dh_table = Tableview(
            self,
            coldata=coldata,
            rowdata=rowdata,
            autofit=True,
            height=8,
        )
        self.dh_table.pack(side="top", padx=10, pady=5, fill="both", expand=True)

        report_frame = ttk.Frame(self)
        report_frame.pack(side="top", fill="both", expand=True, padx=5, pady=5)

        self.report_text = ttk.Text(
            report_frame, wrap="word", width=50, font=("consolas", 10)
        )
        self.report_text.pack(side="left", padx=5, pady=5, fill="both", expand=True)

        self.report_text.insert("1.0", "")

        self.qr_code_label = ttk.Label(report_frame, text="QR Code Placeholder")
        self.qr_code_label.pack(side="right", padx=5, pady=5)


class DHUI(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent)

        self.dh_sidebar = DHSidebar(self)
        self.dh_sidebar.pack(side="left", fill="y", expand=False)

        self.dh_sidebar.button.configure(command=self.on_get_data_dh)

        ttk.Separator(self, orient="vertical").pack(
            side="left", fill="y", padx=5, pady=0
        )
        self.dh_page = DHPage(self)
        self.dh_page.pack(side="left", fill="both", expand=True)

    @async_handler
    async def on_get_data_dh(self) -> None:
        # Implement the logic to handle the "Get Data" button click
        filepath = askopenfilename(
            # initialdir="/",  # Sets the initial directory
            title="Select DH csv file",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
        )
        if filepath:
            # Read/prepare data in background thread
            self.dh_df = await asyncio.to_thread(read_data_dh, filepath)

            def _select_columns(df):
                return df[
                    [
                        "NUMBER",
                        "STATUS",
                        "WORK CENTER TYPE",
                        "PRIORITY",
                        "DESCRIPTION",
                    ]
                ]

            self.filtered_dh_df = await asyncio.to_thread(_select_columns, self.dh_df)

        # Build table data in background thread
        rowdata = await asyncio.to_thread(
            lambda df: [list(row.values()) for row in df.to_dicts()],
            self.filtered_dh_df,
        )
        coldata = await asyncio.to_thread(
            lambda df: [
                (
                    {"text": col, "stretch": True, "width": 300}
                    if col == "DESCRIPTION"
                    else {"text": col, "stretch": False, "width": 100}
                )
                for col in df.columns
            ],
            self.filtered_dh_df,
        )

        # Update GUI table on main thread
        self.dh_page.dh_table.build_table_data(coldata=coldata, rowdata=rowdata)

        # Create report text in background thread then update GUI
        report_text = await asyncio.to_thread(create_dh_report_text, self.dh_df)
        self.dh_page.report_text.delete("1.0", "end")
        self.dh_page.report_text.insert("1.0", report_text)

        # Generate QR (PIL image) in background thread, create ImageTk on main thread
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

        self.dh_page.qr_code_label.configure(image=qr_img_tk, text="")
        self.dh_page.qr_code_label.image = qr_img_tk
        self.dh_page._qr_image = qr_img_tk
