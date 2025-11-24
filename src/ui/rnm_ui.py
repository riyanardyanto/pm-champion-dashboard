import ttkbootstrap as ttk

# from ttkbootstrap.tableview import Tableview
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

        self.button = ttk.Button(self, text="Get Data", bootstyle="primary", width=15)
        self.button.pack(side="top", padx=10, pady=5)


class RnMPage(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent)

        # self.dh_df = read_data_dh()
        # filtered_df = filter_data_dh_by_weeknumber(self.dh_df, 46)

        label = ttk.Label(
            self,
            text="This is R&M Page\n(Under Construction)\nWait for the next update...",
            font=("Segoe UI", 18, "normal"),
            foreground="white",
            bootstyle="primary",
            justify="center",
        )
        label.pack(side="top", anchor="center", padx=(10, 10), pady=(5, 5))


class RnMUI(ttk.Frame):

    def __init__(self, parent: ttk.Frame) -> None:
        super().__init__(parent)

        self.rnm_sidebar = RnMSidebar(self)
        self.rnm_sidebar.pack(side="left", fill="y", expand=False)

        self.rnm_sidebar.button.configure(command=self.on_get_data)

        ttk.Separator(self, orient="vertical").pack(
            side="left", fill="y", padx=5, pady=0
        )
        self.rnm_page = RnMPage(self)
        self.rnm_page.pack(side="left", fill="both", expand=True)

    def on_get_data(self) -> None:
        pass
