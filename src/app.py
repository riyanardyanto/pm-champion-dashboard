from __future__ import annotations


import tkinter as tk

from typing import Optional

import ttkbootstrap as ttk
from PIL import Image, ImageTk
from src.components.side_tab_notebook import SideTabNotebook

from src.ui.bde_ui import BDEUI
from src.ui.dh_ui import DHUI
from src.ui.mps_ui import MPSUI
from src.ui.rnm_ui import RnMUI
from src.utils.helpers import resource_path


class App:
    def __init__(self, root: Optional[ttk.Window] = None) -> None:
        self.root = root or ttk.Window(themename="darkly")
        self.root.title("PM Champion - Dashboard")
        self.root.geometry("1200x670")
        self.root.minsize(1200, 670)
        self.root.iconbitmap(resource_path("assets/pm.ico"))

        # Top header
        header = ttk.Frame(self.root, padding=6)
        header.pack(side="top", fill="x")
        self.photo = ImageTk.PhotoImage(
            image=Image.open(resource_path("assets/pm.png")).resize((40, 40))
        )
        logo = ttk.Label(
            header,
            image=self.photo,
        )
        logo.pack(side="left", padx=(50, 10))
        title = ttk.Label(
            header, text="PM Champion - Dashboard", font=("Segoe UI", 22, "bold")
        )
        title.pack(side="left")

        ttk.Separator(self.root, orient="horizontal").pack(
            side="top", fill="x", padx=5, pady=(5, 0)
        )

        self.style = ttk.Style()

        # Main area
        main = ttk.Frame(self.root)
        main.pack(fill="both", expand=True)

        # Left-side tabbed notebook replacement
        self.notebook = SideTabNotebook(main)
        self.notebook.pack(fill="both", expand=True)

        # DH tab
        self.dh_frame = DHUI(self.notebook)
        self.notebook.add(self.dh_frame, text="DH")

        # MPS tab
        self.mps_frame = MPSUI(self.notebook)
        self.notebook.add(self.mps_frame, text="MPS")

        # R&M tab
        self.rm_frame = RnMUI(self.notebook)
        self.notebook.add(self.rm_frame, text="R&M")

        # BDE tab
        self.bde_frame = BDEUI(self.notebook)
        self.notebook.add(self.bde_frame, text="BDE")

    def create_data_page(self, parent: ttk.Frame) -> None:
        controls = ttk.Frame(parent)
        controls.pack(side="top", fill="x", padx=6, pady=(6, 0))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(controls, textvariable=self.search_var, width=40)
        search_entry.pack(side="left", padx=(0, 8))

        # Treeview for full dataset
        cols = (
            "NUMBER",
            "STATUS",
            "MACHINE AREA",
            "PRIORITY",
            "DESCRIPTION",
            "REPORTED AT",
        )
        self.data_tree = ttk.Treeview(parent, columns=cols, show="headings", height=20)
        for c in cols:
            self.data_tree.heading(c, text=c)
            self.data_tree.column(c, width=160, anchor="w")

        vsb = ttk.Scrollbar(parent, orient="vertical", command=self.data_tree.yview)
        hsb = ttk.Scrollbar(parent, orient="horizontal", command=self.data_tree.xview)
        self.data_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        container = ttk.Frame(parent)
        container.pack(fill="both", expand=True, padx=6, pady=6)
        vsb.pack(in_=container, side="right", fill="y")
        hsb.pack(in_=container, side="bottom", fill="x")
        self.data_tree.pack(in_=container, side="left", fill="both", expand=True)

    def create_settings_page(self, parent: ttk.Frame) -> None:
        ttk.Label(parent, text="Theme", font=("Segoe UI", 12, "bold")).pack(
            anchor="nw", padx=8, pady=(8, 2)
        )
        theme_frame = ttk.Frame(parent)
        theme_frame.pack(anchor="nw", padx=8, pady=4)
        self.available_themes = self.style.theme_names()
        self.theme_var = tk.StringVar(value=self.style.theme_use())
        for theme in self.available_themes:
            r = ttk.Radiobutton(
                theme_frame,
                text=theme,
                variable=self.theme_var,
                value=theme,
                command=self.apply_theme,
            )
            r.pack(anchor="w")

        # Card style toggle
        ttk.Label(parent, text="Cards", font=("Segoe UI", 12, "bold")).pack(
            anchor="nw", padx=8, pady=(8, 2)
        )
        card_frame = ttk.Frame(parent)
        card_frame.pack(anchor="nw", padx=8, pady=4)
        # self.card_light_var = tk.BooleanVar(value=self.dashboard.light_cards)
        card_toggle = ttk.Checkbutton(
            card_frame,
            text="Light cards",
            variable=self.card_light_var,
            # command=lambda: self.dashboard.set_light_cards(self.card_light_var.get()),
        )
        card_toggle.pack(anchor="w")

    def filter_data(self) -> None:
        query = (self.search_var.get() or "").strip().lower()
        if self.df is None:
            return
        if not query:
            filtered = self.df
        else:
            # Simple filter: search in NUMBER and DESCRIPTION
            filtered = self.df[
                self.df["NUMBER"].astype(str).str.lower().str.contains(query)
                | self.df["DESCRIPTION"].astype(str).str.lower().str.contains(query)
            ]
        self.populate_data_tree(filtered)

    def apply_theme(self) -> None:
        theme = self.theme_var.get()
        try:
            self.style.theme_use(theme)
        except Exception:
            pass


def main():
    try:
        app = App()
        app.root.mainloop()
    except Exception as exc:  # pragma: no cover - GUI runtime
        print("Error running app:", exc)
        raise


if __name__ == "__main__":
    main()
