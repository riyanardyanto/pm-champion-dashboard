import ttkbootstrap as ttk
import tkinter as tk


class SideTabNotebook(ttk.Frame):
    """A simple notebook replacement with left-side tab buttons.

    - Left column of buttons acts as tabs.
    - Content frames are shown in the right content area.
    - Buttons are equal-width (by character count) for consistent appearance.

    Usage:
        nb = SideTabNotebook(parent)
        page = ttk.Frame(nb)
        nb.add(page, text="Tab 1")

    The child frames may be created with `nb` as their master (works fine) or
    created elsewhere and passed into `add()`; the widget will pack them
    into the content area when selected.
    """

    def __init__(self, master=None, **kwargs):
        super().__init__(master, **kwargs)
        self.tabs = []  # List of (text, frame) tuples
        self.current_tab = None
        self.max_tab_length = 0

        # Create custom style for darker button frame
        style = ttk.Style()
        style.configure("Darker.TFrame", background="#1a1a1a")  # Very dark gray/black

        # Create custom button styles with matching text colors
        style.configure(
            "Accent.TButton",
            background="#1a1a1a",
            foreground="#007bff",  # Primary blue text for inactive
            borderwidth=0,
            focuscolor="#1a1a1a",  # Match background to hide focus ring
            lightcolor="#1a1a1a",  # Remove border highlights
            darkcolor="#1a1a1a",
        )
        # Subtle hover effect for inactive buttons
        style.map(
            "Accent.TButton",
            background=[("active", "#252525")],  # Slightly lighter on hover
            foreground=[("active", "#007bff")],
        )

        style.configure(
            "AccentActive.TButton",
            background="#2a2a2a",
            foreground="white",  # White text for active
            borderwidth=0,
            focuscolor="#2a2a2a",  # Match background to hide focus ring
            lightcolor="#2a2a2a",  # Remove border highlights
            darkcolor="#2a2a2a",
        )
        # Subtle hover effect for active buttons
        style.map(
            "AccentActive.TButton",
            background=[("active", "#353535")],  # Slightly lighter on hover
            foreground=[("active", "white")],
        )

        # Create the button frame (left side) with minimal padding
        self.button_frame = ttk.Frame(
            self, style="Darker.TFrame", padding=(0, 10, 0, 0)
        )
        self.button_frame.pack(side="left", fill="y")

        # Create the content frame (right side)
        self.content_frame = ttk.Frame(self)
        self.content_frame.pack(side="right", fill="both", expand=True, padx=5, pady=5)

    def add(self, child, text="", **kwargs):
        """Add a new tab with the given child frame and text."""
        if text == "":
            text = f"Tab {len(self.tabs) + 1}"

        # Update max tab length for consistent button widths
        self.max_tab_length = max(self.max_tab_length, len(text)) + 1

        button_area = ttk.Frame(self.button_frame, style="Darker.TFrame")
        button_area.pack(fill="x", pady=1)

        # Wider accent bar for better visibility
        accent = tk.Frame(button_area, width=5, bg="#007bff")
        accent.pack(side="left", fill="y", pady=1)

        # Create the button with custom accent color styling
        button = ttk.Button(
            button_area,
            text=text,
            command=lambda: self.select(child),
            style="Accent.TButton",
        )
        button.pack(side="left", fill="x", expand=True, pady=0, padx=(0, 2))

        # Store the tab info
        self.tabs.append((text, child, button, accent))

        # Update all button widths to be consistent
        for _, _, btn, _ in self.tabs:
            btn.configure(width=self.max_tab_length)

        # If this is the first tab, select it
        if len(self.tabs) == 1:
            self.select(child)

    def select(self, child):
        """Select the tab containing the given child frame."""
        # Hide the current tab
        if self.current_tab:
            self.current_tab.pack_forget()

        # Find the tab for this child
        for text, frame, button, accent in self.tabs:
            if frame == child:
                # Update button style to show it's selected
                for _, _, btn, acc in self.tabs:
                    btn.configure(style="Accent.TButton")  # Reset to accent color text
                    acc.configure(bg="#007bff")  # Inactive accent color
                button.configure(
                    style="AccentActive.TButton"
                )  # Active style with white text
                accent.configure(bg="white")  # Active accent color

                # Show the new tab
                frame.pack(in_=self.content_frame, fill="both", expand=True)
                self.current_tab = frame
                break
