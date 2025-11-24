
# PM Champion Dashboard

This repository contains a Tkinter dashboard application built with [ttkbootstrap](https://github.com/israel-dryer/ttkbootstrap). The app provides multiple views for project management metrics, CSV data exploration, and configuration.

## Features

- Dashboard views for BDE, DH, MPS, and RNM metrics
- Data view for exploring CSV datasets (located in `assets/`)
- Settings view for theme and configuration (uses `config/config.ini`)
- Modular UI components in `src/components/`
- Service layer for data access in `src/services/`
- Utility functions in `src/utils/`

## Project Structure

- `main.py` — Entry point for the application
- `src/app.py` — Main app logic and window
- `src/components/` — UI components (side tabs, dashboards)
- `src/services/` — Data service modules for each dashboard
- `src/ui/` — UI logic for each dashboard
- `assets/` — CSV data files
- `config/` — Configuration files

## Running the App

```powershell
# From the project root
python main.py
```

## Dependencies

All dependencies are listed in `pyproject.toml` and can be installed with:

```powershell
uv pip install -r requirements.txt
```

Main dependencies:
	...existing code...

## Building the Executable (PyInstaller)

To compile the application into a single .exe file using the provided spec file:

```powershell
uv run pyinstaller --clean pm-champion-dashboard.spec
```

The resulting executable will be located in the `dist/` folder. Make sure you have PyInstaller installed in your environment:

```powershell
uv pip install pyinstaller
```

The icon and all required data/config files will be included automatically.
- pandas
- ttkbootstrap
- async-tkinter-loop

## Notes

- The app loads CSV files from the `assets/` folder (e.g., `DH_2025-10-07_16-38_Packer21_Maker21.csv`).
- Configuration is managed via `config/config.ini`.
- For development, use a Python virtual environment and install dependencies as above.
