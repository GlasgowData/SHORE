from tkinter import filedialog as fd
import os
import json
from tkinter import messagebox
import tkinter as tk
import customtkinter as ctk
from tkcalendar import Calendar
from datetime import date

class calendarClass:
    
    def __init__(self):
        self.root = ctk.CTk()
        self.root.withdraw()
        self.selected_date = None

    def get_date(self):
        """Opens the calendar UI and returns the selected date."""
        self.window = ctk.CTkToplevel(self.root)
        self.window.title("Select Start Date")
        self.window.geometry("400x400")

        # Calendar widget
        self.date_picker = Calendar(
            self.window,
            selectmode='day',
            year=date.today().year,
            month=date.today().month,
            day=date.today().day
        )
        self.date_picker.pack(padx=20, pady=20, fill="both", expand=True)

        # OK Button
        ok_button = ctk.CTkButton(
            self.window,
            text="OK",
            command=self._confirm_date
        )
        ok_button.pack(pady=10)

        self.root.wait_window(self.window)
        return self.selected_date

    def _confirm_date(self):
        """Saves selected date and closes window."""
        self.selected_date = self.date_picker.get_date()
        self.window.destroy()

class ConfigureVariables:

    def __init__(self, testlive):
        self.testlive = testlive
        self.config = None

    def get_config_path(self):
        if os.name == "nt":
            base = os.getenv("APPDATA")
        else:
            base = os.path.expanduser("~/.config")
        return os.path.join(base, "myapp_config.json")

    def load_config(self):
        path = self.get_config_path()
        return json.load(open(path)) if os.path.exists(path) else {}

    def save_config(self, config):
        path = self.get_config_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        json.dump(config, open(path, "w"))

    def set_Files_andFolders(self):
        # 1. Pick SQLite database
        db_path = fd.askopenfilename(
            title=f"{self.testlive} SPS Database Location:",
            filetypes=[("SQLite DB", "*.sqlite *.db"), ("All files", "*.*")]
        )

        # 2. Pick Start Date using Calendar
        cal = calendarClass()
        start_date = cal.get_date()

        # 3. Pick reports folder
        reports_folder = fd.askdirectory(
            title=f"{self.testlive} Location of Reporting folder:"
        )

        config_data = {
            "spsDataBaseLocation": db_path,
            "reportsLocation": reports_folder,
            "startDate": start_date
        }
        return config_data

    def run_configuration(self):
        self.config = self.load_config()

        if "spsDataBaseLocation" not in self.config:
            config_data = self.set_Files_andFolders()
            self.save_config(config_data)
            messagebox.showinfo(message="SHORE Config Saved")

    def run_configuration_Overwrite(self):
        config_data = self.set_Files_andFolders()
        self.save_config(config_data)
        messagebox.showinfo(message="SHORE Config Saved")
