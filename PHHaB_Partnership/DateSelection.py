import customtkinter as ctk
from tkinter import filedialog as fd
import tkinter as tk
from PIL import Image, ImageTk
from tkcalendar import Calendar
from dataclasses import dataclass
from datetime import datetime, date
from PHHaB_Partnership.config_statements import ConfigureVariables
# from PHHaB_Reporting.SHALLNOTPASS import send_rsl_pause
import os
import sys


# Appearance settings
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


from dataclasses import dataclass

@dataclass
class UserSelections:
    import_date: date
    file_selected: str
    config_variables: dict
    skip_it: bool = False

class PHHaBProcessDataEntrySelection:
    def __init__(self, TestLive: str):
        self.root = ctk.CTk()
        self.selections = UserSelections(import_date=date.today(), file_selected="", config_variables={}, skip_it=False)
        self.TestLive = TestLive.lower()
        self.root.withdraw()  # Hide root window

    def start(self):
        self.show_splash()
        self.root.mainloop()
        
    def _get_dpi_scaling(self):
        try:
            import ctypes
            user32 = ctypes.windll.user32
            user32.SetProcessDPIAware()
            scale = user32.GetDpiForSystem() / 96  # 96 is standard DPI
            return scale
        except Exception as e:
            print(f"DPI scaling fallback: {e}")
            return 1.0

    def _get_physical_screen_size(self):
        try:
            import ctypes
            user32 = ctypes.windll.user32
            user32.SetProcessDPIAware()
            width = user32.GetSystemMetrics(0)
            height = user32.GetSystemMetrics(1)
            return width, height
        except Exception as e:
            print(f"Fallback to Tkinter screen size: {e}")
            return self.root.winfo_screenwidth(), self.root.winfo_screenheight()

    def show_splash(self):
        splash = tk.Toplevel(self.root)
        splash.overrideredirect(True)
        splash.lift()
        splash.focus_force()
        splash.attributes("-alpha", 0.0)  # Start transparent

        splashDimensionsW = 300
        splashDimensionsH = 200

        # Determine screen dimensions from splash itself
        screen_width = splash.winfo_screenwidth()
        screen_height = splash.winfo_screenheight()
        # x_offset = (screen_width - splashDimensionsW) // 2
        # y_offset = (screen_height - splashDimensionsH) // 2
        # splash.geometry(f"{splashDimensionsW}x{splashDimensionsH}+{x_offset}+{y_offset}")
        splash.geometry(f"{splashDimensionsW}x{splashDimensionsH}+{screen_width}+{screen_height}")
        

        # Resolve image path
        if hasattr(sys, '_MEIPASS'):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        image_path = os.path.join(base_path, 'img', 'PHHaB-favicon.png')

        try:
            img = Image.open(image_path)
            img = img.resize((splashDimensionsW, splashDimensionsH), Image.LANCZOS)
            photo = ImageTk.PhotoImage(img)
            label = tk.Label(splash, image=photo)
            label.image = photo
        except Exception as e:
            print(f"Error loading splash image: {e}")
            print(f"Location attempted: {image_path}")
            label = tk.Label(splash, text="PHHaB Data Entry", font=("Arial", 16), bg="black", fg="white")

        label.pack(fill="both", expand=True)
        splash.update_idletasks()  # Ensure layout is finalized


        def fade_in(alpha=0.0):
            alpha += 0.05
            if alpha <= 1.0 and splash.winfo_exists():
                splash.attributes("-alpha", alpha)
                splash.after(30, lambda: fade_in(alpha))

        fade_in()

        def proceed():
            splash.destroy()
            self._build_main_ui()

        splash.after(3000, proceed)


    def _build_main_ui(self):
        self.root.withdraw()
        self.window = ctk.CTkToplevel(self.root)
        self.window.title("PHHaB Process Data Entry")
        self.window.geometry("600x400")
        self.window.minsize(400, 200)
        self._build_calendar()
        self._build_controls()
        self.root.wait_window(self.window)

    def _build_calendar(self):
        calendar_frame = ctk.CTkFrame(self.window)
        calendar_frame.pack(padx=20, pady=20, fill="both", expand=True)

        self.date_picker = Calendar(
            calendar_frame,
            selectmode='day',
            background="#2d2d2d",
            foreground="#ffffff",
            selectbackground="#007acc",
            selectforeground="#ffffff",
            normalbackground="#3a3a3a",
            normalforeground="#d4d4d4",
            weekendbackground="#3a3a3a",
            weekendforeground="#d4d4d4",
            othermonthbackground="#1e1e1e",
            othermonthforeground="#808080",
            othermonthwebackground="#1e1e1e",
            othermonthweforeground="#808080",
            cursor="hand2",
            year=date.today().year,
            month=date.today().month,
            day=date.today().day,
            showweeknumber=False,
        )
        self.date_picker.pack(fill="both", expand=True)

    def _build_controls(self):
        panel = ctk.CTkFrame(self.window)
        panel.pack(padx=20, pady=10, fill="x")

        button1 = ctk.CTkButton(panel, text="Run PHHaB Data Entry", command=self._run_data_entry)
        button1.grid(row=0, column=0, padx=10, pady=10, sticky="ew")
        
        button2 = ctk.CTkButton(panel, text="Reset Variables", command=self._set_variables)
        button2.grid(row=0, column=2, padx=10, pady=10, sticky="ew")
        
        button3 = ctk.CTkButton(panel, text="Send Emails", command=self._send_email)
        button3.grid(row=0, column=4, padx=10, pady=10, sticky="ew")
        
        cancelButn = ctk.CTkButton(panel, text="Cancel", command=self._cancel)
        cancelButn.grid(row=1, column=0, columnspan=6, padx=10, pady=10, sticky="ew")
            
    def _send_email(self):
        config = ConfigureVariables(self.TestLive)
        config.run_configuration()
        config_vars = config.config
        
        raw_date = self.date_picker.get_date()
        import_date = datetime.strptime(raw_date, "%m/%d/%y").date()


        self.selections = UserSelections(import_date=import_date,
                                    file_selected=None,
                                    config_variables=config_vars,
                                    skip_it=True
                            )

        self.root.destroy()
    

    
    def _run_data_entry(self):
        self._finalize_selection(configure=False)

    def _set_variables(self):
        config = ConfigureVariables(self.TestLive)
        config.run_configuration_Overwrite()
        self.window.destroy()

    def _cancel(self):
        self.root.destroy()
        sys.exit()#"PHHaB Data Entry cancelled by user.")

    def _finalize_selection(self, configure: bool):
        config = ConfigureVariables(self.TestLive)
        config.run_configuration()

        raw_date = self.date_picker.get_date()
        import_date = datetime.strptime(raw_date, "%m/%d/%y").date()

        file_selected = fd.askopenfilename(title='Select ZIP file', filetypes=[('Zip File', '*.zip')])
        if not file_selected:
            return

        config = ConfigureVariables(self.TestLive)
        config.run_configuration()
        config_vars = config.config

        # self.selections = UserSelections(import_date=import_date, file_selected=file_selected, config_variables=config_vars, skip_it="")
        
        self.selections = UserSelections(import_date=import_date,
                                         file_selected=file_selected,
                                         config_variables=config_vars,
                                         skip_it=self.selections.skip_it
                                    )
            
        #hide window
        self.root.destroy()

    def __new__(cls, TestLive: str):
        self = super().__new__(cls)
        cls.__init__(self, TestLive)
        self.start()
        return self.selections