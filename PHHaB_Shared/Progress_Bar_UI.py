import sys
import time


class ProgressBar:
    def __init__(self, width=50):
        self.width = width
        self.ProcessName = None
        self.Starting = True
        self.complete = False
        self.starttime = time.time()

        
    def Process(self, Name):
        self.ProcessName = Name

    def update(self, percent):
        
        def convert_seconds_toTime(seconds):
            min, sec = divmod(seconds, 60)
            hour, min = divmod(min, 60)
            return '%d:%02d:%02d' % (hour, min, sec)
    
        if self.Starting == True:#
            print(f"\n\n\n🐍 {self.ProcessName}")
            self.Starting = False
            
        if self.complete  == True:
            pass
        else:
            percent = max(0, min(percent, 100))  # Clamp between 0 and 100
            filled = int(self.width * percent / 100)
            bar = '#' * filled + '-' * (self.width - filled)
            sys.stdout.write(f"\rProgress: [{bar}] {percent:.1f}%")
            sys.stdout.flush()
            if percent == 100:

                self.complete = True
                endtime = time.time()
                totaltime = endtime - self.starttime
                print(f"\n🐍 {self.ProcessName} now complete!!!! ✅ 🚀 time to Complete⌚: {convert_seconds_toTime(totaltime)} 👟")
                

