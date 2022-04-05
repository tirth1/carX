from datetime import date, datetime

class AppLogger:
    def __init__(self) -> None:
        pass

    def log(self, log_file, log_message):
        self.now = datetime.now()
        self.date = datetime.date()
        self.time = self.now.strftime("%H:%M:%S")
        log_file.write(
            str(self.date) + "/" + str(self.time) + "\t\t" + log_message + "\n"
        )