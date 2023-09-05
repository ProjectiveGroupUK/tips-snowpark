from typing import Dict
import json
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
from datetime import datetime
from colorama import init, Fore, Back
from pathlib import Path
import os

init(autoreset=True)
LOGGER_ROOT_NAME = "tips"


class CustomFormatter(logging.Formatter):
    formatDebug = (
        "%(asctime)s [%(levelname)s]: %(name)s - %(message)s (%(filename)s:%(lineno)d)"
    )
    formatInfo = "%(asctime)s [%(levelname)s]: %(message)s"
    formatError = (
        "%(asctime)s [%(levelname)s]: %(name)s - %(message)s (%(filename)s:%(lineno)d)"
    )

    FORMATS = {
        logging.DEBUG: Fore.WHITE + formatDebug + Fore.RESET,
        logging.INFO: Fore.GREEN + formatInfo + Fore.RESET,
        logging.WARNING: Fore.YELLOW + formatError + Fore.RESET,
        logging.ERROR: Fore.RED + formatError + Fore.RESET,
        logging.CRITICAL: Fore.WHITE + Back.RED + formatError + Fore.RESET + Back.RESET,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class Logger:
    logDir: Path
    logger: Logger
    info_file_handler: RotatingFileHandler
    warning_file_handler: RotatingFileHandler
    error_file_handler: RotatingFileHandler

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance") or not cls.instance:
            cls.instance = super().__new__(cls)

        return cls.instance

    def initialize(self):
        self.logger = logging.getLogger(LOGGER_ROOT_NAME)
        env = os.environ.get("env", "dev")
        console = logging.StreamHandler()

        if env.lower() == "debug":
            self.logger.setLevel(logging.DEBUG)
            console.setLevel(logging.DEBUG)
        elif env.lower() in ("dev", "development"):
            self.logger.setLevel(logging.INFO)
            console.setLevel(logging.INFO)
        elif env.lower() in ("prod", "production"):
            self.logger.setLevel(logging.ERROR)
            console.setLevel(logging.ERROR)
        else:
            self.logger.setLevel(logging.INFO)
            console.setLevel(logging.INFO)

        console.setFormatter(CustomFormatter())
        self.logger.addHandler(console)

        return self.logger

    def addFileHandler(self, processName: str = None):
        self.logger = logging.getLogger(LOGGER_ROOT_NAME)
        # Create logs folder if doesn't exists
        subfldrts = datetime.now().strftime("%Y%m%d%H%M%S")
        if processName is None:
            self.logDir = Path.joinpath(Path.cwd(), "logs", subfldrts)
        else:
            self.logDir = Path.joinpath(
                Path.cwd(), "logs", processName.lower(), subfldrts
            )

        self.logDir.mkdir(parents=True, exist_ok=True)

        info_log_file = Path.joinpath(self.logDir, "info.log")
        self.info_file_handler = RotatingFileHandler(
            filename=info_log_file, maxBytes=10485760, backupCount=20, encoding="utf8"
        )
        warning_log_file = Path.joinpath(self.logDir, "warning.log")
        self.warning_file_handler = RotatingFileHandler(
            filename=warning_log_file,
            maxBytes=10485760,
            backupCount=20,
            encoding="utf8",
        )
        error_log_file = Path.joinpath(self.logDir, "error.log")
        self.error_file_handler = RotatingFileHandler(
            filename=error_log_file, maxBytes=10485760, backupCount=20, encoding="utf8"
        )
        self.info_file_handler.setLevel(logging.INFO)
        self.warning_file_handler.setLevel(logging.WARNING)
        self.error_file_handler.setLevel(logging.ERROR)

        fileFormatter = logging.Formatter(
            fmt="%(asctime)s: %(name)s => %(levelname)s :: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.info_file_handler.setFormatter(fileFormatter)
        self.warning_file_handler.setFormatter(fileFormatter)
        self.error_file_handler.setFormatter(fileFormatter)
        self.logger.addHandler(self.info_file_handler)
        self.logger.addHandler(self.warning_file_handler)
        self.logger.addHandler(self.error_file_handler)

    def removeFileHandler(self):
        self.logger = logging.getLogger(LOGGER_ROOT_NAME)
        self.logger.removeHandler(self.info_file_handler)
        self.logger.removeHandler(self.warning_file_handler)
        self.logger.removeHandler(self.error_file_handler)
        self.info_file_handler.close()
        self.warning_file_handler.close()
        self.error_file_handler.close()

    def getRootLoggerName() -> str:
        return LOGGER_ROOT_NAME

    def getLogDir(self) -> Path:
        return self.logDir

    def writeResultJson(self, resultJson: Dict) -> None:
        self.logger.info("Writing log output to file...")
        out_filename = os.path.join(self.logDir, "result.json")

        with open(out_filename, "w") as outfile:
            json.dump(resultJson, outfile, indent=4)

        self.logger.info("Output written to file!!")


def main():
    logger = Logger().initialize()
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.debug("This is a debug message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")


if __name__ == "__main__":
    main()
