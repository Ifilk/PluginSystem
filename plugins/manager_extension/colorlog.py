import logging
import sys


class ColorFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[37m',     # White
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[1;31m' # Bold Red
    }
    RESET = '\033[0m'

    def format(self, record):
        level_color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{level_color}{message}{self.RESET}"


def patch_logger_with_color(logger_name: str, fmt: str, datefmt: str = '%Y-%m-%d  %H:%M:%S'):
    logger = logging.getLogger(logger_name)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColorFormatter(fmt, datefmt=datefmt))

    logger.addHandler(handler)