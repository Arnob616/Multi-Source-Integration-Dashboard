"""logger_config.py — Coloured console + rotating daily file logging."""

import logging, logging.handlers, sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

class _Colour(logging.Formatter):
    C = {logging.DEBUG:"\x1b[38;5;240m", logging.INFO:"\x1b[36m",
         logging.WARNING:"\x1b[33m", logging.ERROR:"\x1b[31m",
         logging.CRITICAL:"\x1b[31;1m"}
    R = "\x1b[0m"; G = "\x1b[32m"
    def format(self, r):
        r.levelname = f"{self.C.get(r.levelno,'')}{r.levelname:<8}{self.R}"
        r.name      = f"{self.G}{r.name}{self.R}"
        return super().format(r)

def setup_logging(level="INFO"):
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()
    c = logging.StreamHandler(sys.stdout)
    c.setFormatter(_Colour("%(asctime)s  %(levelname)s  %(name)s  %(message)s", "%H:%M:%S"))
    root.addHandler(c)
    f = logging.handlers.TimedRotatingFileHandler(
        str(LOG_DIR / f"pipeline_{datetime.utcnow():%Y-%m-%d}.log"),
        when="midnight", backupCount=30, utc=True)
    f.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s", "%Y-%m-%dT%H:%M:%SZ"))
    root.addHandler(f)
    return logging.getLogger("pipeline")
