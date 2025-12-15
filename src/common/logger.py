from __future__ import annotations

import logging
import sys
from typing import Any, Dict

from rich.console import Console
from rich.logging import RichHandler


def setup_logger(level: str = "INFO") -> logging.Logger:
    console = Console(file=sys.stdout, force_terminal=True)
    handler = RichHandler(console=console, markup=True, show_time=True, show_path=False)
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[handler],
    )
    return logging.getLogger("dist-queue")


def log_structured(logger: logging.Logger, level: str, message: str, **fields: Dict[str, Any]) -> None:
    if level.lower() == "info":
        logger.info(f"{message} | {fields}")
    elif level.lower() == "warning":
        logger.warning(f"{message} | {fields}")
    elif level.lower() == "error":
        logger.error(f"{message} | {fields}")
    else:
        logger.debug(f"{message} | {fields}")
