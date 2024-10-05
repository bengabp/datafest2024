from loguru import logger
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
logger.warning(f"basedir: {BASE_DIR}")
