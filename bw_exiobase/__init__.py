__version__ = (0, 1)

__all__ = ("convert_exiobase",)

from pathlib import Path

CONVERTED_DATA_DIR = Path(__file__, "..").resolve() / "converted"

if not CONVERTED_DATA_DIR.is_dir():
    CONVERTED_DATA_DIR.mkdir()

import bw_default_backend

from .utils import convert_exiobase
from .importer import import_exiobase
