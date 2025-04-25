import os
import sys
from pathlib import Path

DIR = Path(os.path.abspath(__file__))
print(f"{DIR.parent.parent.absolute()}")