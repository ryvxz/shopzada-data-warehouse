import sys
from pathlib import Path
from importlib import import_module
import os
from dotenv import load_dotenv

load_dotenv()

folder = os.getenv("SCRIPTS_FOLDER")
DEFAULT_RETRIES = int(os.getenv("DEFAULT_RETRIES", 3))

def run_script(script_folder: str, script_name: str):
    """A callable to run scripts from a specified script folder."""
    scripts_path = Path(folder or "/opt/airflow/plugins/scripts").joinpath(script_folder)
    sys.path.insert(0, str(scripts_path))
    try:
        module = import_module(script_name)
        module.main()  # Assuming each script has a main() function
    finally:
        sys.path.pop(0)
