import shutil
from pathlib import Path
import os
from dotenv import load_dotenv

def get_env(env_name):
    load_dotenv()
    return os.environ.get(env_name)