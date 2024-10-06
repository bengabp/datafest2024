from loguru import logger
from pathlib import Path
import os
from starlette.config import Config
from supabase import create_client, Client

BASE_DIR = Path(__file__).resolve().parent.parent
logger.warning(f"basedir: {BASE_DIR}")

config = Config(os.path.join(BASE_DIR, ".env"))

url: str = config("SUPABASE_URL")
key: str = config("SUPABASE_KEY")
supabase_client: Client = create_client(url, key)

print(supabase_client)

