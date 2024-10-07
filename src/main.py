import pandas as pd
from src.settings import supabase

data = supabase.table("students").select("*").execute()
