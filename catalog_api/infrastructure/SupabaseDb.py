import os
from supabase import create_client, Client

url: str | None = os.environ.get("SUPABASE_URL")
assert url, "Please set the SUPABASE_URL environment variable"
key: str | None = os.environ.get("SUPABASE_KEY")
assert key, "Please set the SUPABASE_KEY environment variable"
supabase: Client = create_client(url, key)

