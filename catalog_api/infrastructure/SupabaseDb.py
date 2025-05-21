import os
from supabase import create_client, Client


def create_supabase_client() -> Client:
    """
    Create a Supabase client using environment variables for URL and key.

    Returns:
        Supabase client instance
    """
    # Ensure the environment variables are set
    url: str | None = os.environ.get("SUPABASE_URL")
    assert url, "Please set the SUPABASE_URL environment variable"
    key: str | None = os.environ.get("SUPABASE_KEY")
    assert key, "Please set the SUPABASE_KEY environment variable"

    # Create and return the Supabase client
    return create_client(url, key)
