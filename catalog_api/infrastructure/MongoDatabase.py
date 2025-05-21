import pymongo # type: ignore
import os


def create_mongo_client() -> pymongo.MongoClient:
    """
    Create a MongoDB client using the connection string from environment variables.
    
    Returns:
        MongoDB client instance
    """
    # Ensure the environment variable is set
    mongo_client_url: str | None = os.environ.get("MONGODB")
    assert mongo_client_url, "Please set the MONGODB environment variable"
    
    # Create and return the MongoDB client
    return pymongo.MongoClient(mongo_client_url)