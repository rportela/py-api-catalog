import pymongo
import os

MONGO_CLIENT_URL = os.getenv("MONGODB")
MONGO_CLIENT = pymongo.MongoClient(MONGO_CLIENT_URL)