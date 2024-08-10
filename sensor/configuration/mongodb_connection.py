import os
import pymongo
from dotenv import load_dotenv
from sensor.constant.database import DATABASE_NAME

# Load environment variables from .env file
load_dotenv()


class MongoDBClient:
    
    ## MongoDB connection string
    CONN_STRING = os.getenv('MONGODB_CONN_STRING')
    CLIENT = None
    
    def __init__(self, db_name=DATABASE_NAME) -> None:
        
        try:
            if MongoDBClient.CLIENT is None:
                MongoDBClient.CLIENT = pymongo.MongoClient(MongoDBClient.CONN_STRING)
            
            self.client = MongoDBClient.CLIENT
            self.database = self.client[db_name]
            self.db_name = db_name
            
        except Exception as e:
            raise e
        