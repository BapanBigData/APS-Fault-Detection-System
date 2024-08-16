import os, sys
import pymongo
from dotenv import load_dotenv
from sensor.constant.database import DATABASE_NAME
from sensor.exception import SensorException

# Load environment variables from .env file
load_dotenv()


class MongoDBClient:
    
    ## MongoDB connection string
    CONN_STRING = os.getenv('MONGODB_CONN_STRING')
    CLIENT = None
    
    def __init__(self, database_name=DATABASE_NAME) -> None:
        
        try:
            if MongoDBClient.CLIENT is None:
                MongoDBClient.CLIENT = pymongo.MongoClient(MongoDBClient.CONN_STRING)
            
            self.client = MongoDBClient.CLIENT
            self.database = self.client[database_name]
            self.database_name = database_name
            
        except Exception as e:
            raise SensorException(e, sys)
        