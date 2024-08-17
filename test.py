import sys
from sensor.configuration.mongodb_connection import MongoDBClient
from sensor.exception import SensorException
#from sensor.logger import logging
from sensor.pipeline.training_pipeline import TrainingPipeline

# def test_exception():
#     try:
#         logging.info('We are dividing 1 by 0.')
#         x = 1 / 0
#     except Exception as e:
#         raise SensorException(e, sys)

if __name__ == '__main__':
    try:
        train_pipeline = TrainingPipeline()
        train_pipeline.run_pipeline()
    except Exception as e:
        raise SensorException(e, sys)

    # test_exception()
        
    # mongo_client = MongoDBClient()
    # print(mongo_client.database)
    # print(mongo_client.database.list_collection_names())
    
    # # Close the connection
    # mongo_client.client.close()
    