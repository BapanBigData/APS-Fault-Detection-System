import os, sys
from pandas import DataFrame
from sklearn.model_selection import train_test_split
from sensor.exception import SensorException
from sensor.logger import logging
from sensor.entity.config_entity import DataIngestionConfig
from sensor.entity.artifact_entity import DataIngestionArtifact
from sensor.data_access.sensor_data import SensorData
from sensor.utils.main_utils import read_yaml_file
from sensor.constant.training_pipeline import SCHEMA_FILE_PATH


class DataIngestion:
    
    def __init__(self, data_ingestion_config: DataIngestionConfig) -> None:
        try:
            self.data_ingestion_config = data_ingestion_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise SensorException(e, sys)
    
    def export_data_into_feature_store(self) -> DataFrame:
        """_summary_
        Export MongoDB collection records as dataframe into feature store

        Returns:
            DataFrame: _description_
        """
        try:
            logging.info('exporting data from mongodb to feature store...')
            
            sensor_data = SensorData()
            df = sensor_data.export_collection_as_dataframe(collection_name=self.data_ingestion_config.collection_name)
            
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            
            # create folder
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            
            df.to_csv(feature_store_file_path, index=False, header=True)
            
            logging.info('data succesfully written to feature store!')
            
            return df
        
        except Exception as e:
            raise SensorData(e, sys)
    
    def split_data_as_train_test(self, dataframe: DataFrame) -> None:
        """
        Feature store dataset will be splited into train and test 
        """
        try:
            # Perform train-test split on the provided dataframe
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio, random_state=47
            )
            logging.info("successfully performed train-test split on the dataframe.")

            # Define the directory path for saving the train and test datasets
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            # Save the train and test datasets to the specified file paths
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)
            logging.info("train and test datasets have been exported successfully.")

        except Exception as e:
            raise SensorException(e, sys)
    
    
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            df = self.export_data_into_feature_store()
            df.drop(self._schema_config["drop_columns"], axis=1, inplace=True)
            self.split_data_as_train_test(dataframe=df)
            data_ingestion_artifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path, 
                                                            test_file_path=self.data_ingestion_config.testing_file_path)
            return data_ingestion_artifact
        
        except Exception as e:
            raise SensorException(e, sys)
        