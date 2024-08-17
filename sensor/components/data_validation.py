import os, sys
import pandas as pd
from scipy.stats import ks_2samp
from sensor.constant.training_pipeline import SCHEMA_FILE_PATH
from sensor.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from sensor.entity.config_entity import DataValidationConfig
from sensor.exception import SensorException
from sensor.logger import logging
from sensor.utils.main_utils import read_yaml_file, write_yaml_file


class DataValidation:
    
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, 
                        data_validation_config: DataValidationConfig) -> None:
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise SensorException(e, sys)
    
    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            logging.info(f"required number of columns: {len(self._schema_config['columns'])}")
            logging.info(f"data frame has columns: {dataframe.shape[1]}")
            
            return len(self._schema_config['columns']) == dataframe.shape[1]
        except Exception as e:
            raise SensorException(e, sys)
    
    def is_numerical_column_exists(self, dataframe: pd.DataFrame) -> bool:
        try:
            numerical_columns = self._schema_config["numerical_columns"]
            dataframe_columns = dataframe.columns

            numerical_column_present = True
            missing_numerical_columns = []
            for num_column in numerical_columns:
                if num_column not in dataframe_columns:
                    numerical_column_present=False
                    missing_numerical_columns.append(num_column)
            
            logging.info(f"missing numerical columns: [{missing_numerical_columns}]")
            return numerical_column_present
        
        except Exception as e:
            raise SensorException(e, sys)
    
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise SensorException(e, sys)
    
    def detect_dataset_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, 
                                    threshold: float=0.05) -> bool:
        try:
            status=True
            report ={}
            
            for column in base_df.columns:
                d1 = base_df[column]
                d2  = current_df[column]
                test = ks_2samp(d1, d2)
                
                # not have enough evidence to reject the null hypothesis (H0)
                # both the data seems to come from the same distribution
                if (test.pvalue >= threshold):
                    is_found = False
                else:
                    is_found = True 
                    status = False
                    
                report[column] = {
                    "p_value":float(test.pvalue),
                    "drift_status": is_found
                }
            
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            
            # create directory
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report, replace=True)
            
            return status
        
        except Exception as e:
            raise SensorException(e, sys)
    
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            error_message = ''
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            
            # reading data from train and test location
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)
            
            # validate number of columns
            status = self.validate_number_of_columns(dataframe=train_dataframe)
            if not status:
                error_message = f"{error_message}train dataframe does not contain all columns.\n"
                
            status = self.validate_number_of_columns(dataframe=test_dataframe)
            if not status:
                error_message = f"{error_message}test dataframe does not contain all columns.\n"
        

            # validate numerical columns
            status = self.is_numerical_column_exists(dataframe=train_dataframe)
            if not status:
                error_message = f"{error_message}train dataframe does not contain all numerical columns.\n"
            
            status = self.is_numerical_column_exists(dataframe=test_dataframe)
            if not status:
                error_message = f"{error_message}test dataframe does not contain all numerical columns.\n"
            
            if len(error_message) > 0:
                raise Exception(error_message)
            
            # let's check data drift
            status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)

            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            logging.info(f"data validation artifact: {data_validation_artifact}")
            
            return data_validation_artifact
        
        except Exception as e:
            raise SensorException(e, sys)
        