import json
import os
import re
import shutil
from numpy import true_divide
import pandas as pd

from logger.logger import AppLogger
from file_operations.file_utils import FileUtils

class RawDataValidation:
    
    """
            All the validation to be done on Raw Training Data.
    """
    def __init__(self, path) -> None:
        self.batch_directory = path
        self.schema_path = 'schema_training.json'
        self.logger = AppLogger()
        self.log_file_name = 'training_logs/SchemaValidationLog.txt'

    def values_from_schema(self):
        log_file = open(self.log_file_name, 'a+')
        try:
            with open(self.schema_path, 'r') as f:
                schema = json.load(f)
                f.close()

            file_name_pattern = schema['SampleFileName']
            length_of_date_stamp = schema['LengthOfDateStampInFile']
            length_of_time_stamp = schema['LengthOfTimeStampInFile']
            column_name = schema['ColumnNames']
            number_of_columns = schema['NumberOfColumns']

            message =  f"LengthOfDateStamp: {length_of_date_stamp}, \t LengthOfTimeStamp: {length_of_time_stamp}, \t NoOfColumns: {number_of_columns} \n"
            self.logger.log(log_file, message)
    
        except Exception as e:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, str(e))
            log_file.close()
            raise e
        
        log_file.close()
        return length_of_date_stamp, length_of_time_stamp, column_name, number_of_columns

    def filename_regex(self):
        regex = "['data']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def validate_file_name(self):
        FileUtils.delete_existing_data_training_folders()
        FileUtils.create_raw_good_bad_data_directory()
        
        file_name_regex = self.filename_regex()
        length_of_date_stamp, length_of_time_stamp, column_name, number_of_columns = self.values_from_schema()
        
        try:
            log_file = open(self.log_file_name, 'a+')
            raw_files = [raw_file for raw_file in os.listdir(self.batch_directory)]
            for raw_file in raw_files:
                if(re.match(file_name_regex, raw_file)):
                    split_at_dot = re.split('.csv', raw_file)
                    split_at_dot = (re.split('_', split_at_dot[0]))
                    if len(split_at_dot[1]) == length_of_date_stamp:
                        if len(split_at_dot[2]) == length_of_time_stamp:
                            shutil.move(os.path.join(self.batch_directory, raw_file), "training_validated_raw_files/good_data")
                            self.logger.log(log_file, f"File moved to raw validated good data folder:: {raw_file}")
                        else:
                            shutil.move(os.path.join(self.batch_directory, raw_file), "training_validated_raw_files/bad_data")
                            self.logger.log(log_file, f"Invalid length of time stamp!! File moved to raw validated bad data folder:: {raw_file}")
                    
                    else:
                        shutil.move(os.path.join(self.batch_directory, raw_file), "training_validated_raw_files/bad_data")
                        self.logger.log(log_file, f"Invalid length of date stamp!! File moved to raw validated bad data folder:: {raw_file}")

                else:
                    
                    shutil.move(os.path.join(self.batch_directory, raw_file), "training_validated_raw_files/bad_data")
                    self.logger.log(log_file, f"Invalid Filename!! File moved to raw validated bad data folder:: {raw_file}")
            log_file.close()
        except Exception as e:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Error while validating file name: {e}")
            log_file.close()
            raise e

    def validate_column_length(self, number_of_columns):
        try:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Validating column length")
            
            for file in os.listdir("training_validated_raw_files/good_data/"):
                csv = pd.read_csv("training_validated_raw_files/good_data/" + file)
                if csv.shape[1] != number_of_columns:
                    shutil.move("training_validated_raw_files/good_data/" + file, "training_validated_raw_files/bad_data")
                    self.logger.log(log_file, "Invalid Column Length:: {file}")

            self.logger.log(log_file, "Column length validation completed")
            log_file.close()

        except Exception as e:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Error while validating column length: {e}")
            log_file.close()
            raise e

    def validate_missing_values_in_whole_column(self):
        try:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, "Validating missing values in whole column")
            source = "training_validated_raw_files/good_data/"
            
            for file in os.listdir(source):
                csv = pd.read_csv(source + file)
                is_bad_file = False

                for column in csv:
                    if csv[column].isna().sum() == len(csv[column]):
                        is_bad_file = True
                        shutil.move(source+file, "training_validated_raw_files/bad_data")
                        self.logger.log(log_file, "Invalid Column for the file, File moved to raw bad data folder:: {file}")
                
            self.logger.log(log_file, "Completed Validating missing values in whole column")
            log_file.close()

        except Exception as e:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Error while validating missing values in whole column: {e}")
            log_file.close()
            raise e