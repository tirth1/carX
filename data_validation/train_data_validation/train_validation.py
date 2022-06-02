from data_validation.train_data_validation.raw_validation import RawDataValidation
from db_operation.db_utils import DBOperation
from logger.logger import AppLogger
from data_transform.train_data_transform import TrainDataTransform
from file_operations.file_utils import FileUtils

class TrainValidation:
    def __init__(self, path) -> None:
        self.raw_data = RawDataValidation(path)
        self.dataTransform = TrainDataTransform()
        self.dbOperation = DBOperation()
        self.log_file_name = "training_logs/training_main_log.txt"
        self.logger = AppLogger()
        self.fileUtils = FileUtils()

    def validation(self):
        try:
            self.log_file = open(self.log_file_name, 'a+')
            self.logger.log(self.log_file, "Start of Train Validation")
            self.logger.log(self.log_file, "Raw data validation Started!!")
            length_of_date_stamp, length_of_time_stamp, column_name, number_of_columns = self.raw_data.values_from_schema()
            
            self.raw_data.validate_file_name()
            self.raw_data.validate_column_length(number_of_columns)
            self.raw_data.validate_missing_values_in_whole_column()
            self.logger.log(self.log_file, "Raw data validation Completed!!")
            
            self.logger.log(self.log_file, "Data Transformations started")
            #self.dataTransform.addQuotesToCategoricalColumns()
            self.logger.log(self.log_file, "Data Transformations completed")

            self.logger.log(self.log_file, "DB operations Started!!")
            self.dbOperation.create_table('data', 'training', column_name)
            self.dbOperation.insert_into_table('data', 'training')
            self.fileUtils.move_bad_files_to_archive()
            self.dbOperation.selecting_data_from_table_into_csv('data', 'training')
            self.fileUtils.delete_existing_data_training_folders()
            self.logger.log(self.log_file, "DB operations Completed!!")
            self.log_file.close()
        except Exception as e:
            self.logger.log(self.log_file, f"-----Training Validation failed because: {e}-----")
            raise e
