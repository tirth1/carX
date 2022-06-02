from asyncio.log import logger
import os
import pandas as pd

from logger.logger import AppLogger

class TrainDataTransform:
    def __init__(self) -> None:
        self.good_data_path = "training_validated_raw_files/good_data/"
        self.logger = AppLogger()
        self.log_file_name = "training_logs/DataTransformationLog.txt"
    
    def addQuotesToCategoricalColumns(self):
        try:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, "Replacing missing value with null")

            good_training_files = [file for file in os.listdir(self.good_data_path)]
            for file in good_training_files:
                df = pd.read_csv(self.good_data_path + "/" + file)  
                cat_cols = ["full_name", "selling_price", "new-price", "seller_type", "km_driven", "owner_type", "fuel_type", "transmission_type", "mileage", "engine", "max_power", "seats"]

                for col in cat_cols:
                    df[col] = df[col].apply(lambda x : "'" + str(x) + "'")
                
                df.to_csv(self.good_data_path + "/" + file, index=False)
                self.logger.log(log_file, f"Quotes added successfully!! {file}")
            log_file.close()
        except Exception as e:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Error while adding quotes because: {e}")
            log_file.close()
            raise e