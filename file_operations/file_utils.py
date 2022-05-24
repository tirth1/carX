from datetime import datetime
import os
import shutil

from logger.logger import AppLogger

class FileUtils:
    def __init__(self) -> None:
        FileUtils.log_file_name = "training_logs/general_logs.txt"
        FileUtils.logger =  AppLogger()

    @staticmethod
    def create_raw_good_bad_data_directory():
        try:
            good_data_path = os.path.join("training_validated_raw_files", "good_data")
            if not os.path.isdir(good_data_path):
                os.makedirs(good_data_path)
                
                log_file = open(FileUtils.log_file_name, 'a+')
                FileUtils.logger.log(log_file, "Created training raw validated good data directory")
                log_file.close()

            bad_data_path = os.path.join("training_validated_raw_files/", "bad_data/")
            if not os.path.isdir(bad_data_path):
                os.makedirs(bad_data_path)

                log_file = open(FileUtils.log_file_name, 'a+')
                FileUtils.logger.log(log_file, "Created training raw validated bad data directory")
                log_file.close()

        except Exception as e:
            log_file = open(FileUtils.log_file_name, 'a+')
            FileUtils.logger.log(log_file, "Error while creating training raw validated good bad data directory")
            log_file.close()
            raise e

    @staticmethod
    def delete_existing_data_training_folders():
        try:
            path = "training_validated_raw_files/"

            if os.path.isdir(path + "good_data/"):
                shutil.rmtree(path + "good_data/")
                log_file = open(FileUtils.log_file_name, 'a+')
                FileUtils.logger.log(log_file, "Deleted training raw validated good data directory")
                log_file.close()

            if os.path.isdir(path + "bad_data/"):
                shutil.rmtree(path + "bad_data/")
                log_file = open(FileUtils.log_file_name, 'a+')
                FileUtils.logger.log(log_file, "Deleted training raw validated bad data directory")
                log_file.close()

        except Exception as e:
            log_file = open(FileUtils.log_file_name, 'a+')
            FileUtils.logger.log(log_file, "Error while Deleting training raw validated good bad data directory")
            log_file.close()
            raise e

    @staticmethod
    def move_bad_files_to_archive():
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")

        try:
            source = "training_validated_raw_files/bad_data/"
            if os.path.isdir(source):
                path = "training_archive/"
                if not os.path.isdir(path):
                    os.makedirs(path)
                
                destination = os.path.join(path, "bad_data/")
                if not os.path.isdir(destination):
                    os.makedirs(destination)

                files = os.listdir(source)
                for file in files:
                    if file not in os.listdir(destination):
                        shutil.move(source+file, destination)

                log_file = open(FileUtils.log_file_name, 'a+')
                FileUtils.logger.log(log_file, "training raw validated bad files moved to archive")

                if os.path.isdir(source):
                    shutil.rmtree(source)
                
                FileUtils.logger.log(log_file, "training raw validated bad files Deleted successfully")
                log_file.close()
                
        except Exception as e:
            log_file = open(FileUtils.log_file_name, 'a+')
            FileUtils.logger.log(log_file, f"Error while moving bad raw files to archive bad folder:: {str(e)}")
            log_file.close()
            raise e

