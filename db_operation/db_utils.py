from email import header
from operator import ne
from pickle import APPEND
import shutil
import sqlite3
from numpy import append
import pandas as pd
from datetime import datetime
import os
import csv
from unittest import result

from logger.logger import AppLogger

class DBOperation:
    def __init__(self) -> None:
        self.path = 'training_database/'
        self.good_file_path = 'training_validated_raw_files/good_data'
        self.bad_file_path = 'training_validated_raw_files/bad_data'
        self.logger = AppLogger()
        self.log_file_name = "training_logs/DBConnectionLog.txt"

    def db_connection(self, db_name):
        try:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Connecting to database:: {db_name}")
            
            conn = sqlite3.connect(self.path + db_name + '.db')
            
            self.logger.log(log_file, f"Connected to database:: {db_name}")
            log_file.close()

            return conn

        except Exception as e:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Error while connecting to database:: {db_name}")
            log_file.close()
            raise e

    def create_table(self, db_name, columns):
        conn = None
        try:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Creating table into database:: {db_name}")

            conn = self.db_connection(db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = 'good_data'")

            if cursor.fetchone()[0] == 1:
                self.logger.log(log_file, f"good_data already exists in {db_name} database")

            else:

                for column in columns.keys():
                    type = columns[column]

                    try:
                        conn.execute('ALTER TABLE good_data ADD COLUMN "{column_name}" {dataType}'.format(column_name=column,dataType=type))

                    except:
                        conn.execute('CREATE TABLE  good_data ({column_name} {dataType})'.format(column_name=column, dataType=type))
                
                self.logger.log(log_file, f"Table good_data created in {db_name} database")

            conn.close()
            self.logger.log(log_file, f"Closed {db_name} database")
            log_file.close()
        except Exception as e:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, f"Error while creating table:: {e}")
            if conn is not None:
                conn.close()
                self.logger.log(log_file, f"Closed {db_name} database")
                log_file.close()
            raise e

    def insert_into_table_good_data(self, db_name):
        conn = self.db_connection(db_name)
        log_file = open(self.log_file_name, 'a+')
        self.logger.log(log_file, "Inserting into table good_data")
        
        onlyfiles = [f for f in os.listdir(self.good_file_path)]
        for file in onlyfiles:
            try:
                with open(self.good_file_path+'/'+file, "r") as f:
                    df = pd.read_csv(f)
                    df.to_sql('good_data', conn, if_exists='append', index=False)
                self.logger.log(log_file, "Insertion Completed!!")
            except Exception as e:
                conn.rollback()
                self.logger.log(log_file,"Error while inserting into table: %s " % e)
                shutil.move(self.good_file_path+'/' + file, self.bad_file_path)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()

    def selecting_data_from_table_into_csv(self, db_name):
        self.destination = 'training_file_from_db/'
        self.file_name = 'InputFile.csv'
        
        try:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, "Generating CSV from DB")

            conn = self.db_connection(db_name)
            cursor = conn.cursor()

            query = 'SELECT * FROM good_data'
            cursor.execute(query)
            results = cursor.fetchall()

            # Get the headers of the csv file
            header = [i[0] for i in cursor.description]

            # Make the csv output directory
            if not os.path.isdir(self.destination):
                os.makedirs(self.destination)
            
            # Open csv file for writing
            csv_file = csv.writer(open(self.destination + self.file_name, 'w', newline=''), delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            csv_file.writerow(header)
            csv_file.writerows(results)
            
            self.logger.log(log_file, "Generated CSV from DB")
            log_file.close()
            
        except Exception as e:
            log_file = open(self.log_file_name, 'a+')
            self.logger.log(log_file, "Error while generating CSV file from DB")
            log_file.close()
            raise e

    def load_data(self):
        log_file = open(self.log_file_name, 'a+')
        self.logger.log(log_file, "Loading the data from load_data function")
        try:
            self.data = pd.read_csv("training_file_from_db/InputFile.csv")
            self.logger.log(log_file, "Data Loaded Successfully!!")
            log_file.close()
            return self.data
        except Exception as e:
            self.logger.log(log_file, f"Loading the data Failed because: {e}")
            log_file.close()