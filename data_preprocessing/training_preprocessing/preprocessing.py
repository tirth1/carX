import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import StratifiedShuffleSplit
import pickle

class TrainPreprocessor:
    def __init__(self, data) -> None:
        self.data = data
    
    def train_data_preprocessing(self):
        self.handle_null_values()
        self.generate_brand_model()
        self.convert_seats()
        self.remove_categorical_col()
        self.selling_price_in_lakhs()
        self.convert_km()
        self.convert_mileage()
        self.convert_max_power()
        self.convert_engine()
        self.convert_new_price()
        self.findYearsOld()
        return self.data.drop('selling_price', axis=1), self.data['selling_price']

    def handle_null_values(self):
        self.data.dropna(axis=0, inplace=True)

    def generate_brand_model(self):
        df_name = self.data['full_name'].str.split(' ', expand=True)
        #df['brand'] = df_name[0].str.lower()
        #df['model'] = df_name[1].str.lower()
        df_name = df_name.fillna('')
        self.data['name'] = df_name[0].str.lower() + ' ' + df_name[1].str.lower()
        self.data = self.data.drop('full_name', axis=1)
    
    def convert_seats(self):
        self.data['seats'] = self.data['seats'].str.split('Seats', expand=True)[1].astype(int)
    
    def remove_categorical_col(self):
        cat_cols = ['transmission_type', 'fuel_type', 'seller_type', 'name']
        self.data = self.data.reset_index(drop=True)
        for col in cat_cols:
            onehotencoder = OneHotEncoder()
            X = onehotencoder.fit_transform(self.data[col].values.reshape(-1,1)).toarray()
            #To add this back into the original dataframe 
            dfOneHot = pd.DataFrame(X, columns = [col+"_"+str(int(i)) for i in range(X.shape[1])]) 
            self.data = pd.concat([self.data, dfOneHot], axis=1, join='inner')
            pickle.dump(onehotencoder, open(f"artifacts/models/OneHotEncoder_{col}.pickle", 'wb'))
        self.data.drop(cat_cols, inplace=True, axis=1)
        owner = {
            'First Owner': 1,
            'Second Owner': 2,
            'Third Owner': 3
        }
        
        self.data['owner_type'] = self.data['owner_type'].replace(owner)

    def selling_price_in_lakhs(self):
        price_df = self.data['selling_price'].str.split(' ', expand=True).drop(1, axis=1)
        temp_df = self.data[price_df[0].str.contains(',')]
        temp_price_df = temp_df['selling_price'].str.split(',', expand=True).drop(1, axis=1)
        temp_price_df = temp_price_df[0].astype(int)/100
        temp_df.loc[:, 'selling_price'] = temp_price_df
        self.data.drop(temp_df.index, axis=0, inplace=True)
        price_df.drop(temp_df.index, axis=0, inplace=True)
        self.data['selling_price'] = price_df[0].astype(float)
        self.data = self.data.append(temp_df)

    def convert_km(self):
        self.data['km_driven'] = self.data['km_driven'].str.split(' ', expand=True).drop(1, axis=1)[0]
        self.data['km_driven'] = self.data['km_driven'].replace('[\,]', '', regex=True).astype(float)/1000
    
    def convert_mileage(self):
        self.data['mileage'] = self.data['mileage'].str.split(' ', expand=True)[0].str.split('Mileage', expand=True)[1].astype(float)

    def convert_max_power(self):
        self.data['max_power'] = self.data['max_power'].str.split(' ', expand=True)[1].str.split('Power', expand=True)[1]
        drop_row = self.data[self.data['max_power'] == 'null'].index
        self.data = self.data.drop(drop_row, axis=0)
        self.data['max_power'] = self.data['max_power'].astype(float)

    def convert_engine(self):
        self.data['engine'] = self.data['engine'].str.split(' ', expand=True)[0].str.split('Engine', expand=True)[1]
        drop_row = self.data[self.data['engine'] == ''].index
        self.data.drop(drop_row, axis=0, inplace=True)
        self.data['engine'] = self.data['engine'].astype(int)

    def convert_new_price(self):
        self.data.drop(self.data[self.data['new-price'].str.endswith('Cr*')].index, axis=0, inplace=True)
        new_price_df = self.data['new-price'].str.split(' ', expand=True)[5].str.split('.', expand=True)
        temp_df = self.data[new_price_df[3].isna()]
        temp_df.loc[:,'new-price'] = new_price_df[new_price_df[3].isna()][1] + new_price_df[new_price_df[3].isna()][2]
        temp_df.loc[:, 'new-price'] = temp_df['new-price'].astype(float)/100
        
        drop_row = new_price_df[new_price_df[3].isna()].index
        self.data.drop(drop_row, axis=0, inplace=True)
        new_price_df.drop(drop_row, axis=0, inplace=True)
        
        temp_new_price_df = new_price_df[2].str.split('-', expand=True)
        self.data['new-price'] = ((new_price_df[1] + temp_new_price_df[0]).astype(float) +  (temp_new_price_df[1] + new_price_df[3]).astype(float))/200
        self.data = self.data.append(temp_df)
        
        self.data = self.data.reset_index(drop=True)
    
    def findYearsOld(self):
        self.data['yearsOld'] = 2022-self.data['year']
        self.data.drop('year', inplace=True, axis=1)

    def feature_scaling(self, X):
        scaler = MinMaxScaler()
        scaled_X = pd.DataFrame(scaler.fit_transform(X))
        #scaled_X_test = pd.DataFrame(scaler.transform(X_test))
        pickle.dump(scaler, open(f"artifacts/models/minMaxScaler.pickle", 'wb'))
        return scaled_X

    def train_test_split(self, data):
        data['selling_price_cat'] = pd.cut(data['selling_price'],
                                bins=[0, 10, 20, 30, 40, 50, 60, np.inf],
                                labels=[1, 2, 3, 4, 5, 6, 7])

        sss = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        for train_index, test_index in sss.split(data, data['selling_price_cat']):
            train_set = data.loc[train_index]
            test_set = data.loc[test_index]

        X_train = train_set.drop(['selling_price','selling_price_cat'], axis=1)
        y_train = train_set['selling_price']
        X_test = test_set.drop(['selling_price','selling_price_cat'], axis=1)
        y_test = test_set['selling_price']
        return X_train, y_train, X_test, y_test