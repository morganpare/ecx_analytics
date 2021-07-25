import pathlib
FILEPATH = pathlib.Path(__file__).parent.resolve()
PROJECTPATH = FILEPATH.parent.parent.resolve()

import sys
sys.path.append(str(PROJECTPATH))

import pandas as pd
import numpy as np
import datetime as dt
import joblib
import logging

logging.basicConfig(level = logging.INFO)

class ECXPredictor:
    def __init__(self, project_path=PROJECTPATH):
        self.project_path = project_path
        self.data_path = pathlib.Path.joinpath(self.project_path, "data")
        self.model_path = pathlib.Path.joinpath(self.project_path, "models")

    def load_features_df(self, symbol):
        file_path = pathlib.Path.joinpath(self.data_path, "feature_store", f"{symbol}_complete_features.pkl")
        df = pd.read_pickle(file_path)
        return df

    def load_model(self, symbol):
        file_path = pathlib.Path.joinpath(self.model_path, f"{symbol}_model.modelpickle")
        return joblib.load(file_path) 

    @staticmethod
    def get_x(df, target_date, target_col='mid_price', drop_cols=['trade_date'], date_col='trade_date'):
        df = df.copy()
        df = df[df[date_col] == target_date]
        cols_to_drop = [target_col] + drop_cols
        df.drop(columns=cols_to_drop, inplace=True)
        x = df.values.reshape(1, -1)
        features = df.to_dict('records')
        return x, features

    @staticmethod
    def make_prediction(model, x):
        return round(model.predict(x)[0], 2)

    def predict(self, symbol, target_date):
        logging.info(f'Loading features and model for symbol {symbol} and target date {target_date}')
        df = self.load_features_df(symbol)
        model = self.load_model(symbol)
        x, features = self.get_x(df, target_date)
        logging.info('Making prediction for symbol {symbol} and target date {target_date}')
        prediction = self.make_prediction(model, x)
        out_object = {
            'value': prediction, 
            'features': features
        }
        logging.info('Prediction made for symbol {symbol} and target date {target_date}')        
        return out_object

if __name__ == '__main__':
    for w in ['LUBP4','LUBP3','ULK5','UFRAUG']:
        predictor = ECXPredictor()
        print(predictor.predict(w, dt.datetime(2018,4,3)))
