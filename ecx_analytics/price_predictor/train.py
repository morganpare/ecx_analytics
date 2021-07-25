import pathlib
FILEPATH = pathlib.Path(__file__).parent.resolve()
PROJECTPATH = FILEPATH.parent.parent.resolve()

import sys
sys.path.append(str(PROJECTPATH))

import pandas as pd
from sklearn.linear_model import ElasticNet
import matplotlib.pyplot as plt
import numpy as np
import datetime as dt
import joblib
import logging

logging.basicConfig(level = logging.INFO)


from ecx_analytics.price_predictor.utils.hyperparameter_tuner import HyperparameterTuner


class ECXTrainer:
    def __init__(self, project_path=PROJECTPATH):
        self.project_path = project_path
        self.data_path = pathlib.Path.joinpath(self.project_path, "data")
        self.model_path = pathlib.Path.joinpath(self.project_path, "models")

    def load_features_df(self, warehouse):
        file_path = pathlib.Path.joinpath(self.data_path, "feature_store", f"{warehouse}_with_target_features.pkl")
        df = pd.read_pickle(file_path)
        return df

    @staticmethod
    def get_X_y(df, start=dt.datetime(2012,4,1), end=dt.datetime(2018,3,31), target_col='mid_price', drop_cols=['trade_date'], date_col='trade_date'):
        df = df.copy()
        df = df[(df[date_col] >= start) & (df[date_col] <= end)]
        cols_to_drop = [target_col] + drop_cols
        X = df.drop(columns=cols_to_drop).values
        y = df[target_col].values
        return X, y

    def initialise_elastic_net_model(self):
        self.model = ElasticNet(random_state=0, max_iter=100000)
        hyperparameter_set = [
            {'name':'alpha', 'type':'Real', 'lower': 1e-6, 'upper': 500., 'method':'uniform'},
            {'name':'l1_ratio', 'type':'Real', 'lower': 0., 'upper': 1., 'method':'uniform'}
        ]
        return self.model, hyperparameter_set

    @staticmethod
    def find_hyperparameters(model, hyperparameter_set, X, y):
        hyp_tuner = HyperparameterTuner(model, hyperparameter_set, X,y, n_calls=50)
        params = hyp_tuner.find_params()
        logging.info('Hyperparameters found')
        return params

    def update_hyperparameters(self, final_hyperparameters):
        self.model.set_params(**final_hyperparameters)

    def fit(self, X, y):
        self.model.fit(X, y)

    def save_model(self, model, warehouse):
        file_path = pathlib.Path.joinpath(self.model_path, f"{warehouse}_model.modelpickle")
        print(file_path)
        joblib.dump(model, file_path) 

    def train(self, warehouse):
        logging.info(f'Training model for {warehouse}')
        df = self.load_features_df(warehouse)
        X, y = self.get_X_y(df)
        model, param_set = self.initialise_elastic_net_model()
        logging.info(f'Finding hyperparameters for {warehouse}')
        params = self.find_hyperparameters(model, param_set, X, y)
        logging.info(f'Hyperparameters found for {warehouse}')
        self.update_hyperparameters(params)
        logging.info(f'Fitting model for {warehouse}')
        self.fit(X, y)
        self.save_model(self.model, warehouse)
        logging.info(f'Model trained and saved for {warehouse}')

if __name__ == '__main__':
    for w in ['LUBP4','LUBP3','ULK5','UFRAUG']:
        trainer = ECXTrainer()
        trainer.train(w)