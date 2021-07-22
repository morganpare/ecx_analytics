import numpy as np
from numpy.core.fromnumeric import mean
import pandas as pd
import pathlib

class SilverToFeatureStore:
    def __init__(self, data_path, symbols=['LUBP4','LUBP3','ULK5','UFRAUG']):
        self.data_path = data_path
        try:
            self.silver_path = pathlib.Path.joinpath(self.data_path, "silver")
        except:
            print(f"Silver path does not exist! Please create directory")
        try:
            self.feature_store_path = pathlib.Path.joinpath(self.data_path, "feature_store")
        except:
            print(f"Feature store path does not exist! Please create directory")
        self.symbols = symbols

    def load_silver_df(self, filename="ecx_silver.pkl"):
        file_path = pathlib.Path.joinpath(self.silver_path, filename)
        df = pd.read_pickle(file_path)
        return df

    @staticmethod
    def filter_by_date_range(df, start='2012-01-06', end='2018-05-11'):
        return df[(df['trade_date'] >= start) & (df['trade_date'] < end)].copy()
    
    @staticmethod
    def filter_by_symbol(df, symbol):
        return df[df['symbol'] == symbol].copy()

    @staticmethod
    def get_prices_single(df):
        return df[['trade_date','mid_price']].copy()

    @staticmethod
    def calculate_last_price_columns(prices_df):
        prices_df = prices_df.copy()
        prices_df['joined_date'] = prices_df['trade_date']
        df_ = pd.merge_asof(prices_df, prices_df, on='trade_date', suffixes=('','_lagged'), allow_exact_matches=False)
        df_['last_price_distance'] = (df_['trade_date'] - df_['joined_date_lagged']).dt.days

        df_.fillna(method='bfill', inplace=True)
        df__ = pd.merge_asof(df_, prices_df, left_on='joined_date_lagged', right_on='trade_date', suffixes=('','_twice'), allow_exact_matches=False)
        df__['second_last_price_distance'] = (df__['trade_date'] - df__['joined_date_twice']).dt.days


        return pd.DataFrame({
            'lag_price':df_['mid_price_lagged'].copy(),
            'lag_distance':df_['last_price_distance'].copy(),
            'lag_price_twice':df__['mid_price_twice'].copy(),
            'second_last_price_distance':df__['second_last_price_distance'].copy()
        }).reset_index(drop=True)

    @staticmethod
    def calculate_moving_averages(prices_df):
        prices_df = prices_df.copy()
        start = prices_df['trade_date'].min()
        end = prices_df['trade_date'].max()
        time_index = pd.date_range(start=start, end=end, freq='1D')
        full_df = pd.DataFrame({'full_ts':time_index})
        full_df = pd.merge(full_df, prices_df, how='left', left_on='full_ts', right_on='trade_date')
        full_df.set_index(['full_ts'], inplace=True)
        full_df.drop(columns=['trade_date'], inplace=True)
        ma_30 = full_df.rolling(window=30, min_periods=0).mean()
        std_30 = full_df.rolling(window=30, min_periods=0).std()
        count_30 = full_df.rolling(window=30, min_periods=0).count()
        ema_01 = full_df.ewm(alpha=0.1, adjust=False, min_periods=0).mean()
        ema_03 = full_df.ewm(alpha=0.3, adjust=False, min_periods=0).mean()
        estd_01 = full_df.ewm(alpha=0.1, adjust=False, min_periods=0).std()
        estd_03 = full_df.ewm(alpha=0.3, adjust=False, min_periods=0).std()

        ma_30 = pd.merge(prices_df, ma_30, left_on='trade_date', right_index=True, suffixes=('','_ma_30'))[['mid_price_ma_30']]
        std_30 = pd.merge(prices_df, std_30, left_on='trade_date', right_index=True, suffixes=('','_std_30'))[['mid_price_std_30']]
        count_30 = pd.merge(prices_df, count_30, left_on='trade_date', right_index=True, suffixes=('','_count_30'))[['mid_price_count_30']]
        ema_01 = pd.merge(prices_df, ema_01, left_on='trade_date', right_index=True, suffixes=('','_ema_01'))[['mid_price_ema_01']]
        ema_03 = pd.merge(prices_df, ema_03, left_on='trade_date', right_index=True, suffixes=('','_ema_03'))[['mid_price_ema_03']]
        estd_01 = pd.merge(prices_df, estd_01, left_on='trade_date', right_index=True, suffixes=('','_estd_01'))[['mid_price_estd_01']]
        estd_03 = pd.merge(prices_df, estd_03, left_on='trade_date', right_index=True, suffixes=('','_estd_03'))[['mid_price_estd_03']]

        return pd.concat([
            ma_30, 
            std_30,
            count_30,
            ema_01,
            ema_03,
            estd_01,
            estd_03,
        ], axis=1).reset_index(drop=True)

        # df_ = pd.DataFrame({
        #     'ma_30': list(ma_30),
        #     'std_30': std_30,
        #     'count_30': count_30,
        #     'ema_01': ema_01,
        #     'ema_03': ema_03,
        #     'estd_01': estd_01,
        #     'estd_03': estd_03,
        # }, index=ma_30.index)

        # return df_

    @staticmethod
    def calculate_spread_metrics(df):
        df_ = df[['trade_date','spread']].copy()
        start = df_['trade_date'].min()
        end = df_['trade_date'].max()
        time_index = pd.date_range(start=start, end=end, freq='1D')
        full_df = pd.DataFrame({'full_ts':time_index})
        full_df = pd.merge(full_df, df_, how='left', left_on='full_ts', right_on='trade_date')
        full_df.set_index(['full_ts'], inplace=True)
        full_df.drop(columns=['trade_date'], inplace=True)

        mean_df = full_df.rolling(window=30, min_periods=0).mean()
        spread_df = pd.merge(df_, mean_df, left_on='trade_date', right_index=True, suffixes=('','_mean'))[['spread_mean']]

        return spread_df.reset_index(drop=True)

    @staticmethod
    def calculate_volume_metrics(df):
        df_ = df[['trade_date','volume_ton']].copy()
        start = df_['trade_date'].min()
        end = df_['trade_date'].max()
        time_index = pd.date_range(start=start, end=end, freq='1D')
        full_df = pd.DataFrame({'full_ts':time_index})
        full_df = pd.merge(full_df, df_, how='left', left_on='full_ts', right_on='trade_date')
        full_df.set_index(['full_ts'], inplace=True)
        full_df.drop(columns=['trade_date'], inplace=True)

        mean_df = full_df.rolling(window=30, min_periods=0).mean()
        total_df = full_df.rolling(window=30, min_periods=0).sum()
        
        mean_df = pd.merge(df_, mean_df, left_on='trade_date', right_index=True, suffixes=('','_mean'))[['volume_ton_mean']]
        total_df = pd.merge(df_, total_df, left_on='trade_date', right_index=True, suffixes=('','_sum'))[['volume_ton_sum']]

        return pd.concat([mean_df, total_df], axis=1).reset_index(drop=True)

    @staticmethod
    def get_timestamp_features(df):
        weekday_number = df['trade_date'].dt.dayofweek
        month = df['trade_date'].dt.month
        day_of_month = df['trade_date'].dt.day

        weekday_sin = np.sin(2 * np.pi * weekday_number/6.0)
        weekday_cos = np.cos(2 * np.pi * weekday_number/6.0)

        month_sin = np.sin(2 * np.pi * month/11.0)
        month_cos = np.cos(2 * np.pi * month/11.0)

        day_of_month_sin = np.sin(2 * np.pi * day_of_month/31.0)
        day_of_month_cos = np.cos(2 * np.pi * day_of_month/31.0)


        return pd.DataFrame({
            'weekday_sin': weekday_sin,
            'weekday_cos': weekday_cos,
            'month_sin': month_sin,
            'month_cos': month_cos,
            'day_of_month_sin': day_of_month_sin,
            'day_of_month_cos': day_of_month_cos
        }).reset_index(drop=True)


    def append_features(self, df):
        df = df.copy()
        prices_df = self.get_prices_single(df)
        lag_df = self.calculate_last_price_columns(prices_df)
        ma_df = self.calculate_moving_averages(prices_df)
        spread_df = self.calculate_spread_metrics(df)
        vol_df = self.calculate_volume_metrics(df)
        ts_df = self.get_timestamp_features(df)
        print(df)
        print(lag_df)
        print(ma_df)
        print(spread_df)
        print(vol_df)
        print(ts_df)
        return pd.concat([df.reset_index(drop=True), lag_df, ma_df, spread_df, vol_df, ts_df], axis=1)

    def store_as_pickle(self, df, symbol):
        file_path = pathlib.Path.joinpath(self.feature_store_path, f"{symbol}_features.pkl")
        df.to_pickle(file_path)

    def run(self):
        df = self.load_silver_df()
        df = self.filter_by_date_range(df)
        for s in self.symbols:
            df_ = self.filter_by_symbol(df, s)
            df_ = self.append_features(df_)
            df_ = self.filter_by_date_range(df_, start='2012-03-01')
            self.store_as_pickle(df_, s)
        return True