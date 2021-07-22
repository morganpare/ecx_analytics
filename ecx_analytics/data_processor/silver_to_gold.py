import numpy as np
import pandas as pd
import pathlib

# aggregates:
# opening: 'min', 'max', 'std'
# closing: 'min', 'max', 'std', 25, 50, 100 quantiles
# high: 'min', 'max', 'std'
# low: 'min', 'max', 'std'
# change: 'min', 'max', 'std'
# volume: 'min', 'max', mean, 'std', 25, 50, 100 quantiles, sum
# count records

# dimensions:
# day
# month
# year
# day of week
# symbol
# warehouse
# production year
# warehouse, year
# symbol, year
# volume buckets

class SilverToGold:
    def __init__(self, data_path):
        self.data_path = data_path
        try:
            self.silver_path = pathlib.Path.joinpath(self.data_path, "silver")
        except:
            print(f"Silver path does not exist! Please create directory")
        try:
            self.gold_path = pathlib.Path.joinpath(self.data_path, "gold")
        except:
            print(f"Gold path does not exist! Please create directory")

    def load_silver_df(self, filename="ecx_silver.pkl"):
        file_path = pathlib.Path.joinpath(self.silver_path, filename)
        df = pd.read_pickle(file_path)
        return df
    
    @staticmethod
    def percentile(n):
        def percentile_(x):
            return np.percentile(x, n)
        percentile_.__name__ = 'percentile_%s' % n
        return percentile_

    def create_aggregate(self, df, dimensions):
        df['opening_price_x_volume'] = df['opening_price'] * df['volume_ton']
        df['closing_price_x_volume'] = df['closing_price'] * df['volume_ton']
        df['high_x_volume'] = df['high'] * df['volume_ton']
        df['low_x_volume'] = df['low'] * df['volume_ton']
        df['change_x_volume'] = df['change'] * df['volume_ton']
        df['spread_x_volume'] = df['spread'] * df['volume_ton']

        agg_dict = {
            'opening_price': ['count','min', 'max', 'std'],
            'closing_price': ['min', 'max', 'std', self.percentile(25), self.percentile(50), self.percentile(100)],
            'high': ['min', 'max', 'std'],
            'low': ['min', 'max', 'std'],
            'change': ['min', 'max', 'std', 'mean'],
            'spread': ['min', 'max', 'std'],
            'volume_ton': ['min', 'max', 'mean', 'std', self.percentile(25), self.percentile(50), self.percentile(100), 'sum'],
            'opening_price_x_volume': ['sum'],
            'closing_price_x_volume': ['sum'],
            'high_x_volume': ['sum'],
            'low_x_volume': ['sum'],
            'change_x_volume': ['sum'],
            'spread_x_volume': ['sum'],
        }

        agg_df = df.groupby(dimensions).agg(agg_dict)

        agg_df['weighted_mean_opening_price'] = agg_df['opening_price_x_volume_sum'] / agg_df['sum_volume_ton']
        agg_df['weighted_mean_closing_price'] = agg_df['closing_price_x_volume_sum'] / agg_df['sum_volume_ton']
        agg_df['weighted_mean_high'] = agg_df['high_x_volume_sum'] / agg_df['sum_volume_ton']
        agg_df['weighted_mean_low'] = agg_df['low_x_volume_sum'] / agg_df['sum_volume_ton']
        agg_df['weighted_mean_change'] = agg_df['change_x_volume_sum'] / agg_df['sum_volume_ton']
        agg_df['weighted_mean_spread'] = agg_df['spread_x_volume_sum'] / agg_df['sum_volume_ton']


        return agg_df

    def store_as_pickle(self, agg_df, dimensions):
        if isinstance(dimensions, str):
            filename = dimensions
        elif isinstance(dimensions, list):
            filename = dimensions.join('_')
        else:
            raise Exception('String or List not passed')
        filename += ".pkl"
        file_path = pathlib.Path.joinpath(self.gold_path, filename)
        agg_df.to_pickle(file_path)

    def run(self, df):
        df['day_of_week'] = df['trade_date'].dt.day_name()
        df['month'] = df['trade_date'].dt.month
        df['year'] = df['trade_date'].dt.year
        for d in [
            'day',
            'month',
            'year',
            'day_of_week',
            'symbol',
            'warehouse_name',
            'production year',
            ['warehouse_name', 'year'],
            ['symbol', 'year']
        ]:
            agg_df = self.create_aggregate(df, d)
            self.store_as_pickle(agg_df, d)