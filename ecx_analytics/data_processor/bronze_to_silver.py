import pandas as pd
import pathlib
import os

class BronzeToSilver:
    def __init__(self, data_path):
        self.data_path = data_path
        try:
            self.bronze_path = pathlib.Path.joinpath(self.data_path, "bronze")
        except:
            print(f"Bronze path does not exist! Please create directory")
        try:
            self.silver_path = pathlib.Path.joinpath(self.data_path, "silver")
        except:
            print(f"Silver path does not exist! Please create directory")

    def load_bronze_df(self, filename="ecx_bronze.pkl"):
        file_path = pathlib.Path.joinpath(self.bronze_path, filename)
        df = pd.read_pickle(file_path)
        return df
    
    @staticmethod
    def handle_clean_datetime_columns(df):
        return pd.to_datetime(df['Trade Date'])

    @staticmethod
    def handle_warehouse_names(df):
        warehouse_name_dict = {
            "BG": "Bonga Gimbo",
            "HW": "Hawasa",
            "GM": "Gimbi",
            "SC": "SC Warehouse",
            "DD": "Dire Dawa",
            "DL": "Dilla Wenago",
            "JM": "Jimma Kersa", 
            "BH": "Bule Hora",
            "BD": "Badele Zuria",
            "WS": "Weliso",
            "ME": "Metu Zuria",
            "H1": "H1 Warehouse",
            "D1": "D1 Warehouse",
            "B2": "B2 Warehouse",
            "S1": "S1 Warehouse",
            "W1": "W1 Warehouse",
            "J1": "J1 Warehouse"
        }

        return df['Warehouse'].replace(warehouse_name_dict)

    @staticmethod
    def handle_percentages(df):
        return df['Persetntage Change'].apply(lambda x: float(x.strip('%'))/100)

    @staticmethod
    def calculate_spread(df):
        return df['High'] - df['Low']

    @staticmethod
    def calculate_mid_price(df):
        return (df['High'] + df['Low']) / 2

    @staticmethod
    def format_column_name(col_name):
        return col_name.replace('(','').replace(')','').replace(' ','_').lower()

    @staticmethod
    def clean_number_col(df, col):
        return df[col].str.replace(',', '').astype(float)

    def conduct_cleaning(self, df):
        for c in ['High','Low','Opening Price', 'Closing Price']:
            df[c] = self.clean_number_col(df, c)
        df['Trade Date'] = self.handle_clean_datetime_columns(df)
        df['warehouse_name'] = self.handle_warehouse_names(df)
        df['percentage_change'] = self.handle_percentages(df)
        df['spread'] = self.calculate_spread(df)
        df['mid_price'] = self.calculate_mid_price(df)
        df.drop(columns = ['Persetntage Change'], inplace=True)
        df.rename(columns=lambda x: x.replace('(','').replace(')','').replace(' ','_').lower(), inplace=True)
        df = df.sort_values(by=['symbol','trade_date','production_year', 'warehouse'])
        df = df.drop_duplicates(subset=['symbol','trade_date'], keep="last")
        return df

    def store_as_pickle(self, df, filename="ecx_silver.pkl"):
        file_path = pathlib.Path.joinpath(self.silver_path, filename)
        df.to_pickle(file_path)

    def run(self):
        df = self.load_bronze_df()
        df = self.conduct_cleaning(df)
        self.store_as_pickle(df)
        return True     
