import pandas as pd
import pathlib
import os

class RawToBronze:
    def __init__(self, data_path):
        self.data_path = data_path
        try:
            self.raw_path = pathlib.Path.joinpath(self.data_path, "raw")
        except:
            print(f"Raw path does not exist! Please create directory")
        try:
            self.bronze_path = pathlib.Path.joinpath(self.data_path, "bronze")
        except:
            print(f"Bronze path does not exist! Please create directory")

    def load_single_file_to_df(self, filename):
        try:
            file_path = pathlib.Path.joinpath(self.raw_path, filename)
        except:
            print(f"Couldn't find file {filename} in path {self.raw_path}")
            return None
        try:
            df = pd.read_excel(file_path, sheet_name='rptCoffee', header=2)
        except:
            print(f"Couldn't load file {file_path}")
            return None
        return df
    
    def load_multiple_files_to_df(self, filename_list):
        dfs = []
        for f in filename_list:
            dfs.append(self.load_single_file_to_df(f))
        # remove any Nones resulting from errors
        res = [i for i in dfs if isinstance(i, pd.DataFrame)]
        return pd.concat(dfs)
    
    def store_as_pickle(self, df, filename='ecx_bronze.pkl'):
        file_path = pathlib.Path.joinpath(self.bronze_path, filename)
        df.to_pickle(file_path)

    def get_filenames(self):
        file_list = os.listdir(self.raw_path)
        return file_list

    def run(self):
        self.get_filenames()
        fnames = self.get_filenames()
        df = self.load_multiple_files_to_df(fnames)
        self.store_as_pickle(df)
        return True
