import os
import sys
import pathlib
import logging
logging.basicConfig(level = logging.INFO)


FILEPATH = pathlib.Path(__file__).parent.resolve()
PROJECTPATH = FILEPATH.parent.parent.resolve()
DATAPATH = pathlib.Path.joinpath(FILEPATH.parent.parent, 'data')

sys.path.append(str(PROJECTPATH))

from ecx_analytics.data_processor.raw_to_bronze import RawToBronze
from ecx_analytics.data_processor.bronze_to_silver import BronzeToSilver
from ecx_analytics.data_processor.silver_to_gold import SilverToGold
from ecx_analytics.data_processor.silver_to_feature_store import SilverToFeatureStore

class DataProcessor:

    def __init__(self, data_path=DATAPATH):
        logging.info("Initialising Data Processor")
        self.data_path = data_path

    def raw_to_bronze(self):
        logging.info("Converting Raw Data To Bronze")
        raw_to_bronze = RawToBronze(self.data_path)
        raw_to_bronze.run()
        logging.info("Raw Data converted to Bronze")

    def bronze_to_silver(self):
        logging.info("Converting Bronze Data To Silver")
        bronze_to_silver = BronzeToSilver(self.data_path)
        bronze_to_silver.run()
        logging.info("Bronze Data converted to Silver")

    def silver_to_gold(self):
        logging.info("Converting Silver Data To Gold")
        silver_to_gold = SilverToGold(self.data_path)
        silver_to_gold.run()
        logging.info("Silver Data converted to Gold")

    def silver_to_feature_store(self):
        logging.info("Converting Silver Data To Feature Store")
        feature_processor = SilverToFeatureStore(self.data_path)
        feature_processor.run()
        logging.info("Silver Data converted to Feature Store")

if __name__ == '__main__':
    data_processor = DataProcessor()
    # data_processor.raw_to_bronze()
    # data_processor.bronze_to_silver()
    # data_processor.silver_to_gold()
    data_processor.silver_to_feature_store()