import pandas as pd
import numpy as np
import pyreadr
from tqdm import tqdm
import datetime
import warnings
from DataModules import *

class dataLoader():
    def __init__(self, start_date = '2022-01-01', end_date = '2022-01-18'):
        self.start_date = start_date
        self.end_date = end_date
        self.init_module()


    def init_module(self):
        self.datapipe = DataPipeline(start_date = self.start_date, end_date = self.end_date)

    def fetch_timeseries(self, RIC):
        fetched_data = self.datapipe.fetchTimeSeries(RIC, timeframe='daily', field_name=['CLOSE'])
        fetched_data = fetched_data.rename(RIC)
        return fetched_data

    def fetch_data(self,product, spread_name, target_fields, outrights):
        fetched_data = self.datapipe.fetchData(product, spread_name, target_fields, outrights=outrights)

        return fetched_data


def main():
    load_data = dataLoader(start_date = '2022-01-01', end_date = '2022-01-18')


    


