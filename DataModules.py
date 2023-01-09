import pandas as pd
import numpy as np
import eikon as ek
import datetime
import warnings
import plotly.express as px
import matplotlib
import matplotlib.pyplot as plt
ek.set_app_key('02741dc6b2924c0fad37758127abe4b3ba62b3db')


class DataPipeline():
    def __init__(self, start_date, end_date, num_points=50):
        self.start_date = start_date
        self.end_date = end_date
        self.num_points = num_points

    def fetchData(self, product_name,  month_name, fields_list, outrights = None):
        params_list = []
        final_df = pd.DataFrame()
# 
        # call_params = f'{fields_list[0]}(SDate={self.start_date}, EDate={self.end_date})'
# 
        # params_list.append(call_params+'.timestamp')


        for field_name in fields_list:
            if field_name == 'TR.OPENINTEREST':
                for o1 in outrights:
                    ric_name = product_name + o1
            
                    params_list = []

                    call_params = f'{field_name}(SDate={self.start_date}, EDate={self.end_date})'
                    params_list.append(call_params+'.timestamp')

                    call_params = f'{field_name}(SDate={self.start_date}, EDate={self.end_date})'
                    params_list.append(call_params)

                    ek_response = ek.get_data(ric_name, params_list)
                    ek_response = ek_response[0]  # .iloc[::-1]
                    ek_response.index = pd.to_datetime(ek_response['Timestamp'])
                    # ek_response.index = ek_response['Timestamp']
                    ek_response.index = ek_response.index.date
                    ek_response.drop(columns=['Timestamp'], inplace=True)
                    ek_response = ek_response[~ek_response.index.duplicated(keep='last')]
                    ek_response = ek_response.drop('Instrument', axis=1)
                    final_df = pd.concat([final_df, ek_response['Open Interest'].rename(o1+' OI')], axis=1)
            else:
                ric_name = product_name + month_name
                params_list = []
                call_params = f'{field_name}(SDate={self.start_date}, EDate={self.end_date})'
                params_list.append(call_params+'.timestamp')
                call_params = f'{field_name}(SDate={self.start_date}, EDate={self.end_date})'
                params_list.append(call_params)
                ek_response = ek.get_data(ric_name, params_list)
                ek_response = ek_response[0]  # .iloc[::-1]
                print(ek_response)
                ek_response.index = pd.to_datetime(ek_response['Timestamp'])
                # ek_response.index = ek_response['Timestamp']
                ek_response.index = ek_response.index.date
                ek_response.drop(columns=['Timestamp'], inplace=True)
                ek_response = ek_response[~ek_response.index.duplicated(keep='last')]
                final_df = pd.concat([final_df, ek_response.drop('Instrument', axis=1)], axis=1)

        return final_df.sort_index()


    def fetchTimeSeries(self, ric_name, timeframe='daily', field_name=['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'], start_date=None, end_date=None):

        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        data_df = ek.get_timeseries(rics=ric_name, interval=timeframe,
                                    fields=field_name, start_date=start_date, end_date=end_date)

        data_df.index = pd.to_datetime(data_df.index).date

        data_df = data_df[~data_df.index.duplicated(keep='last')]

        return data_df

    def calcCorr(self, series_1, series_2):
        series_1 = series_1[~series_1.index.duplicated(keep='first')].dropna()
        series_2 = series_2[~series_2.index.duplicated(keep='first')].dropna()
        combined_series = pd.concat(
            [series_1, series_2], axis=1, join='inner').astype(float)

        corr_coef = np.corrcoef(
            combined_series.iloc[:, 0], combined_series.iloc[:, 1])

        return corr_coef[0][1]

    def checkCrossCorr(self, series_1, series_2, max_lag=10):
        series_1 = series_1[~series_1.index.duplicated(keep='first')].dropna()
        series_2 = series_2[~series_2.index.duplicated(keep='first')].dropna()
        combined_series = pd.concat(
            [series_1, series_2], axis=1, join='inner').astype(float)

        # print(combined_series)

        plt.xcorr(combined_series.iloc[:, 0], combined_series.iloc[:, 1], usevlines=True,
                  maxlags=max_lag, normed=True)

        # plt.show()

    def curveFit(self, data_df, order=10):
        # print(data_df)
        # data_df = data_df.values
        z = np.polyfit(range(0, len(data_df.dropna())), data_df.dropna().values, order)
        p = np.poly1d(z)

        tmp = []
        nans = (len(data_df) - len(data_df.dropna()))
        for i in range(0, nans):
            tmp.append(np.nan)
        for i in range(0, len(data_df.dropna())):
            tmp.append(p(i))

        tmp = pd.Series(tmp, index = data_df.index)

        return tmp.mean()

    def load_rds_data(self, ric_name, path_name = 'I:/GurgaonQuant/Puneet/TAS_1m_RDS/'):
        file_name = path_name + ric_name + '.rds'
        result = pyreadr.read_r(file_name)[None]
        result.index = pd.to_datetime(result['Timestamp'])
        result = result.drop(columns=['Timestamp'])
        return result
