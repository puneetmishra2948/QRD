import streamlit as st
import pandas as pd
import numpy as np
# import eikon as ek
import pyreadr
import altair as alt
from tqdm import tqdm
import datetime
import warnings
import plotly
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from product_mapper import *
import plotly.io as pio
pio.templates.default = "plotly_white"
import matplotlib
import matplotlib.pyplot as plt
from DataModules import DataPipeline
matplotlib.rcParams['figure.figsize'] = (30.0, 15.0)
plt.style.use('ggplot')
warnings.simplefilter(action='ignore',  category=Warning)
st.set_page_config(layout="wide")
# ek.set_app_key('02741dc6b2924c0fad37758127abe4b3ba62b3db')

@st.cache
def loadData(product, spread_name, outrights, cfds_ric, cfds_true=True):
    datapipe = DataPipeline(start_date='2022-01-01', end_date='2023-01-18')

    target_fields = ['TR.SETTLEMENTPRICE', 'TR.TSVWAP', 'TR.OPENINTEREST']

    # options_fields = ['TR.30DAYATTHEMONEYIMPLIEDVOLATILITYINDEXFORCALLOPTIONS',
    #                 'TR.60DAYATTHEMONEYIMPLIEDVOLATILITYINDEXFORCALLOPTIONS', 'TR.90DAYATTHEMONEYIMPLIEDVOLATILITYINDEXFORCALLOPTIONS']

    futures_df = datapipe.fetchData(product, spread_name, target_fields, outrights=outrights) 
    # options_df = datapipe.fetchData(product, 'ATMIV', options_fields)
    # options_df.columns = ['30D ATM IV', '60D ATM IV', '90D ATM IV']
    # futures_df['Close Price'] = futures_df['Close Price'].shift(-1)

    ohlcv_df = datapipe.fetchTimeSeries(product + spread_name)
    futures_df = pd.concat([ohlcv_df, futures_df], axis=1)

    futures_df = futures_df.dropna()

    if cfds_true==True:

        cfds_df = datapipe.fetchTimeSeries(cfds_ric, timeframe='daily', field_name=['CLOSE']).astype(int)
        cfds_df.index = pd.to_datetime(cfds_df.index)
        cfds_df.columns = [product]
        cfds_df.index = cfds_df.index.date

        return cfds_df, futures_df
    elif cfds_true == False:
        return None, futures_df

def getFeatures(data_df):
    tmp_df = data_df.copy()
    tmp_df['SettlevsVWAP'] = (tmp_df['Settlement Price'] - tmp_df['VWAP']).astype(float)
    return tmp_df

def createPage(product_tabs, tab_id, product_name, cfds_true=True):
    product_params = product_mapping[product_name]
    with product_tabs[tab_id]:
        cfds_df, futures_df = loadData(product=product_params['product'],
                                    spread_name=product_params['spread_name'],
                                    outrights=product_params['outrights'],
                                    cfds_ric=product_params['cfds'],
                                    cfds_true=cfds_true)

        feature_df = getFeatures(futures_df)

        tab_CFDS_table, tab_CFDS_chart, tab_SVWAP_chart = st.tabs(["TABLE", "CFDS", "Settle vs VWAP"])

        if cfds_true==True:

            with tab_CFDS_table:

                # st.write("SOYBEAN CFDS")
                st.write(cfds_df[product_name])
            
            with tab_CFDS_chart:
                period_length = st.number_input('Enter Period Length', value=5, step=1, min_value=1, key=product_name + '_cfds_num')
                average_type = st.radio(
                    "Average Type",
                    ('Mean', 'Median'), key=product_name + '_radio')
                if average_type=='Mean':
                    data_df = cfds_df[product_name].rolling(period_length).mean()
                elif average_type=='Median':
                    data_df = cfds_df[product_name].rolling(period_length).median()
                pos_delta = data_df.loc[data_df>0]
                neg_delta = data_df.loc[data_df<=0]

                fig = make_subplots(rows=2, cols=1, row_width=[0.5, 0.5], vertical_spacing=0, shared_xaxes=True, specs=[[{"secondary_y": True}], [{"secondary_y": True}]])

                fig.add_trace(go.Candlestick(
                        x = data_df.index,
                        open = feature_df['OPEN'], 
                        high = feature_df['HIGH'],
                        low = feature_df['LOW'],
                        close = feature_df['CLOSE'],
                        name = 'OHLCV',
                        increasing = {'fillcolor' : 'green', 'line':{'color':'green'}},
                        decreasing = {'fillcolor' : 'red', 'line':{'color':'red'}},
                        line = {'width':1}), row=1, col=1)


                fig.add_trace(go.Bar(x = pos_delta.index, y = pos_delta,  marker_color='green', opacity=1, name='Hedge Fund Buying'), row=2, col=1)
                fig.add_trace(go.Bar(x = neg_delta.index, y = neg_delta,  marker_color='maroon',opacity=1, name='Hedge Fund Selling'), row=2, col=1)

                fig.update_layout(xaxis_rangeslider_visible=False, height=600)
                st.plotly_chart(fig, use_container_width=True)

        with tab_SVWAP_chart:

            ric_name = st.selectbox(
            'RIC',
            tuple(active_spreads[product_name]), key=product_name + '_spread')

            product_params = product_mapping[product_name]

            outrights = ric_name.split('-')

            cfds_df, futures_df = loadData(product=product_params['product'],
                                    spread_name = ric_name,
                                    outrights = outrights,
                                    cfds_ric=None,
                                    cfds_true=False)

            feature_df = getFeatures(futures_df)

            period_length = st.number_input('Enter Period Length', value=5, step=1, min_value=1, key=product_name + ric_name + '_vwap')

            average_type = st.radio(
                "Average Type",
                ('Mean', 'Median'), key=product_name + ric_name + '_vwap_radio')

            if average_type=='Mean':
                data_df = feature_df['SettlevsVWAP'].rolling(period_length).mean()

            elif average_type=='Median':
                data_df = feature_df['SettlevsVWAP'].rolling(period_length).median()

            pos_delta = data_df.loc[data_df>0]
            neg_delta = data_df.loc[data_df<=0]

            fig = make_subplots(rows=2, cols=1, row_width=[0.5, 0.5], vertical_spacing=0, shared_xaxes=True, specs=[[{"secondary_y": True}], [{"secondary_y": True}]])

            fig.add_trace(go.Candlestick(
                    x = data_df.index,
                    open = feature_df['OPEN'], 
                    high = feature_df['HIGH'],
                    low = feature_df['LOW'],
                    close = feature_df['CLOSE'],
                    name = 'OHLCV',
                    increasing = {'fillcolor' : 'green', 'line':{'color':'green'}},
                    decreasing = {'fillcolor' : 'red', 'line':{'color':'red'}},
                    line = {'width':1}), row=1, col=1)


            fig.add_trace(go.Bar(x = pos_delta.index, y = pos_delta,  marker_color='green', opacity=1, name='Settle vs VWAP'), row=2, col=1)
            fig.add_trace(go.Bar(x = neg_delta.index, y = neg_delta,  marker_color='maroon',opacity=1, name='Settle vs VWAP'), row=2, col=1)

            fig.update_layout(xaxis_rangeslider_visible=False, height=600)
            st.plotly_chart(fig, use_container_width=True)


def main():

    product_tabs = st.tabs(["SoyBean", "Corn", "SoyMeal", "SoyOil", "SRW Wheat", "Brent"])


    createPage(product_tabs, 0, '1S', True)
    createPage(product_tabs, 1, '1C', True)
    createPage(product_tabs, 2, 'SM', True)
    createPage(product_tabs, 3, 'BO', True)
    createPage(product_tabs, 4, '1W', True)
    createPage(product_tabs, 5, 'LCO', False)


if __name__ == "__main__":
    main()