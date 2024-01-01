# based on Arnold and Earl (2007)

import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime
from util import regular_time_to_unix
import numpy as np
import math
pd.options.mode.chained_assignment = None

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def find_nearest_integers(number_list:int, target_number:int) -> list:
    # Find the absolute differences between each number in the list and the target number
    differences = [abs(num - target_number) for num in number_list]

    # Find the index of the minimum difference
    min_difference_index = differences.index(min(differences))

    # Get the nearest integers
    nearest_floor = int(number_list[min_difference_index])
    nearest_ceil = int(number_list[min_difference_index+ 1] )

    return [nearest_floor, nearest_ceil]

database_ip = '10.0.0.123'
database_user = 'boblinlj'
database_pw = os.environ["mydb"]
database_port = 3306
database_nm = 'financial_data'

cnn = create_engine(f"""mysql+mysqlconnector://{database_user}"""
                    f""":{database_pw}"""
                    f"""@{database_ip}"""
                    f""":{database_port}"""
                    f"""/{database_nm}""",
                    pool_size=30,
                    max_overflow=0)

run_date = '2023-12-29'
stock = 'AAPL'
iv_days = 30.0
rf_rate = 0.02

sql = f"""
    select c.stock
        , c.strike
        , c.expirationUnix
        , c.expirationDate
        , (c.bid+c.ask)/2 as mid_price_call
        , c.inTheMoney as MS_C
        , (p.bid+p.ask)/2 as mid_price_put
        , p.inTheMoney as MC_P
    from financial_data.yahoo_options c
    left join financial_data.yahoo_options p
        on c.stock = p.stock and c.strike = p.strike and c.currency = p.currency and c.expirationUnix = p.expirationUnix and c.updated_dt = p.updated_dt and p.optionType = 'PUT'
    where c.stock = '{stock}' and c.updated_dt = '{run_date}' and c.optionType = 'CALL' and FLOOR((DayOfMonth(c.expirationDate)-1)/7)+1 = 3
"""

df = pd.read_sql(con = cnn, sql = sql)
df['expirationDate'] = pd.to_datetime(df['expirationDate'])
currentUnixTime = regular_time_to_unix(datetime.strptime(run_date, '%Y-%m-%d').date())
next_maturity_date = currentUnixTime + iv_days*24*60*60
maturity_unix_lst = df['expirationUnix'].unique()
target_maturity_dates = find_nearest_integers(maturity_unix_lst, next_maturity_date)
df_near = df.loc[df['expirationUnix'] == target_maturity_dates[0]]
df_far = df.loc[df['expirationUnix'] == target_maturity_dates[1]]

def calculate_vix(input_df) -> float:
    # calculate delta strike = next higher strike - current strike, carry the last data for the missing value
    input_df['next_strike'] = input_df.groupby('expirationUnix')['strike'].shift(-1)
    input_df['delta_strike'] = input_df['next_strike'] - input_df['strike']
    input_df['delta_strike'] = input_df['delta_strike'].fillna(method='backfill')
    
    # find the ATM option by looking up the contract with lowest call - put price
    input_df['c_p_prices_diff'] = abs(input_df['mid_price_call'] - input_df['mid_price_put'])
    input_df['target_strike'] = np.where(input_df['c_p_prices_diff']==input_df['c_p_prices_diff'].min(), 1, 0)

    # calcualte the c/p near
    input_df['c_p_near'] = input_df[['mid_price_call','mid_price_put']].values.min(axis=1)
    input_df['c_p_near'].loc[input_df['target_strike']==1] = (input_df['mid_price_call'] + input_df['mid_price_put'])/2

    # calculate T, vix, f, sigma
    input_df['currentUnixTime'] = currentUnixTime
    input_df['T'] = (input_df['expirationUnix'] - input_df['currentUnixTime'])/31_536_000.0
    input_df['vix'] = input_df['delta_strike'] / np.square(input_df['strike']) * np.exp(rf_rate * input_df['T']) * input_df['c_p_near']
    input_df['F'] = input_df['strike'] + np.exp(rf_rate * input_df['T'])*(input_df['mid_price_call'] - input_df['mid_price_put'])
    input_df['total_vix'] = input_df['vix'].sum()
    input_df['sigma_square'] = input_df['total_vix']*(2/input_df['T']) - (np.square(input_df['F']/input_df['strike']-1)/input_df['T'])
    output = input_df.loc[input_df['target_strike'] == 1]['sigma_square'].values[0]
    
    return output

seg1 = calculate_vix(df_near)
seg2 = calculate_vix(df_far)


print(seg1, seg2)