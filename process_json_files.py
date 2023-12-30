import os
import json
import pandas as pd
import glob
from util import unix_to_regular_time, unix_milliseconds_to_regular_time, timer_func, split_dataframe
import numpy as np
from logger import get_logger
from tqdm import tqdm
from base_class import Parse_One_JSON_file_to_DataFrame
from job_mapping import option_mapping
import re

logger = get_logger()

class ParseYahooFs(Parse_One_JSON_file_to_DataFrame):
    def _load_to_dataframe(self, data:dict) -> pd.DataFrame:
        if data is None:return pd.DataFrame()
        
        try:
            target = data['timeseries']['result']
        except KeyError as error:
            logger.debug(f"Faile to extract {self.file_name},with content {data}")
            return pd.DataFrame()

        _final_results = []

        for each_item in target[:]:
            stock = each_item['meta']['symbol'][0]
            item = each_item['meta']['type'][0]
            # good data has exact 3 sections, meta, timestamp, and the item
            if len(each_item) < 3:
                pass
            else:
                for each_data_line in each_item[item]:
                    _final_results.append({'stock':stock,
                                        'item':item,
                                        'currencyCode':each_data_line['currencyCode'],
                                        'asOfDate': each_data_line['asOfDate'],
                                        'periodType':each_data_line['periodType'],
                                        'value':each_data_line['reportedValue']['raw'],
                                        'updated_dt':self.data_date})

        return pd.DataFrame(_final_results) 
    
    def _transform(self, data:pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()
        
        data.dropna(subset=['stock','asOfDate'], axis=0, inplace=True)
        data.set_index(keys='stock', inplace=True)
        return data

class ParseYahooStats(Parse_One_JSON_file_to_DataFrame):
    def _load_to_dataframe(self, data:dict) -> pd.DataFrame:
        if data is None:return pd.DataFrame()
        elif data['quoteSummary']['result'] is None:return pd.DataFrame()
        
        try:
            target = data['quoteSummary']['result'][0]
        except KeyError:
            logger.debug(f"Faile to extract {self.file_name},with content {data}")
            return pd.DataFrame()
        
        df_lst = []
        for item in ['price', 'summaryDetail', 'defaultKeyStatistics', 'financialData']:
            content = target.get(item)
            if content is not None:
                try:
                    _temp_df = pd.DataFrame(content)
                except ValueError as err:
                    _temp_df = pd.DataFrame(content, index=['raw'])
                if _temp_df.shape[0] == 3:
                    df_lst.append(_temp_df.loc['raw'])
                else:
                    pass
        
        if len(df_lst) == 0:
            return pd.DataFrame()
        else:
            df_final = pd.concat(df_lst)

        return df_final
    
    def _transform(self, data:pd.DataFrame) -> pd.DataFrame:
        data = data[~data.index.duplicated(keep='first')]
        data.drop(labels=['maxAge','priceHint','currencySymbol', 
                              'algorithm','preMarketSource','postMarketSource',
                              'regularMarketSource','quoteSourceName','marketState',
                              'circulatingSupply','maxSupply','tradeable','toCurrency',
                              'fromCurrency','coinMarketCapLink','underlyingSymbol',
                              'exchangeDataDelayedBy','lastMarket','symbol'], 
                      inplace=True, errors='ignore')
        data['stock'] = self.stock
        data['updated_dt'] = self.data_date
        data.replace(to_replace={'Infinity':np.nan}, inplace=True)
        data.rename({'52WeekChange':'fiftytwoWeekChange'}, inplace=True)
        
        for time_change in ['mostRecentQuarter', 'postMarketTime', 'nextFiscalYearEnd',
                            'lastFiscalYearEnd', 'fundInceptionDate', 'regularMarketTime',
                            'sharesShortPreviousMonthDate','exDividendDate', 'expireDate',
                            'startDate','dateShortInterest','lastSplitDate',
                            'lastDividendDate']:
            if time_change in data.index:
                data[time_change] = unix_to_regular_time(data[time_change])
        
        return data

class ParseYahooSc(Parse_One_JSON_file_to_DataFrame):
    def _load_to_dataframe(self, data:dict) -> pd.DataFrame:
        data = self._open_and_read_file()
        
        if data is None:return pd.DataFrame()
        else:output = pd.DataFrame(data)
        
        return output
    
    def _transform(self, data:pd.DataFrame) -> pd.DataFrame:
        data.rename(columns={'symbol':'stock'}, inplace=True)
        data['updated_dt'] = self.data_date
        data.drop(columns=['fiftyTwoWeekRange', 'regularMarketDayRange', 'newListingDate'], errors='ignore', inplace=True)

        for time_change in ['dividendDate', 'regularMarketTime',
                            'earningsTimestamp','earningsTimestampStart', 'earningsTimestampEnd',
                            'postMarketTime']:
            if time_change in data.columns:
                data[time_change] = data[time_change].apply(unix_to_regular_time)    
                
                   
        for time_change in ['firstTradeDateMilliseconds']:
            if time_change in data.columns:
                data[time_change] = data[time_change].apply(unix_milliseconds_to_regular_time)

        return data

class ParseYahooSp(Parse_One_JSON_file_to_DataFrame):
    def _load_to_dataframe(self, data:dict) -> pd.DataFrame:
        # return at least empty DataFrame
        data = self._open_and_read_file()
        try:
            
            if data['quoteSummary']['result'][0]['summaryProfile']['sector'] != "" and data['quoteSummary']['result'][0]['summaryProfile']['industry'] != "":
                output = pd.DataFrame({'sector':data['quoteSummary']['result'][0]['summaryProfile']['sector'],
                                    'industry':data['quoteSummary']['result'][0]['summaryProfile']['industry']
                                    }, index=[0])
                return output
            else:
                return pd.DataFrame()
            
        except (TypeError, KeyError):
            return pd.DataFrame()

    def _transform(self, data:pd.DataFrame) -> pd.DataFrame:
        if not data.empty:
            data['stock'] = self.stock
            data['updated_dt'] = self.data_date
            return data
        else:
            return pd.DataFrame()

class ParseYahooOp(Parse_One_JSON_file_to_DataFrame):
    def _load_to_dataframe(self, data:dict) -> pd.DataFrame:
        data = self._open_and_read_file()
        output = pd.DataFrame(data)
        return output
    
    def _transform(self, data:pd.DataFrame) -> pd.DataFrame:
        if not data.empty:
            data['lastTradeDate'] = data['lastTradeDate'].apply(unix_to_regular_time)
            data.rename(columns={'expiration':'expirationUnix','change':'priceChange'}, inplace=True)
            data['inTheMoney'] = data['inTheMoney'].apply(lambda x: 1 if x == True else 0)
            # parse option contract name
            pattern = re.compile(r'[A-Za-z]+|\d+')
            data['stock'] = data['contractSymbol'].apply(lambda x:re.findall(pattern, x)[0])
            data['expirationDate'] = data['contractSymbol'].apply(lambda x:re.findall(pattern, x)[1])
            data['expirationDate'] = pd.to_datetime(data['expirationDate'], format='%y%m%d')
            data['optionType'] = data['contractSymbol'].apply(lambda x: 'CALL' if re.findall(pattern, x)[2] =='C' else 'PUT')
            data['updated_dt'] = self.data_date
            return data
        else:
            return pd.DataFrame()

class ParseManyJSON():
       
    def __init__(self, file_name_pattern:str, disable_tqdm:bool = False) -> None:

        self.file_name_pattern = file_name_pattern
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
        self.data_date = re.findall(date_pattern, file_name_pattern)[0]
        self.parse_option = file_name_pattern.split('_')[1]
        self.disable_tqdm = disable_tqdm
        
        if self.parse_option == 'yahoosc':
            self.parse_option_file_name = '_'.join([file_name_pattern.split('_')[1],file_name_pattern.split('_')[2]])
        else:
            self.parse_option_file_name = self.parse_option

    def loop_folder(self) -> list():
        _temp_df_lst = []
        for file in tqdm(glob.glob(os.path.join('staging', self.data_date, self.file_name_pattern))[:],
                         disable = self.disable_tqdm,
                         desc = f'{self.parse_option}',
                         ncols=80):
            _temp_df = getattr(eval(option_mapping[self.parse_option]['parse_object'])(file), 'parse')()
            if not _temp_df.empty:
                _temp_df_lst.append(_temp_df.T)
        
        return _temp_df_lst

    def compose_final_df(self, df_list) -> pd.DataFrame:
        # Exit: no file exist in the folder
        if len(df_list) == 0:
            return pd.DataFrame()
        
        output_df = pd.concat(df_list, axis=1).transpose()
        return output_df
    
    @timer_func
    def write_final_file(self, df, file_type='csv') -> None:
        _df_list = split_dataframe(df, chunk_size=500_000)
        file_counter = 1
        
        if not os.path.exists(os.path.join('final', f'{self.parse_option_file_name}_{self.data_date}.csv')):
            os.mkdir(os.path.join('final', f'{self.parse_option_file_name}_{self.data_date}.csv'))
        
        for _ in tqdm(_df_list,
                      disable = self.disable_tqdm,
                      desc = f'Write Files',
                      ncols=80):
            if file_type == 'csv':
                _.to_csv(os.path.join('final', f'{self.parse_option_file_name}_{self.data_date}.csv', f'{self.parse_option_file_name}_{self.data_date}_{file_counter}.csv'))
                logger.info(f"{self.parse_option_file_name}_{self.data_date}_{file_counter}.csv is created in {os.path.join('final', f'{self.parse_option_file_name}_{self.data_date}.csv')}")
            elif file_type == 'parquet':
                # need to replace nan to '' to avoid errors in export
                _.replace(to_replace={'NaN':''}, inplace=True)
                _.to_parquet(os.path.join('final', f'{self.parse_option_file_name}_{self.data_date}.parquet', f'{self.parse_option_file_name}_{self.data_date}_{file_counter}.parquet'))
                logger.info(f"{self.parse_option_file_name}_{self.data_date}_{file_counter}.csv is created in {os.path.join('final', f'{self.parse_option_file_name}_{self.data_date}.csv')}")
            
            file_counter += 1
    
    def delete_staging_file(self) -> None:
        path = os.path.join('staging',self.data_date)
        _no_deleted = 0
        for file in glob.glob(os.path.join(path,self.file_name_pattern))[:]:
            if os.path.isfile(file):
                os.remove(file)
                _no_deleted += 1
                logger.info(f"{file} has been deleted")
            else:
                logger.info(f"{file} does not exist")
                
        logger.info(f"{_no_deleted} files have been deleted from '{path}'")
    
    def run(self, save_as) -> None:
        file_list = self.loop_folder()
        self.write_final_file(self.compose_final_df(file_list), 
                              file_type=save_as)

if __name__  == "__main__":
    
    pattern1 = 'yahoo_yahoofs_*_2023-12-26.txt'
    pattern2 = 'yahoo_yahoostats_*_2023-12-26.txt'
    pattern3 = 'yahoo_yahoosc_mostactives_2023-12-28.txt'
    pattern4 = 'yahoo_yahoosp_*_2023-12-28.txt'
    pattern5 = 'yahoo_yahooop_*_2023-12-25.txt'
    obj = ParseManyJSON(pattern5)
    obj.run('csv')
    
    # obj.delete_staging_file()
    # obj.write_final_file(df, file_type='csv')
    
    # obj = ParseOneJSON('staging/2023-12-26/yahoo_yahoosc_topetfsus_2023-12-26.txt')
    # obj.parse_yahoosc_json()