import time
from base_class import YahooAPI_to_JSON_file
import random
from os import path
import json
import pandas as pd
from logger import get_logger

logger = get_logger()


# the Yahoo defined elements are saved in yahoo_financial_statements_items.py as a list called items
from yahoo_items import financial_statement_items as items

class YahooFS(YahooAPI_to_JSON_file):  

    name = 'yahoofs'
    description = 'Yahoo Financial Statements'
    
    def yahoo_url(self) -> str:
        return f"https://query{random.choice(['1','2'])}.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{self.stock}?symbol={self.stock}&type={'%2C'.join(items)}&merge=false&period1=493590046&period2={int(time.time())}&crumb={self.crumb}"

class YahooStats(YahooAPI_to_JSON_file):  

    name = 'yahoostats'
    description = 'Yahoo Fundatemntals'

    def yahoo_url(self) -> str:
        return f"https://query{random.choice(['1','2'])}.finance.yahoo.com/v10/finance/quoteSummary/{self.stock}?modules=defaultKeyStatistics%2CfinancialData%2Cprice%2CsummaryDetail&crumb={self.crumb}"

class YahooScreener(YahooAPI_to_JSON_file):
    
    name = 'yahoosc'
    description = 'Yahoo screener'
    
    def yahoo_url(self, start) -> str:
        # use 'stock' as the holder to screener
        return f"https://query{random.choice(['1','2'])}.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&lang=en-US&region=US&scrIds={self.stock}&count=250&start={start}&crumb={self.crumb}"

    def load_to_staging(self) -> None:
        
        n = 250
        start = 0
        _temp_lst = []
        
        while n == 250:
            url = self.yahoo_url(start)
            logger.info(f'{url}')
            try:
                results = self.read_yahoo_api(url)
                results_json = json.loads(results)
                lst = results_json['finance']['result'][0]['quotes']
                _temp_lst = _temp_lst + lst
                n = results_json['finance']['result'][0]['count']
            except:
                logger.debug(f"Error: there is something wrong with {url}")
                break
            start = start + n

        file_name = f"yahoo_yahoosc_{self.stock.replace('_','').lower()}_{self.run_date}.txt"
        
        if len(_temp_lst) == 0:
            logger.info(f'{self.stock} does not have any information')
        else:
            pd.DataFrame(_temp_lst).to_json(path.join('staging', self.run_date, file_name),orient='records')

class YahooSp(YahooAPI_to_JSON_file):  

    name = 'yahoosp'
    description = 'Yahoo stock profile'
    
    def yahoo_url(self) -> str:
        return f"https://query{random.choice(['1','2'])}.finance.yahoo.com/v10/finance/quoteSummary/{self.stock}?modules=summaryProfile&crumb={self.crumb}"

class YaooOp(YahooAPI_to_JSON_file):

    name = 'yahooop'
    description = 'Yahoo options'

    def yahoo_url(self) -> str:
        return ''

if __name__ == "__main__":
    
    obj = YahooSp('AAPL', '2023-12-25')
    print(obj.yahoo_url)