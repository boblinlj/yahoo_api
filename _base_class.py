from abc import ABC
import requests
from header import headers
from proxy import proxies
import os
from datetime import datetime
from functools import lru_cache
from logger import get_logger
import json
import pandas as pd
import re
# header is used in a number  of places

logger = get_logger()

class YahooAPI_to_JSON_file(ABC):
    
    _session = requests.session()
    _session.proxies.update(proxies)
    
    def __init__(self, stock, run_date, crumb, cookies) -> None:
        self.stock = stock
        self.run_date = run_date
        self.crumb = crumb
        self.cookies = cookies

    def yahoo_url(self, *args, **kwargs) -> str:
        pass
    
    def check_and_mkdir(self, path) -> None:
        if not os.path.exists(path):
             os.mkdir(path)
    
    def read_yahoo_api(self, url) -> str:
        try:
            response=self._session.get(url=url, 
                                       headers = headers,
                                       cookies=self.cookies)
            return response.text
        except requests.exceptions.RequestException as err:
            print(err)
            
    def load_to_staging(self) -> None:
             
        self.check_and_mkdir(os.path.join('staging'))
        self.check_and_mkdir(os.path.join('staging', self.run_date))
        
        url = self.yahoo_url()

        file_name = f'yahoo_{self.name}_{self.stock}_{self.run_date}.txt'
        
        with open(os.path.join('staging',self.run_date, file_name), 'w', newline='') as f:
            f.write(self.read_yahoo_api(url))
            

@lru_cache
def get_yahoo_cookies() -> dict:

    response = requests.get(
        url = "https://fc.yahoo.com", 
        headers=headers, 
        allow_redirects=True
        )
    cookies = list(response.cookies)[0]
    
    logger.info(f"Yahoo cookies are {cookies.name} : {cookies.value}")
    
    return {cookies.name:cookies.value}

def get_yahoo_crumb(cookies:dict) -> str:
    crumb = None
    crumb_response = requests.get(
                                "https://query1.finance.yahoo.com/v1/test/getcrumb",
                                headers=headers,
                                cookies=cookies,
                                allow_redirects=True,
                                )
    crumb = crumb_response.text
    logger.info(f"Yahoo crumb is {crumb}")

    if crumb is None:
        raise Exception("Failed to retrieve Yahoo crumb.")
    
    return crumb

class Parse_One_JSON_file_to_DataFrame(ABC):
    
    # something wrong with the extraction
    _bad_data = 0
    
    # something wrong with one record
    _bad_record = 0
    
    def __init__(self, file_name) -> None:
        # file name should always like "staging/<run_date>/yahoo_<run_option>_<stock/item>_<run_date>.txt"
        self.file_name = file_name
        # get the stock name and extrated date from file name
        self.stock = self.file_name.split('_')[2]
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
        self.data_date = re.findall(date_pattern, self.file_name)[0]
        
    def _open_and_read_file(self) -> dict:
        f = open(os.path.abspath(self.file_name), "r").read()
        try:
            data = json.loads(f)
            return data
        except json.decoder.JSONDecodeError:
            logger.debug(f'{self.file_name} is not a JSON file')
            self._bad_data = 1
            return None
        
    def _load_to_dataframe(self, data:dict) -> pd.DataFrame:
        # return at least empty DataFrame
        pass
    
    def _transform(self, data:pd.DataFrame) -> pd.DataFrame:
        pass
    
    def parse(self):
        data_as_dict = self._open_and_read_file()
        data_as_df = self._load_to_dataframe(data_as_dict)
        output_df = self._transform(data_as_df)
        
        return output_df
        
    
    @property
    def issue(self) -> bool:
        return self._bad_data