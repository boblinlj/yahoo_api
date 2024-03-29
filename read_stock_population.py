import pandas as pd
import os
from logger import get_logger

logger = get_logger()

def get_population(filename):
    df = pd.read_csv(os.path.join('python_prod', 'yahoo_api','pop',filename),sep='\t', header=None)
    
    return df[0].str.upper().to_list()

if __name__ == '__main__':
    print(get_population('sec_ticker_2023-12-22.txt'))
    # print(get_sec_population(path.join('screener_2023-12-25.txt')))