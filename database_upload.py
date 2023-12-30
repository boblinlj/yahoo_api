from sqlalchemy import create_engine
import os
import pandas as pd
from util import parallel_process, split_dataframe, timer_func
from logger import get_logger
import glob
import re
from tqdm import tqdm

logger = get_logger()

database_ip = '10.0.0.123'
database_user = 'boblinlj'
database_pw = os.environ["mydb"]
database_port = 3306
database_nm = 'financial_data'

class WriteToDB():
    cnn = create_engine(f"""mysql+mysqlconnector://{database_user}"""
                    f""":{database_pw}"""
                    f"""@{database_ip}"""
                    f""":{database_port}"""
                    f"""/{database_nm}""",
                    pool_size=30,
                    max_overflow=0)
    
    def __init__(self, file_name_pattern, table, chunk_size=10_000) -> None:
        self.file_name_pattern = file_name_pattern
        self.table = table
        self.chunk_size = chunk_size
        date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
        self.data_date = re.findall(date_pattern, self.file_name_pattern)[0]
        self.error = 0
        
    def create_dataframe(self) -> pd.DataFrame:
        csv_files = glob.glob(os.path.join('final', self.file_name_pattern, '*.csv'))
        logger.info(f"Program will read the following files: {csv_files}")
        
        if len(csv_files) == 0:
            logger.info(f"""No data in "{os.path.join('final', self.file_name_pattern)}" """)
            return pd.DataFrame()
        else:
            df_list = [pd.read_csv(file) for file in tqdm(csv_files)]
            logger.info(f'Creating dataframe from csv files...')
            df =  pd.concat(df_list, ignore_index=True)
            if 'Unnamed: 0' in df.columns:
                df.drop(columns='Unnamed: 0', inplace=True)
            return df
    
    def write_to_db(self, dataframe) -> None:
        try:
            dataframe.to_sql(name=self.table,
                                con=self.cnn,
                                if_exists='append',
                                index=False,
                                chunksize=1,
                                method=None)
        except Exception as err:
            print(err)
    
    def remove_already_entered(self, new_df) -> pd.DataFrame:
        
        existing_check_sql = {
            'yahoo_fundamentals':f"select distinct stock from financial_data.yahoo_fundamentals where updated_dt = '{self.data_date}'",
            'yahoo_financial_statements':f'select distinct stock, asOfDate from financial_data.yahoo_financial_statements',
            'yahoo_financial_statements_quarter':f'select distinct stock, asOfDate from financial_data.yahoo_financial_statements_quarter',
            'yahoo_financial_statements_annual':f'select distinct stock, asOfDate from financial_data.yahoo_financial_statements_annual',
            'yahoo_most_shorted_stocks':f"SELECT distinct stock FROM financial_data.yahoo_most_shorted_stocks WHERE updated_dt = '{self.data_date}'",
            'yahoo_most_actives':f"SELECT distinct stock FROM financial_data.yahoo_most_actives WHERE updated_dt = '{self.data_date}'",
            'yahoo_top_etfs_us':f"SELECT distinct stock FROM financial_data.yahoo_top_etfs_us WHERE updated_dt = '{self.data_date}'",
            'yahoo_stock_profile':f"select distinct stock from financial_data.yahoo_stock_profile"
        }
        
        df_to_insert = pd.DataFrame()

        existing_df = pd.read_sql(sql = existing_check_sql[self.table], con=self.cnn)
        
        if existing_df.empty:
            logger.info(f'Will directly insert {new_df.shape[0]} rows, no existing data')
            df_to_insert = new_df
        
        elif self.table in ['yahoo_fundamentals', 'yahoo_most_shorted_stocks', 'yahoo_most_actives' , 'yahoo_top_etfs_us', 'yahoo_stock_profile']:
            logger.info(f'There are {new_df.shape[0]} rows before removing existing data')
            df_to_insert = new_df.loc[~new_df['stock'].isin(existing_df['stock'])]
            logger.info(f'There are {df_to_insert.shape[0]} rows after removing existing data, {new_df.shape[0]-df_to_insert.shape[0]} removed')
        
        elif self.table in ['yahoo_financial_statements','yahoo_financial_statements_quarter','yahoo_financial_statements_annual']:
            logger.info(f'There are {new_df.shape[0]} rows before removing existing data')
            df_to_insert = new_df.loc[(~new_df['stock'].isin(existing_df['stock'])) & 
                                      (~new_df['asOfDate'].isin(existing_df['asOfDate']))]
            logger.info(f'There are {df_to_insert.shape[0]} rows after removing existing data, {new_df.shape[0]-df_to_insert.shape[0]} removed')        
        else:
            pass

        return df_to_insert
    
    @timer_func
    def write_to_db_parallel(self, dataframe) -> None:
        # dedupped_df = self.remove_already_entered(dataframe)
        smaller_df_lst = split_dataframe(dataframe)
        parallel_process(smaller_df_lst, self.write_to_db)
        
    def pivot_table_yahoofs(self, dataframe:pd.DataFrame, where=None) -> pd.DataFrame:
        if where is None:
            pvt = pd.pivot_table(dataframe,
                                values='value',
                                index=['stock', 'asOfDate'],
                                columns=['item'],
                                aggfunc='sum')
        else: 
            pvt = pd.pivot_table(dataframe.loc[eval(where)],
                                values='value',
                                index=['stock', 'asOfDate'],
                                columns=['item'],
                                aggfunc='sum')
            
        pvt.reset_index(inplace=True)
        
        return pvt
        
if __name__ == "__main__":
    obj = WriteToDB('yahooop_2023-12-25.csv',
                    table='yahoo_options',
                    chunk_size=1_000)
    df = obj.create_dataframe()
    # df2 = obj.pivot_table_yahoofs(df, "df['periodType'] == '3M'")
    obj.write_to_db_parallel(df)
    
    
    # pvt = pd.pivot_table(df.loc[],
    #                      values='value',
    #                      index=['stock', 'asOfDate'],
    #                      columns=['item'],
    #                      aggfunc='sum')
    # print(pvt.reset_index(inplace=True))
    # print(pvt.head())
    # print(pvt.shape)
    
    # obj.write_to_db_parallel(df)