from sqlalchemy import create_engine
from sqlalchemy import exc
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
        except exc.IntegrityError:
            # each teble is created with a unique index
            # do nothing if found duplicates
            pass
        except Exception as err:
            print(err)
    
    @timer_func
    def write_to_db_parallel(self, dataframe) -> None:
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