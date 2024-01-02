from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import text
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
    
    @property
    def check_entries(self) -> None:
        sql = f"""
                    SELECT count(1) as no_of_entries
                    FROM financial_data.{self.table} 
                    where updated_dt = '{self.data_date}'
                """
        df = pd.read_sql(con=self.cnn,sql = sql)
        no_of_entries = df['no_of_entries'][0]
        
        return no_of_entries
    
    def check_if_table_exist(self) -> bool:
        sql = f"""
                show tables like '{self.table}'
            """
        df = pd.read_sql(con=self.cnn,sql = sql)
        logger.info(f"Check table={self.table} {'passed' if df.empty==False else 'failed'}")
        if df.empty:
            return False
        else:
            return True
    
    def create_table(self) -> bool:
        ddl_file = f'create_{self.table}.sql'
        with self.cnn.connect() as c:
            path = os.path.join('ddl',ddl_file)
            if not os.path.exists(path):
                logger.info(f"New table:{self.table} cannot be created due to missing sql file in the ddl folder")
                return False
            else:
                with open(path) as f:
                    sql = text(f.read())
                    c.execute(sql)
                    logger.info(f"New table:{self.table} is created!")
                return True
        
    def run(self) -> None:
        if not self.check_if_table_exist():
            if self.create_table():
                df = self.create_dataframe()
                self.write_to_db_parallel(df)
                logger.info(f"{self.check_entries} lines of data hve been entered to {self.table}")
            else:
                logger.debug('Data upload failed')
        else:
            df = self.create_dataframe()
            self.write_to_db_parallel(df)
            logger.info(f"{self.check_entries} lines of data hve been entered to {self.table}")
        
if __name__ == "__main__":
    obj = WriteToDB('yahooop_2023-12-29.csv',
                    table='abc',
                    chunk_size=1_000)
    df = obj.create_table()