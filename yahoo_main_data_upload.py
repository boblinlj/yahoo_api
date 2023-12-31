from _database_upload import WriteToDB
from datetime import date
from typing import Union
from logger import get_logger
from job_mapping import option_mapping

logger = get_logger()

def main(options: Union[str,list], run_date = date.today().strftime('%Y-%m-%d')):
    
    if type(options) != list:options = [options]
    
    logger.info(f"Start database uplading jobs for {','.join(options)} on {run_date}")
    
    for each_option in options:
        if option_mapping[each_option]['multi_table'] == True:
            for each_table in option_mapping[each_option]['table']:
                logger.info(f"Uplading {each_table}...")
                obj = WriteToDB(f"{each_option}_{each_table.replace('_','')}_{run_date}.csv",
                                table=f"yahoo_{each_table}",
                                chunk_size=1_000)
                df = obj.create_dataframe()
                obj.write_to_db_parallel(df)
        else:
            each_table = option_mapping[each_option]['table']
            logger.info(f"Uplading {each_table}...")
            obj = WriteToDB(f'{each_option}_{run_date}.csv',
                            table=f"yahoo_{each_table}",
                            chunk_size=1_000)
            df = obj.create_dataframe()
            obj.write_to_db_parallel(df)

    
if __name__ == '__main__':
    import sys
    logger.info(f"Options are: {sys.argv[1:]}")
    options = sys.argv[1]
    run_date = sys.argv[2]
    
    main(options=options, run_date=run_date)