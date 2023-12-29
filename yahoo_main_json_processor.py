from process_json_files import ParseManyJSON
from database_upload import WriteToDB
from logger import get_logger
from datetime import date
from typing import Union
from job_mapping import option_mapping

logger = get_logger()

def main_josn_procsser(pattern, save_as, delete_after_read=False) -> None:
    logger.info(f"Start to process files like {pattern}, which will be saved as {save_as} in the final folder")
    obj = ParseManyJSON(pattern)
    obj.run(save_as)
    if delete_after_read: obj.delete_staging_file()

# --------------------------------entrance------------------------
def main(options: Union[str,list], run_date = date.today().strftime('%Y-%m-%d')) -> None:
    
    if type(options) != list:options = [options]
    
    logger.info(f"Start {','.join(options)} jobs for {run_date}")
    
    for each_option in options:
        if type(option_mapping[each_option]['table']) != dict:
            # parse JSON to create a csv in the final folder
            logger.info(f"Start to process {each_option}")
            file_pattern = f'yahoo_{each_option}_*_{run_date}.txt'
            main_josn_procsser(file_pattern, 'csv')
            # upload csv to database
            obj = WriteToDB(f'{each_option}_{run_date}.csv',
                            table=option_mapping[each_option]['table'],
                            chunk_size=1_000)
            if obj.error == 0:
                df = obj.create_dataframe()
                obj.write_to_db_parallel(df)
        else:
            for each_table in option_mapping[each_option]['table']:
                logger.info(f"Start to process {each_option}: {each_table}")
                file_pattern = f"yahoo_{each_option}_{each_table.replace('_', '').lower()}_{run_date}.txt"
                main_josn_procsser(file_pattern, 'csv')
                
                obj = WriteToDB(f"{each_option}_{each_table.replace('_', '').lower()}_{run_date}.csv",
                                table=option_mapping[each_option]['table'][each_table],
                                chunk_size=1_000)
                df = obj.create_dataframe()
                obj.write_to_db_parallel(df)


if __name__ == '__main__':
    # main(['yahoosc'], run_date='2023-12-27')
    # main(['yahoofs'], run_date='2023-12-26')
    # main(['yahoostats'], run_date='2023-12-27')
    
    import sys
    logger.info(f"Options are: {sys.argv[1:]}")
    main(sys.argv[1:])