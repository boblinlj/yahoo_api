from _process_json_files import ParseManyJSON
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
        if option_mapping[each_option]['multi_table'] == True:
            for each_table in option_mapping[each_option]['table']:
                logger.info(f"Start to process {each_option}: {each_table}")
                file_pattern = f'yahoo_{each_option}_{each_table.replace("_","")}_{run_date}.txt'
                main_josn_procsser(file_pattern, 'csv')
        else:
            logger.info(f"Start to process {each_option}...")
            file_pattern = f'yahoo_{each_option}_*_{run_date}.txt'
            main_josn_procsser(file_pattern, 'csv')


if __name__ == '__main__':
    import sys
    logger.info(f"Options are: {sys.argv[1:]}")
    options = sys.argv[1]
    run_date = sys.argv[2]
    
    if run_date == 'today':
        run_date = date.today()
    
    main(options=options, run_date=run_date)