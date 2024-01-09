from _database_upload import WriteToDB
from datetime import date
from logger import get_logger
from job_mapping import option_mapping

logger = get_logger()

def main(options: str, insert_size = 500, run_date = date.today().strftime('%Y-%m-%d')):
    
    option_lst = options.split("+")
    
    logger.info(f"Start database uplading jobs for {','.join(options)} on {run_date}")
    
    for each_option in option_lst:
        if each_option not in option_mapping.keys():
            logger.debug(f"Wrong option ({each_option}) used")
            return None

        if option_mapping[each_option]['multi_table'] == True:
            for each_table in option_mapping[each_option]['table']:
                logger.info(f"Uplading {each_table}...")
                obj = WriteToDB(f"{each_option}_{each_table.replace('_','')}_{run_date}.csv",
                                table=f"yahoo_{each_table}",
                                insert_size=insert_size)
                obj.run()
        else:
            each_table = option_mapping[each_option]['table']
            logger.info(f"Uplading {each_table}...")
            obj = WriteToDB(f'{each_option}_{run_date}.csv',
                            table=f"{each_table}",
                            insert_size=insert_size)
            obj.run()

    
if __name__ == '__main__':
    import sys
    logger.info(f"Options are: {sys.argv[1:]}")
    options = sys.argv[1]
    run_date = sys.argv[2]
    
    main(options=options, insert_size= 500, run_date=run_date)