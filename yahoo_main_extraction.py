from read_stock_population import get_population
from base_class import get_yahoo_cookies, get_yahoo_crumb
from get_yahoo_api import YahooStats, YahooFS, YahooScreener, YahooSp, YahooOp
from job_mapping import option_mapping
from datetime import date
from util import parallel_process
from logger import get_logger
from typing import Union
import glob

logger = get_logger()

# --------------------------------entrance------------------------
def main(options: Union[str,list], run_date = date.today(), n_work=30) -> None:
   
    # calling function
    def _each_stock_run(run_class, ticker, crumb, cookies, run_date) -> None:
        run_class(ticker, run_date, crumb, cookies).load_to_staging() 
    
    if type(options) != list:options = [options]
    
    logger.info(f"----------Start {','.join(options)} jobs for {run_date}----------")
    
    logger.info(f"----------Getting Yahoo Cookie----------")
    cookies = get_yahoo_cookies()
    logger.info(f"----------Getting Yahoo crumb----------")
    try:
        crumb = get_yahoo_crumb(cookies)
    except Exception as err:
        logger.debug(err)
    
    for each_option in options:
        logger.info(f"Start to process {each_option}")
        arr = [{'run_class':eval(option_mapping[each_option]['extract_object']),
                'ticker':i, 
                'run_date':run_date, 
                'crumb':crumb,
                'cookies':cookies} for i in get_population(option_mapping[each_option]['population'])[:]]
        # parallel run with 30 jobs at a time
        parallel_process(arr, _each_stock_run, use_kwargs=True, n_jobs=n_work, use_tqdm=True)

        # check number of files entered
        file_pattern = f'staging/{run_date}/yahoo_{each_option}_*_{run_date}.txt'
        number_of_file = len(glob.glob(file_pattern))
        logger.info(f"There are {number_of_file} files entered.")
        
if __name__ == '__main__':
    import sys
    logger.info(f"Options are: {sys.argv[1:]}")
    options = sys.argv[1]
    run_date = sys.argv[2]
    n_work = sys.argv[3]

    main(options=options, run_date=run_date, n_work = int(n_work))
