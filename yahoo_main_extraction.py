from read_stock_population import get_population
from base_class import get_yahoo_cookies, get_yahoo_crumb
from get_yahoo_api import YahooStats, YahooFS, YahooScreener, YahooSp
from job_mapping import option_mapping
from datetime import date
from util import parallel_process
from logger import get_logger
from typing import Union

logger = get_logger()

# --------------------------------entrance------------------------
def main(options: Union[str,list], run_date = date.today().strftime('%Y-%m-%d')) -> None:
   
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
                'cookies':cookies} for i in get_population(option_mapping[each_option]['population'])]
        # parallel run with 30 jobs at a time
        parallel_process(arr, _each_stock_run, use_kwargs=True, n_jobs=30, use_tqdm=True)
        
if __name__ == '__main__':
    import sys
    logger.info(f"Options are: {sys.argv[1:]}")
    main(sys.argv[1:])
