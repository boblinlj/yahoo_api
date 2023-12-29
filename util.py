from tqdm import tqdm
import concurrent.futures
import math
import datetime
import numpy as np
from time import time
from logger import get_logger

logger =  get_logger()


def parallel_process(array, function, n_jobs=30, use_kwargs=False, front_num=3, use_tqdm=True):
    """
        A parallel version of the map function with a progress bar.

        Args:
            array (array-like): An array to iterate over.
            function (function): A python function to apply to the elements of array
            n_jobs (int, default=16): The number of cores to use
            use_kwargs (boolean, default=False): Whether to consider the elements of array as dictionaries of
                keyword arguments to function
            front_num (int, default=3): The number of iterations to run serially before kicking off the parallel job.
                Useful for catching bugs
        Returns:
            [function(array[0]), function(array[1]), ...]
            :param use_tqdm:
    """
    # We run the first few iterations serially to catch bugs
    front = []
    if front_num > 0:
        front = [function(**a) if use_kwargs else function(a) for a in array[:front_num]]
    # If we set n_jobs to 1, just run a list comprehension. This is useful for benchmarking and debugging.
    if n_jobs == 1:
        if use_tqdm:
            return front + [function(**a) if use_kwargs else function(a) for a in tqdm(array[front_num:])]
        else:
            return front + [function(**a) if use_kwargs else function(a) for a in array[front_num:]]
    # Assemble the workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs) as pool:
    # with concurrent.futures.ProcessPoolExecutor(max_workers=n_jobs) as pool:
        # Pass the elements of array into function
        if use_kwargs:
            futures = [pool.submit(function, **a) for a in array[front_num:]]
            # futures = {pool.submit(function, **a) for a in array[front_num:]}
        else:
            # futures = [pool.submit(function, a) for a in array[front_num:]]
            futures = {pool.submit(function, a): a for a in array[front_num:]}

        # print(futures)
        kwargs = {
            'total': len(futures),
            'unit': 'it',
            'unit_scale': True,
            'leave': True,
            'ncols': 80
        }
        # Print out the progress as tasks complete
        if use_tqdm:
            for f in tqdm(concurrent.futures.as_completed(futures), **kwargs):
                pass
        else:
            for f in concurrent.futures.as_completed(futures):
                pass
    out = []
    # Get the results from the futures.
    if use_tqdm:
        for i, future in tqdm(enumerate(futures)):
            try:
                out.append(future.result())
            except Exception as e:
                out.append(e)
        return front + out
    else:
        for i, future in enumerate(futures):
            try:
                out.append(future.result())
            except Exception as e:
                out.append(e)
        return front + out


def regular_time_to_unix(date):
    if date is None:
        return np.nan
    else:
        return int((date - datetime.date(1970, 1, 1)).total_seconds())


def unix_to_regular_time(unix):
    if unix is None:
        return np.nan
    elif math.isnan(unix):
        return np.nan
    elif isinstance(unix, np.int64):
        return np.nan
    else:
        datetime_value = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=(unix))
        
        return datetime_value.strftime('%Y-%m-%d')

def unix_milliseconds_to_regular_time(unix):
    if unix is None:
        return np.nan
    elif math.isnan(unix):
        return np.nan
    else:
        datetime_value = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=(unix/1000))
        
        return datetime_value.strftime('%Y-%m-%d')
    
def timer_func(func):
    # This function shows the execution time of
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        logger.info(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func

def split_dataframe(df, chunk_size = 10_000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks
