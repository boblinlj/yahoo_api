import numpy as np

option_mapping = {
        'yahoostats':{'extract_object': 'YahooStats',
                      'parse_object':'ParseYahooStats',
                      'description':'Download Yahoo Statistics',
                      'multi_table':False,
                      'table':'yahoo_fundamentals',
                      'table_data_type_for_pd':None,
                      'population':'sec_ticker_2023-12-22.txt'},
        
        'yahoofs':{'extract_object': 'YahooFS', 
                   'parse_object':'ParseYahooFs',
                   'description':'Download Yahoo Financial Statements',
                   'multi_table':False,
                   'table':'yahoo_financial_statements',
                   'table_data_type_for_pd':None,
                   'population':'sec_ticker_2023-12-22.txt'},
        
        'yahoosc':{'extract_object': 'YahooScreener', 
                   'parse_object':'ParseYahooSc',
                   'description':'Download Yahoo Financial Screeners',
                   'multi_table':True,
                   'table':{
                                'most_shorted_stocks':'yahoo_most_shorted_stocks',
                                'top_etfs_us':'yahoo_top_etfs_us',
                                'most_actives':'yahoo_most_actives'
                            },
                   'table_data_type_for_pd':None,
                   'population':'screener_2023-12-25.txt'},
        
        'yahoosp':{'extract_object': 'YahooSp', 
                   'parse_object':'ParseYahooSp',
                   'description':'Download Yahoo stock profile',
                   'multi_table':False,
                   'table':'yahoo_stock_profile',
                   'table_data_type_for_pd':None,
                   'population':'sec_ticker_2023-12-22.txt'},
        
        'yahooop':{'extract_object': 'YahooOp', 
                   'parse_object':'ParseYahooOp',
                   'description':'Download Yahoo Options',
                   'multi_table':False,
                   'table':'yahoo_options',
                   'table_data_type_for_pd':None,
                   'population':'sec_ticker_2023-12-22.txt'},
        
        'yahoopr':{'extract_object': 'YahooPr', 
                   'parse_object':'ParseYahooPr',
                   'description':'Download Yahoo prices',
                   'multi_table':False,
                   'table':'yahoo_prices',
                   'table_data_type_for_pd':{"dateUnix": np.float64,
                                                "high": np.float64, 
                                                "low":np.float64,
                                                "open":np.float64,
                                                "close":np.float64,
                                                "volume":np.float64,
                                                "yh_adjclose":np.float64,
                                                "dividend":np.float64,
                                                "denominator":np.float64,
                                                "numerator":np.float64,
                                                "splitRatio":str,
                                                "stock":str,
                                                "date":str,
                                                "updated_dt":str
                                                },
                   'population':'sec_ticker_2023-12-22.txt'},
    }