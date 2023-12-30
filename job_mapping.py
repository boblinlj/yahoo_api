option_mapping = {
        'yahoostats':{'extract_object': 'YahooStats',
                      'parse_object':'ParseYahooStats',
                      'description':'Download Yahoo Statistics',
                      'multi_table':False,
                      'table':'yahoo_fundamentals',
                      'population':'sec_ticker_2023-12-22.txt'},
        
        'yahoofs':{'extract_object': 'YahooFS', 
                   'parse_object':'ParseYahooFs',
                   'description':'Download Yahoo Financial Statements',
                   'multi_table':False,
                   'table':'yahoo_financial_statements',
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
                   'population':'screener_2023-12-25.txt'},
        
        'yahoosp':{'extract_object': 'YahooSp', 
                   'parse_object':'ParseYahooSp',
                   'description':'Download Yahoo stock profile',
                   'multi_table':False,
                   'table':'yahoo_stock_profile',
                   'population':'sec_ticker_2023-12-22.txt'},
        
        'yahooop':{'extract_object': 'YahooOp', 
                   'parse_object':'ParseYahooOp',
                   'description':'Download Yahoo Options',
                   'table':'yahoo_options',
                   'population':'sec_ticker_2023-12-22.txt'},
    }