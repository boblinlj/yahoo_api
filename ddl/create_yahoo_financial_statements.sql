CREATE TABLE `yahoo_financial_statements` (
   `data_id` bigint(20) NOT NULL AUTO_INCREMENT,
   `stock` varchar(45) NOT NULL,
   `item` varchar(250) NOT NULL,
   `currencyCode` varchar(45) DEFAULT NULL,
   `asOfDate` date DEFAULT NULL,
   `periodType` varchar(3) DEFAULT NULL,
   `value` double DEFAULT NULL,
   `updated_dt` date DEFAULT NULL,
   PRIMARY KEY (`data_id`,`stock`,`item`),
   UNIQUE KEY `unique` (`stock`,`asOfDate`,`item`),
   KEY `idx_yahoo_financial_statements_stock` (`stock`),
   KEY `idx_yahoo_financial_statements_item` (`item`),
   KEY `idx_yahoo_financial_statements_asOfDate` (`asOfDate`),
   KEY `idx_yahoo_financial_statements_periodType` (`periodType`),
   KEY `idx_yahoo_financial_statements_updated_dt` (`updated_dt`)
 ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/AAPL?'