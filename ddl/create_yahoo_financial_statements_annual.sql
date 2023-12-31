CREATE TABLE `yahoo_financial_statements_annual` (
   `data_id` bigint(20) NOT NULL AUTO_INCREMENT,
   `stock` varchar(20) NOT NULL,
   `asOfDate` date NOT NULL,
   `annualAccountsPayable` bigint(20) DEFAULT NULL,
   `annualAccountsReceivable` bigint(20) DEFAULT NULL,
   `annualAccumulatedDepreciation` bigint(20) DEFAULT NULL,
   `annualBasicAverageShares` bigint(20) DEFAULT NULL,
   `annualBasicEPS` double DEFAULT NULL,
   `annualBeginningCashPosition` bigint(20) DEFAULT NULL,
   `annualCapitalExpenditure` bigint(20) DEFAULT NULL,
   `annualCapitalStock` bigint(20) DEFAULT NULL,
   `annualCashAndCashEquivalents` bigint(20) DEFAULT NULL,
   `annualCashCashEquivalentsAndShortTermInvestments` bigint(20) DEFAULT NULL,
   `annualCashDividendsPaid` bigint(20) DEFAULT NULL,
   `annualCashFlowFromContinuingFinancingActivities` bigint(20) DEFAULT NULL,
   `annualChangeInAccountPayable` bigint(20) DEFAULT NULL,
   `annualChangeInCashSupplementalAsReported` bigint(20) DEFAULT NULL,
   `annualChangeInInventory` bigint(20) DEFAULT NULL,
   `annualChangeInWorkingCapital` bigint(20) DEFAULT NULL,
   `annualChangesInAccountReceivables` bigint(20) DEFAULT NULL,
   `annualCommonStockIssuance` bigint(20) DEFAULT NULL,
   `annualCostOfRevenue` bigint(20) DEFAULT NULL,
   `annualCurrentAccruedExpenses` bigint(20) DEFAULT NULL,
   `annualCurrentAssets` bigint(20) DEFAULT NULL,
   `annualCurrentDebt` bigint(20) DEFAULT NULL,
   `annualCurrentDeferredRevenue` bigint(20) DEFAULT NULL,
   `annualCurrentLiabilities` bigint(20) DEFAULT NULL,
   `annualDeferredIncomeTax` bigint(20) DEFAULT NULL,
   `annualDepreciationAndAmortization` bigint(20) DEFAULT NULL,
   `annualDilutedAverageShares` bigint(20) DEFAULT NULL,
   `annualDilutedEPS` double DEFAULT NULL,
   `annualEndCashPosition` bigint(20) DEFAULT NULL,
   `annualFreeCashFlow` bigint(20) DEFAULT NULL,
   `annualGainsLossesNotAffectingRetainedEarnings` bigint(20) DEFAULT NULL,
   `annualGoodwill` bigint(20) DEFAULT NULL,
   `annualGrossPPE` bigint(20) DEFAULT NULL,
   `annualGrossProfit` bigint(20) DEFAULT NULL,
   `annualIncomeTaxPayable` bigint(20) DEFAULT NULL,
   `annualInterestExpense` bigint(20) DEFAULT NULL,
   `annualInventory` bigint(20) DEFAULT NULL,
   `annualInvestingCashFlow` bigint(20) DEFAULT NULL,
   `annualInvestmentsAndAdvances` bigint(20) DEFAULT NULL,
   `annualLongTermDebt` bigint(20) DEFAULT NULL,
   `annualNetIncome` bigint(20) DEFAULT NULL,
   `annualNetIncomeCommonStockholders` bigint(20) DEFAULT NULL,
   `annualNetIncomeContinuousOperations` bigint(20) DEFAULT NULL,
   `annualNetOtherFinancingCharges` bigint(20) DEFAULT NULL,
   `annualNetOtherInvestingChanges` bigint(20) DEFAULT NULL,
   `annualNetPPE` bigint(20) DEFAULT NULL,
   `annualNonCurrentDeferredRevenue` bigint(20) DEFAULT NULL,
   `annualNonCurrentDeferredTaxesLiabilities` bigint(20) DEFAULT NULL,
   `annualOperatingCashFlow` bigint(20) DEFAULT NULL,
   `annualOperatingExpense` bigint(20) DEFAULT NULL,
   `annualOperatingIncome` bigint(20) DEFAULT NULL,
   `annualOtherCurrentAssets` bigint(20) DEFAULT NULL,
   `annualOtherCurrentLiabilities` bigint(20) DEFAULT NULL,
   `annualOtherIncomeExpense` bigint(20) DEFAULT NULL,
   `annualOtherIntangibleAssets` bigint(20) DEFAULT NULL,
   `annualOtherNonCashItems` bigint(20) DEFAULT NULL,
   `annualOtherNonCurrentAssets` bigint(20) DEFAULT NULL,
   `annualOtherNonCurrentLiabilities` bigint(20) DEFAULT NULL,
   `annualOtherShortTermInvestments` bigint(20) DEFAULT NULL,
   `annualPretaxIncome` bigint(20) DEFAULT NULL,
   `annualPurchaseOfBusiness` bigint(20) DEFAULT NULL,
   `annualPurchaseOfInvestment` bigint(20) DEFAULT NULL,
   `annualRepaymentOfDebt` bigint(20) DEFAULT NULL,
   `annualRepurchaseOfCapitalStock` bigint(20) DEFAULT NULL,
   `annualResearchAndDevelopment` bigint(20) DEFAULT NULL,
   `annualRetainedEarnings` bigint(20) DEFAULT NULL,
   `annualSaleOfInvestment` bigint(20) DEFAULT NULL,
   `annualSellingGeneralAndAdministration` bigint(20) DEFAULT NULL,
   `annualStockBasedCompensation` bigint(20) DEFAULT NULL,
   `annualStockholdersEquity` bigint(20) DEFAULT NULL,
   `annualTaxProvision` bigint(20) DEFAULT NULL,
   `annualTotalAssets` bigint(20) DEFAULT NULL,
   `annualTotalLiabilitiesNetMinorityInterest` bigint(20) DEFAULT NULL,
   `annualTotalNonCurrentAssets` bigint(20) DEFAULT NULL,
   `annualTotalNonCurrentLiabilitiesNetMinorityInterest` bigint(20) DEFAULT NULL,
   `annualTotalRevenue` bigint(20) DEFAULT NULL,
   PRIMARY KEY (`data_id`,`stock`,`asOfDate`),
   UNIQUE KEY `unique` (`stock`,`asOfDate`),
   KEY `idx_yahoo_financial_statements_annual_stock` (`stock`),
   KEY `idx_yahoo_financial_statements_annual_asOfDate` (`asOfDate`)
 ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8