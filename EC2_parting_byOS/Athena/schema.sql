CREATE EXTERNAL TABLE `tb_monthly_cost_summary`(
  `bill_payeraccountid` string, 
  `bill_billingentity` string, 
  `lineitem_usageaccountid` string, 
  `lineitem_productcode` string, 
  `lineitem_lineitemdescription` string, 
  `lineitem_lineitemtype` string, 
  `product_productname` string, 
  `discount_applied` string, 
  `cost_allocation` string, 
  `sum_lineitem_unblendedcost` float, 
  `sum_lineitem_blendedcost` float, 
  `sum_list_price` double, 
  `sum_effective_cost` double, 
  `sum_savings` double,
  `product_operatingsystem` string
)
PARTITIONED BY ( 
  `yearmonth` string,
  `modelid` string)
STORED AS PARQUET
LOCATION
  's3://prism-curated-dev/tb_monthly_cost_summary'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY'
);