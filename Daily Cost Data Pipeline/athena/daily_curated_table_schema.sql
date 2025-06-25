CREATE EXTERNAL TABLE `tb_daily_cost_summary`(
  `bill_payeraccountid` string, 
  `bill_billingentity` string, 
  `lineitem_usageaccountid` string, 
  `lineitem_productcode` string, 
  `lineitem_lineitemdescription` string, 
  `product_productname` string, 
  `discount_applied` string, 
  `list_price` double,
  `lineitem_unblendedcost` double,
  `lineitem_blendedcost` double,
  `effective_cost` double,
  `savings` double,
  `coverage_type` string
)
PARTITIONED BY ( 
  `yearmonth` string, 
  `lineitem_usagedate` date, 
  `modelid` string)
  
STORED AS PARQUET
LOCATION
  's3://prism-curated-dev/tb_daily_cost_summary/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY');



