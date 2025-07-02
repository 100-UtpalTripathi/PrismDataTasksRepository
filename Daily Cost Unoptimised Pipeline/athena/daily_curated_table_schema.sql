CREATE EXTERNAL TABLE `tb_daily_cost_summary` (
  `bill_payeraccountid`         STRING,
  `bill_billingentity`          STRING,
  `lineitem_usageaccountid`     STRING,
  `lineitem_productcode`        STRING,
  `lineitem_lineitemdescription` STRING,
  `product_productname`         STRING,
  `discount_applied`            STRING,
  `list_price`                  DOUBLE,
  `lineitem_unblendedcost`      DOUBLE,
  `lineitem_blendedcost`        DOUBLE,
  `effective_cost`              DOUBLE,
  `savings`                     DOUBLE,
  `lineitem_usagedate`          DATE,        
  `week_end_date`                   DATE         
)
PARTITIONED BY (
  `yearmonth` STRING,
  `modelid`   STRING
)
STORED AS PARQUET
LOCATION 's3://prism-curated-dev/tb_daily_cost_summary/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY'
);
