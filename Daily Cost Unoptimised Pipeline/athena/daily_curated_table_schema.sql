CREATE EXTERNAL TABLE `tb_cost_history_summary` (
  `bill_payeraccountid`         STRING,
  `bill_billingentity`          STRING,
  `lineitem_usageaccountid`     STRING,
  `lineitem_productcode`        STRING,
  `lineitem_lineitemdescription` STRING,
  `lineitem_lineitemtype`       STRING,
  `product_productname`         STRING,
  `discount_applied`            STRING,
  `sum_list_price`                  DOUBLE,
  `sum_lineitem_unblendedcost`      DOUBLE,
  `sum_lineitem_blendedcost`        DOUBLE,
  `sum_effective_cost`              DOUBLE,
  `sum_savings`                     DOUBLE,
  `lineitem_usagedate`          DATE,        
  `week_end_date`                   DATE         
)
PARTITIONED BY (
  `yearmonth` STRING,
  `modelid`   STRING
)
STORED AS PARQUET
LOCATION 's3://prism-curated-dev/tb_cost_history_summary/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY'
);


