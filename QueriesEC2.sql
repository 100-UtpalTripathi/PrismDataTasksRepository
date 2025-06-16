CREATE TABLE "prism-raw"."ec2_dashboard_dataset"
WITH (
    format = 'PARQUET',
    external_location = 's3://ec2-dashboard-data-tripathi/team-insights-data/cur-subsets/ec2-dashboard/',
    partitioned_by = ARRAY['yearmonth', 'modelid']
) AS
SELECT
    -- Time & Partitioning
    lineitem_usagestartdate,
    lineitem_usageenddate,

    -- Account Information
    lineitem_usageaccountid,
    bill_payeraccountid,
    bill_billingentity,

    -- Product Info
    product_productname,
    product_region,
    product_instancetype,
    product_instancefamily,
    product_operatingsystem,
    product_tenancy,
    product_marketoption,
    product_capacitystatus,
    product_usagefamily,

    -- Usage Attributes
    lineitem_lineitemtype,
    lineitem_operation,
    lineitem_usagetype,
    lineitem_availabilityzone,
    lineitem_lineitemdescription,
    lineitem_productcode,

    -- Coverage Classification Support
    reservation_reservationarn,
    pricing_offeringclass,
    pricing_term,
    pricing_leasecontractlength,
    pricing_purchaseoption,
    savingsplan_savingsplanarn,
    savingsplan_offeringtype,

    -- Cost Metrics
    lineitem_unblendedcost,
    lineitem_blendedcost,
    lineitem_unblendedrate,
    pricing_publicondemandcost,
    savingsplan_savingsplanrate,
    savingsplan_savingsplaneffectivecost,
    reservation_effectivecost,
    reservation_upfrontvalue,
    reservation_recurringfeeforusage,
    reservation_amortizedupfrontcostforusage,
    reservation_netrecurringfeeforusage,
    list_price,
    effective_cost,

    -- Normalized Usage
    lineitem_usageamount,
    lineitem_normalizedusageamount,

    -- Tags
    resourcetags,
    yearmonth,
    modelid

FROM "prism-raw"."etl_output"

WHERE 
    yearmonth LIKE '2025%'
    AND modelid IN (
        '0532721b-5c2d-43dc-9e1b-dc6f047e59ab', 
        'bfb69248-c991-463f-824f-0317df6e57b5'
    )
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS';








-- To detect spot coverage type: 
SELECT 
  yearmonth,
  modelid,
  product_productname,
  product_region,
  lineitem_lineitemtype,
  lineitem_operation,
  lineitem_usagetype,
  reservation_reservationarn,
  savingsplan_savingsplanarn,
  lineitem_usageaccountid,
  lineitem_resourceid,
  lineitem_blendedcost,
  lineitem_unblendedcost,
  lineitem_usageamount
FROM "prism-raw"."etl_output"
WHERE lineitem_usagetype LIKE '%Spot%'
  AND bill_billingentity = 'AWS'
  AND yearmonth LIKE '202501%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
LIMIT 100;




-- To detect On demand coverage type: 
SELECT 
  yearmonth,
  modelid,
  product_productname,
  product_region,
  lineitem_lineitemtype,
  lineitem_operation,
  lineitem_usagetype,
  reservation_reservationarn,
  savingsplan_savingsplanarn,
  lineitem_usageaccountid,
  lineitem_resourceid,
  lineitem_blendedcost,
  lineitem_unblendedcost,
  lineitem_usageamount
FROM "prism-raw"."etl_output"
WHERE (savingsplan_savingsplanarn IS NULL OR savingsplan_savingsplanarn = '')
  AND (reservation_reservationarn IS NULL OR reservation_reservationarn = '')
  AND lineitem_usagetype NOT LIKE '%Spot%'
  AND lineitem_lineitemtype = 'Usage'
  AND lineitem_usagetype LIKE '%BoxUsage%'
  AND bill_billingentity = 'AWS'
  AND yearmonth LIKE '202501%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
LIMIT 100;


-- To detect Standard RI coverage type: 
SELECT 
  yearmonth,
  modelid,
  product_productname,
  product_region,
  lineitem_lineitemtype,
  lineitem_operation,
  lineitem_usagetype,
  reservation_reservationarn,
  savingsplan_savingsplanarn,
  lineitem_usageaccountid,
  lineitem_resourceid,
  lineitem_blendedcost,
  lineitem_unblendedcost,
  lineitem_usageamount
FROM "prism-raw"."etl_output"
WHERE lineitem_lineitemtype = 'DiscountedUsage'
  AND reservation_reservationarn IS NOT NULL AND reservation_reservationarn <> ''
  AND pricing_offeringclass = 'standard'
  AND bill_billingentity = 'AWS'
  AND yearmonth LIKE '202501%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
LIMIT 100;


-- To detect Convertible RI coverage type: 
SELECT 
  yearmonth,
  modelid,
  product_productname,
  product_region,
  lineitem_lineitemtype,
  lineitem_operation,
  lineitem_usagetype,
  reservation_reservationarn,
  savingsplan_savingsplanarn,
  lineitem_usageaccountid,
  lineitem_resourceid,
  lineitem_blendedcost,
  lineitem_unblendedcost,
  lineitem_usageamount
FROM "prism-raw"."etl_output"
WHERE lineitem_lineitemtype = 'DiscountedUsage'
  AND reservation_reservationarn IS NOT NULL AND reservation_reservationarn <> ''
  AND pricing_offeringclass = 'convertible'
  AND bill_billingentity = 'AWS'
  AND yearmonth LIKE '202501%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
LIMIT 100;


-- To detect Compute Savings plan coverage type:
SELECT 
  yearmonth,
  modelid,
  product_productname,
  product_region,
  lineitem_lineitemtype,
  lineitem_operation,
  lineitem_usagetype,
  reservation_reservationarn,
  savingsplan_savingsplanarn,
  savingsplan_offeringtype,
  lineitem_usageaccountid,
  lineitem_resourceid,
  lineitem_blendedcost,
  lineitem_unblendedcost,
  lineitem_usageamount
FROM "prism-raw"."etl_output"
WHERE savingsplan_savingsplanarn IS NOT NULL 
  AND savingsplan_savingsplanarn <> ''
  AND savingsplan_offeringtype = 'ComputeSavingsPlans'
  AND lineitem_lineitemtype LIKE 'SavingsPlan%'
  AND bill_billingentity = 'AWS'
  AND yearmonth LIKE '202501%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
LIMIT 100;



SELECT
  modelid,
  DATE(lineitem_usagestartdate) AS usage_day,

  -- EC2 Coverage Type Classification logic
  CASE
    WHEN savingsplan_savingsplanarn IS NOT NULL AND savingsplan_savingsplanarn <> '' THEN
      CASE
        WHEN savingsplan_offeringtype = 'ComputeSavingsPlans' THEN 'Compute Savings Plan'
        WHEN savingsplan_offeringtype = 'EC2InstanceSavingsPlans' THEN 'EC2 Savings Plan'
        ELSE 'Savings Plan'
      END

    WHEN reservation_reservationarn IS NOT NULL AND reservation_reservationarn <> '' AND pricing_offeringclass = 'standard' THEN 'Standard RI'
    WHEN reservation_reservationarn IS NOT NULL AND reservation_reservationarn <> '' AND pricing_offeringclass = 'convertible' THEN 'Convertible RI'

    WHEN lineitem_usagetype LIKE '%SpotUsage%' THEN 'Spot'

    WHEN (
      (savingsplan_savingsplanarn IS NULL OR savingsplan_savingsplanarn = '')
      AND (reservation_reservationarn IS NULL OR reservation_reservationarn = '')
      AND lineitem_lineitemtype = 'Usage'
      AND lineitem_usagetype LIKE '%BoxUsage%'
      AND lineitem_usagetype NOT LIKE '%Spot%'
    ) THEN 'On Demand'

    ELSE 'Other EC2 costs'
  END AS ec2_coverage_type,

  -- Basic cost metrics only
  SUM(lineitem_blendedcost) AS total_blended_cost,
  SUM(lineitem_unblendedcost) AS total_unblended_cost

FROM "prism-raw"."ec2_dashboard_dataset"

WHERE 
  yearmonth LIKE '2025%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
  AND bill_billingentity = 'AWS'

GROUP BY 1, 2, 3
ORDER BY 1, 2;


-- Cost based on EC2 instancetypes

SELECT
  modelid,
  DATE(lineitem_usagestartdate) AS usage_day,
  product_instancetype,

  SUM(lineitem_blendedcost) AS total_blended_cost,
  SUM(lineitem_unblendedcost) AS total_unblended_cost

FROM "prism-raw"."ec2_dashboard_dataset"

WHERE 
  yearmonth LIKE '2025%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
  AND bill_billingentity = 'AWS'
  AND product_instancetype IS NOT NULL

GROUP BY 1, 2, 3
ORDER BY 1, 2;



-- Cost Based on availability_zone
SELECT
  modelid,
  DATE(lineitem_usagestartdate) AS usage_day,
  lineitem_availabilityzone,

  SUM(lineitem_blendedcost) AS total_blended_cost,
  SUM(lineitem_unblendedcost) AS total_unblended_cost

FROM "prism-raw"."ec2_dashboard_dataset"

WHERE 
  yearmonth LIKE '2025%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
  AND bill_billingentity = 'AWS'
  AND product_instancetype IS NOT NULL
  AND lineitem_availabilityzone IS NOT NULL

GROUP BY 1, 2, 3
ORDER BY 1, 2;


-- EC2 Daily Cost by Tenancy
SELECT
  modelid,
  DATE(lineitem_usagestartdate) AS usage_day,
  product_tenancy,

  SUM(lineitem_blendedcost) AS total_blended_cost,
  SUM(lineitem_unblendedcost) AS total_unblended_cost

FROM "prism-raw"."ec2_dashboard_dataset"

WHERE 
  yearmonth LIKE '2025%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
  AND bill_billingentity = 'AWS'
  AND product_instancetype IS NOT NULL
  AND product_tenancy IN ('Shared', 'Dedicated', 'Host')

GROUP BY 1, 2, 3
ORDER BY 1, 2;








-- UPDATED WORKING QUERY

SELECT bill_billingentity,
bill_payeraccountid,
lineitem_usageaccountid,
DATE (lineitem_usagestartdate) lineitem_usagedate,
CASE
    WHEN ("bill_billingentity" = 'AWS Marketplace' AND "lineitem_lineitemtype" NOT LIKE '%Discount%')
    THEN "product_productname"
    WHEN ("bill_billingentity" = 'AWS') THEN "lineitem_productcode"
    END                                            AS "Service",
-- CASE
--            WHEN (lineitem_usagetype LIKE '%SpotUsage%') THEN SPLIT_PART(lineitem_usagetype, ':', 2)
--            ELSE product_instancetype
--         END                                        AS case_product_instance_type,
product_instancetype, product_instancefamily,-- lineitem_usagetype,
CASE
           WHEN (savingsplan_savingsplanarn <> '') THEN 'SavingsPlan'
           WHEN (reservation_reservationarn <> '') THEN 'Reserved'
           WHEN (lineitem_usagetype LIKE '%Spot%') THEN 'Spot'
           ELSE 'OnDemand'
        END                                        AS                   case_purchase_option,
coalesce(pricing_offeringclass, 'N/A') AS RI_CoverageType,
coalesce(savingsplan_offeringtype, 'N/A') AS SP_CoverageType,
SUM(lineitem_usageamount)                          AS                   sum_line_item_usage_amount,
SUM(lineitem_unblendedcost)                        AS                   sum_line_item_unblended_cost,
SUM(CASE
               WHEN (lineitem_lineitemtype = 'SavingsPlanCoveredUsage') THEN savingsplan_savingsplaneffectivecost
               WHEN (lineitem_lineitemtype = 'SavingsPlanRecurringFee')
                   THEN (savingsplan_totalcommitmenttodate - savingsplan_usedcommitment)
               WHEN (lineitem_lineitemtype = 'SavingsPlanNegation') THEN 0
               WHEN (lineitem_lineitemtype = 'SavingsPlanUpfrontFee') THEN 0
               WHEN (lineitem_lineitemtype = 'DiscountedUsage') THEN reservation_effectivecost
               WHEN (lineitem_lineitemtype = 'RIFee') THEN (
                   reservation_unusedamortizedupfrontfeeforbillingperiod + reservation_unusedrecurringfee)
               WHEN ((lineitem_lineitemtype = 'Fee') AND (reservation_reservationarn <> '')) THEN 0
               ELSE lineitem_unblendedcost
           END)                                    AS                   amortized_cost,
(SUM(lineitem_unblendedcost)
   - SUM(CASE
             WHEN (lineitem_lineitemtype = 'SavingsPlanCoveredUsage')
                 THEN savingsplan_savingsplaneffectivecost
             WHEN (lineitem_lineitemtype = 'SavingsPlanRecurringFee')
                 THEN (savingsplan_totalcommitmenttodate - savingsplan_usedcommitment)
             WHEN (lineitem_lineitemtype = 'SavingsPlanNegation') THEN 0
             WHEN (lineitem_lineitemtype = 'SavingsPlanUpfrontFee') THEN 0
             WHEN (lineitem_lineitemtype = 'DiscountedUsage') THEN reservation_effectivecost
             WHEN (lineitem_lineitemtype = 'RIFee') THEN (
                 reservation_unusedamortizedupfrontfeeforbillingperiod + reservation_unusedrecurringfee)
             WHEN ((lineitem_lineitemtype = 'Fee') AND (reservation_reservationarn <> '')) THEN 0
             ELSE lineitem_unblendedcost
       END)
   )                                              AS                   ri_sp_trueup,
SUM(CASE
       WHEN (lineitem_lineitemtype = 'SavingsPlanUpfrontFee') THEN lineitem_unblendedcost
       WHEN ((lineitem_lineitemtype = 'Fee') AND (reservation_reservationarn <> ''))
           THEN lineitem_unblendedcost
       ELSE 0
   END)                                           AS                   ri_sp_upfront_fees
FROM "prism-raw".etl_output
WHERE yearmonth = '202504'
  and modelid in ('6886a459-b349-433f-bcc2-fdcf36583672', '2c563e06-888a-44d3-bad5-630c8458bf90',
                  '72a53c10-8c20-4ad4-8b0f-ecc0c0ae5181')
  AND lineitem_usagetype <> 'Route53-Domains'
  AND lineitem_lineitemtype IN
      ('DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage', 'SavingsPlanNegation', 'SavingsPlanRecurringFee',
       'SavingsPlanUpfrontFee', 'RIFee', 'Fee')
  AND cost_allocation = 'Customer'
  AND lineitem_productcode = 'AmazonEC2'
GROUP BY bill_billingentity, bill_payeraccountid,
         lineitem_usageaccountid,
         3, 4, 5, 6, 7, 8, 9, 10
ORDER BY bill_billingentity ASC,
         lineitem_usagedate ASC,
         bill_payeraccountid ASC,
         case_purchase_option,
         product_instancefamily,
         sum_line_item_unblended_cost DESC;


-- CTAS QUERY...
CREATE TABLE "prism-raw"."ec2_curated_int"
WITH (
    format = 'PARQUET',
    external_location = 's3://Enter_ARN_of_S3_bucket/',
    partitioned_by = ARRAY['yearmonth', 'modelid']
) AS
SELECT
    -- Time
    lineitem_usagestartdate,

    -- Account Info
    bill_billingentity,
    bill_payeraccountid,
    lineitem_usageaccountid,

    -- Product Info
    product_productname,
    lineitem_productcode,
    product_instancetype,
    product_instancefamily,

    -- Usage & Line Type
    lineitem_lineitemtype,
    lineitem_usagetype,

    -- Coverage Attributes
    reservation_reservationarn,
    pricing_offeringclass,
    savingsplan_savingsplanarn,
    savingsplan_offeringtype,

    -- Cost Metrics
    lineitem_unblendedcost,
    lineitem_usageamount,
    savingsplan_savingsplaneffectivecost,
    savingsplan_totalcommitmenttodate,
    savingsplan_usedcommitment,
    reservation_effectivecost,
    reservation_unusedamortizedupfrontfeeforbillingperiod,
    reservation_unusedrecurringfee,

    -- Partitioning
    yearmonth,
    modelid

FROM "prism-raw"."etl_output"
WHERE 
    lineitem_productcode = 'AmazonEC2'
    AND bill_billingentity IN ('AWS', 'AWS Marketplace')
    AND lineitem_usagetype <> 'Route53-Domains'
    AND cost_allocation = 'Customer'
    AND lineitem_lineitemtype IN (
        'DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage',
        'SavingsPlanNegation', 'SavingsPlanRecurringFee',
        'SavingsPlanUpfrontFee', 'RIFee', 'Fee'
    );






    // Restarting the query to ensure it works:

    // DDL for creating the curated table
-- DDL for creating the curated table
-- This table will summarize monthly EC2 costs with necessary partitions
-- and attributes for further analysis.

    CREATE EXTERNAL TABLE IF NOT EXISTS prism_curated_dev.tb_monthly_ec2_cost_summary(
  bill_billingentity                              string,
  bill_payeraccountid                             string,
  lineitem_usageaccountid                         string,

  lineitem_productcode                            string,
  product_productname                             string,
  product_instancetype                            string,
  product_instancefamily                          string,

  lineitem_lineitemtype                           string,
  lineitem_usagetype                              string,
  lineitem_operation                              string,

  reservation_reservationarn                      string,
  pricing_offeringclass                           string,
  pricing_term                                    string,
  pricing_purchaseoption                          string,
  savingsplan_savingsplanarn                      string,
  savingsplan_offeringtype                        string,
  savingsplan_totalcommitmenttodate               double,
  savingsplan_usedcommitment                      double,
  savingsplan_savingsplaneffectivecost            double,
  reservation_effectivecost                       double,
  reservation_unusedamortizedupfrontfeeforbillingperiod double,
  reservation_unusedrecurringfee                  double,

  lineitem_unblendedcost                          double,
  lineitem_blendedcost                            double,
  list_price                                      double,
  effective_cost                                  double,
  savings                                         double,

  lineitem_lineitemdescription                    string,
  cost_allocation                                 string
)
PARTITIONED BY (
  yearmonth string,
  modelid string
)
STORED AS PARQUET
LOCATION 's3://prism-curated-dev/tb_monthly_ec2_cost_summary/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY'
);



-- was getting Athena partition limit, so need to insert data in batches
-- prepared statement to insert data into the curated table by accepting yearmonth and list of modelids

PREPARE insert_monthly_ec2_summary
FROM
INSERT INTO prism_curated_dev.tb_monthly_ec2_cost_summary
SELECT
  -- Account Info
  bill_billingentity,
  bill_payeraccountid,
  lineitem_usageaccountid,

  -- Product Info
  lineitem_productcode,
  product_productname,
  product_instancetype,
  product_instancefamily,

  -- Line Item & Usage
  lineitem_lineitemtype,
  lineitem_usagetype,
  lineitem_operation,

  -- Coverage-related Fields
  reservation_reservationarn,
  pricing_offeringclass,
  pricing_term,
  pricing_purchaseoption,
  savingsplan_savingsplanarn,
  savingsplan_offeringtype,
  savingsplan_totalcommitmenttodate,
  savingsplan_usedcommitment,
  savingsplan_savingsplaneffectivecost,
  reservation_effectivecost,
  reservation_unusedamortizedupfrontfeeforbillingperiod,
  reservation_unusedrecurringfee,

  -- Cost & Pricing
  lineitem_unblendedcost,
  lineitem_blendedcost,
  list_price,
  effective_cost,
  savings,

  -- Misc
  lineitem_lineitemdescription,
  cost_allocation,

  -- Partition Columns
  yearmonth,
  modelid

FROM "prism-raw".etl_output

WHERE
  yearmonth = ?
  AND modelid IN (
    SELECT value 
    FROM (
      WITH ids AS (
        SELECT ? AS data
      )
      SELECT value 
      FROM ids 
      CROSS JOIN UNNEST(split(ids.data, ',')) AS x(value)
    )
  )
  AND lineitem_productcode = 'AmazonEC2'
  AND bill_billingentity IN ('AWS', 'AWS Marketplace')
  AND lineitem_usagetype <> 'Route53-Domains'
  AND cost_allocation = 'Customer'
  AND lineitem_lineitemtype IN (
    'DiscountedUsage', 'Usage', 'SavingsPlanCoveredUsage',
    'SavingsPlanNegation', 'SavingsPlanRecurringFee',
    'SavingsPlanUpfrontFee', 'RIFee', 'Fee'
  );


 

 