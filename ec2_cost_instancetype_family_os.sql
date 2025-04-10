-- ec2 data for each coverage type - OnDemand, SPot adn reserved
-- ec2 cost dat for each instance type families 
-- and instance types
-- ec2 cost data for each Operating System
-- Get distinct EC2 operation types for AWS billing
SELECT DISTINCT lineitem_operation
FROM "prism-raw"."etl_output"
WHERE bill_billingentity = 'AWS'
    AND product_productname = 'Amazon Elastic Compute Cloud';
-- Get distinct EC2 line item types for AWS billing
SELECT DISTINCT lineitem_lineitemtype
FROM "prism-raw"."etl_output"
WHERE bill_billingentity = 'AWS'
    AND product_productname = 'Amazon Elastic Compute Cloud';
-- Aggregate costs by model and EC2 coverage type (OnDemand, Spot, Reserved, SavingsPlan)
SELECT yearmonth,
    modelid,
    product_productname,
    CASE
        WHEN lineitem_usagetype LIKE '%SpotUsage%' THEN 'Spot'
        WHEN lineitem_lineitemtype IN ('DiscountedUsage', 'RIFee') THEN 'Reserved'
        WHEN lineitem_lineitemtype IN ('SavingsPlanCoveredUsage', 'SavingsPlanNegation') THEN 'SavingsPlan'
        WHEN lineitem_lineitemtype = 'Usage'
        AND lineitem_operation = 'RunInstances' THEN 'OnDemand'
        ELSE 'Other'
    END AS coverage_type,
    SUM(lineitem_unblendedcost) AS UnblendedCost,
    SUM(lineitem_blendedcost) AS BlendedCost,
    SUM(list_price) AS ListPrice,
    SUM(effective_cost) AS EffectiveCost
FROM "prism-raw"."etl_output"
WHERE yearmonth LIKE '2025%'
    AND modelid IN (
        '0532721b-5c2d-43dc-9e1b-dc6f047e59ab',
        'bfb69248-c991-463f-824f-0317df6e57b5'
    )
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS'
    AND lineitem_operation IN (
        'RunInstances',
        'SpotUsage',
        'SavingsPlanCoveredUsage',
        'ReservedInstances',
        'DedicatedHostUsage'
    )
GROUP BY 1,
    2,
    3,
    4
ORDER BY 1,
    4;
-- Aggregate EC2 costs by instance family, inferred from usage type
SELECT yearmonth,
    modelid,
    product_productname,
    lineitem_usagetype,
    split_part(split_part(lineitem_usagetype, ':', 2), '.', 1) AS instance_family,
    SUM(lineitem_unblendedcost) AS UnblendedCost,
    SUM(lineitem_blendedcost) AS BlendedCost,
    SUM(list_price) AS ListPrice,
    SUM(effective_cost) AS EffectiveCost
FROM "prism-raw"."etl_output"
WHERE yearmonth LIKE '2025%'
    AND modelid IN (
        '0532721b-5c2d-43dc-9e1b-dc6f047e59ab',
        'bfb69248-c991-463f-824f-0317df6e57b5'
    )
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS'
    AND (
        lineitem_usagetype LIKE '%BoxUsage:%'
        OR lineitem_usagetype LIKE '%SpotUsage:%'
        OR lineitem_usagetype LIKE '%HeavyUsage:%'
    )
GROUP BY 1,
    2,
    3,
    4,
    5
ORDER BY 1,
    4;
-- Get distinct usage types related to EC2 usage in 2025
SELECT DISTINCT lineitem_usagetype
FROM "prism-raw"."etl_output"
WHERE yearmonth LIKE '2025%'
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS'
    AND lineitem_usagetype LIKE '%Usage%';
-- Aggregate EC2 costs by instance type (parsed from usage type)
SELECT yearmonth,
    modelid,
    product_productname,
    split_part(lineitem_usagetype, ':', 2) AS instance_type,
    SUM(lineitem_unblendedcost) AS UnblendedCost,
    SUM(lineitem_blendedcost) AS BlendedCost,
    SUM(list_price) AS ListPrice,
    SUM(effective_cost) AS EffectiveCost
FROM "prism-raw"."etl_output"
WHERE yearmonth LIKE '2025%'
    AND modelid IN (
        '0532721b-5c2d-43dc-9e1b-dc6f047e59ab',
        'bfb69248-c991-463f-824f-0317df6e57b5'
    )
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS'
    AND (
        lineitem_usagetype LIKE '%BoxUsage:%'
        OR lineitem_usagetype LIKE '%SpotUsage:%'
        OR lineitem_usagetype LIKE '%HeavyUsage:%'
    )
GROUP BY 1,
    2,
    3,
    4
ORDER BY 1,
    4;
-- Aggregate EC2 costs by operating system group
SELECT yearmonth,
    modelid,
    product_productname,
    CASE
        WHEN product_operatingsystem IN ('Linux', 'Ubuntu Pro') THEN 'Linux/UNIX'
        WHEN product_operatingsystem IN ('RHEL', 'Red Hat Enterprise Linux with HA') THEN 'RHEL'
        WHEN product_operatingsystem = 'SUSE' THEN 'SUSE'
        WHEN product_operatingsystem = 'Windows' THEN 'Windows'
        ELSE 'Unknown/Other'
    END AS os_group,
    SUM(lineitem_unblendedcost) AS UnblendedCost,
    SUM(lineitem_blendedcost) AS BlendedCost,
    SUM(list_price) AS ListPrice,
    SUM(effective_cost) AS EffectiveCost
FROM "prism-raw"."etl_output"
WHERE yearmonth LIKE '2025%'
    AND modelid IN (
        '0532721b-5c2d-43dc-9e1b-dc6f047e59ab',
        'bfb69248-c991-463f-824f-0317df6e57b5'
    )
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS'
    AND (
        lineitem_usagetype LIKE '%BoxUsage:%'
        OR lineitem_usagetype LIKE '%SpotUsage:%'
        OR lineitem_usagetype LIKE '%HeavyUsage:%'
    )
GROUP BY 1,
    2,
    3,
    4
ORDER BY 1,
    4;
-- Get distinct operating systems for EC2 usage
SELECT DISTINCT product_operatingsystem AS operating_system
FROM "prism-raw"."etl_output"
WHERE yearmonth LIKE '2025%'
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS'
    AND (
        lineitem_usagetype LIKE '%BoxUsage:%'
        OR lineitem_usagetype LIKE '%SpotUsage:%'
        OR lineitem_usagetype LIKE '%HeavyUsage:%'
    );



-- Instance Cost by Days and Coverage Type
-- Filters - ???
-- Interval
-- daily
-- Weekly
-- Monthly
-- X-Axis
-- Days
-- Accounts
-- Availabiltiy Zones
-- Billing Accounts
-- Category(Colored on bar on hte graph)
-- Billing Accounts
-- Coverage Type
-- Compute Savings Plan
-- On Demand
-- Standard RI
-- Spot
-- EC2 Svaings Plan
-- Convertible RI
-- Accounts
-- Availability Zone
-- Instance Type
-- Different instance types
-- Reservation Type
-- No Upfront
-- On Demand
-- Partial Upfront
-- Spot
-- All Upront
-- Tenancy - Low Priority
-- VPC
-- --  



Step1 : create s3 bucket...



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



-- To detect EC2 savings plan coverage type: 
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
  AND savingsplan_offeringtype = 'EC2InstanceSavingsPlans'
  AND lineitem_lineitemtype LIKE 'SavingsPlan%'
  AND bill_billingentity = 'AWS'
  AND yearmonth LIKE '202501%'
  AND product_productname = 'Amazon Elastic Compute Cloud'
LIMIT 100;































