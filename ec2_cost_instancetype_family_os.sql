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
SELECT 
    yearmonth, 
    modelid, 
    product_productname,
    CASE
        WHEN lineitem_usagetype LIKE '%SpotUsage%' THEN 'Spot'
        WHEN lineitem_lineitemtype IN ('DiscountedUsage', 'RIFee') THEN 'Reserved'
        WHEN lineitem_lineitemtype IN ('SavingsPlanCoveredUsage', 'SavingsPlanNegation') THEN 'SavingsPlan'
        WHEN lineitem_lineitemtype = 'Usage' AND lineitem_operation = 'RunInstances' THEN 'OnDemand'
        ELSE 'Other'
    END AS coverage_type,
    SUM(lineitem_unblendedcost) AS UnblendedCost, 
    SUM(lineitem_blendedcost) AS BlendedCost, 
    SUM(list_price) AS ListPrice, 
    SUM(effective_cost) AS EffectiveCost 
FROM "prism-raw"."etl_output"
WHERE 
    yearmonth LIKE '2025%'  
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
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4;

-- Aggregate EC2 costs by instance family, inferred from usage type
SELECT 
    yearmonth, 
    modelid, 
    product_productname,
    lineitem_usagetype,
    split_part(split_part(lineitem_usagetype, ':', 2), '.', 1) AS instance_family,
    SUM(lineitem_unblendedcost) AS UnblendedCost, 
    SUM(lineitem_blendedcost) AS BlendedCost, 
    SUM(list_price) AS ListPrice, 
    SUM(effective_cost) AS EffectiveCost 
FROM "prism-raw"."etl_output"
WHERE 
    yearmonth LIKE '2025%'  
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
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 4;

-- Get distinct usage types related to EC2 usage in 2025
SELECT DISTINCT lineitem_usagetype
FROM "prism-raw"."etl_output"
WHERE 
    yearmonth LIKE '2025%'
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS'
    AND lineitem_usagetype LIKE '%Usage%';

-- Aggregate EC2 costs by instance type (parsed from usage type)
SELECT 
    yearmonth, 
    modelid, 
    product_productname,
    split_part(lineitem_usagetype, ':', 2) AS instance_type,
    SUM(lineitem_unblendedcost) AS UnblendedCost, 
    SUM(lineitem_blendedcost) AS BlendedCost, 
    SUM(list_price) AS ListPrice, 
    SUM(effective_cost) AS EffectiveCost 
FROM "prism-raw"."etl_output"
WHERE 
    yearmonth LIKE '2025%'  
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
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4;

-- Aggregate EC2 costs by operating system group
SELECT 
    yearmonth, 
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
WHERE 
    yearmonth LIKE '2025%'  
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
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4;

-- Get distinct operating systems for EC2 usage
SELECT DISTINCT product_operatingsystem AS operating_system
FROM "prism-raw"."etl_output"
WHERE 
    yearmonth LIKE '2025%'  
    AND product_productname = 'Amazon Elastic Compute Cloud'
    AND bill_billingentity = 'AWS'
    AND (
        lineitem_usagetype LIKE '%BoxUsage:%'
        OR lineitem_usagetype LIKE '%SpotUsage:%'
        OR lineitem_usagetype LIKE '%HeavyUsage:%'
    );
