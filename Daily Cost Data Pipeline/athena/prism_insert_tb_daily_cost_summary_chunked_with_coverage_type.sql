PREPARE prism_insert_tb_daily_cost_summary_chunked
FROM
INSERT INTO prism_curated_dev.tb_daily_cost_summary
SELECT bill_payeraccountid,
  bill_billingentity,
  lineitem_usageaccountid,
  lineitem_productcode,
  lineitem_lineitemdescription,
  product_productname,
  discount_applied,
  SUM(list_price) as list_price,
  SUM(lineitem_unblendedcost) as lineitem_unblendedcost,
  SUM(lineitem_blendedcost) as lineitem_blendedcost,
  SUM(effective_cost) as effective_cost,
  SUM(savings) as savings,
  CASE
    WHEN savingsplan_savingsplanarn IS NOT NULL
    AND savingsplan_savingsplanarn <> ''
    AND savingsplan_offeringtype = 'ComputeSavingsPlans'
    AND lineitem_lineitemtype LIKE 'SavingsPlan%' THEN 'Compute Savings Plan'
    WHEN savingsplan_savingsplanarn IS NOT NULL
    AND savingsplan_savingsplanarn <> ''
    AND savingsplan_offeringtype = 'EC2InstanceSavingsPlans'
    AND lineitem_lineitemtype LIKE 'SavingsPlan%' THEN 'EC2 Instance Savings Plan'
    WHEN reservation_reservationarn IS NOT NULL
    AND reservation_reservationarn <> ''
    AND lineitem_lineitemtype = 'DiscountedUsage'
    AND pricing_offeringclass = 'standard' THEN 'Standard RI'
    WHEN reservation_reservationarn IS NOT NULL
    AND reservation_reservationarn <> ''
    AND lineitem_lineitemtype = 'DiscountedUsage'
    AND pricing_offeringclass = 'convertible' THEN 'Convertible RI'
    WHEN lineitem_usagetype LIKE '%Spot%' THEN 'Spot'
    WHEN (savingsplan_savingsplanarn IS NULL OR savingsplan_savingsplanarn = '')
    AND (reservation_reservationarn IS NULL OR reservation_reservationarn = '')
    AND lineitem_lineitemtype = 'Usage'
    AND lineitem_usagetype LIKE '%BoxUsage%'
    THEN 'On Demand'
    ELSE 'NA'
  END AS coverage_type,
  yearmonth,
  DATE(lineitem_usagestartdate) AS lineitem_usagedate,
  modelid
FROM "prism-raw"."etl_output"
WHERE yearmonth = ?
  AND DATE(lineitem_usagestartdate) = CAST(? AS DATE)
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
  AND cost_allocation = 'Customer'
  AND lineitem_productcode = 'AmazonEC2'
GROUP BY 16, 14, 15, 1, 2, 3, 4, 5, 6, 7, 13;




EXECUTE prism_insert_tb_daily_cost_summary_chunked USING 
  '202502',
  '2025-02-01',
  'da24ebf9-3b3f-45e3-aeaf-cf4b31a8f43f,7cda1c66-ceb3-45ba-9721-3e534db659f3,aef278a0-9730-4837-8940-87e65062a84d';


