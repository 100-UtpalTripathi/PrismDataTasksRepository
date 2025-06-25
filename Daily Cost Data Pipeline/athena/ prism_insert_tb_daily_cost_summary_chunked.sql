
PREPARE prism_insert_tb_daily_cost_summary_chunked
FROM
INSERT INTO prism_curated_dev.tb_daily_cost_summary
SELECT
  bill_payeraccountid,
  bill_billingentity,
  lineitem_usageaccountid,
  lineitem_productcode,
  lineitem_lineitemdescription,
  lineitem_lineitemtype,
  lineitem_usagetype,
  lineitem_operation,
  lineitem_resourceid,
  product_productname,
  product_instancetype,
  product_instancefamily,
  product_productfamily,
  product_region,
  product_location,
  product_locationtype,
  product_availabilityzone,
  product_databaseengine,
  product_deploymentoption,
  product_cacheengine,
  product_enginecode,
  product_storageclass,
  product_volumetype,
  product_usagefamily,
  product_operatingsystem,
  product_tenancy,
  product_licensemodel,
  product_servicename,
  reservation_reservationarn,
  reservation_effectivecost,
  reservation_unusedamortizedupfrontfeeforbillingperiod,
  reservation_unusedrecurringfee,
  reservation_totalreservedunits,
  reservation_unusedquantity,
  savingsplan_savingsplanarn,
  savingsplan_offeringtype,
  savingsplan_totalcommitmenttodate,
  savingsplan_usedcommitment,
  savingsplan_savingsplaneffectivecost,
  discount_applied,
  cost_allocation,
  sum(list_price) as sum_list_price,
  sum(lineitem_unblendedcost) as sum_lineitem_unblendedcost,
  sum(lineitem_blendedcost) as sum_lineitem_blendedcost,
  sum(effective_cost) as sum_effective_cost,
  sum(savings) as sum_savings,
  yearmonth,
  DATE(lineitem_usagestartdate) AS lineitem_usagedate,
  modelid
FROM "prism-raw"."etl_output"
WHERE
  yearmonth = ?
  AND DATE(lineitem_usagestartdate) = CAST(? AS DATE)
  AND modelid IN (
    SELECT value FROM (
      WITH models AS (
        SELECT ? AS data
      )
      SELECT value FROM models
      CROSS JOIN UNNEST(split(models.data, ',')) AS x(value)
    )
  )
  AND cost_allocation = 'Customer'
GROUP BY
  modelid,
  yearmonth,
  DATE(lineitem_usagestartdate),
  bill_payeraccountid,
  bill_billingentity,
  lineitem_usageaccountid,
  lineitem_productcode,
  lineitem_lineitemdescription,
  lineitem_lineitemtype,
  lineitem_usagetype,
  lineitem_operation,
  lineitem_resourceid,
  product_productname,
  product_instancetype,
  product_instancefamily,
  product_productfamily,
  product_region,
  product_location,
  product_locationtype,
  product_availabilityzone,
  product_databaseengine,
  product_deploymentoption,
  product_cacheengine,
  product_enginecode,
  product_storageclass,
  product_volumetype,
  product_usagefamily,
  product_operatingsystem,
  product_tenancy,
  product_licensemodel,
  product_servicename,
  reservation_reservationarn,
  reservation_effectivecost,
  reservation_unusedamortizedupfrontfeeforbillingperiod,
  reservation_unusedrecurringfee,
  reservation_totalreservedunits,
  reservation_unusedquantity,
  savingsplan_savingsplanarn,
  savingsplan_offeringtype,
  savingsplan_totalcommitmenttodate,
  savingsplan_usedcommitment,
  savingsplan_savingsplaneffectivecost,
  discount_applied,
  cost_allocation;