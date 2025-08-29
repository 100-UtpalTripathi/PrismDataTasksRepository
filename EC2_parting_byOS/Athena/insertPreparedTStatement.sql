-- DEALLOCATE PREPARE prism_dev_insert_tb_monthly_cost_summary_v4

PREPARE prism_dev_insert_tb_monthly_cost_summary_v3 FROM
INSERT INTO "prism_curated_dev"."tb_monthly_cost_summary"
SELECT
     "bill_payeraccountid",
     "bill_billingentity",
     "lineitem_usageaccountid", 
     "lineitem_productcode",
     "lineitem_lineitemdescription",
     "lineitem_lineitemtype",
     "product_productname",
     "discount_applied",
     "cost_allocation",
     SUM("lineitem_unblendedcost") as "sum_lineitem_unblendedcost",
     SUM("lineitem_blendedcost") as "sum_lineitem_blendedcost",
     SUM("list_price") as "sum_list_price",
     SUM("effective_cost") as "sum_effective_cost",
     SUM("savings") as "sum_savings",
     "product_operatingsystem",
     "yearMonth",
     "modelid"
FROM "prism-raw".etl_output
WHERE "yearMonth" = ?
  AND "modelid" IN (
    SELECT value FROM (
      WITH M AS (
        SELECT ? AS data
      )
      SELECT value
      FROM M
      CROSS JOIN UNNEST(split(M.data, ',')) AS x(value)
    )
)
GROUP BY 
    "bill_billingentity",
    "discount_applied", 
    "yearMonth",              
    "modelid", 
    "bill_payeraccountid",      
    "lineitem_usageaccountid", 
    "product_productname",
    "lineitem_productcode",
    "product_operatingsystem",  
    "cost_allocation",          
    "lineitem_lineitemtype",    
    "lineitem_lineitemdescription";
      