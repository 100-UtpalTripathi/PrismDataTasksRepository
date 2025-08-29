PREPARE prism_prod_get_monthly_prism_discount_summary_v2
FROM
SELECT 
    "modelid",
    "yearMonth",
    "lineitem_productcode",
    "product_operatingsystem",
    substring("yearMonth", 1, 4) AS "Year",
    substring("yearMonth", 5, 2) AS "Month",
    ROUND(SUM(sum_list_price), 2) AS "List Price",
    ROUND(SUM(sum_effective_cost), 2) AS "Effective Cost",
    ROUND(SUM(sum_savings), 2) AS "PRISM Discount"
FROM "prism_curated_prod"."tb_monthly_cost_summary" 
WHERE 
    "yearMonth" IN (
        SELECT value FROM (
            WITH Y AS (SELECT ? AS data)
            SELECT value FROM Y CROSS JOIN UNNEST(split(Y.data, ',')) AS x(value)
        )
    )
    AND "modelid" IN (
        SELECT value FROM (
            WITH M AS (SELECT ? AS data)
            SELECT value FROM M CROSS JOIN UNNEST(split(M.data, ',')) AS x(value)
        )
    )
    AND "cost_allocation" = 'Customer'
    AND "discount_applied" = 'PRISM' 
GROUP BY 1, 2, 3, 4
ORDER BY "yearMonth" ASC;



-- EXECUTE prism_prod_get_monthly_prism_discount_summary_v2
-- 0bec4e0b-cc83-4852-81c8-7163364c4c25,171e929f-b407-4546-907e-0cbf9ae66de2,7376761e-dbfb-4223-bd0b-3b403313f66a,83b766e1-9c15-4c73-81f3-c74f2b19a1dc