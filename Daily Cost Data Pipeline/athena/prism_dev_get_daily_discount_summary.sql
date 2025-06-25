PREPARE prism_dev_get_daily_discount_summary FROM
SELECT 
  modelid,
  lineitem_usagedate AS date,
  lineitem_productcode,
  ROUND(SUM(sum_list_price), 2)       AS list_price,
  ROUND(SUM(sum_effective_cost), 2)   AS effective_cost,
  ROUND(SUM(sum_savings), 2)          AS prism_discount
FROM prism_curated_dev.tb_daily_cost_summary
WHERE 
  lineitem_usagedate BETWEEN date_parse(?, '%Y-%m-%d') AND date_parse(COALESCE(NULLIF(?, ''), CAST(current_date AS VARCHAR)), '%Y-%m-%d')
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
  AND (lineitem_productcode = ? OR ? = '')
  AND cost_allocation = 'Customer'
GROUP BY modelid, lineitem_usagedate, lineitem_productcode
ORDER BY lineitem_usagedate ASC;




EXECUTE prism_dev_get_daily_discount_summary 
USING 
  '2025-05-01',                      -- startDate
  '',                      -- endDate
  '3a82de03-9d24-4e3d-9f69-ac62cf5a731c,8d452c5e-7c38-42d7-b24d-c9a7b9d702ef,8cc0c480-8f61-4485-8964-6c3f81a54bc6,8b9d91cf-be08-4693-b339-f77f9e95d115,55aabe68-2611-4fa7-b5b5-ca2bad092e3d,cdc71285-5977-4fe4-9954-a1ad605991f1,e811e9bc-5aaf-4e83-a9f9-b42782c68e5a,41c89953-3820-42c9-a9f5-69547b9223ab', -- modelIds
  'AmazonEC2',                        -- service_name
  'AmazonEC2';                        

