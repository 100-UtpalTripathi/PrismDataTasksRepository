PREPARE prism_insert_tb_daily_cost_summary_chunked
FROM
INSERT INTO prism_curated_dev.tb_daily_cost_summary
SELECT
  bill_payeraccountid,                                                   
  bill_billingentity,                                                    
  lineitem_usageaccountid,                                              
  lineitem_productcode,                                                 
  lineitem_lineitemdescription,                                         
  product_productname,                                                   
  discount_applied,                                                      
  SUM(list_price) AS list_price,                                        
  SUM(lineitem_unblendedcost) AS lineitem_unblendedcost,                
  SUM(lineitem_blendedcost) AS lineitem_blendedcost,                    
  SUM(effective_cost) AS effective_cost,                            
  SUM(savings) AS savings,                                              
  DATE(lineitem_usagestartdate) AS lineitem_usagedate,                  
  date_add(
    'day',
    6,
    date_trunc('week', DATE(lineitem_usagestartdate))
  ) AS week_end_date,

  yearmonth,                                                            
  modelid                                                               
FROM "prism-raw"."etl_output"
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
  AND cost_allocation = 'Customer'
GROUP BY
  15, 
  16,  
  14,  
  13,  
  1,   
  2,   
  3,   
  4,   
  5,  
  6,  
  7;   
  
  
  
  EXECUTE prism_insert_tb_daily_cost_summary_chunked USING 
  '202506',  
  '3a82de03-9d24-4e3d-9f69-ac62cf5a731c,8d452c5e-7c38-42d7-b24d-c9a7b9d702ef,8cc0c480-8f61-4485-8964-6c3f81a54bc6,8b9d91cf-be08-4693-b339-f77f9e95d115,55aabe68-2611-4fa7-b5b5-ca2bad092e3d,cdc71285-5977-4fe4-9954-a1ad605991f1,e811e9bc-5aaf-4e83-a9f9-b42782c68e5a,41c89953-3820-42c9-a9f5-69547b9223ab';

  
  
  