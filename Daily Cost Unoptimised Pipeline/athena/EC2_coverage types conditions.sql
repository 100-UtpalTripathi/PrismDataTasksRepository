CASE
  -- Spot
  WHEN lineitem_usagetype LIKE '%Spot%' THEN 'Spot'

  -- Compute Savings Plan
  WHEN savingsplan_savingsplanarn IS NOT NULL AND savingsplan_savingsplanarn <> ''
    AND savingsplan_offeringtype = 'ComputeSavingsPlans'
    AND lineitem_lineitemtype LIKE 'SavingsPlan%'
    THEN 'Compute Savings Plan'

  -- EC2 Instance Savings Plan
  WHEN savingsplan_savingsplanarn IS NOT NULL AND savingsplan_savingsplanarn <> ''
    AND savingsplan_offeringtype = 'EC2InstanceSavingsPlans'
    AND lineitem_lineitemtype LIKE 'SavingsPlan%'
    THEN 'EC2 Instance Savings Plan'

  -- Standard Reserved Instances
  WHEN lineitem_lineitemtype = 'DiscountedUsage'
    AND reservation_reservationarn IS NOT NULL AND reservation_reservationarn <> ''
    AND pricing_offeringclass = 'standard'
    THEN 'Standard RI'

  -- Convertible Reserved Instances
  WHEN lineitem_lineitemtype = 'DiscountedUsage'
    AND reservation_reservationarn IS NOT NULL AND reservation_reservationarn <> ''
    AND pricing_offeringclass = 'convertible'
    THEN 'Convertible RI'

  -- On-Demand
  WHEN (savingsplan_savingsplanarn IS NULL OR savingsplan_savingsplanarn = '')
    AND (reservation_reservationarn IS NULL OR reservation_reservationarn = '')
    AND lineitem_lineitemtype = 'Usage'
    AND lineitem_usagetype LIKE '%BoxUsage%'
    THEN 'On Demand'

  ELSE 'Other'
END AS coverage_type
