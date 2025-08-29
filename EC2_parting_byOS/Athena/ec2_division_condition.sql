CASE
    WHEN product_operatingsystem = 'Linux' THEN 'Linux'
    WHEN product_operatingsystem = 'Ubuntu Pro' THEN 'Ubuntu Pro'
    WHEN product_operatingsystem IN ('RHEL', 'Red Hat Enterprise Linux with HA') THEN 'RHEL'
    WHEN product_operatingsystem = 'SUSE' THEN 'SUSE'
    WHEN product_operatingsystem = 'Windows' THEN 'Windows'
    ELSE 'Unknown/Other'
END AS os_group