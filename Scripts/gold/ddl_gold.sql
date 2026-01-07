/*
===============================================================================
STAGING TO GOLD: Sales Analytics Star Schema Data Model
===============================================================================
PURPOSE:
    This script transforms cleansed data from the 'silver' layer into a 
    production-ready Star Schema (Gold Layer). It creates dimensions and 
    fact views optimized for Power BI, Tableau, and ad-hoc analytics.
USAGE:
    1. Deployment: Run this script in a SQL environment with 'silver' schema 
       access to (re)build the analytical layer.
    2. Logic: Each view utilizes 'Drop-if-Exists' logic for idempotency, 
       meaning the script can be executed multiple times without errors.
    3. Analytics: Join fact_sales to dimensions using the Surrogate Keys (_sk) 
       rather than Business Keys for optimal performance.
===============================================================================
/*
===============================================================================
                          dim_customers VIEW
===============================================================================
*/
-- DROPING gold.dim_customers VIEW
IF OBJECT('gold.dim_customers','V') IS NOT NULL
  DROP VIEW gold.dim_customers;
GO
-- CREATEING gold.dim_customers VIEW
CREATE VIEW gold.dim_customers AS 
(
SELECT 
ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_sk,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
cl.cntry AS country,
ci.cst_material_status AS material_status,
CASE 
	WHEN ci.cst_gndr != 'Unidentified' THEN ci.cst_gndr
	ELSE ISNULL(ca.gen,'Unidentified')
END AS gender,
ca.bdate AS birthdate,
ci.dwh_create_date
FROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as cl
on ci.cst_key = cl.cid
)
/*
===============================================================================
                          dim_product VIEW
===============================================================================
*/
-- DROPING gold.dim_customers VIEW
IF OBJECT('gold.dim_product','V') IS NOT NULL
  DROP VIEW gold.dim_product;
GO
-- CREATEING gold.dim_product VIEW
CREATE VIEW gold.dim_product AS (
SELECT
ROW_NUMBER() OVER(ORDER BY cpi.prd_id) AS product_sk,
cpi.prd_id AS product_id,
cpi.prd_key AS product_number,
cpi.prd_nm AS product_name,
cpi.cat_id AS category_id,
epc.cat AS category_name,
epc.subcat AS subcategory,
epc.maintenance,
cpi.prd_cost AS cost,
cpi.prd_line AS product_line,
cpi.prd_start_dt AS product_start_date
FROM silver.crm_prd_info AS cpi
LEFT JOIN silver.erp_px_cat_g1v2 AS epc
ON cpi.cat_id = epc.id
WHERE prd_end_dt IS NULL
)
/*
===============================================================================
                          fact_sales VIEW
===============================================================================
*/
-- DROPING gold.fact_sales VIEW
IF OBJECT('gold.fact_sales','V') IS NOT NULL
  DROP VIEW gold.fact_sales;
GO
-- CREATEING gold.fact_sales VIEW
CREATE VIEW gold.fact_sales AS 
(
SELECT
dc.customer_sk,
dp.product_sk,
sd.sls_ord_num AS order_number,
sd.sls_order_dt AS order_date,
sd.sls_due_dt AS due_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_quantity AS quantity,
sd.sls_price AS price,
sd.sls_sales AS sales
FROM silver.crm_sale_details as sd
LEFT JOIN gold.dim_customers as dc
ON sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_product as dp
ON sd.sls_prd_key = dp.product_number
)
