/*
===============================================================================
Performance Analysis (Year-over-Year, Month-over-Month)
===============================================================================
Purpose:
    - To measure the performance of products, customers, or regions over time.
    - For benchmarking and identifying high-performing entities.
    - To track yearly trends and growth.

SQL Functions Used:
    - LAG(): Accesses data from previous rows.
    - AVG() OVER(): Computes average values within partitions.
    - CASE: Defines conditional logic for trend analysis.
===============================================================================
*/

/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */
WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY 
        YEAR(f.order_date),
        p.product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
    -- Year-over-Year Analysis
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
    CASE 
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No Change'
    END AS py_change
FROM yearly_product_sales
ORDER BY product_name, order_year;



/* Analyze the monthly performance of products by comparing their sales 
to both the average sales performance of the product and the previous month's sales */
WITH monthly_product_sale AS (
    SELECT
        YEAR(f.order_date) AS order_year,
        MONTH(f.order_date) AS order_month,
        DATENAME(MONTH, f.order_date) AS month_name,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY 
        YEAR(f.order_date),
        MONTH(f.order_date),
        DATENAME(MONTH, f.order_date),
        p.product_name
)

SELECT
    order_year,
    order_month,
    month_name,
    product_name,
    current_sales,

    -- Average monthly sales per product
    AVG(current_sales) OVER (
        PARTITION BY product_name
    ) AS avg_sales,

    -- Difference from average
    current_sales - AVG(current_sales) OVER (
        PARTITION BY product_name
    ) AS diff_avg,

    CASE 
        WHEN current_sales - AVG(current_sales) OVER (
            PARTITION BY product_name
        ) > 0 THEN 'Above avg'

        WHEN current_sales - AVG(current_sales) OVER (
            PARTITION BY product_name
        ) < 0 THEN 'Below avg'

        ELSE 'avg'
    END AS avg_change,

    -- Previous month sales
    LAG(current_sales) OVER (
        PARTITION BY product_name
        ORDER BY order_year, order_month
    ) AS prev_month_sales,

    -- Month-on-month difference
    current_sales - LAG(current_sales) OVER (
        PARTITION BY product_name
        ORDER BY order_year, order_month
    ) AS mom_sale_diff,

    -- Month-on-month status
    CASE 
        WHEN current_sales - LAG(current_sales) OVER (
            PARTITION BY product_name
            ORDER BY order_year, order_month
        ) > 0 THEN 'Increase'

        WHEN current_sales - LAG(current_sales) OVER (
            PARTITION BY product_name
            ORDER BY order_year, order_month
        ) < 0 THEN 'Decrease'

        ELSE 'No Change'
    END AS mom_change

FROM monthly_product_sale
ORDER BY product_name, order_year, order_month;
