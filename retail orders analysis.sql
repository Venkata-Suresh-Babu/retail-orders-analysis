CREATE SCHEMA retail;

--Extracted column names to create a table
/*
Order Id	
Order Date
Ship Mode
Segment
Country
City
State
Postal Code
Region
Category
Sub Category
Product Id
cost price
List Price
Quantity
Discount Percent
*/

-- Create a table

CREATE TABLE retail.orders (
	"Order Id" INT NOT NULL,
	"Order Date" DATE,
	"Ship Mode" VARCHAR(100),
	"Segment" VARCHAR(100),
	"Country" VARCHAR(100),
	"City" VARCHAR(100),
	"State" VARCHAR(100),
	"Postal Code" BIGINT,
	"Region" VARCHAR(50),
	"Category" VARCHAR(100),
	"Sub Category" VARCHAR(100),
	"Product Id" VARCHAR(150),
	"cost price" INT,
	"List Price" INT,
	"Quantity" INT,
	"Discount Percent" INT
);

-- load the data using import funtion

-- data check

SELECT *
FROM retail.orders;

-- Data cleaning
-- changing the column names perfectly


DO $$                      
DECLARE                    
    r record;              
BEGIN                      
    FOR r IN               
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'orders'     
          AND table_schema = 'retail'   
    LOOP                  
        EXECUTE format('ALTER TABLE %I.%I RENAME COLUMN %I TO %I', 
            'retail',              
            'orders',              
            r.column_name,         
            lower(replace(r.column_name, ' ', '_'))); 
    END LOOP;            
END $$;


-- column names changed

SELECT * FROM retail.orders;


-- find null values

SELECT
	key AS column_name, 
	COUNT(*) - COUNT(value) AS null_count
FROM retail.orders t
CROSS JOIN LATERAL jsonb_each_text(to_jsonb(t)) --Converts the entire row into a JSON object
GROUP BY key; -- jsonb_each_text Breaks that object into rows of key-value pairs (e.g., order_id | 101)

-- find duplicates
SELECT order_id, order_date, ship_mode, segment, country, city, state, postal_code, region, category, sub_category, product_id,
cost_price, list_price, quantity, discount_percent, COUNT(*) AS duplicates
FROM retail.orders
GROUP BY order_id, order_date, ship_mode, segment, country, city, state, postal_code, region, category, sub_category, product_id,
cost_price, list_price, quantity, discount_percent
HAVING COUNT(*) > 1;

-- find unique values in columns
SELECT DISTINCT ship_mode, country, category
FROM retail.orders;

-- we found that some values like Not Available, unknown, N/A
-- remove those rows for data quality
WITH rows_to_drop AS (
	SELECT order_id
	FROM retail.orders
	WHERE ship_mode ILIKE 'Not Available' OR 
		  ship_mode ILIKE 'unknown' OR  
		  ship_mode ILIKE'N/A'
)
DELETE FROM retail.orders
WHERE order_id IN (SELECT order_id FROM rows_to_drop);

SELECT COUNT(*) AS total_rows
FROM retail.orders;

-- create columns and find the sale_price, discount, profit

-- Discount
ALTER TABLE retail.orders ADD COLUMN discount numeric;

UPDATE retail.orders
SET discount = list_price * discount_percent * 0.01;

SELECT order_id, discount
FROM retail.orders;

-- sale_price
ALTER TABLE retail.orders ADD COLUMN sale_price numeric;

UPDATE retail.orders
SET sale_price = list_price - discount;

SELECT order_id, cost_price, sale_price
FROM retail.orders;

-- Profit
ALTER TABLE retail.orders ADD COLUMN profit numeric;

UPDATE retail.orders
SET profit = sale_price - cost_price;

SELECT cost_price, discount, sale_price, profit 
FROM retail.orders;

-- revenue
ALTER TABLE retail.orders ADD COLUMN revenue numeric;
UPDATE retail.orders
SET revenue = sale_price * quantity;

-- check the updated table
SELECT * 
FROM retail.orders;

-- total revenue and Avg revenue
SELECT ROUND(SUM(revenue::NUMERIC), 2) AS total_revenue, 
	   ROUND(AVG(revenue::NUMERIC), 2) AS avg_revenue
FROM retail.orders;

-- revenue by category
SELECT ROUND(SUM(revenue::NUMERIC), 2) AS total_revenue_$, category
FROM retail.orders
GROUP BY category
ORDER BY total_revenue_$ DESC;

-- revenue by sub category
SELECT DISTINCT category, 
				ROUND(SUM(revenue::NUMERIC) OVER (PARTITION BY category), 2) AS total_revenue_by_category_$
FROM retail.orders
ORDER BY total_revenue_by_category_$;

--  Revenue by Furniture category
SELECT DISTINCT
		category, sub_category,
		ROUND(SUM(revenue::NUMERIC) OVER(PARTITION BY sub_category),2) AS Furniture_total_revenue_$
FROM retail.orders
WHERE category ILIKE 'Furniture'
ORDER BY Furniture_total_revenue_$ DESC;

-- Revenue by Office supplies category
SELECT DISTINCT
		category, sub_category,
		ROUND(SUM(revenue::NUMERIC) OVER(PARTITION BY sub_category),2) AS Office_Supplies_total_revenue_$
FROM retail.orders
WHERE category ILIKE 'Office Supplies'
ORDER BY Office_Supplies_total_revenue_$ DESC;

-- Revenue by Technology category
SELECT DISTINCT
		category, sub_category,
		ROUND(SUM(revenue::NUMERIC) OVER(PARTITION BY sub_category),2) AS Technology_total_revenue_$
FROM retail.orders
WHERE category ILIKE 'Technology'
ORDER BY Technology_total_revenue_$ DESC;

SELECT MAX(revenue) AS high_revenue_$
FROM retail.orders;

SELECT SUM(revenue) AS total_revenue_$
FROM retail.orders;

-- Demographic analysis
SELECT city, SUM(revenue) AS total_revenue_$
FROM retail.orders
GROUP BY city
ORDER BY total_revenue_$ DESC;

-- Top 10 cities in high revenue
SELECT city, SUM(revenue) AS total_revenue_$
FROM retail.orders
GROUP BY city
ORDER BY total_revenue_$ DESC
LIMIT 10;


-- top 10 highest revenue generating products
SELECT 
    product_id, category, sub_category,
    SUM(sale_price) OVER(PARTITION BY product_id, category, sub_category) AS sales
FROM retail.orders
ORDER BY sales DESC
LIMIT 10;


-- Top 5 selling products in each region
WITH cte AS(
	SELECT DISTINCT(region) AS region, product_id, sub_category, SUM(sale_price) AS sales
	FROM retail.orders
	GROUP BY DISTINCT(region), product_id, sub_category)
SELECT * FROM (
	SELECT *
	, ROW_NUMBER() OVER(PARTITION BY region ORDER BY sales DESC) AS rn
FROM cte) A
WHERE rn <= 5;


-- sales for each category by month
WITH cte AS (
	SELECT category, 
		    TO_CHAR(order_date, 'MonthYYYY' ) AS order_year_month, 
			SUM(sale_price) AS sales
	FROM retail.orders
	GROUP BY category, TO_CHAR(order_date, 'MonthYYYY' )
)
SELECT * FROM (
	SELECT *, 
	ROW_NUMBER() OVER(PARTITION BY sales ORDER BY sales DESC) as rn
	FROM cte
) a
WHERE rn = 1
ORDER BY sales DESC;


-- find month over month growth comparison for 2022 and 2023 sales
WITH cte AS (
		SELECT EXTRACT(YEAR FROM order_date) AS order_year, 
		EXTRACT(MONTH FROM order_date) AS month_num,
		TO_CHAR(order_date, 'Month') AS month_name,
		SUM(sale_price) AS sales
		FROM retail.orders
		GROUP BY 1, 2, 3
)
SELECT TRIM(month_name) AS month,
	   SUM(CASE WHEN order_year = 2022 THEN sales ELSE 0 END) AS sales_2022,
	   SUM(CASE WHEN order_year = 2023 THEN sales ELSE 0 END) AS sales_2023
FROM cte
GROUP BY month_num, month_name
ORDER BY sales_2022, sales_2023 DESC;


-- profits by year
SELECT EXTRACT(YEAR FROM order_date) AS order_year, SUM(profit) AS profit
FROM retail.orders
GROUP BY order_year
ORDER BY order_year DESC;

-- profits by category
SELECT category, SUM(profit) AS profits
FROM retail.orders
GROUP BY category
ORDER BY profits DESC;

-- ship mode useage
SELECT ship_mode, COUNT(ship_mode) AS count
FROM retail.orders
GROUP BY ship_mode;

-- revenue by ship mode
SELECT ship_mode, SUM(revenue) AS total_revenue_$
FROM retail.orders
GROUP BY ship_mode
ORDER BY total_revenue_$ DESC;

-- profits by ship mode

SELECT ship_mode, SUM(profit) AS total_profit_$
FROM retail.orders
GROUP BY ship_mode
ORDER BY total_profit_$ DESC;

-- comparing both revenue and profit

SELECT ship_mode, SUM(profit) AS total_profit_$, SUM(revenue) AS total_revenue_$
FROM retail.orders
GROUP BY ship_mode
ORDER BY total_profit_$, total_revenue_$ DESC;


-- shipping mode by category
SELECT category, ship_mode, COUNT(ship_mode) AS ship_mode_count
FROM retail.orders
GROUP BY category, ship_mode
ORDER BY ship_mode_count DESC;

-- Total profit and revenue
SELECT SUM(profit) AS total_profit_$, SUM(revenue) AS total_revenue_$
FROM retail.orders;

-- Discounts by category
SELECT category, SUM(discount) AS discount_$
FROM retail.orders
GROUP BY category
ORDER BY discount_$ DESC;

-- Discounts by ship mode
SELECT ship_mode, SUM(discount) AS discount_$
FROM retail.orders
GROUP BY ship_mode
ORDER BY discount_$ DESC;
