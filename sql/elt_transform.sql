DELETE FROM sales_report_elt;

WITH dedup_raw_orders AS (
    SELECT DISTINCT ON (order_id)
        order_id,
        customer_id,
        product_id,
        quantity,
        order_date
    FROM raw_orders
    WHERE order_id ~ '^[0-9]+$'
      AND customer_id ~ '^[0-9]+$'
      AND product_id ~ '^[0-9]+$'
      AND quantity ~ '^[0-9]+$'
    ORDER BY order_id
),
dedup_raw_customers AS (
    SELECT DISTINCT ON (customer_id)
        customer_id,
        customer_name,
        city,
        signup_date
    FROM raw_customers
    WHERE customer_id ~ '^[0-9]+$'
    ORDER BY customer_id
),
dedup_raw_products AS (
    SELECT DISTINCT ON (product_id)
        product_id,
        product_name,
        category,
        price
    FROM raw_products
    WHERE product_id ~ '^[0-9]+$'
      AND price ~ '^[0-9]+(\.[0-9]+)?$'
    ORDER BY product_id
)
INSERT INTO sales_report_elt (
    order_id,
    order_date,
    customer_id,
    customer_name,
    city,
    product_id,
    product_name,
    category,
    quantity,
    price,
    total_amount
)
SELECT
    CAST(ro.order_id AS INTEGER) AS order_id,
    CAST(ro.order_date AS DATE) AS order_date,
    CAST(ro.customer_id AS INTEGER) AS customer_id,
    INITCAP(TRIM(rc.customer_name)) AS customer_name,
    INITCAP(TRIM(rc.city)) AS city,
    CAST(ro.product_id AS INTEGER) AS product_id,
    INITCAP(TRIM(rp.product_name)) AS product_name,
    INITCAP(TRIM(rp.category)) AS category,
    CAST(ro.quantity AS INTEGER) AS quantity,
    CAST(rp.price AS NUMERIC(10,2)) AS price,
    CAST(ro.quantity AS INTEGER) * CAST(rp.price AS NUMERIC(10,2)) AS total_amount
FROM dedup_raw_orders ro
JOIN dedup_raw_customers rc
    ON ro.customer_id = rc.customer_id
JOIN dedup_raw_products rp
    ON ro.product_id = rp.product_id;
    