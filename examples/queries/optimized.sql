-- Example query after optimization
WITH order_counts AS (
    SELECT 
        product_id,
        COUNT(*) as total_orders
    FROM 
        order_items
    GROUP BY 
        product_id
)
SELECT 
    p.product_id,
    p.name,
    c.category_name,
    i.quantity_in_stock,
    COALESCE(o.total_orders, 0) as total_orders
FROM 
    products p
    JOIN inventory i ON p.product_id = i.product_id
    JOIN categories c ON p.category_id = c.category_id
    LEFT JOIN order_counts o ON p.product_id = o.product_id
WHERE 
    p.active = true
    AND i.quantity_in_stock > 0
ORDER BY 
    o.total_orders DESC NULLS LAST;
