-- Example query before optimization
SELECT 
    p.product_id,
    p.name,
    c.category_name,
    i.quantity_in_stock,
    COUNT(o.order_id) as total_orders
FROM 
    products p
    LEFT JOIN categories c ON p.category_id = c.category_id
    LEFT JOIN inventory i ON p.product_id = i.product_id
    LEFT JOIN order_items o ON p.product_id = o.product_id
WHERE 
    p.active = true
    AND i.quantity_in_stock > 0
GROUP BY 
    p.product_id,
    p.name,
    c.category_name,
    i.quantity_in_stock
ORDER BY 
    total_orders DESC;
