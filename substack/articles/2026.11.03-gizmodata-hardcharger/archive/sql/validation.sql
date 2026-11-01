SELECT
    COUNT(*) AS output_rows,
    SUM(CASE WHEN order_count < 0 OR line_item_count < order_count THEN 1 ELSE 0 END)
        AS invalid_counts,
    SUM(CASE
        WHEN ABS(net_revenue - (gross_revenue - discount_amount)) > 0.01 THEN 1
        ELSE 0
    END) AS invalid_revenue
FROM analytics.main.daily_sales
WHERE order_date = CAST(? AS DATE);
