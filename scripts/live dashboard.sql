SELECT * FROM biz_user_segments 
ORDER BY total_lifetime_spend DESC 
LIMIT 10;

SELECT * FROM biz_supplier_performance 
ORDER BY total_profit_generated DESC;

SELECT * FROM biz_category_market_share 
ORDER BY market_share_pct DESC;

SELECT * FROM biz_hourly_sales_velocity 
ORDER BY sale_hour ASC;

SELECT * FROM biz_top_selling_products 
ORDER BY units_sold DESC;

SELECT * FROM batch_profit_analysis 
LIMIT 10;