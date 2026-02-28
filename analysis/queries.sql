-- =============================================================================
-- queries.sql  —  Retail Data Warehouse · KPI Queries
-- =============================================================================

-- ── 1. Revenue Overview ───────────────────────────────────────────────────────
-- Total gross revenue, net revenue, COGS and gross margin (completed orders only)
SELECT
    SUM(gross_revenue)                                  AS total_gross_revenue,
    SUM(net_revenue)                                    AS total_net_revenue,
    SUM(cogs)                                           AS total_cogs,
    SUM(gross_margin)                                   AS total_gross_margin,
    ROUND(SUM(gross_margin) / SUM(net_revenue) * 100, 2) AS margin_pct
FROM fact_sales
WHERE status = 'completed';


-- ── 2. Monthly Revenue Trend ──────────────────────────────────────────────────
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id)      AS total_orders,
    SUM(f.net_revenue)              AS net_revenue,
    SUM(f.gross_margin)             AS gross_margin,
    ROUND(AVG(f.net_revenue), 2)    AS avg_order_value
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.status = 'completed'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- ── 3. Revenue by Sales Channel ───────────────────────────────────────────────
SELECT
    c.channel_name,
    c.channel_type,
    COUNT(DISTINCT f.order_id)                               AS total_orders,
    SUM(f.net_revenue)                                       AS net_revenue,
    ROUND(SUM(f.net_revenue) * 100.0 /
          SUM(SUM(f.net_revenue)) OVER (), 2)                AS revenue_share_pct
FROM fact_sales f
JOIN dim_channel c ON f.channel_id = c.channel_id
WHERE f.status = 'completed'
GROUP BY c.channel_name, c.channel_type
ORDER BY net_revenue DESC;


-- ── 4. Top 10 Products by Net Revenue ────────────────────────────────────────
SELECT
    p.product_name,
    p.category,
    SUM(f.quantity)                 AS units_sold,
    SUM(f.net_revenue)              AS net_revenue,
    SUM(f.gross_margin)             AS gross_margin,
    ROUND(SUM(f.gross_margin) /
          SUM(f.net_revenue) * 100, 2) AS margin_pct
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
WHERE f.status = 'completed'
GROUP BY p.product_id
ORDER BY net_revenue DESC
LIMIT 10;


-- ── 5. Revenue by Product Category ───────────────────────────────────────────
SELECT
    p.category,
    COUNT(DISTINCT f.order_id)      AS total_orders,
    SUM(f.quantity)                 AS units_sold,
    SUM(f.net_revenue)              AS net_revenue,
    SUM(f.gross_margin)             AS gross_margin,
    ROUND(SUM(f.gross_margin) /
          SUM(f.net_revenue) * 100, 2) AS margin_pct
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
WHERE f.status = 'completed'
GROUP BY p.category
ORDER BY net_revenue DESC;


-- ── 6. Customer Segments Performance ─────────────────────────────────────────
SELECT
    c.segment,
    COUNT(DISTINCT c.customer_id)   AS total_customers,
    COUNT(DISTINCT f.order_id)      AS total_orders,
    SUM(f.net_revenue)              AS net_revenue,
    ROUND(SUM(f.net_revenue) /
          COUNT(DISTINCT c.customer_id), 2) AS revenue_per_customer
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.status = 'completed'
GROUP BY c.segment
ORDER BY net_revenue DESC;


-- ── 7. Top 10 Customers by Lifetime Value ────────────────────────────────────
SELECT
    c.customer_id,
    c.full_name,
    c.city,
    c.segment,
    COUNT(DISTINCT f.order_id)      AS total_orders,
    SUM(f.net_revenue)              AS lifetime_value,
    MIN(d.full_date)                AS first_purchase,
    MAX(d.full_date)                AS last_purchase
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date     d ON f.date_id     = d.date_id
WHERE f.status = 'completed'
GROUP BY c.customer_id
ORDER BY lifetime_value DESC
LIMIT 10;


-- ── 8. Return & Cancellation Rate ────────────────────────────────────────────
SELECT
    status,
    COUNT(DISTINCT order_id)                                AS total_orders,
    ROUND(COUNT(DISTINCT order_id) * 100.0 /
          SUM(COUNT(DISTINCT order_id)) OVER (), 2)         AS pct_of_orders,
    SUM(gross_revenue)                                      AS gross_revenue_at_risk
FROM fact_sales
GROUP BY status
ORDER BY total_orders DESC;


-- ── 9. Weekend vs Weekday Sales ───────────────────────────────────────────────
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT f.order_id)      AS total_orders,
    SUM(f.net_revenue)              AS net_revenue,
    ROUND(AVG(f.net_revenue), 2)    AS avg_item_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.status = 'completed'
GROUP BY day_type;


-- ── 10. Year-over-Year Growth ─────────────────────────────────────────────────
WITH yearly AS (
    SELECT
        d.year,
        SUM(f.net_revenue) AS net_revenue
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.status = 'completed'
    GROUP BY d.year
)
SELECT
    y.year,
    y.net_revenue,
    LAG(y.net_revenue) OVER (ORDER BY y.year) AS prev_year_revenue,
    ROUND((y.net_revenue - LAG(y.net_revenue) OVER (ORDER BY y.year)) /
           LAG(y.net_revenue) OVER (ORDER BY y.year) * 100, 2) AS yoy_growth_pct
FROM yearly y
ORDER BY y.year;
