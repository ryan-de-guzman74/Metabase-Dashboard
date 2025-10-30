#!/bin/bash
# =========================================================
# 🧠 ETL MASTER AGGREGATION SCRIPT (SKU-based join)
# Purpose: Consolidate all WooCommerce country stores into woo_master_orders
# =========================================================
set -e
source ./etl_config.env

echo "🚀 Building master_orders and dimension tables from all country stores..."

# ---------------------------------------------------------
# 1️⃣ Truncate master table
# ---------------------------------------------------------
mysql -h "$LOCAL_HOST" -P "$LOCAL_PORT" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
USE woo_master_orders;
TRUNCATE TABLE master_orders;
"
mysql -h "$LOCAL_HOST" -P "$LOCAL_PORT" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
SET GLOBAL max_allowed_packet = 1073741824;
SET GLOBAL net_buffer_length = 1000000;
SET GLOBAL wait_timeout = 28800;
SET GLOBAL interactive_timeout = 28800;
"
# ---------------------------------------------------------
# 2️⃣ Loop through all Woo stores
# ---------------------------------------------------------
for COUNTRY in  TR DE FR NL BE BEFRLU AT DK ES IT SE FI PT CZ HU RO SK UK OPS; do
  DB_NAME="woo_${COUNTRY,,}"
  echo "🔗 Processing data from $DB_NAME ..."

  mysql -h "$LOCAL_HOST" -P "$LOCAL_PORT" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
  USE woo_master_orders;

  INSERT INTO master_orders (
    order_number, order_date, customer_id, product_id, sku,
    country_code, channel, quantity, unit_price, total_price,
    tax_amount, shipping_fee, refunded_amount, discount_amount,
    cogs, logistics_cost, ads_cost, marketplace_fee, other_costs,
    net_profit, net_margin, category, product_name, customer_name,
    city, state, country, source_store
  )
  SELECT 
    o.order_number,
    o.order_date,
    o.customer_id,
    o.product_id,
    COALESCE(pim.sku, p.sku) AS sku,
    o.country_code,
    o.channel,
    COALESCE(o.quantity, 1),
    COALESCE(p.retail_price, 0) AS unit_price,
    COALESCE(o.total_price, 0),
    COALESCE(o.tax_amount, 0),
    COALESCE(o.shipping_fee, 0),
    COALESCE(o.refunded_amount, 0),
    COALESCE(o.discount_amount, 0),

    -- 🔧 UPDATED: COGS from PIM (TRY currency)
    COALESCE(pim.cost_price, 0) AS cogs,
    0 AS logistics_cost,
    0 AS ads_cost,
    0 AS marketplace_fee,
    0 AS other_costs,

    -- 🔧 UPDATED: Profit & margin from PIM cost
    COALESCE(o.total_price, 0) - COALESCE(pim.cost_price, 0) AS net_profit,
    CASE 
      WHEN o.total_price > 0 THEN ROUND(((o.total_price - pim.cost_price)/o.total_price)*100, 2)
      ELSE 0 
    END AS net_margin,

    COALESCE(pim.category, p.category) AS category,
    COALESCE(pim.product_name, p.product_name) AS product_name,
    c.customer_name,
    c.city,
    c.state,
    c.country,
    '$DB_NAME'
  FROM ${DB_NAME}.orders o
  -- 🔧 FIXED: correct joins
  LEFT JOIN ${DB_NAME}.products p 
    ON o.product_id = p.product_id
  LEFT JOIN woo_pim.products pim 
    ON pim.product_id = p.sku  -- PIM post_id = SKU in store DBs
  LEFT JOIN ${DB_NAME}.customers c 
    ON o.customer_id = c.customer_id;
  "
done

echo "✅ All stores merged successfully."

# ---------------------------------------------------------
# 3️⃣ Refresh dim_products
# ---------------------------------------------------------
echo "📦 Refreshing dim_products ..."
mysql -h "$LOCAL_HOST" -P "$LOCAL_PORT" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
USE woo_master_orders;
TRUNCATE TABLE dim_products;
INSERT INTO dim_products (product_id, sku, product_name, category, cost_price, retail_price, created_at)
SELECT product_id, sku, product_name, category, cost_price, retail_price, created_at
FROM woo_pim.products
WHERE product_id IS NOT NULL;
"

# ---------------------------------------------------------
# 4️⃣ Refresh dim_customers
# ---------------------------------------------------------
echo "🧍 Refreshing dim_customers ..."
mysql -h "$LOCAL_HOST" -P "$LOCAL_PORT" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
USE woo_master_orders;
TRUNCATE TABLE dim_customers;
"

for COUNTRY in TR DE FR NL BE BEFRLU AT DK ES IT SE FI PT CZ HU RO SK UK OPS; do
  DB_NAME="woo_${COUNTRY,,}"
  mysql -h "$LOCAL_HOST" -P "$LOCAL_PORT" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
  USE woo_master_orders;
  INSERT IGNORE INTO dim_customers (customer_id, customer_name, email, country, state, city, created_at)
  SELECT customer_id, customer_name, email, country, state, city, created_at
  FROM ${DB_NAME}.customers
  WHERE customer_id IS NOT NULL;
  "
done

# ---------------------------------------------------------
# 5️⃣ Refresh config_costs
# ---------------------------------------------------------
echo "⚙️ Ensuring config_costs baseline exists ..."
mysql -h "$LOCAL_HOST" -P "$LOCAL_PORT" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
USE woo_master_orders;
INSERT IGNORE INTO config_costs (country_code, cost_type, cost_value)
VALUES 
('TR', 'LOGISTICS', 4.5),
('TR', 'ADS', 2.0),
('DE', 'MARKETPLACE', 3.0),
('UK', 'MARKETPLACE', 5.0)
ON DUPLICATE KEY UPDATE updated_at = NOW();
"

echo "🎯 ETL for woo_master_orders + dimensions completed successfully!"

