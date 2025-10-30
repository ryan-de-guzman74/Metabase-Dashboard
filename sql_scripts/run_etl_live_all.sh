#!/bin/bash
set -e
set -o pipefail

# =========================================================
# 🌍 Environment Detection — Works in Docker & Host
# =========================================================
if [ -f "/.dockerenv" ] || grep -qa "docker" /proc/1/cgroup 2>/dev/null; then
  ENVIRONMENT="docker"
else
  ENVIRONMENT="host"
fi

# =========================================================
# 🌍 Universal Path Setup — Works on Both Docker & Ubuntu
# =========================================================

# Find script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Determine environment (Docker or Host)
if grep -q docker /proc/1/cgroup 2>/dev/null; then
  ENVIRONMENT="docker"
else
  ENVIRONMENT="host"
fi

# Load configuration
if [ -f "$SCRIPT_DIR/etl_config.env" ]; then
  source "$SCRIPT_DIR/etl_config.env"
elif [ -f "$SCRIPT_DIR/../etl_config.env" ]; then
  source "$SCRIPT_DIR/../etl_config.env"
else
  echo "❌ etl_config.env not found near $SCRIPT_DIR"
  exit 1
fi

# Set log directory
if [ "$ENVIRONMENT" = "docker" ]; then
  LOG_DIR="/app/logs"
else
  LOG_DIR="$SCRIPT_DIR/logs"
fi
mkdir -p "$LOG_DIR"

# Log setup
LOG_FILE="$LOG_DIR/etl_$(date +%Y-%m-%d).log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "🌍 Running in $ENVIRONMENT environment"
echo "📂 Logs: $LOG_FILE"


# =========================================================
# 🧩 WooCommerce ETL per Country
# =========================================================
run_etl() {
  COUNTRY=$1
  echo "🚀 Starting ETL for $COUNTRY ..."
  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS[$COUNTRY]}"

  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for $COUNTRY"
    return
  fi

  # =========================================================
  # 🟡 1️⃣ Extract Orders
  # =========================================================
  echo "📦 Extracting Orders for $COUNTRY ..."
  mysql -h "$HOST" -P 3306 -u "$USER" -p"$PASS" "$DB" -e "
    SELECT DISTINCT
      p.ID AS order_id,
      p.post_date AS order_date,
      p.post_status AS order_status,
      MAX(CASE WHEN pm.meta_key = '_customer_user' THEN pm.meta_value END) AS customer_id,
      '$COUNTRY' AS country_code,
      'WOO-$COUNTRY' AS channel,
      SUBSTRING_INDEX(
        SUBSTRING_INDEX(
          MAX(CASE WHEN pm.meta_key = '_wcst_order_track_http_url' THEN pm.meta_value END),
          '/', 3
        ),
        '/', -1
      ) AS source_site,
      MAX(CASE WHEN pm.meta_key = '_billing_country' THEN pm.meta_value END) AS billing_country,
      MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END) AS billing_city,

      COALESCE((
        SELECT COUNT(DISTINCT COALESCE(NULLIF(pl_v.sku, ''), NULLIF(pl_p.sku, ''), oim_prod.meta_value))
        FROM wp_woocommerce_order_items oi
        LEFT JOIN wp_woocommerce_order_itemmeta oim_prod
          ON oi.order_item_id = oim_prod.order_item_id
          AND oim_prod.meta_key = '_product_id'
        LEFT JOIN wp_woocommerce_order_itemmeta oim_var
          ON oi.order_item_id = oim_var.order_item_id
          AND oim_var.meta_key = '_variation_id'
        LEFT JOIN wp_wc_product_meta_lookup pl_p
          ON pl_p.product_id = CAST(oim_prod.meta_value AS UNSIGNED)
        LEFT JOIN wp_wc_product_meta_lookup pl_v
          ON pl_v.product_id = CAST(oim_var.meta_value AS UNSIGNED)
        WHERE oi.order_item_type = 'line_item'
          AND oi.order_id = p.ID
      ), 0) AS units_total,

      -- ✅ Ordered items count
      COALESCE((
        SELECT SUM(CAST(oim.meta_value AS DECIMAL(12,2)))
        FROM wp_woocommerce_order_items oi
        JOIN wp_woocommerce_order_itemmeta oim
          ON oi.order_item_id = oim.order_item_id
        WHERE oi.order_item_type = 'line_item'
          AND oim.meta_key = '_qty'
          AND oi.order_id = p.ID
      ), 0) AS ordered_items_count,

      -- ✅ Ordered item SKUs
      COALESCE((
        SELECT GROUP_CONCAT(DISTINCT
          CASE
            WHEN (pl_p.sku IS NULL OR pl_p.sku = '') AND (pl_v.sku IS NULL OR pl_v.sku = '')
              THEN '(Unregistered SKU)'
            WHEN pl_v.sku IS NOT NULL AND pl_v.sku <> pl_p.sku
              THEN CONCAT(pl_p.sku, '(', pl_v.sku, ')')
            ELSE COALESCE(pl_p.sku, pl_v.sku, '(Unregistered SKU)')
          END SEPARATOR ', ')
        FROM wp_woocommerce_order_items oi
        LEFT JOIN wp_woocommerce_order_itemmeta oim_prod
          ON oi.order_item_id = oim_prod.order_item_id
          AND oim_prod.meta_key = '_product_id'
        LEFT JOIN wp_woocommerce_order_itemmeta oim_var
          ON oi.order_item_id = oim_var.order_item_id
          AND oim_var.meta_key = '_variation_id'
        LEFT JOIN wp_wc_product_meta_lookup pl_p
          ON pl_p.product_id = CAST(oim_prod.meta_value AS UNSIGNED)
        LEFT JOIN wp_wc_product_meta_lookup pl_v
          ON pl_v.product_id = CAST(oim_var.meta_value AS UNSIGNED)
        WHERE oi.order_item_type = 'line_item'
          AND oi.order_id = p.ID
      ), '') AS ordered_items_skus,

      MAX(CASE WHEN pm.meta_key = '_payment_method_title' THEN pm.meta_value END) AS payment_method,
      MAX(CASE WHEN pm.meta_key = '_order_currency' THEN pm.meta_value END) AS currency_code,
      MAX(CASE WHEN pm.meta_key = '_order_total' THEN pm.meta_value END) AS total_price,

      -- ✅ Gross Total Calculation
      COALESCE((
        (
          SELECT SUM(CAST(oim.meta_value AS DECIMAL(12,2)))
          FROM wp_woocommerce_order_items oi
          JOIN wp_woocommerce_order_itemmeta oim
            ON oi.order_item_id = oim.order_item_id
          WHERE oi.order_item_type = 'line_item'
            AND oim.meta_key = '_line_subtotal'
            AND oi.order_id = p.ID
        )
        + COALESCE(MAX(CASE WHEN pm.meta_key = '_order_tax' THEN pm.meta_value END), 0)
        + COALESCE((
          SELECT SUM(CAST(oim.meta_value AS DECIMAL(12,2)))
          FROM wp_woocommerce_order_items oi
          JOIN wp_woocommerce_order_itemmeta oim
            ON oi.order_item_id = oim.order_item_id
          WHERE oi.order_item_type = 'fee'
            AND oim.meta_key = '_fee_amount'
            AND oi.order_id = p.ID
        ), 0)
        + COALESCE((
          SELECT SUM(CAST(oim.meta_value AS DECIMAL(12,2)))
          FROM wp_woocommerce_order_items oi
          JOIN wp_woocommerce_order_itemmeta oim
            ON oi.order_item_id = oim.order_item_id
          WHERE oi.order_item_type = 'shipping'
            AND oim.meta_key = 'cost'
            AND oi.order_id = p.ID
        ), 0)
      ), 0) AS gross_total,

      -- ✅ Subtotal
      COALESCE((
        SELECT SUM(CAST(oim.meta_value AS DECIMAL(12,4)))
        FROM wp_woocommerce_order_items oi
        INNER JOIN wp_woocommerce_order_itemmeta oim
          ON oi.order_item_id = oim.order_item_id
        WHERE oi.order_item_type = 'line_item'
          AND oim.meta_key = '_line_subtotal'
          AND oi.order_id = p.ID
      ), 0) AS subtotal,

      -- ✅ COGS
      COALESCE((
        SELECT SUM(
          CAST(oim_qty.meta_value AS DECIMAL(12,2)) *
          CAST(pm_costs.meta_value AS DECIMAL(12,2))
        )
        FROM wp_woocommerce_order_items oi
        JOIN wp_woocommerce_order_itemmeta oim_qty
          ON oi.order_item_id = oim_qty.order_item_id
          AND oim_qty.meta_key = '_qty'
        JOIN wp_woocommerce_order_itemmeta oim_pid
          ON oi.order_item_id = oim_pid.order_item_id
          AND oim_pid.meta_key = '_product_id'
        JOIN wp_postmeta pm_costs
          ON pm_costs.post_id = oim_pid.meta_value
          AND pm_costs.meta_key IN ('_alg_wc_cog_cost', '_wc_cog_cost')
        WHERE oi.order_item_type = 'line_item'
          AND oi.order_id = p.ID
      ), 0) AS cogs,

      MAX(CASE WHEN pm.meta_key = '_order_tax' THEN pm.meta_value END) AS tax_amount,

      (SELECT oim.meta_value
       FROM wp_woocommerce_order_items oi
       JOIN wp_woocommerce_order_itemmeta oim
         ON oim.order_item_id = oi.order_item_id
       WHERE oi.order_item_type = 'shipping'
         AND oim.meta_key = 'cost'
         AND oi.order_id = p.ID
       LIMIT 1) AS shipping_fee,

      (SELECT oim.meta_value
       FROM wp_woocommerce_order_items oi
       JOIN wp_woocommerce_order_itemmeta oim
         ON oim.order_item_id = oi.order_item_id
       WHERE oi.order_item_type = 'fee'
         AND oim.meta_key = '_fee_amount'
         AND oi.order_id = p.ID
       LIMIT 1) AS fee_amount,

      MAX(CASE WHEN pm.meta_key = '_cart_discount' THEN pm.meta_value END) AS discount_amount,
      MAX(CASE WHEN pm.meta_key = '_refund_amount' THEN pm.meta_value END) AS refunded_amount,

      0 AS ads_spend,
      0 AS logistics_cost,
      0 AS other_costs,
      0 AS net_profit,
      0 AS net_revenue,
      0 AS net_margin

    FROM wp_posts p
    LEFT JOIN wp_postmeta pm ON p.ID = pm.post_id
    WHERE p.post_type IN ('shop_order', 'shop_order_refund')
    GROUP BY p.ID;
  " > "temp_${COUNTRY}_orders.tsv"

  echo "📥 Loading Orders into local DB..."
  echo "🧱 Ensuring local database woo_${COUNTRY,,} exists..."
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    CREATE DATABASE IF NOT EXISTS woo_${COUNTRY,,};
  "

  # =========================================================
  # 📥 Load Orders into Local Database
  # =========================================================
  echo "🧱 Ensuring tables exist in woo_${COUNTRY,,}..."
  if [ "$COUNTRY" != "TR" ]; then
    mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
      USE woo_${COUNTRY,,};
      DROP TABLE IF EXISTS orders;
      CREATE TABLE IF NOT EXISTS orders LIKE woo_tr.orders;
    "
  else
    echo "⚙️ Skipping table clone for TR (base schema already exists)."
  fi

  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_${COUNTRY,,};
    TRUNCATE TABLE orders;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_orders.tsv'
    INTO TABLE orders
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (
      order_id, order_date, order_status, customer_id, country_code, channel, site,
      billing_country, billing_city, units_total, ordered_items_count,
      ordered_items_skus, payment_method, currency_code, total_price,
      gross_total, subtotal, cogs, tax_amount, shipping_fee, fee_amount,
      discount_amount, refunded_amount, ads_spend, logistics_cost,
      other_costs, net_profit, net_revenue, net_margin
    );
  "
  rm -f "temp_${COUNTRY}_orders.tsv"
  echo "✅ Orders for $COUNTRY loaded successfully."


  # =========================================================
  # 🟡 2️⃣ Extract Order Items (No SKU Yet)
  # =========================================================
  echo "📦 Extracting Order Items for $COUNTRY ..."
  mysql -h "$HOST" -P 3306 -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      oi.order_item_id,
      oi.order_id,
      MAX(CASE WHEN oim.meta_key = '_product_id' THEN oim.meta_value END) AS product_id,
      MAX(CASE WHEN oim.meta_key = '_variation_id' THEN oim.meta_value END) AS variation_id,
      '' AS sku,
      oi.order_item_name,
      MAX(CASE WHEN oim.meta_key = '_qty' THEN oim.meta_value END) AS quantity,
      MAX(CASE WHEN oim.meta_key = '_line_total' THEN oim.meta_value END) AS line_total,
      MAX(CASE WHEN oim.meta_key = '_line_tax' THEN oim.meta_value END) AS line_tax,
      MAX(CASE WHEN oim.meta_key = '_refunded_item_id' THEN oim.meta_value END) AS refund_reference,
      MAX(CASE WHEN pm.meta_key = '_order_currency' THEN pm.meta_value END) AS currency_code,
      p.post_date AS created_at
    FROM wp_woocommerce_order_items oi
    LEFT JOIN wp_woocommerce_order_itemmeta oim
      ON oi.order_item_id = oim.order_item_id
    LEFT JOIN wp_posts p
      ON oi.order_id = p.ID
    LEFT JOIN wp_postmeta pm
      ON p.ID = pm.post_id AND pm.meta_key = '_order_currency'
    WHERE oi.order_item_type = 'line_item'
    GROUP BY oi.order_item_id, oi.order_id, oi.order_item_name;
  " > "temp_${COUNTRY}_order_items.tsv"

  echo "📥 Loading Order Items (no SKU yet) into local DB..."
  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_${COUNTRY,,};
    DROP TABLE IF EXISTS order_items;
    CREATE TABLE IF NOT EXISTS order_items LIKE woo_tr.order_items;
    TRUNCATE TABLE order_items;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_order_items.tsv'
    INTO TABLE order_items
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
  "
  rm -f "temp_${COUNTRY}_order_items.tsv"
  echo "✅ Order Items for $COUNTRY loaded successfully."


  # =========================================================
  # 🟢 3️⃣ Update SKUs from PIM Database
  # =========================================================
  echo "🔍 Updating SKU values in order_items from PIM ..."
  mysql -h 188.68.58.232 -u bi-dashboard-pim -p5rB4gGW6K76tu6A2gWXs -D mbu-trade-pim -e "
    SELECT product_id, sku
    FROM wp_wc_product_meta_lookup
    WHERE sku IS NOT NULL AND sku <> '';
  " > temp_pim_sku.tsv

  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_${COUNTRY,,};
    DROP TABLE IF EXISTS pim_sku_map;
    CREATE TABLE pim_sku_map (
      product_id BIGINT PRIMARY KEY,
      sku VARCHAR(100)
    );
    LOAD DATA LOCAL INFILE '$(pwd)/temp_pim_sku.tsv'
    INTO TABLE pim_sku_map
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
  "

  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_${COUNTRY,,};
    UPDATE order_items oi
    LEFT JOIN pim_sku_map ps_parent ON ps_parent.product_id = oi.product_id
    LEFT JOIN pim_sku_map ps_var ON ps_var.product_id = oi.variation_id
    SET oi.sku = CASE
      WHEN oi.variation_id IS NULL OR oi.variation_id = 0 THEN ps_parent.sku
      WHEN ps_parent.sku IS NULL THEN ps_var.sku
      WHEN ps_var.sku IS NULL THEN ps_parent.sku
      ELSE CONCAT(ps_parent.sku, '(', ps_var.sku, ')')
    END;
    DROP TABLE IF EXISTS pim_sku_map;
  "
  rm -f temp_pim_sku.tsv
  echo "✅ SKU values updated successfully for $COUNTRY."
  
  

  # =========================================================
  # 💰 Refresh PIM COGS and Update ${COUNTRY} Orders
  # =========================================================
  echo "💰 Updating COGS for $COUNTRY ..."

  # --- Pull latest COGS from remote PIM ---
  IFS=',' read -r PIM_HOST PIM_DB PIM_USER PIM_PASS <<< "${REMOTE_DBS["PIM"]}"
  mysql -h "$PIM_HOST" -u "$PIM_USER" -p"$PIM_PASS" -D "$PIM_DB" -e "
    SELECT p.ID AS product_id,
           MAX(CAST(pm.meta_value AS DECIMAL(12,4))) AS cog_value
    FROM wp_postmeta pm
    JOIN wp_posts p ON p.ID = pm.post_id
    WHERE pm.meta_key IN ('_wc_cog_cost','_alg_wc_cog_cost')
      AND pm.meta_value REGEXP '^[0-9]+(\\.[0-9]+)?$'
      AND CAST(pm.meta_value AS DECIMAL(12,4)) > 0
    GROUP BY p.ID;
  " > temp_pim_cogs.tsv

  # --- Load into local staging table ---
  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    CREATE DATABASE IF NOT EXISTS woo_master;
    USE woo_master;
    DROP TABLE IF EXISTS pim_cogs;
    CREATE TABLE pim_cogs (
      product_id BIGINT PRIMARY KEY,
      cog_value DECIMAL(12,4)
    );
    LOAD DATA LOCAL INFILE '$(pwd)/temp_pim_cogs.tsv'
    INTO TABLE pim_cogs
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;
  "
  rm -f temp_pim_cogs.tsv

  # --- Update COGS in local orders ---
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_${COUNTRY,,};
    UPDATE orders o
    JOIN (
      SELECT 
        oi.order_id,
        SUM(
          CAST(oi.quantity AS DECIMAL(12,4)) *
          CAST(pc.cog_value AS DECIMAL(12,4))
        ) AS total_cogs
      FROM order_items oi
      JOIN woo_master.pim_cogs pc
        ON pc.product_id = oi.product_id
      GROUP BY oi.order_id
    ) calc ON o.order_id = calc.order_id
    SET o.cogs = calc.total_cogs;
  "
  # Drop the staging table afterward
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    DROP TABLE IF EXISTS pim_cogs;
  "
  echo "✅ COGS updated for $COUNTRY."


  # =========================================================
  # 💵 Calculate and Update Net Revenue, Profit, Margin
  # =========================================================
  echo "📊 Calculating Net Revenue, Net Profit, and Net Margin for $COUNTRY ..."
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_${COUNTRY,,};

    -- 🧩 Step 1: Ensure all numeric fields are non-null
    UPDATE orders
    SET 
      total_price      = COALESCE(total_price, 0),
      gross_total      = COALESCE(gross_total, 0),
      discount_amount  = COALESCE(discount_amount, 0),
      refunded_amount  = COALESCE(refunded_amount, 0),
      cogs             = COALESCE(cogs, 0),
      logistics_cost   = COALESCE(logistics_cost, 0),
      ads_spend        = COALESCE(ads_spend, 0),
      other_costs      = COALESCE(other_costs, 0);

    -- 🧮 Step 2: Compute profit metrics
    UPDATE orders
    SET 
      net_revenue = ROUND(
        (gross_total - discount_amount - refunded_amount),
        2
      ),
      net_profit = ROUND(
        (gross_total - discount_amount - refunded_amount)
        - (cogs + logistics_cost + ads_spend + other_costs),
        2
      ),
      net_margin = CASE 
        WHEN (gross_total - discount_amount - refunded_amount) > 0
          THEN GREATEST(
            LEAST(
              ROUND(
                (
                  ((gross_total - discount_amount - refunded_amount)
                  - (cogs + logistics_cost + ads_spend + other_costs))
                  / (gross_total - discount_amount - refunded_amount)
                ) * 100,
                2
              ),
              9999.99
            ),
            -9999.99
          )
        ELSE 0
      END;
  "
  echo "✅ Net Revenue, Net Profit, and Net Margin updated for $COUNTRY."



  
  # =========================================================
  # 🟢 4️⃣ Extract and Build Customers Table (per store)
  # =========================================================
  echo "👤 Extracting and aggregating Customers for $COUNTRY ..."

  # Step 1️⃣: Extract base customer info
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      u.ID AS customer_id,
      TRIM(
        CONCAT_WS(' ',
          NULLIF(CONCAT_WS(' ', COALESCE(fn.meta_value, ''), COALESCE(ln.meta_value, '')), ''),
          NULLIF(CONCAT_WS(' ', COALESCE(bfn.meta_value, ''), COALESCE(bln.meta_value, '')), ''),
          NULLIF(CONCAT_WS(' ', COALESCE(sfn.meta_value, ''), COALESCE(sln.meta_value, '')), '')
        )
      ) AS full_name,
      LOWER(u.user_email) AS email,
      bp.meta_value AS phone,
      u.user_registered AS registered_at,
      bc.meta_value AS billing_country,
      bci.meta_value AS billing_city
    FROM wp_users u
    LEFT JOIN wp_usermeta fn  ON u.ID = fn.user_id  AND fn.meta_key  = 'first_name'
    LEFT JOIN wp_usermeta ln  ON u.ID = ln.user_id  AND ln.meta_key  = 'last_name'
    LEFT JOIN wp_usermeta bfn ON u.ID = bfn.user_id AND bfn.meta_key = 'billing_first_name'
    LEFT JOIN wp_usermeta bln ON u.ID = bln.user_id AND bln.meta_key = 'billing_last_name'
    LEFT JOIN wp_usermeta sfn ON u.ID = sfn.user_id AND sfn.meta_key = 'shipping_first_name'
    LEFT JOIN wp_usermeta sln ON u.ID = sln.user_id AND sln.meta_key = 'shipping_last_name'
    LEFT JOIN wp_usermeta bp  ON u.ID = bp.user_id  AND bp.meta_key  = 'billing_phone'
    LEFT JOIN wp_usermeta bc  ON u.ID = bc.user_id  AND bc.meta_key  = 'billing_country'
    LEFT JOIN wp_usermeta bci ON u.ID = bci.user_id AND bci.meta_key = 'billing_city';
  " > "temp_${COUNTRY}_customers_base.tsv"

  # Step 2️⃣: Aggregate order metrics
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      CAST(pm.meta_value AS UNSIGNED) AS customer_id,
      MIN(p.post_date) AS first_order_date,
      MAX(p.post_date) AS last_order_date,
      COUNT(p.ID) AS orders_count,
      SUM(CASE WHEN pm2.meta_key = '_order_total'
               THEN CAST(pm2.meta_value AS DECIMAL(12,2)) ELSE 0 END) AS ltv
    FROM wp_posts p
    JOIN wp_postmeta pm  ON p.ID = pm.post_id  AND pm.meta_key = '_customer_user'
    LEFT JOIN wp_postmeta pm2 ON p.ID = pm2.post_id AND pm2.meta_key = '_order_total'
    WHERE p.post_type = 'shop_order'
      AND pm.meta_value IS NOT NULL AND pm.meta_value <> ''
    GROUP BY pm.meta_value;
  " > "temp_${COUNTRY}_orders_agg.tsv"

  # Step 3️⃣: Aggregate units sold
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      CAST(pm.meta_value AS UNSIGNED) AS customer_id,
      SUM(CASE WHEN oi.order_item_type = 'line_item' THEN 1 ELSE 0 END) AS units_total
    FROM wp_posts p
    JOIN wp_postmeta pm ON p.ID = pm.post_id AND pm.meta_key = '_customer_user'
    JOIN wp_woocommerce_order_items oi ON p.ID = oi.order_id
    WHERE p.post_type = 'shop_order'
    GROUP BY pm.meta_value;
  " > "temp_${COUNTRY}_units_agg.tsv"

  # Step 3b️⃣: Aggregate refunds
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      CAST(cust.meta_value AS UNSIGNED) AS customer_id,
      SUM(CAST(COALESCE(rm.meta_value, 0) AS DECIMAL(12,2))) AS refunds_total
    FROM wp_posts refund
    LEFT JOIN wp_postmeta rm
      ON refund.ID = rm.post_id AND rm.meta_key = '_refund_amount'
    LEFT JOIN wp_posts parent
      ON refund.post_parent = parent.ID
    LEFT JOIN wp_postmeta cust
      ON parent.ID = cust.post_id AND cust.meta_key = '_customer_user'
    WHERE refund.post_type = 'shop_order_refund'
      AND cust.meta_value IS NOT NULL
    GROUP BY cust.meta_value;
  " > "temp_${COUNTRY}_refunds_agg.tsv"

  # Step 4️⃣: Merge and load into local DB
  echo "📥 Loading combined Customers data into woo_${COUNTRY,,}.customers ..."
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_${COUNTRY,,};
    DROP TABLE IF EXISTS customers;
    CREATE TABLE IF NOT EXISTS customers LIKE woo_tr.customers;
    TRUNCATE TABLE customers;
  "

  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_${COUNTRY,,};
    CREATE TEMPORARY TABLE base (
      customer_id BIGINT, full_name VARCHAR(255), email VARCHAR(255),
      phone VARCHAR(50), registered_at DATETIME,
      billing_country VARCHAR(100), billing_city VARCHAR(100)
    );
    LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_customers_base.tsv'
    INTO TABLE base
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;

    CREATE TEMPORARY TABLE orders_agg (
      customer_id BIGINT, first_order_date DATETIME,
      last_order_date DATETIME, orders_count INT, ltv DECIMAL(12,2)
    );
    LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_orders_agg.tsv'
    INTO TABLE orders_agg
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;

    CREATE TEMPORARY TABLE units_agg (customer_id BIGINT, units_total INT);
    LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_units_agg.tsv'
    INTO TABLE units_agg
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;

    CREATE TEMPORARY TABLE refunds_agg (customer_id BIGINT, refunds_total DECIMAL(12,2));
    LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_refunds_agg.tsv'
    INTO TABLE refunds_agg
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;

    INSERT INTO customers (
      customer_id, full_name, email, phone, registered_at,
      first_order_date, last_order_date,
      orders_count, units_total, ltv, aov,
      refunds_total, source_store,
      billing_country, billing_city
    )
    SELECT
      b.customer_id,
      MAX(b.full_name) AS full_name,
      MAX(b.email) AS email,
      MAX(b.phone) AS phone,
      MAX(b.registered_at) AS registered_at,
      MAX(o.first_order_date) AS first_order_date,
      MAX(o.last_order_date) AS last_order_date,
      COALESCE(SUM(o.orders_count), 0) AS orders_count,
      COALESCE(SUM(u.units_total), 0) AS units_total,
      COALESCE(SUM(o.ltv), 0) AS ltv,
      CASE WHEN COALESCE(SUM(o.orders_count), 0) > 0
           THEN ROUND(SUM(o.ltv) / SUM(o.orders_count), 2)
           ELSE 0 END AS aov,
      COALESCE(SUM(r.refunds_total), 0) AS refunds_total,
      '$COUNTRY' AS source_store,
      MAX(b.billing_country) AS billing_country,
      MAX(b.billing_city) AS billing_city
    FROM base b
    LEFT JOIN orders_agg  o ON b.customer_id = o.customer_id
    LEFT JOIN units_agg   u ON b.customer_id = u.customer_id
    LEFT JOIN refunds_agg r ON b.customer_id = r.customer_id
    GROUP BY b.customer_id;

    DROP TEMPORARY TABLE base;
    DROP TEMPORARY TABLE orders_agg;
    DROP TEMPORARY TABLE units_agg;
    DROP TEMPORARY TABLE refunds_agg;
  "

  rm -f "temp_${COUNTRY}_customers_base.tsv" \
        "temp_${COUNTRY}_orders_agg.tsv" \
        "temp_${COUNTRY}_units_agg.tsv" \
        "temp_${COUNTRY}_refunds_agg.tsv"

  echo "✅ Customers table built successfully for $COUNTRY."
}

# =========================================================
# 🏗️  SPECIAL ETL: OPS SITE (Marketplace Orders Only)
# =========================================================
run_etl_ops() {
  COUNTRY="OPS"
  echo "🚀 Starting ETL for OPS site (Marketplace Orders Only)..."

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS[$COUNTRY]}"

  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for OPS"
    return
  fi

  # =========================================================
  # 1️⃣ Extract and Load Orders (All Orders First)
  # =========================================================
  echo "📦 Extracting all orders from OPS..."
  mysql  --local-infile=1 -h "$HOST" -P 3306 -u "$USER" -p"$PASS" "$DB" -e "
    SELECT DISTINCT
      p.ID AS order_id,
      p.post_date AS order_date,
      p.post_status AS order_status,
      MAX(CASE WHEN pm.meta_key = '_customer_user' THEN pm.meta_value END) AS customer_id,
      'OPS' AS country_code,
      'WOO-OPS' AS channel,
      MAX(CASE WHEN pm.meta_key = '_billing_country' THEN pm.meta_value END) AS billing_country,
      MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END) AS billing_city,
      MAX(CASE WHEN pm.meta_key = '_payment_method' THEN pm.meta_value END) AS payment_method,
      MAX(CASE WHEN pm.meta_key = '_order_currency' THEN pm.meta_value END) AS currency_code,
      MAX(CASE WHEN pm.meta_key = '_order_total' THEN pm.meta_value END) AS total_price
    FROM wp_posts p
    LEFT JOIN wp_postmeta pm ON p.ID = pm.post_id
    WHERE p.post_type IN ('shop_order','shop_order_refund')
    GROUP BY p.ID;
  " > "temp_ops_orders.tsv"

  echo "📥 Loading OPS orders into local DB..."
  mysql  --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    CREATE DATABASE IF NOT EXISTS woo_ops;
    USE woo_ops;
    DROP TABLE IF EXISTS orders;
    CREATE TABLE orders LIKE woo_tr.orders;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_ops_orders.tsv'
    INTO TABLE orders
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;
  "
  rm -f "temp_ops_orders.tsv"
  echo "✅ OPS orders loaded locally."

  # =========================================================
  # 2️⃣ Extract Order Items (same as standard stores)
  # =========================================================
  echo "📦 Extracting order items for OPS..."
  mysql  --local-infile=1 -h "$HOST" -P 3306 -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      oi.order_item_id,
      oi.order_id,
      MAX(CASE WHEN oim.meta_key = '_product_id' THEN oim.meta_value END) AS product_id,
      MAX(CASE WHEN oim.meta_key = '_variation_id' THEN oim.meta_value END) AS variation_id,
      oi.order_item_name,
      MAX(CASE WHEN oim.meta_key = '_qty' THEN oim.meta_value END) AS quantity,
      MAX(CASE WHEN oim.meta_key = '_line_total' THEN oim.meta_value END) AS line_total,
      MAX(CASE WHEN oim.meta_key = '_line_tax' THEN oim.meta_value END) AS line_tax
    FROM wp_woocommerce_order_items oi
    LEFT JOIN wp_woocommerce_order_itemmeta oim ON oi.order_item_id = oim.order_item_id
    GROUP BY oi.order_item_id, oi.order_id;
  " > "temp_ops_order_items.tsv"

  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_ops;
    DROP TABLE IF EXISTS order_items;
    CREATE TABLE order_items LIKE woo_tr.order_items;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_ops_order_items.tsv'
    INTO TABLE order_items
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;
  "
  rm -f "temp_ops_order_items.tsv"
  echo "✅ OPS order items loaded successfully."

  # =========================================================
  # 3️⃣ Extract Customers
  # =========================================================
  echo "👤 Extracting customers for OPS..."
  mysql  --local-infile=1 -h "$HOST" -P 3306 -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      u.ID AS customer_id,
      CONCAT_WS(' ', fn.meta_value, ln.meta_value) AS full_name,
      LOWER(u.user_email) AS email,
      bp.meta_value AS phone,
      u.user_registered AS registered_at,
      bc.meta_value AS billing_country,
      bci.meta_value AS billing_city
    FROM wp_users u
    LEFT JOIN wp_usermeta fn  ON u.ID = fn.user_id  AND fn.meta_key  = 'first_name'
    LEFT JOIN wp_usermeta ln  ON u.ID = ln.user_id  AND ln.meta_key  = 'last_name'
    LEFT JOIN wp_usermeta bp  ON u.ID = bp.user_id  AND bp.meta_key  = 'billing_phone'
    LEFT JOIN wp_usermeta bc  ON u.ID = bc.user_id  AND bc.meta_key  = 'billing_country'
    LEFT JOIN wp_usermeta bci ON u.ID = bci.user_id AND bci.meta_key = 'billing_city';
  " > "temp_ops_customers.tsv"

  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_ops;
    DROP TABLE IF EXISTS customers;
    CREATE TABLE customers LIKE woo_tr.customers;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_ops_customers.tsv'
    INTO TABLE customers
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;
  "
  rm -f "temp_ops_customers.tsv"
  echo "✅ OPS customers loaded successfully."

  # =========================================================
  # 4️⃣ Merge into woo_master (Filtered)
  # =========================================================
  echo "🧩 Merging OPS marketplace orders (filtered) into woo_master..."
mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
  USE woo_master;

  -- 🧾 Insert Orders (explicit column mapping)
  INSERT INTO orders (
    order_number_formatted, source_store, order_id, order_date, order_status,
    customer_id, country_code, channel, site, billing_country, billing_city,
    units_total, ordered_items_count, ordered_items_skus, payment_method,
    currency_code, subtotal, gross_total, cogs, total_price,
    tax_amount, shipping_fee, fee_amount, discount_amount,
    refunded_amount, ads_spend, logistics_cost, other_costs,
    net_profit, net_revenue, net_margin
  )
  SELECT
    CONCAT('OPS-', o.order_id) AS order_number_formatted,
    'OPS' AS source_store,
    o.order_id, o.order_date, o.order_status, o.customer_id,
    o.country_code, o.channel, NULL AS site,
    o.billing_country, o.billing_city,
    o.units_total, o.ordered_items_count, o.ordered_items_skus,
    o.payment_method, o.currency_code, o.subtotal, o.gross_total,
    o.cogs, o.total_price, o.tax_amount, o.shipping_fee,
    o.fee_amount, o.discount_amount, o.refunded_amount,
    o.ads_spend, o.logistics_cost, o.other_costs,
    o.net_profit, o.net_revenue, o.net_margin
  FROM woo_ops.orders o
  WHERE o.payment_method IN ('other', 'bol');

  -- 📦 Insert Order Items (linked to valid OPS orders)
  INSERT INTO order_items (
    order_item_id, order_id, product_id, variation_id, sku,
    order_item_name, quantity, line_total, line_tax,
    refund_reference, currency_code, source_store
  )
  SELECT
    oi.order_item_id, oi.order_id, oi.product_id, oi.variation_id,
    oi.sku, oi.order_item_name, oi.quantity, oi.line_total,
    oi.line_tax, oi.refund_reference, oi.currency_code,
    'OPS' AS source_store
  FROM woo_ops.order_items oi
  JOIN woo_ops.orders o ON oi.order_id = o.order_id
  WHERE o.payment_method IN ('other', 'bol');

  -- 👥 Insert Customers (linked to valid OPS marketplace orders)
  INSERT INTO customers (
    customer_id, full_name, email, phone, registered_at,
    first_order_date, last_order_date, orders_count, units_total,
    ltv, aov, refunds_total, billing_country, billing_city,
    source_store
  )
  SELECT DISTINCT
    c.customer_id, c.full_name, c.email, c.phone, c.registered_at,
    NULL AS first_order_date, NULL AS last_order_date,
    NULL AS orders_count, NULL AS units_total, NULL AS ltv,
    NULL AS aov, NULL AS refunds_total,
    c.billing_country, c.billing_city,
    'OPS' AS source_store
  FROM woo_ops.customers c
  JOIN woo_ops.orders o ON c.customer_id = o.customer_id
  WHERE o.payment_method IN ('other', 'bol');
"


  echo "✅ OPS ETL (Marketplace Only) completed successfully."
}

# =========================================================
# 🧠 5️⃣ Master Products ETL (OPS + PIM → woo_master.products)
# =========================================================
run_master_products_etl() {
  echo "🧠 Building Master Products Table from OPS ..."
  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["OPS"]}"

  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for OPS"
    return
  fi

  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      p.ID AS product_id,
      p.post_title AS title,
      MAX(CASE WHEN pm.meta_key = '_sku' THEN pm.meta_value END) AS sku,
      MAX(CASE WHEN pm.meta_key = '_main_SKU' THEN pm.meta_value END) AS parent_sku,
      MAX(CASE WHEN pm.meta_key = '_product_attributes' THEN pm.meta_value END) AS attributes,
      MAX(wc.stock_quantity) AS stock_qty,
      NULL AS categories,
      NULL AS tags,
      MAX(CASE WHEN pm.meta_key = '_regular_price' THEN pm.meta_value END) AS regular_price,
      MAX(CASE WHEN pm.meta_key = '_sale_price' THEN pm.meta_value END) AS sale_price,
      MAX(CASE WHEN pm.meta_key = '_product_image_gallery' THEN pm.meta_value END) AS image_url,
      MAX(CASE WHEN pm.meta_key IN ('_alg_wc_cog_cost','_wc_cog_cost') THEN pm.meta_value END) AS cogs
    FROM wp_posts p
    LEFT JOIN wp_postmeta pm ON p.ID = pm.post_id
    LEFT JOIN wp_wc_product_meta_lookup wc ON wc.product_id = p.ID
    WHERE p.post_type IN ('product','product_variation')
    GROUP BY p.ID;
  " > temp_master_products.tsv

  echo "🧩 Loading products into woo_master.products ..."
  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    CREATE DATABASE IF NOT EXISTS woo_master;
    USE woo_master;
    CREATE TABLE IF NOT EXISTS products (
      product_id BIGINT PRIMARY KEY,
      title VARCHAR(255),
      sku VARCHAR(100),
      parent_sku VARCHAR(100),
      attributes LONGTEXT,
      stock_qty INT,
      categories VARCHAR(255),
      tags VARCHAR(255),
      regular_price DECIMAL(12,2),
      sale_price DECIMAL(12,2),
      image_url LONGTEXT,
      cogs DECIMAL(12,2),
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    TRUNCATE TABLE products;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_master_products.tsv'
    INTO TABLE products
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (product_id, title, sku, parent_sku, attributes, stock_qty, categories, tags, regular_price, sale_price, image_url, cogs);
  "

  rm -f temp_master_products.tsv
  echo "✅ Master Products Table built successfully in woo_master.products"
}


# =========================================================
# 🧩 9️⃣ Merge All Store Orders → woo_master.orders & order_items
# =========================================================
run_master_orders_etl() {
  echo "🧩 Building Master Orders and Order Items Tables from all stores ..."

  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    CREATE DATABASE IF NOT EXISTS woo_master;
    USE woo_master;
    TRUNCATE TABLE orders;
    TRUNCATE TABLE order_items;
  "

  for COUNTRY in TR DE FR NL BE BEFRLU AT DK ES IT SE FI PT CZ HU RO SK UK; do
    echo "🔗 Merging orders and items for $COUNTRY ..."
    
  # ⚙️ Check if the store database exists first (safe check)
  DB_EXISTS=$(mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -N -B -e "
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME = 'woo_${COUNTRY,,}';
  " 2>/dev/null || echo 0)

  if [ "$DB_EXISTS" -eq 0 ]; then
    echo "⚠️  Database woo_${COUNTRY,,} does not exist — skipping $COUNTRY."
    continue
  fi


    HAS_NAME=$(mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -N -B -e "
      SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'woo_${COUNTRY,,}'
        AND TABLE_NAME = 'order_items'
        AND COLUMN_NAME = 'order_item_name';
    ")

    NAME_SELECT=$([ "$HAS_NAME" -eq 1 ] && echo "oi.order_item_name" || echo "NULL")

    mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
      USE woo_master;
      INSERT INTO orders (
        order_number_formatted, source_store, order_id, order_date, order_status,
        customer_id, country_code, channel, site, billing_country, billing_city,
        units_total, ordered_items_count, ordered_items_skus, payment_method,
        currency_code, subtotal, gross_total, cogs, total_price,
        tax_amount, shipping_fee, fee_amount, discount_amount,
        refunded_amount, ads_spend, logistics_cost, other_costs,
        net_profit, net_revenue, net_margin
      )
      SELECT
        CASE
          WHEN '$COUNTRY' = 'TR' THEN CONCAT(o.order_id)
          WHEN '$COUNTRY' = 'NL' THEN CONCAT('101-', o.order_id)
          WHEN '$COUNTRY' = 'BE' THEN CONCAT('201-', o.order_id)
          WHEN '$COUNTRY' = 'DE' THEN CONCAT('301-', o.order_id)
          WHEN '$COUNTRY' = 'AT' THEN CONCAT('401-', o.order_id)
          WHEN '$COUNTRY' = 'CZ' THEN CONCAT('461-', o.order_id)
          WHEN '$COUNTRY' = 'HU' THEN CONCAT('441-', o.order_id)
          WHEN '$COUNTRY' = 'BEFRLU' THEN CONCAT('241-', o.order_id)
          WHEN '$COUNTRY' = 'FR' THEN CONCAT('501-', o.order_id)
          WHEN '$COUNTRY' = 'RO' THEN CONCAT('531-', o.order_id)
          WHEN '$COUNTRY' = 'SK' THEN CONCAT('561-', o.order_id)
          WHEN '$COUNTRY' = 'FI' THEN CONCAT('641-', o.order_id)
          WHEN '$COUNTRY' = 'PT' THEN CONCAT('741-', o.order_id)
          WHEN '$COUNTRY' = 'ES' THEN CONCAT('701-', o.order_id)
          WHEN '$COUNTRY' = 'IT' THEN CONCAT('801-', o.order_id)
          WHEN '$COUNTRY' = 'SE' THEN CONCAT('901-', o.order_id)
          WHEN '$COUNTRY' = 'DK' THEN CONCAT('601-', o.order_id)
          WHEN '$COUNTRY' = 'UK' THEN CONCAT('161-', o.order_id)
          ELSE CONCAT('$COUNTRY', '-', o.order_id)
        END AS order_number_formatted,
        '$COUNTRY' AS source_store,
        o.order_id, o.order_date, o.order_status, o.customer_id,
        o.country_code, o.channel, o.site, o.billing_country, o.billing_city,
        o.units_total, o.ordered_items_count, o.ordered_items_skus, o.payment_method,
        o.currency_code, o.subtotal, o.gross_total, o.cogs, o.total_price,
        o.tax_amount, o.shipping_fee, o.fee_amount, o.discount_amount,
        o.refunded_amount, o.ads_spend, o.logistics_cost, o.other_costs,
        o.net_profit, o.net_revenue, o.net_margin
      FROM woo_${COUNTRY,,}.orders o;

      INSERT INTO order_items (
        order_item_id, order_id, product_id, variation_id, sku,
        order_item_name, quantity, line_total, line_tax,
        refund_reference, currency_code, source_store
      )
      SELECT
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.variation_id,
        oi.sku,
        ${NAME_SELECT},
        oi.quantity,
        oi.line_total,
        oi.line_tax,
        oi.refund_reference,
        oi.currency_code,
        '$COUNTRY'
      FROM woo_${COUNTRY,,}.order_items oi;
    "
  done

  echo "✅ Master Orders and Order Items tables merged successfully."
}

# =========================================================
# 🔁 8️⃣ MASTER RETURNS ETL (Laravel Portal → woo_master.returns)
# =========================================================
run_master_returns_etl() {
  echo "🔁 Extracting Returns from Laravel Portal..."

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["RETURNS"]}"

  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for RETURNS"
    return
  fi

  # Extract from Laravel portal schema
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      s.order_id AS order_number,
      s.order_email,
      s.order_date,
      s.created_at AS return_request_date,
      s.order_site,
      MAX(sd.shiping_method) AS shipping_method,
      MAX(sd.shiping_price) AS payment_for_return_fee,
      s.shipment_status AS return_request_status,
      COUNT(si.id) AS return_requested_items_count,
      GROUP_CONCAT(si.product_sku SEPARATOR ', ') AS return_requested_items_sku,
      GROUP_CONCAT(si.attributes SEPARATOR ' || ') AS return_requested_items_attributes,
      GROUP_CONCAT(DISTINCT si.return_reason SEPARATOR ', ') AS return_reason,
      MAX(sd.payment_method) AS return_method,
      SUM(si.total) AS return_requested_total_amount,
      MAX(sa.country) AS country_code,
      MAX(sa.city) AS city,
      MAX(sd.currency) AS currency
    FROM shipments s
    LEFT JOIN shipment_items si ON s.id = si.shipment_id
    LEFT JOIN shipment_details sd ON s.id = sd.shipment_id
    LEFT JOIN shipment_addresses sa ON s.id = sa.shipment_id
    GROUP BY s.id;
  " > temp_master_returns.tsv

  # Load into master DB
  echo "📥 Loading returns into woo_master.returns..."
  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    TRUNCATE TABLE returns;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_master_returns.tsv'
    INTO TABLE returns
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (order_number, order_email, order_date, return_request_date, order_site,
     shipping_method, payment_for_return_fee, return_request_status,
     return_requested_items_count, return_requested_items_sku,
     return_requested_items_attributes, return_reason, return_method,
     return_requested_total_amount, country_code, city, currency);
  "

  rm -f temp_master_returns.tsv
  echo "✅ Master Returns table updated successfully!"
}

# =========================================================
# 👤 10️⃣ MERGE ALL STORE CUSTOMERS → woo_master.customers
# =========================================================
run_master_customers_etl() {
  echo "👥 Building Master Customers Table from all stores ..."

  # 🧹 Clean or create master table
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    CREATE DATABASE IF NOT EXISTS woo_master;
    USE woo_master;
    CREATE TABLE IF NOT EXISTS customers (
      customer_id BIGINT,
      customer_number_formatted VARCHAR(50),
      full_name VARCHAR(255),
      email VARCHAR(255),
      phone VARCHAR(50),
      registered_at DATETIME,
      first_order_date DATETIME,
      last_order_date DATETIME,
      orders_count INT,
      units_total INT,
      ltv DECIMAL(12,2),
      aov DECIMAL(12,2),
      refunds_total DECIMAL(12,2),
      billing_country VARCHAR(100),
      billing_city VARCHAR(100),
      source_store VARCHAR(50),
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (customer_id, source_store),
      INDEX idx_customers_email (email)
    );
    TRUNCATE TABLE customers;
  "

  # 🔄 Merge customers from all stores
  for COUNTRY in TR DE FR NL BE BEFRLU AT DK ES IT SE FI PT CZ HU RO SK UK; do
    echo "🔗 Merging customers for $COUNTRY ..."
    
    
    # ⚙️ Check if the store database exists first
    DB_EXISTS=$(mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -N -B -e "
      SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'woo_${COUNTRY,,}';
    ")

    if [ "$DB_EXISTS" -eq 0 ]; then
      echo "⚠️  Database woo_${COUNTRY,,} does not exist — skipping $COUNTRY."
      continue
    fi

    mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
      USE woo_master;
      INSERT INTO customers (
        customer_number_formatted, customer_id, full_name, email, phone,
        registered_at, first_order_date, last_order_date,
        orders_count, units_total, ltv, aov, refunds_total,
        billing_country, billing_city, source_store
      )
      SELECT
        CASE
          WHEN '$COUNTRY' = 'TR' THEN CONCAT(c.customer_id)
          WHEN '$COUNTRY' = 'NL' THEN CONCAT('101-', c.customer_id)
          WHEN '$COUNTRY' = 'BE' THEN CONCAT('201-', c.customer_id)
          WHEN '$COUNTRY' = 'DE' THEN CONCAT('301-', c.customer_id)
          WHEN '$COUNTRY' = 'AT' THEN CONCAT('401-', c.customer_id)
          WHEN '$COUNTRY' = 'CZ' THEN CONCAT('461-', c.customer_id)
          WHEN '$COUNTRY' = 'HU' THEN CONCAT('441-', c.customer_id)
          WHEN '$COUNTRY' = 'BEFRLU' THEN CONCAT('241-', c.customer_id)
          WHEN '$COUNTRY' = 'FR' THEN CONCAT('501-', c.customer_id)
          WHEN '$COUNTRY' = 'RO' THEN CONCAT('531-', c.customer_id)
          WHEN '$COUNTRY' = 'SK' THEN CONCAT('561-', c.customer_id)
          WHEN '$COUNTRY' = 'FI' THEN CONCAT('641-', c.customer_id)
          WHEN '$COUNTRY' = 'PT' THEN CONCAT('741-', c.customer_id)
          WHEN '$COUNTRY' = 'ES' THEN CONCAT('701-', c.customer_id)
          WHEN '$COUNTRY' = 'IT' THEN CONCAT('801-', c.customer_id)
          WHEN '$COUNTRY' = 'SE' THEN CONCAT('901-', c.customer_id)
          WHEN '$COUNTRY' = 'DK' THEN CONCAT('601-', c.customer_id)
          WHEN '$COUNTRY' = 'UK' THEN CONCAT('161-', c.customer_id)
          ELSE CONCAT('$COUNTRY', '-', c.customer_id)
        END AS customer_number_formatted,
        c.customer_id,
        c.full_name,
        c.email,
        c.phone,
        c.registered_at,
        c.first_order_date,
        c.last_order_date,
        c.orders_count,
        c.units_total,
        c.ltv,
        c.aov,
        c.refunds_total,
        c.billing_country,
        c.billing_city,
        '$COUNTRY' AS source_store
      FROM woo_${COUNTRY,,}.customers c;
    "
  done

  echo "✅ Master Customers table merged successfully."
}

# =========================================================
# 🧩 7️⃣ NORMALIZE PRODUCT IMAGE GALLERY (OPS → woo_master.product_gallery_map)
# =========================================================
run_master_product_gallery_map_etl() {
  echo "🧩 Building normalized Product Gallery Map from OPS ..."

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["OPS"]}"
  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for OPS"
    return
  fi

  # Step 1️⃣: Extract product_id → comma-separated image_ids from OPS
  echo "📦 Extracting product → gallery IDs from OPS..."
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      p.ID AS product_id,
      MAX(CASE WHEN pm.meta_key = '_product_image_gallery' THEN pm.meta_value END) AS image_ids
    FROM wp_posts p
    LEFT JOIN wp_postmeta pm ON p.ID = pm.post_id
    WHERE p.post_type IN ('product', 'product_variation')
    GROUP BY p.ID;
  " > temp_master_gallery_ids.tsv

  # Step 2️⃣: Split comma-separated IDs into normalized rows using Bash + awk
  echo "🧩 Splitting gallery IDs into product_id, image_id pairs..."
  awk -F'\t' 'NR>1 {
    n=split($2, arr, ",");
    for (i=1; i<=n; i++) if (arr[i] != "") print $1 "\t" arr[i];
  }' temp_master_gallery_ids.tsv > temp_product_gallery_pairs.tsv

  # Step 3️⃣: Load into local normalized table
  echo "📥 Creating temporary product_gallery_map and loading data..."
  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    DROP TABLE IF EXISTS product_gallery_map;  -- 🧹 ensure old version removed
    CREATE TABLE product_gallery_map (         -- 🟩 temporary in ETL only
      product_id BIGINT,
      image_id BIGINT,
      PRIMARY KEY (product_id, image_id),
      INDEX idx_gallery_image_id (image_id)
    );
    LOAD DATA LOCAL INFILE '$(pwd)/temp_product_gallery_pairs.tsv'
    INTO TABLE product_gallery_map
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    (product_id, image_id);
  "

  # 🧹 Cleanup
  rm -f temp_master_gallery_ids.tsv temp_product_gallery_pairs.tsv
  echo "✅ Normalized product_gallery_map table built successfully in woo_master."
}

# =========================================================
# 🖼️ MASTER PRODUCT IMAGES ETL (OPS → woo_master.products)
# =========================================================
run_master_product_images_etl() {
  echo "🖼️ Updating Master Product Image URLs (normalized join)..."

  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;

    -- Preload image URLs from OPS
    DROP TABLE IF EXISTS temp_image_urls;
    CREATE TABLE temp_image_urls (
      image_id BIGINT,
      image_url TEXT
    );
  "

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["OPS"]}"
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT ID AS image_id, guid AS image_url
    FROM wp_posts
    WHERE post_type='attachment' AND post_mime_type LIKE 'image/%';
  " > temp_master_image_urls.tsv

  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_master_image_urls.tsv'
    INTO TABLE temp_image_urls
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;
    CREATE INDEX idx_temp_image_id ON temp_image_urls (image_id);
  "

  # ✅ Now join normalized map + URLs to update products
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    SET SESSION group_concat_max_len = 1000000;  -- 🧠 allow long URL lists

    UPDATE products p
    LEFT JOIN (
      SELECT g.product_id, GROUP_CONCAT(i.image_url ORDER BY i.image_id SEPARATOR ',') AS urls
      FROM product_gallery_map g
      LEFT JOIN temp_image_urls i ON g.image_id = i.image_id
      GROUP BY g.product_id
    ) img ON p.product_id = img.product_id
    SET p.image_url = img.urls;

    DROP TABLE temp_image_urls;
    DROP TABLE IF EXISTS product_gallery_map;  
  "

  rm -f temp_master_image_urls.tsv
  echo "✅ Master Products updated with image URLs using normalized mapping."
}

# =========================================================
# 🏷️ 6️⃣ MASTER CATEGORIES & TAGS ETL (OPS → woo_master.products)
# =========================================================
run_master_categories_tags_etl() {
  echo "🏷️ Building Categories & Tags data from OPS ..."

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["OPS"]}"

  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for OPS"
    return
  fi

  # ✅ Extract category & tag data for all products
  echo "📦 Extracting Categories & Tags from OPS..."
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT
      tr.object_id AS product_id,
      GROUP_CONCAT(DISTINCT CASE WHEN tt.taxonomy = 'product_cat' THEN t.name END SEPARATOR ',') AS categories,
      GROUP_CONCAT(DISTINCT CASE WHEN tt.taxonomy <> 'product_cat' THEN t.name END SEPARATOR ',') AS tags
    FROM wp_term_relationships tr
    INNER JOIN wp_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
    INNER JOIN wp_terms t ON tt.term_id = t.term_id
    GROUP BY tr.object_id;
  " > temp_master_categories_tags.tsv

  # ✅ Load into local master DB
  echo "🧩 Loading into woo_master.products..."
    # 💡 Run this small command separately — avoids DROP INDEX parser issue
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    DROP INDEX idx_product_id ON woo_master.temp_categories_tags;
  " 2>/dev/null || true

  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;

    -- Create temp table for category/tag mapping
    DROP TABLE IF EXISTS temp_categories_tags;
    CREATE TABLE temp_categories_tags (
      product_id BIGINT,
      categories TEXT,
      tags TEXT
    );

    LOAD DATA LOCAL INFILE '$(pwd)/temp_master_categories_tags.tsv'
    INTO TABLE temp_categories_tags
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (product_id, categories, tags);
    CREATE INDEX idx_temp_product_id ON temp_categories_tags (product_id);
    
    -- Update master products table with extracted categories and tags
    UPDATE woo_master.products p
    INNER JOIN temp_categories_tags ct ON p.product_id = ct.product_id
    SET p.categories = ct.categories,
        p.tags = ct.tags;

    DROP TABLE temp_categories_tags;
  "

  rm -f temp_master_categories_tags.tsv
  echo "✅ Categories & Tags fields updated successfully in woo_master.products"
}


# =========================================================
# 🚀 Execute All ETL Steps    TR DE FR NL BE AT 
# =========================================================
for COUNTRY in  TR DE FR NL BE AT BEFRLU DK ES IT SE FI PT CZ HU RO SK UK ; do
  run_etl "$COUNTRY"
done
run_etl_ops
run_master_products_etl
run_master_customers_etl
run_master_returns_etl
run_master_orders_etl
run_master_categories_tags_etl
run_master_product_gallery_map_etl
run_master_product_images_etl

echo "🎯 All ETL operations completed successfully."


