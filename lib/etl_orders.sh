#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

# =========================================================
# 🧩 WooCommerce ETL per Country
# =========================================================
run_etl() {
  COUNTRY="$1"  
  # =========================================================
  # 🟡 1️⃣ Extract Orders
  # =========================================================
  echo "📦 Extracting Orders for $COUNTRY ..."
  # ==========================================
  # 🌍 Configure remote WooCommerce DB connection
  # ==========================================
  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS[$COUNTRY]}"

  if [ -z "$HOST" ] || [ -z "$DB" ] || [ -z "$USER" ] || [ -z "$PASS" ]; then
    echo "❌ Missing database credentials for $COUNTRY in REMOTE_DBS."
    return 1
  fi

  echo "🔗 Connecting to remote DB for $COUNTRY:"
  echo "   Host: $HOST"
  echo "   DB:   $DB"

  # 🆕 With a special conditional tweak for OPS only
  if [ "$COUNTRY" = "OPS" ]; then
    echo "🔎 Detected OPS — applying marketplace filters..."
    EXTRA_FILTER="AND p.ID IN (
      SELECT post_id FROM wp_postmeta
      WHERE meta_key='_payment_method' AND meta_value IN ('bol','other')
    )"
  else
    EXTRA_FILTER=""
  fi
  # 🧠 Special handling for TR: order_number_formatted = order_id
  # 🧠 Special handling for TR and Refunds (generate order_number_formatted)
  if [ "$COUNTRY" = "TR" ]; then
    echo "⚙️ Using order_id as order_number_formatted for TR (no _order_number_formatted key)..."
    ORDER_NUMBER_FIELD="CAST(p.ID AS CHAR) AS order_number_formatted"
  else
    # Assign country-specific prefix for refunds
    case "$COUNTRY" in
      NL) PREFIX="101" ;;
      BE) PREFIX="201" ;;
      DE) PREFIX="301" ;;
      AT) PREFIX="401" ;;
      BEFR|BEFRLU) PREFIX="241" ;;
      FR) PREFIX="501" ;;
      DK) PREFIX="601" ;;
      SE) PREFIX="901" ;;
      FI) PREFIX="641" ;;
      PT) PREFIX="741" ;;
      ES) PREFIX="701" ;;
      IT) PREFIX="801" ;;
      CZ) PREFIX="461" ;;
      HU) PREFIX="441" ;;
      RO) PREFIX="531" ;;
      SK) PREFIX="561" ;;
      UK) PREFIX="161" ;;
      OPS) PREFIX="" ;;  # OPS marketplace has no formatted order number
      *) PREFIX="" ;;
    esac

    # Build dynamic SQL field for order_number_formatted
    ORDER_NUMBER_FIELD="CASE
      WHEN p.post_type = 'shop_order_refund'
        THEN CONCAT('$PREFIX', '-REFUND-', CAST(p.ID AS CHAR))
      ELSE MAX(CASE WHEN pm.meta_key = '_order_number_formatted' THEN pm.meta_value END)
    END AS order_number_formatted"
  fi

  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
    SELECT DISTINCT
      $ORDER_NUMBER_FIELD,
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
    $EXTRA_FILTER
    GROUP BY p.ID;
  " > "temp_${COUNTRY}_orders.tsv"

  echo "📥 Loading Orders into local DB..."
  echo "🧱 Ensuring local database woo_${COUNTRY,,} exists..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
    CREATE DATABASE IF NOT EXISTS woo_${COUNTRY,,};
  "

  # =========================================================
  # 📥 Load Orders into Local Database
  # =========================================================
  echo "🧱 Ensuring tables exist in woo_${COUNTRY,,}..."
  if [ "$COUNTRY" != "TR" ]; then
    run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
      USE woo_${COUNTRY,,};
      DROP TABLE IF EXISTS orders;
      CREATE TABLE IF NOT EXISTS orders LIKE woo_tr.orders;
    "
  else
    echo "⚙️ Skipping table clone for TR (base schema already exists)."
  fi


  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_${COUNTRY,,}" "
    USE woo_${COUNTRY,,};
    TRUNCATE TABLE orders;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_orders.tsv'
    INTO TABLE orders
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (
      order_number_formatted, order_id, order_date, order_status, customer_id, country_code, channel, site,
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
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
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
  if [ "$COUNTRY" != "TR" ]; then
    echo "🧱 Creating table order_items in woo_${COUNTRY,,}..."
    run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_${COUNTRY,,}" "
      USE woo_${COUNTRY,,};
      CREATE TABLE IF NOT EXISTS order_items LIKE woo_tr.order_items;
      TRUNCATE TABLE order_items;
      LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_order_items.tsv'
      INTO TABLE order_items
      FIELDS TERMINATED BY '\t'
      LINES TERMINATED BY '\n'
      IGNORE 1 LINES;
    "
  else
    echo "⚙️ Skipping schema clone for TR (base schema already exists)."
    run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_tr" "
      TRUNCATE TABLE order_items;
      LOAD DATA LOCAL INFILE '$(pwd)/temp_${COUNTRY}_order_items.tsv'
      INTO TABLE order_items
      FIELDS TERMINATED BY '\t'
      LINES TERMINATED BY '\n'
      IGNORE 1 LINES;
    "
  fi

  rm -f "temp_${COUNTRY}_order_items.tsv"
  echo "✅ Order Items for $COUNTRY loaded successfully."


  # =========================================================
  # 🟢 3️⃣ Update SKUs from PIM Database
  # =========================================================
  echo "🔍 Updating SKU values in order_items from PIM ..."
  run_mysql_query "188.68.58.232" "bi-dashboard-pim" "5rB4gGW6K76tu6A2gWXs" "mbu-trade-pim" "
    SELECT product_id, sku
    FROM wp_wc_product_meta_lookup
    WHERE sku IS NOT NULL AND sku <> '';
  " > temp_pim_sku.tsv

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_${COUNTRY,,}" "
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

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
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
  run_mysql_query "$PIM_HOST" "$PIM_USER" "$PIM_PASS" "$PIM_DB" "
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
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
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
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
    USE woo_${COUNTRY,,};
    -- (Preserve logic for future reactivation)
    -- UPDATE orders o
    -- JOIN (
    --   SELECT 
    --     oi.order_id,
    --     SUM(
    --       CAST(oi.quantity AS DECIMAL(12,4)) *
    --       CAST(pc.cog_value AS DECIMAL(12,4))
    --     ) AS total_cogs
    --   FROM order_items oi
    --   JOIN woo_master.pim_cogs pc
    --     ON pc.product_id = oi.product_id
    --   GROUP BY oi.order_id
    -- ) calc ON o.order_id = calc.order_id
    -- SET o.cogs = calc.total_cogs;
    -- 🚀 Temporary override: set everything to 0
    UPDATE orders SET cogs = 0;
  "
  # Drop the staging table afterward
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
    USE woo_master;
    DROP TABLE IF EXISTS pim_cogs;
  "
  echo "✅ COGS updated for $COUNTRY."


  # =========================================================
  # 💵 Calculate and Update Net Revenue, Profit, Margin
  # =========================================================
  echo "📊 Calculating Net Revenue, Net Profit, and Net Margin for $COUNTRY ..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
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
  echo "HST: $HOST, USER: $USER, DB: $DB"
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
    SET sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_DATE', '');
    SET sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_IN_DATE', '');
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
      IF(u.user_registered = CAST('0000-00-00 00:00:00' AS CHAR), NULL, u.user_registered) AS registered_at,
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
  echo "📊 $(wc -l < temp_${COUNTRY}_customers_base.tsv) rows in base TSV"

  # Step 2️⃣: Aggregate order metrics
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "  
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
  echo "📊 $(wc -l < temp_${COUNTRY}_orders_agg.tsv) rows in orders agg TSV"

  # Step 3️⃣: Aggregate units sold
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
    SELECT
      CAST(pm.meta_value AS UNSIGNED) AS customer_id,
      SUM(CASE WHEN oi.order_item_type = 'line_item' THEN 1 ELSE 0 END) AS units_total
    FROM wp_posts p
    JOIN wp_postmeta pm ON p.ID = pm.post_id AND pm.meta_key = '_customer_user'
    JOIN wp_woocommerce_order_items oi ON p.ID = oi.order_id
    WHERE p.post_type = 'shop_order'
    GROUP BY pm.meta_value;
  " > "temp_${COUNTRY}_units_agg.tsv"
  echo "📊 $(wc -l < temp_${COUNTRY}_units_agg.tsv) rows in units agg TSV"

  # Step 3b️⃣: Aggregate refunds
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
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
  echo "📊 $(wc -l < temp_${COUNTRY}_refunds_agg.tsv) rows in refunds agg TSV"

  # Step 4️⃣: Merge and load into local DB
  echo "📥 Loading combined Customers data into woo_${COUNTRY,,}.customers ..."

  if [ "$COUNTRY" != "TR" ]; then
    run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
      USE woo_${COUNTRY,,};
      CREATE TABLE IF NOT EXISTS customers LIKE woo_tr.customers;
      TRUNCATE TABLE customers;
    "
  else
    echo "⚙️ Skipping schema clone for TR (base schema already exists)."
    run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
      USE woo_tr;
      TRUNCATE TABLE customers;
    "
  fi

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
    USE woo_${COUNTRY,,};
    CREATE TEMPORARY TABLE base (
      customer_id BIGINT, full_name VARCHAR(255), email VARCHAR(255),
      phone VARCHAR(50), registered_at DATETIME,
      billing_country VARCHAR(100), billing_city VARCHAR(100)
    );
    SET sql_mode = REPLACE(REPLACE(@@sql_mode,'NO_ZERO_DATE',''),'NO_ZERO_IN_DATE','');
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
