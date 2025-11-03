#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_orders_etl() {
  echo "🧩 Building Master Orders and Order Items Tables from all stores ..."

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    TRUNCATE TABLE orders;
    TRUNCATE TABLE order_items;
  "

  for COUNTRY in OPS TR DE FR NL BE BEFRLU AT DK ES IT SE FI PT CZ HU RO SK UK; do
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

run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
  SET SESSION sql_mode = REPLACE(REPLACE(@@sql_mode, 'STRICT_TRANS_TABLES', ''), 'NO_ZERO_DATE', '');
  INSERT INTO woo_master.orders (
    order_number_formatted, source_store, order_id, order_date, order_status,
    customer_id, country_code, channel, site, billing_country, billing_city,
    units_total, ordered_items_count, ordered_items_skus, payment_method,
    currency_code, subtotal, gross_total, cogs, total_price,
    tax_amount, shipping_fee, fee_amount, discount_amount,
    refunded_amount, ads_spend, logistics_cost, other_costs,
    net_profit, net_revenue, net_margin
  )
  SELECT
    COALESCE(NULLIF(TRIM(o.order_number_formatted), ''), CONCAT('ORD', o.order_id)),
    '$COUNTRY',
    o.order_id,
CASE
  WHEN o.order_date IS NULL
       OR TRIM(o.order_date) = ''
       OR o.order_date IN ('0000-00-00', '0000-00-00 00:00:00')
  THEN NULL
  WHEN STR_TO_DATE(o.order_date, '%Y-%m-%d %H:%i:%s') IS NOT NULL
  THEN STR_TO_DATE(o.order_date, '%Y-%m-%d %H:%i:%s')
  ELSE NULL
END AS order_date,
    o.order_status, o.customer_id,
    o.country_code, o.channel, o.site, o.billing_country, o.billing_city,
    o.units_total, o.ordered_items_count, o.ordered_items_skus, o.payment_method,
    o.currency_code, o.subtotal, o.gross_total, o.cogs, o.total_price,
    o.tax_amount, o.shipping_fee, o.fee_amount, o.discount_amount,
    o.refunded_amount, o.ads_spend, o.logistics_cost, o.other_costs,
    o.net_profit, o.net_revenue, o.net_margin
  FROM woo_${COUNTRY,,}.orders o
  WHERE o.order_number_formatted IS NOT NULL
    AND o.order_number_formatted <> ''
    AND LENGTH(TRIM(o.order_number_formatted)) > 0
    AND o.order_number_formatted <> 'NULL';
"

run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
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
    oi.order_item_name,
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
