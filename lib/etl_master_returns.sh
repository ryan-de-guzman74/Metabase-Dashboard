#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_returns_etl() {
  # Full content of your run_master_returns_etl() function
  echo "🔁 Extracting Returns from Laravel Portal..."

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["RETURNS"]}"

  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for RETURNS"
    return
  fi

  # Extract from Laravel portal schema
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
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
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
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
