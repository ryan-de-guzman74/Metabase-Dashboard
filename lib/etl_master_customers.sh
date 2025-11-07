#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_customers_etl() {
  echo "👥 Building Master Customers Table from all stores ..."

  # 🧹 Clean or create master table
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
    USE woo_master;
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

    run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
      SET SESSION sql_mode = REPLACE(@@sql_mode, 'STRICT_TRANS_TABLES', '');
      INSERT INTO customers (
        customer_number_formatted, customer_id, full_name, email, phone,
        registered_at, first_order_date, last_order_date,
        orders_count, units_total, ltv, aov, refunds_total,
        billing_country, billing_city, source_store
      )
      SELECT
        CASE
          WHEN '$COUNTRY' = 'TR' THEN CONCAT(c.customer_id)
          WHEN '$COUNTRY' = 'NL' THEN CONCAT('101-NL-', c.customer_id)
          WHEN '$COUNTRY' = 'BE' THEN CONCAT('201-BE-', c.customer_id)
          WHEN '$COUNTRY' = 'DE' THEN CONCAT('301-DE-', c.customer_id)
          WHEN '$COUNTRY' = 'AT' THEN CONCAT('401-AT-', c.customer_id)
          WHEN '$COUNTRY' = 'CZ' THEN CONCAT('461-CZ-', c.customer_id)
          WHEN '$COUNTRY' = 'HU' THEN CONCAT('441-HU-', c.customer_id)
          WHEN '$COUNTRY' = 'BEFRLU' THEN CONCAT('241-BEFRLU-', c.customer_id)
          WHEN '$COUNTRY' = 'FR' THEN CONCAT('501-FR-', c.customer_id)
          WHEN '$COUNTRY' = 'RO' THEN CONCAT('531-RO-', c.customer_id)
          WHEN '$COUNTRY' = 'SK' THEN CONCAT('561-SK-', c.customer_id)
          WHEN '$COUNTRY' = 'FI' THEN CONCAT('641-FI-', c.customer_id)
          WHEN '$COUNTRY' = 'PT' THEN CONCAT('741-PT-', c.customer_id)
          WHEN '$COUNTRY' = 'ES' THEN CONCAT('701-ES-', c.customer_id)
          WHEN '$COUNTRY' = 'IT' THEN CONCAT('801-IT-', c.customer_id)
          WHEN '$COUNTRY' = 'SE' THEN CONCAT('901-SE-', c.customer_id)
          WHEN '$COUNTRY' = 'DK' THEN CONCAT('601-DK-', c.customer_id)
          WHEN '$COUNTRY' = 'UK' THEN CONCAT('161-UK-', c.customer_id)
          ELSE CONCAT('$COUNTRY', '-', c.customer_id)
        END AS customer_number_formatted,
        c.customer_id,
        c.full_name,
        c.email,
        c.phone,
        CASE
          WHEN c.registered_at REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{2}:[0-9]{2}:[0-9]{2})?$'
          THEN STR_TO_DATE(c.registered_at, '%Y-%m-%d %H:%i:%s')
          ELSE NULL
        END AS registered_at,
        CASE
          WHEN c.first_order_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{2}:[0-9]{2}:[0-9]{2})?$'
          THEN STR_TO_DATE(c.first_order_date, '%Y-%m-%d %H:%i:%s')
          ELSE NULL
        END AS first_order_date,
        CASE
          WHEN c.last_order_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{2}:[0-9]{2}:[0-9]{2})?$'
          THEN STR_TO_DATE(c.last_order_date, '%Y-%m-%d %H:%i:%s')
          ELSE NULL
        END AS last_order_date,
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

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
      SET SESSION sql_mode = REPLACE(@@sql_mode, 'STRICT_TRANS_TABLES', '');
      USE woo_master;
DROP TABLE IF EXISTS woo_master.vw_customers_eur;

CREATE TABLE woo_master.vw_customers_eur AS
SELECT
    woo_master.customers.customer_id,
    woo_master.customers.full_name,
    woo_master.customers.email,
    woo_master.customers.source_store,
    woo_master.customers.billing_country,
    woo_master.customers.billing_city,
    woo_master.customers.ltv AS original_ltv,
    woo_master.customers.aov AS original_aov,
    COALESCE(woo_master.exchange_rates.rate_to_eur, 1.0) AS rate_to_eur,
    ROUND(woo_master.customers.ltv * COALESCE(woo_master.exchange_rates.rate_to_eur, 1.0), 2) AS ltv_eur,
    ROUND(woo_master.customers.aov * COALESCE(woo_master.exchange_rates.rate_to_eur, 1.0), 2) AS aov_eur,
    woo_master.customers.orders_count,
    woo_master.customers.units_total,
    woo_master.customers.registered_at,
    woo_master.customers.last_order_date
FROM woo_master.customers
LEFT JOIN woo_master.exchange_rates
  ON (
    CASE
      WHEN woo_master.customers.source_store LIKE '%FR%' THEN 'EUR'
      WHEN woo_master.customers.source_store LIKE '%TR%' THEN 'TRY'
      WHEN woo_master.customers.source_store LIKE '%HU%' THEN 'HUF'
      WHEN woo_master.customers.source_store LIKE '%DK%' THEN 'DKK'
      WHEN woo_master.customers.source_store LIKE '%CZ%' THEN 'CZK'
      WHEN woo_master.customers.source_store LIKE '%RO%' THEN 'RON'
      WHEN woo_master.customers.source_store LIKE '%SE%' THEN 'SEK'
      WHEN woo_master.customers.source_store LIKE '%GB%' THEN 'GBP'
      ELSE 'EUR'
    END
  ) = woo_master.exchange_rates.currency_code;


    "

  echo "✅ Master Customers table merged successfully."
}
