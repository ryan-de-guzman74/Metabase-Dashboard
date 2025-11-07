#!/bin/bash
# =========================================================
# 💶 ETL: Create EUR Orders Table from woo_master.orders
# =========================================================
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_eur_orders_etl() {
  echo "💶 Converting woo_master.orders → woo_master.eur_orders (EUR)..."
  mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" <<'EOF'
USE woo_master;

DROP TABLE IF EXISTS eur_orders;
CREATE TABLE eur_orders LIKE orders;

INSERT INTO eur_orders
SELECT
  o.order_number_formatted,
  o.source_store,
  o.order_id,
  o.order_date,
  o.order_status,
  o.customer_id,
  o.country_code,
  o.channel,
  o.site,
  o.billing_country,
  o.billing_city,
  o.units_total,
  o.ordered_items_count,
  o.ordered_items_skus,
  o.payment_method,
  o.currency_code,
  ROUND(o.subtotal * r.rate_to_eur, 2),
  ROUND(o.gross_total * r.rate_to_eur, 2),
  ROUND(o.cogs * r.rate_to_eur, 2),
  ROUND(o.total_price * r.rate_to_eur, 2),
  ROUND(o.tax_amount * r.rate_to_eur, 2),
  ROUND(o.shipping_fee * r.rate_to_eur, 2),
  ROUND(o.fee_amount * r.rate_to_eur, 2),
  ROUND(o.discount_amount * r.rate_to_eur, 2),
  ROUND(o.refunded_amount * r.rate_to_eur, 2),
  ROUND(o.ads_spend * r.rate_to_eur, 2),
  ROUND(o.logistics_cost * r.rate_to_eur, 2),
  ROUND(o.other_costs * r.rate_to_eur, 2),
  ROUND(o.net_profit * r.rate_to_eur, 2),
  ROUND(o.net_revenue * r.rate_to_eur, 2),
  o.net_margin
FROM woo_master.orders o
LEFT JOIN woo_master.exchange_rates r
  ON o.currency_code = r.currency_code;
EOF
  echo "✅ EUR Orders table built successfully."
}
