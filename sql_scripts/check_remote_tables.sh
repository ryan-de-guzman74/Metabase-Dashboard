#!/bin/bash
# ============================================================
# 🌍 Remote WooCommerce Table Existence Checker
# ============================================================
set -e
source ./../etl_config.env

# The tables you want to check (21 total)
TABLES=(
  wp_posts
  wp_postmeta
  wp_users
  wp_usermeta
  wp_woocommerce_order_items
  wp_woocommerce_order_itemmeta
  wp_wc_order_stats
  wp_wc_order_product_lookup
  wp_wc_customer_lookup
  wp_terms
  wp_term_taxonomy
  wp_term_relationships
  wp_wc_product_meta_lookup
  wp_alg_wc_cog
  wp_wc_download_log
  wp_wc_order_operational_data
  wp_wc_order_addresses
  wp_wc_order_stats
  wp_wc_reserved_stock
  wp_wc_customer_lookup
  wp_wc_product_meta_lookup
)

echo "Database,Table,Exists" > remote_table_check.csv

# Loop through all configured remote DBs
for COUNTRY in "${!REMOTE_DBS[@]}"; do
  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS[$COUNTRY]}"

  echo "🔍 Checking $DB on $HOST ($COUNTRY)..."

  for TABLE in "${TABLES[@]}"; do
    RESULT=$(mysql -h "$HOST" -u "$USER" -p"$PASS" -Nse "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$DB' AND table_name='$TABLE';" 2>/dev/null || echo 0)
    
    if [[ "$RESULT" -eq 1 ]]; then
      echo "$DB,$TABLE,✅" >> remote_table_check.csv
    else
      echo "$DB,$TABLE,❌" >> remote_table_check.csv
    fi
  done
done

echo "✅ Done! Results saved in remote_table_check.csv"
