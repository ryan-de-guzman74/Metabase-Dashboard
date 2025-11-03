#!/bin/bash
set -e
set -o pipefail

# Load environment and common tools
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/env_loader.sh"
source "$SCRIPT_DIR/lib/utils.sh"
source "$SCRIPT_DIR/lib/etl_ads_insights.sh"

# Load all ETL modules
for module in \
  etl_orders \
  etl_master_products \
  etl_master_customers \
  etl_master_orders \
  etl_master_returns \
  etl_master_categories \
  etl_master_gallery \
  etl_master_images \
  etl_ads_insights; do
  source "$SCRIPT_DIR/lib/${module}.sh"
done

# =========================================================
# 🚀 Execute All ETL Steps
# =========================================================
for COUNTRY in TR DE FR NL BE AT BEFRLU DK ES IT SE FI PT CZ HU RO SK UK OPS; do
  run_etl "$COUNTRY"
done

run_master_products_etl
run_master_customers_etl
run_master_returns_etl
run_master_orders_etl
run_master_categories_tags_etl
run_master_product_gallery_map_etl
run_master_product_images_etl
run_ads_insights_etl

echo "🎯 All ETL operations completed successfully."
