#!/bin/bash
set -e
set -o pipefail

# =========================================================
# 🌍 Universal Path Setup — works on both Docker & Ubuntu
# =========================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load environment with DB connection map
if [ -f "$SCRIPT_DIR/etl_config.env" ]; then
  source "$SCRIPT_DIR/etl_config.env"
elif [ -f "$SCRIPT_DIR/../etl_config.env" ]; then
  source "$SCRIPT_DIR/../etl_config.env"
else
  echo "❌ etl_config.env not found near $SCRIPT_DIR"
  exit 1
fi

echo ""
echo "🌍 Checking 'wp_wc_product_meta_lookup' across WooCommerce stores..."
echo ""

# =========================================================
# 🧩 Step 1: Loop through target stores
# =========================================================
# Customize the list of countries / stores as needed:
STORES=("TR" "NL" "DE" "FR" "DK" "SE" "FI" "AT" "BE" "BEFRLU" "ES" "IT" "PT" "CZ" "HU" "RO" "SK" "UK")

for COUNTRY in "${STORES[@]}"; do
  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS[$COUNTRY]}"

  echo "=============================="
  echo "🔎 STORE: $COUNTRY ($DB @ $HOST)"
  echo "=============================="

  # 🔐 1️⃣ Connection check
  if ! mysql -h "$HOST" -u "$USER" -p"$PASS" -e "SELECT 1;" >/dev/null 2>&1; then
    echo "❌ Unable to connect to $HOST ($COUNTRY)"
    echo ""
    continue
  fi

  # 📦 2️⃣ Check if wp_wc_product_meta_lookup table exists
  HAS_TABLE=$(mysql -N -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e \
    "SHOW TABLES LIKE 'wp_wc_product_meta_lookup';")

  if [ -z "$HAS_TABLE" ]; then
    echo "⚠️  Table 'wp_wc_product_meta_lookup' not found in $COUNTRY."
    echo ""
    continue
  fi


  # 💰 5️⃣ Count rows with nonzero and non-empty _alg_wc_cog_cost
  echo "💰 Counting nonzero _alg_wc_cog_cost entries..."
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT COUNT(*) AS nonzero_cog_rows
    FROM wp_postmeta
    WHERE meta_key = '_alg_wc_cog_cost'
      AND meta_value IS NOT NULL
      AND meta_value <> ''
      AND meta_value <> '0'
      AND meta_value <> '0.00';
  "

  echo ""


done

echo "✅ Done checking wp_wc_product_meta_lookup in all stores."
