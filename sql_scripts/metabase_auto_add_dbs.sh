#!/bin/bash
# ======================================
# Auto-register WooCommerce & Laravel databases in Metabase
# ======================================

METABASE_URL="http://localhost:3000"
METABASE_USER="10082005tr@gmail.com"   # your Metabase login email
METABASE_PASS="Password123!@#"        # your Metabase login password
MYSQL_HOST="mysql"
MYSQL_PORT=3306
MYSQL_USER="root"
MYSQL_PASS="rootpass"

# All DB names to register
DBS=(
  woo_tr woo_nl woo_be woo_befrlu woo_de woo_at woo_fr woo_dk
  woo_es woo_it woo_se woo_fi woo_pt woo_cz woo_hu woo_ro
  woo_sk woo_uk woo_ops woo_master_orders laravel_returns
)

# --- Get Metabase session token ---
SESSION=$(curl -s -X POST -H "Content-Type: application/json" \
  -d "{\"username\":\"$METABASE_USER\", \"password\":\"$METABASE_PASS\"}" \
  "$METABASE_URL/api/session" | grep -o '"id":"[^"]*"' | sed 's/"id":"\(.*\)"/\1/')


if [ "$SESSION" == "null" ] || [ -z "$SESSION" ]; then
  echo "❌ Login failed. Check Metabase credentials."
  exit 1
fi

echo "✅ Logged into Metabase."

# --- Register each database ---
for DB in "${DBS[@]}"; do
  echo "🔗 Adding database: $DB"
  curl -s -X POST -H "Content-Type: application/json" \
    -H "X-Metabase-Session: $SESSION" \
    -d "{
      \"engine\": \"mysql\",
      \"name\": \"$DB\",
      \"details\": {
        \"host\": \"$MYSQL_HOST\",
        \"port\": $MYSQL_PORT,
        \"dbname\": \"$DB\",
        \"user\": \"$MYSQL_USER\",
        \"password\": \"$MYSQL_PASS\"
      },
      \"is_full_sync\": true,
      \"is_on_demand\": false
    }" \
    "$METABASE_URL/api/database" >/dev/null
done

echo "✅ All databases added successfully!"
