#!/bin/bash
set -e

# ==========================================
# 🌍 Detect environment: Docker or Host
# ==========================================
if [ -f "/.dockerenv" ] || grep -qa "docker" /proc/1/cgroup >/dev/null 2>&1; then
  ENVIRONMENT="docker"
else
  ENVIRONMENT="host"
fi
echo "🐳 Running in environment: $ENVIRONMENT"

# ==========================================
# 🧩 Load configuration
# ==========================================
if [ -f /app/etl_config.env ]; then
  source /app/etl_config.env
  CONFIG_SOURCE="/app/etl_config.env"
elif [ -f ../etl_config.env ]; then
  source ../etl_config.env
  CONFIG_SOURCE="../etl_config.env"
else
  echo "❌ etl_config.env not found in either /app or current directory."
  exit 1
fi
echo "🔧 Loaded config from: $CONFIG_SOURCE"

# ==========================================
# 🪣 Choose correct log directory
# ==========================================
if [ "$ENVIRONMENT" = "docker" ]; then
  LOG_BASE="/app/logs"
else
  LOG_BASE="../logs"
fi

mkdir -p "$LOG_BASE/api_data"
chmod -R 777 "$LOG_BASE/api_data"

# ==========================================
# 📅 Determine date range (incremental load)
# ==========================================
DB_HOST=$LOCAL_HOST                       # 🆕 NEW: database connection info
DB_USER=$LOCAL_USER                        # 🆕
DB_PASS=$LOCAL_PASS                    # 🆕
DB_NAME="woo_master"                  # 🆕
DB_PORT=$LOCAL_PORT
# 🆕 NEW: Get latest date from advertisements table to fetch incrementally
LAST_DATE=$(mysql --local-infile=1 -h "$DB_HOST" -P "$DB_PORT" -u"$DB_USER" -p"$DB_PASS" -Nse "SELECT MAX(date_stop) FROM ${DB_NAME}.advertisements;" 2>/dev/null)

if [ -z "$LAST_DATE" ] || [ "$LAST_DATE" = "NULL" ]; then
  SINCE=$(date -d '60 days ago' +%Y-%m-%d)    # 🆕 Initial 60-day history
else
  SINCE=$(date -d "$LAST_DATE + 1 day" +%Y-%m-%d)  # 🆕 Fetch only new days
fi

UNTIL=$(date +%Y-%m-%d)                     # 🆕 Current date
echo "📅 Fetching data from $SINCE to $UNTIL"   # 🆕



# ==========================================
# 📊 Fetch Facebook Ads Insights
# ==========================================
echo "📊 Fetching Facebook Ads Insights..."

API_VERSION="v21.0"
API_URL="https://graph.facebook.com/${API_VERSION}/act_${FB_AD_ACCOUNT_ID}/insights"
FIELDS="campaign_name,ad_name,adset_name,impressions,clicks,spend,cpc,purchase_roas,date_start,date_stop"

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
OUTPUT_JSON="${LOG_BASE}/api_data/fb_ads_${SINCE}_to_${UNTIL}_${TIMESTAMP}.json"
OUTPUT_CSV="${LOG_BASE}/api_data/fb_ads_${SINCE}_to_${UNTIL}_${TIMESTAMP}.csv"

# ==========================================
# 📂 Define output file paths
# ==========================================
mkdir -p "$LOG_BASE/api_data"
TMP_MERGED=$(mktemp)
echo '{"data":[]}' > "$TMP_MERGED"
START_DATE=$(date -d "$SINCE" +%Y-%m-%d)
END_DATE=$(date -d "$UNTIL" +%Y-%m-%d)


while [ "$(date -d "$START_DATE" +%s)" -le "$(date -d "$END_DATE" +%s)" ]; do
  MONTH_START=$(date -d "$START_DATE" +%Y-%m-%d)
  MONTH_END=$(date -d "$(date -d "$MONTH_START +1 month -1 day")" +%Y-%m-%d)
  if [ "$(date -d "$MONTH_END" +%s)" -gt "$(date -d "$END_DATE" +%s)" ]; then
    MONTH_END=$END_DATE
  fi

  echo "🗓 Fetching range: $MONTH_START → $MONTH_END"    # 🆕 CHANGED
  TEMP_FILE="${LOG_BASE}/api_data/fb_ads_${MONTH_START}_to_${MONTH_END}.json"

  HTTP_STATUS=$(curl -s -w "%{http_code}" -o "$TEMP_FILE" -G "$API_URL" \
    -d "access_token=${FB_ACCESS_TOKEN}" \
    -d "fields=${FIELDS}" \
    -d "level=ad" \
    -d "limit=500" \
    -d "time_range[since]=${MONTH_START}" \
    -d "time_range[until]=${MONTH_END}" \
    -d "time_increment=1")

  if [ "$HTTP_STATUS" != "200" ]; then
    echo "⚠️  Failed month $MONTH_START → $MONTH_END (HTTP $HTTP_STATUS)"    # 🆕 CHANGED
    cat "$TEMP_FILE"
    START_DATE=$(date -d "$MONTH_END +1 day" +%Y-%m-%d)
    continue
  fi

  # 🆕 CHANGED: Merge month results into one file
  jq -s '{data: (.[0].data + .[1].data)}' "$TMP_MERGED" "$TEMP_FILE" > "${TMP_MERGED}.tmp" && mv "${TMP_MERGED}.tmp" "$TMP_MERGED"
  START_DATE=$(date -d "$MONTH_END +1 day" +%Y-%m-%d)
done

mv "$TMP_MERGED" "$OUTPUT_JSON"
TOTAL_RECORDS=$(jq '.data | length' "$OUTPUT_JSON")
echo "✅ All data fetched and merged. Total records: $TOTAL_RECORDS"


# ==========================================
# 🔄 Convert JSON → CSV
# ==========================================
echo "🧮 Processing data → CSV..."
if ! command -v jq &>/dev/null; then
  echo "❌ 'jq' not found. Please install jq (apt install jq or yum install jq)."
  exit 1
fi

# Extract key data fields
jq -r '
  (["campaign_name","ad_name","adset_name","impressions","clicks","spend","cpc","purchase_roas_value","date_start","date_stop"] | @csv),
  (.data[] | [
    .campaign_name,
    .ad_name,
    .adset_name,
    .impressions,
    .clicks,
    .spend,
    .cpc,
    (.purchase_roas[0].value // 0),
    .date_start,
    .date_stop
  ] | @csv)
' "$OUTPUT_JSON" > "$OUTPUT_CSV"

echo "✅ Saved JSON: $OUTPUT_JSON"
echo "✅ Saved CSV:  $OUTPUT_CSV"


# ==========================================
# 💾 Load CSV → MySQL
# ==========================================
echo "💾 Inserting data into MySQL..."    # 🆕

# 🆕 NEW: Bulk insert into woo_master.advertisements
mysql --local-infile=1 -h "$DB_HOST" -u "$DB_USER" -P "$DB_PORT" -p"$DB_PASS" "$DB_NAME" <<EOF
DROP TEMPORARY TABLE IF EXISTS tmp_ads_import;
CREATE TEMPORARY TABLE tmp_ads_import LIKE advertisements;

LOAD DATA LOCAL INFILE '$(realpath "$OUTPUT_CSV")'
INTO TABLE tmp_ads_import
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(campaign_name, ad_name, adset_name, impressions, clicks, spend, cpc, purchase_roas, date_start, date_stop);

INSERT INTO advertisements
SELECT * FROM tmp_ads_import
ON DUPLICATE KEY UPDATE
  impressions=VALUES(impressions),
  clicks=VALUES(clicks),
  spend=VALUES(spend),
  cpc=VALUES(cpc),
  purchase_roas=VALUES(purchase_roas);
EOF

echo "✅ Data successfully loaded into woo_master.advertisements"   # 🆕
echo "🕒 Completed at $(date '+%Y-%m-%d %H:%M:%S')"
# ==========================================
# ✅ Done
# ==========================================
