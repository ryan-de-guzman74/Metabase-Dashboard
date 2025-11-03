#!/bin/bash
# =========================================================
# 📊 ETL: Facebook Ads Insights → woo_master.advertisements
# Author: Phung (clean version)
# =========================================================

source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_ads_insights_etl() {
  echo "🚀 Starting Facebook Ads Insights ETL..."

  # ==========================================
  # 🌍 Detect environment (Docker vs Host)
  # ==========================================
  if [ -f "/.dockerenv" ] || grep -qa "docker" /proc/1/cgroup >/dev/null 2>&1; then
    ENVIRONMENT="docker"
  else
    ENVIRONMENT="host"
  fi
  echo "🐳 Environment detected: $ENVIRONMENT"

  # ==========================================
  # 🧩 Load environment config
  # ==========================================
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

  if [ -f "$SCRIPT_DIR/etl_config.env" ]; then
    source "$SCRIPT_DIR/etl_config.env"
    CONFIG_SOURCE="$SCRIPT_DIR/etl_config.env"
  elif [ -f "$SCRIPT_DIR/../etl_config.env" ]; then
    source "$SCRIPT_DIR/../etl_config.env"
    CONFIG_SOURCE="$SCRIPT_DIR/../etl_config.env"
  elif [ -f /app/etl_config.env ]; then
    source /app/etl_config.env
    CONFIG_SOURCE="/app/etl_config.env"
  else
    echo "❌ etl_config.env not found near $SCRIPT_DIR or /app."
    exit 1
  fi
  echo "🔧 Loaded config from: $CONFIG_SOURCE"

  # ==========================================
  # 📂 Define directories and logging
  # ==========================================
  if [ "$ENVIRONMENT" = "docker" ]; then
    LOG_BASE="/app/logs"
  else
    LOG_BASE="../logs"
  fi

  mkdir -p "$LOG_BASE/api_data"
  chmod -R 777 "$LOG_BASE/api_data"

  # ==========================================
  # 🗓 Determine date range (incremental)
  # ==========================================
  DB_HOST="$LOCAL_HOST"
  DB_USER="$LOCAL_USER"
  DB_PASS="$LOCAL_PASS"
  DB_PORT="${LOCAL_PORT:-3306}"
  DB_NAME="woo_master"

  echo "🧠 Checking last imported date..."
LAST_DATE=$(mysql --local-infile=1 -h "$DB_HOST" -P "$DB_PORT" -u"$DB_USER" -p"$DB_PASS" -Nse \
  "SELECT MAX(date_stop) FROM ${DB_NAME}.advertisements;" 2>/dev/null | tr -d '\r')

TODAY=$(date +%Y-%m-%d)

if [[ -z "$LAST_DATE" || "$LAST_DATE" == "NULL" ]]; then
  # 🆕 Initial 60-day history
  SINCE=$(date -d '60 days ago' +%Y-%m-%d)
elif [[ "$(date -d "$LAST_DATE" +%s)" -ge "$(date -d "$TODAY" +%s)" ]]; then
  # 🧭 If somehow last date >= today, reset
  SINCE=$(date -d '60 days ago' +%Y-%m-%d)
else
  # ✅ Continue incrementally
  SINCE=$(date -d "$LAST_DATE + 1 day" +%Y-%m-%d)
fi

UNTIL="$TODAY"
echo "📅 Fetching from $SINCE → $UNTIL"
  echo "📅 Fetching from $SINCE → $UNTIL"

  # ==========================================
  # 📊 Facebook Ads API setup
  # ==========================================
  API_VERSION="v21.0"
  API_URL="https://graph.facebook.com/${API_VERSION}/act_${FB_AD_ACCOUNT_ID}/insights"
  FIELDS="campaign_name,ad_name,adset_name,impressions,clicks,spend,cpc,purchase_roas,date_start,date_stop"

  TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
  OUTPUT_JSON="${LOG_BASE}/api_data/fb_ads_${SINCE}_to_${UNTIL}_${TIMESTAMP}.json"
  OUTPUT_CSV="${LOG_BASE}/api_data/fb_ads_${SINCE}_to_${UNTIL}_${TIMESTAMP}.csv"

  echo '{"data":[]}' >"$OUTPUT_JSON"

  # ==========================================
  # 🧭 Fetch monthly chunks
  # ==========================================
  START_DATE="$SINCE"
  END_DATE="$UNTIL"

  while [ "$(date -d "$START_DATE" +%s)" -le "$(date -d "$END_DATE" +%s)" ]; do
    MONTH_START=$(date -d "$START_DATE" +%Y-%m-%d)
    MONTH_END=$(date -d "$(date -d "$MONTH_START +1 month -1 day")" +%Y-%m-%d)
    if [ "$(date -d "$MONTH_END" +%s)" -gt "$(date -d "$END_DATE" +%s)" ]; then
      MONTH_END=$END_DATE
    fi

    TEMP_FILE="${LOG_BASE}/api_data/fb_ads_${MONTH_START}_to_${MONTH_END}.json"
    echo "📆 Fetching range: $MONTH_START → $MONTH_END"

    HTTP_STATUS=$(curl -s -w "%{http_code}" -o "$TEMP_FILE" -G "$API_URL" \
      -d "access_token=${FB_ACCESS_TOKEN}" \
      -d "fields=${FIELDS}" \
      -d "level=ad" \
      -d "limit=500" \
      -d "time_range[since]=${MONTH_START}" \
      -d "time_range[until]=${MONTH_END}" \
      -d "time_increment=1")

    if [ "$HTTP_STATUS" != "200" ]; then
      echo "⚠️ Failed to fetch $MONTH_START → $MONTH_END (HTTP $HTTP_STATUS)"
      START_DATE=$(date -d "$MONTH_END +1 day" +%Y-%m-%d)
      continue
    fi

    # ✅ Merge JSON data safely (handle empty or missing .data arrays)
    jq -s '{
      data: ([.[].data] | add | map(select(. != null)))
    }' "$OUTPUT_JSON" "$TEMP_FILE" >"${OUTPUT_JSON}.tmp" && mv "${OUTPUT_JSON}.tmp" "$OUTPUT_JSON"

    START_DATE=$(date -d "$MONTH_END +1 day" +%Y-%m-%d)
  done

  TOTAL_RECORDS=$(jq '.data | length' "$OUTPUT_JSON")
  echo "✅ Data fetched and merged ($TOTAL_RECORDS records)"

  # ==========================================
  # 🧮 Convert JSON → CSV
  # ==========================================
  echo "🧮 Converting JSON → CSV..."
  jq -r '
    (["campaign_name","ad_name","adset_name","impressions","clicks","spend","cpc","purchase_roas","date_start","date_stop"] | @csv),
    (.data[]? | [
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
  ' "$OUTPUT_JSON" >"$OUTPUT_CSV"

  echo "✅ CSV ready at: $OUTPUT_CSV"

  # ==========================================
  # 💾 Load CSV → MySQL
  # ==========================================
  echo "💾 Loading data into MySQL (${DB_NAME}.advertisements)..."

  mysql --local-infile=1 -h "$DB_HOST" -u "$DB_USER" -P "$DB_PORT" -p"$DB_PASS" "$DB_NAME" <<EOF
CREATE TABLE IF NOT EXISTS advertisements (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  campaign_name VARCHAR(255),
  ad_name VARCHAR(255),
  adset_name VARCHAR(255),
  impressions INT,
  clicks INT,
  spend DECIMAL(12,2),
  cpc DECIMAL(12,2),
  purchase_roas DECIMAL(12,4),
  date_start DATE,
  date_stop DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_ad (campaign_name, ad_name, date_start)
);

-- 🧩 Create a temporary table for clean import
DROP TEMPORARY TABLE IF EXISTS tmp_ads;
CREATE TEMPORARY TABLE tmp_ads LIKE advertisements;

LOAD DATA LOCAL INFILE '$(realpath "$OUTPUT_CSV")'
INTO TABLE tmp_ads
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(campaign_name, ad_name, adset_name, impressions, clicks, spend, cpc, purchase_roas, date_start, date_stop);

INSERT INTO  advertisements
(campaign_name, ad_name, adset_name, impressions, clicks, spend, cpc, purchase_roas, date_start, date_stop)
SELECT
  campaign_name, ad_name, adset_name, impressions, clicks,
  spend, cpc, purchase_roas, date_start, date_stop
FROM tmp_ads
ON DUPLICATE KEY UPDATE
  impressions=VALUES(impressions),
  clicks=VALUES(clicks),
  spend=VALUES(spend),
  cpc=VALUES(cpc),
  purchase_roas=VALUES(purchase_roas),
  date_stop=VALUES(date_stop);
DROP TEMPORARY TABLE tmp_ads;
EOF

  echo "✅ Data successfully loaded into MySQL"
  echo "🕒 Completed at $(date '+%Y-%m-%d %H:%M:%S')"
}

