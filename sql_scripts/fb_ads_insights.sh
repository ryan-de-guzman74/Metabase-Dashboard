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

# ==========================================
# 📊 Fetch Facebook Ads Insights
# ==========================================
echo "📊 Fetching Facebook Ads Insights..."

API_VERSION="v21.0"
API_URL="https://graph.facebook.com/${API_VERSION}/act_${FB_AD_ACCOUNT_ID}/insights"
FIELDS="campaign_name,ad_name,adset_name,impressions,clicks,spend,actions"

# Save JSON output to correct location
TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
OUTPUT_JSON="${LOG_BASE}/api_data/fb_ads_${TIMESTAMP}.json"
OUTPUT_CSV="${LOG_BASE}/api_data/fb_ads_${TIMESTAMP}.csv"
# Perform API call
HTTP_RESPONSE=$(curl -s -w "%{http_code}" -o "$OUTPUT_JSON" -G "$API_URL" \
  -d "access_token=$FB_ACCESS_TOKEN" \
  -d "fields=$FIELDS" \
  -d "level=ad" \
  -d "time_range[since]=$(date -d 'yesterday' +%Y-%m-%d)" \
  -d "time_range[until]=$(date +%Y-%m-%d)")

# ==========================================
# 🧪 Validate response
# ==========================================
if [ "$HTTP_RESPONSE" != "200" ]; then
  echo "❌ Failed to fetch Facebook data. HTTP $HTTP_RESPONSE"
  echo "📄 Response content:"
  cat "$OUTPUT_JSON"
  exit 1
fi
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
  (["campaign_name","ad_name","adset_name","impressions","clicks","spend","date_start","date_stop"] | @csv),
  (.data[] | [
    .campaign_name,
    .ad_name,
    .adset_name,
    .impressions,
    .clicks,
    .spend,
    .date_start,
    .date_stop
  ] | @csv)
' "$OUTPUT_JSON" > "$OUTPUT_CSV"

# ==========================================
# ✅ Done
# ==========================================
echo "✅ Facebook Ads data fetched successfully → JSON: $OUTPUT_JSON"
echo "✅ Processed CSV file created at: $OUTPUT_CSV"
