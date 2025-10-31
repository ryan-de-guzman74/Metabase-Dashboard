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
OUTPUT_JSON="${LOG_BASE}/api_data/fb_ads_$(date +%Y-%m-%d_%H-%M-%S).json"

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
# ✅ Success
# ==========================================
echo "✅ Facebook Ads data fetched successfully → saved to $OUTPUT_JSON"
