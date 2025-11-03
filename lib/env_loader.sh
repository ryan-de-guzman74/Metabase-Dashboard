#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ✅ Check both locations for flexibility
if [ -f "$SCRIPT_DIR/etl_config.env" ]; then
  source "$SCRIPT_DIR/etl_config.env"
elif [ -f "$SCRIPT_DIR/config/etl_config.env" ]; then
  source "$SCRIPT_DIR/config/etl_config.env"
else
  echo "❌ etl_config.env not found in $SCRIPT_DIR or $SCRIPT_DIR/config"
  exit 1
fi

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/etl_$(date +%Y-%m-%d).log"
exec > >(tee -a "$LOG_FILE") 2>&1
export -A REMOTE_DBS
echo "🌍 Environment initialized."
echo "📂 Logs: $LOG_FILE"


