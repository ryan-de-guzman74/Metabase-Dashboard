#!/bin/bash
set -e
set -o pipefail

# =========================================================
# 🔄 Reliable MySQL Runner with Retry Logic
# =========================================================
run_mysql_query() {
  local HOST=$1
  local USER=$2
  local PASS=$3
  local DB=$4
  local QUERY=$5
  local MAX_RETRIES=3
  local RETRY_DELAY=5

  for ((attempt=1; attempt<=MAX_RETRIES; attempt++)); do
    echo "⚙️  [Attempt $attempt/$MAX_RETRIES] Running MySQL query on $DB ..."
    mysql --local-infile=1 -h "$HOST" -P 3306 -u "$USER" -p"$PASS" "$DB" -e "$QUERY" && return 0
    echo "⚠️  Query failed. Retrying in ${RETRY_DELAY}s..."
    sleep $RETRY_DELAY
    RETRY_DELAY=$((RETRY_DELAY * 2))
  done

  echo "❌ MySQL query failed after $MAX_RETRIES attempts"
  return 1
}
