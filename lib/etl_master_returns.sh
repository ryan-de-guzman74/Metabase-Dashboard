#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_returns_etl() {
  echo "🔁 Extracting Returns from Laravel Portal..."

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["RETURNS"]}"
  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for RETURNS"
    return
  fi

  # ==========================================================
  # 🧩 STEP 1️⃣ Extract raw return data from Laravel
  # ==========================================================
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
    SELECT
      s.order_id AS order_number,
      s.order_email,
      s.order_date,
      s.created_at AS return_request_date,
      s.order_site,
      MAX(sd.shiping_method) AS shipping_method,
      MAX(sd.shiping_price) AS payment_for_return_fee,
      s.shipment_status AS return_request_status,
      COUNT(si.id) AS return_requested_items_count,
      GROUP_CONCAT(si.product_sku SEPARATOR ', ') AS return_requested_items_sku,
      GROUP_CONCAT(si.attributes SEPARATOR ' || ') AS return_requested_items_attributes,
      GROUP_CONCAT(DISTINCT si.return_reason SEPARATOR ', ') AS return_reason,
      MAX(sd.payment_method) AS return_method,
      SUM(si.total) AS return_requested_total_amount,
      MAX(sa.country) AS country_code,
      MAX(sa.city) AS city,
      MAX(sd.currency) AS currency
    FROM shipments s
    LEFT JOIN shipment_items si ON s.id = si.shipment_id
    LEFT JOIN shipment_details sd ON s.id = sd.shipment_id
    LEFT JOIN shipment_addresses sa ON s.id = sa.shipment_id
    GROUP BY s.id;
  " > temp_master_returns.tsv


  # ==========================================================
  # 🆕 STEP 2️⃣ Load wp_terms map (from OPS) into local temp table
  # ==========================================================
  echo "📦 Extracting wp_terms from OPS for readable attribute mapping..."
  IFS=',' read -r HOST_OPS DB_OPS USER_OPS PASS_OPS <<< "${REMOTE_DBS["OPS"]}"

  mysql -h "$HOST_OPS" -u "$USER_OPS" -p"$PASS_OPS" "$DB_OPS" -e "
    SELECT term_id, slug, name FROM wp_terms;
  " > temp_wp_terms.tsv

  echo "🧩 Loading OPS term map locally..."
  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    DROP TABLE IF EXISTS temp_terms_map;
    CREATE TABLE temp_terms_map (
      term_id BIGINT,
      slug VARCHAR(255),
      term_name VARCHAR(255)
    );
    LOAD DATA LOCAL INFILE '$(pwd)/temp_wp_terms.tsv'
    INTO TABLE temp_terms_map
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (term_id, slug, term_name);
  "

  # Build two-way lookup: slug→name, id→name
  mysql -N -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    SELECT slug, term_name FROM temp_terms_map
    UNION
    SELECT term_id, term_name FROM temp_terms_map;
  " > term_map.tsv


  # ==========================================================
  # 🧹 STEP 3️⃣ Parse JSON and map term IDs/slugs → readable names
  # ==========================================================

echo "🧹 Translating JSON attributes using OPS term map..."

# 🧩 Build dictionary (slug/id -> readable name)
declare -A term_dict
while IFS=$'\t' read -r key val; do
  term_dict["$key"]="$val"
done < term_map.tsv

# 🧩 Write header first
head -n 1 temp_master_returns.tsv > temp_master_returns_cleaned.tsv

# 🧩 Process each record safely
tail -n +2 temp_master_returns.tsv | while IFS=$'\t' read -r line; do
  raw_attr=$(echo "$line" | cut -f11)
  IFS='||' read -ra json_parts <<< "$raw_attr"

  formatted_all=""

  for json_blob in "${json_parts[@]}"; do
    json_blob=$(echo "$json_blob" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    if [[ "$json_blob" =~ ^\{.*\}$ ]]; then
      # Extract key-value pairs using jq
      formatted=$(echo "$json_blob" | jq -r '
        to_entries |
        map(
          if .key=="pa_renk" then "Color:" + .value.value
          elif .key=="pa_beden" then "Size:" + .value.value
          elif .key=="pa_materyal" then "Material:" + .value.value
          elif .key=="pa_cinsiyet" then "Gender:" + .value.value
          elif .key=="pa_ozellik" then "Feature:" + .value.value
          else empty end
        ) | join(", ")
      ' 2>/dev/null)

      # Replace slugs/IDs with readable names if available
      if [ -n "$formatted" ]; then
        newpretty=""
IFS=',' read -ra pairs <<< "$formatted"
for pair in "${pairs[@]}"; do
  # Clean whitespace
  pair=$(echo "$pair" | xargs)
  key=$(echo "$pair" | cut -d':' -f1 | xargs)
  val=$(echo "$pair" | cut -d':' -f2- | xargs)

  # Skip blanks
  if [ -z "$key" ] || [ -z "$val" ]; then
    continue
  fi

  # Map slugs/IDs → readable term name safely
  if [[ -n "${term_dict[$val]}" ]]; then
    mapped="${term_dict[$val]}"
  else
    mapped="$val"
  fi

  # Build final string
  if [ -z "$newpretty" ]; then
    newpretty="${key}:${mapped}"
  else
    newpretty="${newpretty}, ${key}:${mapped}"
  fi
done

        if [ -z "$formatted_all" ]; then
          formatted_all="$newpretty"
        else
          formatted_all="$formatted_all || $newpretty"
        fi
      fi
    fi
  done

  # Replace field 11 with formatted attributes
  before=$(echo "$line" | cut -f1-10)
  after=$(echo "$line" | cut -f12-)
  echo -e "${before}\t${formatted_all}\t${after}" >> temp_master_returns_cleaned.tsv
done



  # ==========================================================
  # 🧩 STEP 4️⃣ Clean invalid UTF-8 and finalize
  # ==========================================================
  iconv -f UTF-8 -t UTF-8//IGNORE temp_master_returns_cleaned.tsv -o temp_master_returns_utf8.tsv
  mv temp_master_returns_utf8.tsv temp_master_returns.tsv
  rm -f temp_master_returns_cleaned.tsv temp_wp_terms.tsv term_map.tsv


  # ==========================================================
  # 🧩 STEP 5️⃣ Load into master DB
  # ==========================================================
  echo "📥 Loading returns into woo_master.returns..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    USE woo_master;
    TRUNCATE TABLE returns;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_master_returns.tsv'
    INTO TABLE returns
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (order_number, order_email, order_date, return_request_date, order_site,
     shipping_method, payment_for_return_fee, return_request_status,
     return_requested_items_count, return_requested_items_sku,
     return_requested_items_attributes, return_reason, return_method,
     return_requested_total_amount, country_code, city, currency);
    DROP TABLE IF EXISTS woo_master.temp_terms_map;
  "

  rm -f temp_master_returns.tsv
  echo "✅ Master Returns table updated successfully!"
}
