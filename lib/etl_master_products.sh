#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_products_etl() {
  echo "🧠 Building Master Products Table from OPS ..."
  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["OPS"]}"

  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for OPS"
    return
  fi

  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
    SELECT
      p.ID AS product_id,
      p.post_title AS title,
      MAX(CASE WHEN pm.meta_key = '_sku' THEN pm.meta_value END) AS sku,
      MAX(CASE WHEN pm.meta_key = '_main_SKU' THEN pm.meta_value END) AS parent_sku,
      MAX(CASE WHEN pm.meta_key = '_product_attributes' THEN pm.meta_value END) AS attributes,
      MAX(wc.stock_quantity) AS stock_qty,
      NULL AS categories,
      NULL AS tags,
      MAX(CASE WHEN pm.meta_key = '_regular_price' THEN pm.meta_value END) AS regular_price,
      MAX(CASE WHEN pm.meta_key = '_sale_price' THEN pm.meta_value END) AS sale_price,
      MAX(CASE WHEN pm.meta_key = '_product_image_gallery' THEN pm.meta_value END) AS image_url,
      MAX(CASE WHEN pm.meta_key IN ('_alg_wc_cog_cost','_wc_cog_cost') THEN pm.meta_value END) AS cogs
    FROM wp_posts p
    LEFT JOIN wp_postmeta pm ON p.ID = pm.post_id
    LEFT JOIN wp_wc_product_meta_lookup wc ON wc.product_id = p.ID
    WHERE p.post_type IN ('product','product_variation')
    GROUP BY p.ID;
  " > temp_master_products.tsv

  echo "📦 Extracting term_id → name map from OPS..."
  mysql -h "$HOST" -u "$USER" -p"$PASS" "$DB" -e "
    SELECT term_id, name FROM wp_terms;
  " > temp_wp_terms.tsv

  echo "🧩 Loading term map locally..."
  mysql --local-infile=1 -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    DROP TABLE IF EXISTS temp_terms_map;
    CREATE TABLE temp_terms_map (
      term_id BIGINT,
      term_name VARCHAR(255)
    );
    LOAD DATA LOCAL INFILE '$(pwd)/temp_wp_terms.tsv'
    INTO TABLE temp_terms_map
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (term_id, term_name);
  "

  echo "🧹 Translating serialized attributes into readable English names..."

  mysql -N -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -e "
    USE woo_master;
    SELECT term_id, term_name FROM temp_terms_map;
  " > term_map.tsv

  awk -F'\t' 'NR==FNR {dict[$1]=$2; next}
  BEGIN {OFS="\t"}
  NR==1 {print; next}
  {
    raw=$5
    gsub(/\r/,"",raw)
    
    n=split(raw, arr, "s:[0-9]+:\"pa_")
    pretty=""
    for (i=2; i<=n; i++) {
      key=substr(arr[i], 1, index(arr[i], "\"")-1)
      valmatch=match(arr[i], /s:[0-9]+:\"[0-9\|]+\"/)
      if (valmatch) {
        val=substr(arr[i], RSTART, RLENGTH)
        gsub(/s:[0-9]+:\"/, "", val)
        gsub(/\"/, "", val)
        gsub(/\|/, ",", val)

        split(val, ids, ",")
        newvals=""
        for (j in ids) {
          id=ids[j]
          if (id in dict) {
            if (newvals=="") newvals=dict[id]; else newvals=newvals "," dict[id]
          } else {
            if (newvals=="") newvals=id; else newvals=newvals "," id
          }
        }
        val=newvals

        if (key=="renk") key="Color"
        else if (key=="beden") key="Size"
        else if (key=="materyal") key="Material"
        else if (key=="cinsiyet") key="Gender"
        else if (key=="ozellik") key="Feature"
        else key=toupper(substr(key,1,1)) substr(key,2)
        
        if (pretty == "") {
          pretty = key ":" val
        } else {
          pretty = pretty ", " key ":" val
        }
      }
    }
    $5=pretty
    print
  }' term_map.tsv temp_master_products.tsv > temp_master_products_cleaned.tsv

  # ✅ FIXED: clean encoding on the cleaned file (not the raw one)
  LANG=C.UTF-8 LC_ALL=C.UTF-8 iconv -f UTF-8 -t UTF-8//IGNORE temp_master_products_cleaned.tsv -o temp_master_products_utf8.tsv
  mv temp_master_products_utf8.tsv temp_master_products.tsv

  rm -f temp_wp_terms.tsv term_map.tsv

  echo "🧩 Loading products into woo_master.products ..."
  mysql --local-infile=1 --default-character-set=utf8mb4 -h "$LOCAL_HOST" -P 3306 -u "$LOCAL_USER" -p"$LOCAL_PASS" "" -e "
    CREATE DATABASE IF NOT EXISTS woo_master;
    USE woo_master;
    CREATE TABLE IF NOT EXISTS products (
      product_id BIGINT PRIMARY KEY,
      title VARCHAR(255),
      sku VARCHAR(100),
      parent_sku VARCHAR(100),
      attributes LONGTEXT,
      stock_qty INT,
      categories VARCHAR(255),
      tags VARCHAR(255),
      regular_price DECIMAL(12,2),
      sale_price DECIMAL(12,2),
      image_url LONGTEXT,
      cogs DECIMAL(12,2),
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    TRUNCATE TABLE products;
    SET NAMES utf8mb4;
    LOAD DATA LOCAL INFILE '"$(pwd)/temp_master_products.tsv"'
    INTO TABLE products
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (product_id, title, sku, parent_sku, attributes, stock_qty, categories, tags, regular_price, sale_price, image_url, cogs);
    DROP TABLE IF EXISTS woo_master.temp_terms_map;
  "

  rm -f temp_master_products.tsv
  echo "✅ Master Products Table built successfully in woo_master.products"
}
