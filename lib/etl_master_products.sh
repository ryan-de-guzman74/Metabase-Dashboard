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

  echo "🧩 Loading products into woo_master.products ..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "" "
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
    );
    TRUNCATE TABLE products;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_master_products.tsv'
    INTO TABLE products
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (product_id, title, sku, parent_sku, attributes, stock_qty, categories, tags, regular_price, sale_price, image_url, cogs);
  "

  rm -f temp_master_products.tsv
  echo "✅ Master Products Table built successfully in woo_master.products"
}
