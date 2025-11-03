#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_product_images_etl() {
  echo "🖼️ Updating Master Product Image URLs (normalized join)..."

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    DROP TABLE IF EXISTS temp_image_urls;
    CREATE TABLE temp_image_urls (
      image_id BIGINT,
      image_url TEXT
    );
  "

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["OPS"]}"
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
    SELECT ID AS image_id, guid AS image_url
    FROM wp_posts
    WHERE post_type='attachment' AND post_mime_type LIKE 'image/%';
  " > temp_master_image_urls.tsv

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    USE woo_master;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_master_image_urls.tsv'
    INTO TABLE temp_image_urls
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;
    CREATE INDEX idx_temp_image_id ON temp_image_urls (image_id);
  "

  # ✅ Now join normalized map + URLs to update products
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" " 
    SET SESSION group_concat_max_len = 1000000;  -- 🧠 allow long URL lists

    UPDATE products p
    LEFT JOIN (
      SELECT g.product_id, GROUP_CONCAT(i.image_url ORDER BY i.image_id SEPARATOR ',') AS urls
      FROM product_gallery_map g
      LEFT JOIN temp_image_urls i ON g.image_id = i.image_id
      GROUP BY g.product_id
    ) img ON p.product_id = img.product_id
    SET p.image_url = img.urls;

    DROP TABLE temp_image_urls;
    DROP TABLE IF EXISTS product_gallery_map;  
  "

  rm -f temp_master_image_urls.tsv
  echo "✅ Master Products updated with image URLs using normalized mapping."
}
