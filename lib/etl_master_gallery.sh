#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_product_gallery_map_etl() {
  echo "🧩 Building normalized Product Gallery Map from OPS ..."

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["OPS"]}"
  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for OPS"
    return
  fi

  # Step 1️⃣: Extract product_id → comma-separated image_ids from OPS
  echo "📦 Extracting product → gallery IDs from OPS..."
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
    SELECT
      p.ID AS product_id,
      MAX(CASE WHEN pm.meta_key = '_product_image_gallery' THEN pm.meta_value END) AS image_ids
    FROM wp_posts p
    LEFT JOIN wp_postmeta pm ON p.ID = pm.post_id
    WHERE p.post_type IN ('product', 'product_variation')
    GROUP BY p.ID;
  " > temp_master_gallery_ids.tsv

  # Step 2️⃣: Split comma-separated IDs into normalized rows using Bash + awk
  echo "🧩 Splitting gallery IDs into product_id, image_id pairs..."
  awk -F'\t' 'NR>1 {
    n=split($2, arr, ",");
    for (i=1; i<=n; i++) if (arr[i] != "") print $1 "\t" arr[i];
  }' temp_master_gallery_ids.tsv > temp_product_gallery_pairs.tsv

  # Step 3️⃣: Load into local normalized table
  echo "📥 Creating temporary product_gallery_map and loading data..."

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    DROP TABLE IF EXISTS product_gallery_map;  -- 🧹 ensure old version removed
    CREATE TABLE product_gallery_map (         -- 🟩 temporary in ETL only
      product_id BIGINT,
      image_id BIGINT,
      PRIMARY KEY (product_id, image_id),
      INDEX idx_gallery_image_id (image_id)
    );
    LOAD DATA LOCAL INFILE '$(pwd)/temp_product_gallery_pairs.tsv'
    INTO TABLE product_gallery_map
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    (product_id, image_id);
  "

  # 🧹 Cleanup
  rm -f temp_master_gallery_ids.tsv temp_product_gallery_pairs.tsv
  echo "✅ Normalized product_gallery_map table built successfully in woo_master."
}
