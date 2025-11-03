#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_categories_tags_etl() {
  echo "🏷️ Building Categories & Tags data from OPS ..."

  IFS=',' read -r HOST DB USER PASS <<< "${REMOTE_DBS["OPS"]}"

  if [ -z "$HOST" ]; then
    echo "❌ No remote configuration found for OPS"
    return
  fi

  # ✅ Extract category & tag data for all products
  echo "📦 Extracting Categories & Tags from OPS..."
  run_mysql_query "$HOST" "$USER" "$PASS" "$DB" "
    SELECT
      tr.object_id AS product_id,
      GROUP_CONCAT(DISTINCT CASE WHEN tt.taxonomy = 'product_cat' THEN t.name END SEPARATOR ',') AS categories,
      GROUP_CONCAT(DISTINCT CASE WHEN tt.taxonomy <> 'product_cat' THEN t.name END SEPARATOR ',') AS tags
    FROM wp_term_relationships tr
    INNER JOIN wp_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
    INNER JOIN wp_terms t ON tt.term_id = t.term_id
    GROUP BY tr.object_id;
  " > temp_master_categories_tags.tsv

  # ✅ Step 2: Prepare target temp table
  echo "🧱 Preparing temp table in woo_master..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    DROP TABLE IF EXISTS temp_categories_tags;
    CREATE TABLE temp_categories_tags (
      product_id BIGINT,
      categories TEXT,
      tags TEXT
    );
  "

  # ✅ Step 3: Load extracted TSV file
  echo "📥 Loading category/tag data into temp table..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    LOAD DATA LOCAL INFILE '$(pwd)/temp_master_categories_tags.tsv'
    INTO TABLE temp_categories_tags
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (product_id, categories, tags);
  "

  # ✅ Step 4: Create index
  echo "⚙️  Creating index for fast joins..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    CREATE INDEX idx_temp_product_id ON temp_categories_tags (product_id);
  " 2>/dev/null || true

  # ✅ Step 5: Update products table
  echo "🔗 Updating master products table..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    UPDATE products p
    INNER JOIN temp_categories_tags ct ON p.product_id = ct.product_id
    SET p.categories = ct.categories,
        p.tags = ct.tags;
  "

  # ✅ Step 6: Cleanup
  echo "🧹 Cleaning up temp files..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    DROP TABLE IF EXISTS temp_categories_tags;
  "
  rm -f temp_master_categories_tags.tsv

  echo "✅ Categories & Tags fields updated successfully in woo_master.products"
}
