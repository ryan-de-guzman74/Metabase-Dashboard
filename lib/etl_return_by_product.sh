#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_return_by_product_etl() {
  echo "🔁 Building Return By Product table..."

  # ==========================================================
  # 🧩 STEP 1️⃣ Explode and aggregate returns by SKU
  # ==========================================================

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    -- Create temp exploded table for SKUs
DROP TABLE IF EXISTS woo_master.temp_return_skus;
CREATE TABLE woo_master.temp_return_skus AS

    WITH exploded AS (
      SELECT
        r.id AS return_id,
        r.order_number,
        r.order_date,
        TRIM(
          REPLACE(
            REPLACE(
              REPLACE(
                SUBSTRING_INDEX(SUBSTRING_INDEX(r.return_requested_items_sku, ',', n.n), ',', -1),
                '\r', ''
              ),
              '\n', ''
            ),
            ' ', ''
          )
        ) AS sku_clean
      FROM woo_master.returns r
      JOIN (
        SELECT a.N + b.N * 10 + 1 AS n
        FROM 
          (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
           UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
          (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
           UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
      ) n
      WHERE
        r.return_requested_items_sku IS NOT NULL
        AND n.n <= 1 + LENGTH(r.return_requested_items_sku) - LENGTH(REPLACE(r.return_requested_items_sku, ',', ''))
    )
    SELECT
      e.sku_clean AS sku,
      p.title AS product_name,
      p.parent_sku,
      p.categories,
      COUNT(DISTINCT e.return_id) AS total_returns,
      MAX(r.order_date) AS last_return_date,
      MIN(r.order_date) AS first_return_date
    FROM exploded e
    LEFT JOIN woo_master.products p
      ON e.sku_clean = p.sku
    LEFT JOIN woo_master.returns r
      ON e.return_id = r.id
    WHERE
      e.sku_clean IS NOT NULL
      AND e.sku_clean <> ''
      AND e.sku_clean NOT LIKE '%NULL%'
      AND e.sku_clean NOT LIKE '%null%'
    GROUP BY
      e.sku_clean, p.title, p.parent_sku, p.categories;
  "

  # ==========================================================
  # 🧩 STEP 2️⃣ Export temp results to TSV
  # ==========================================================
  echo "📤 Exporting aggregated data..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    SELECT * FROM temp_return_skus;
  " > temp_return_by_product.tsv


  # ==========================================================
  # 🧹 STEP 3️⃣ Load into main return_by_product table
  # ==========================================================
  echo "📥 Loading into woo_master.return_by_product..."
  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
  CREATE TABLE IF NOT EXISTS woo_master.return_by_product (
  id INT AUTO_INCREMENT PRIMARY KEY,
  sku VARCHAR(255),
  product_name VARCHAR(500),
  parent_sku VARCHAR(255),
  categories VARCHAR(1000),
  total_returns INT DEFAULT 0,
  first_return_date DATETIME,
  last_return_date DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_sku (sku),
  INDEX idx_total_returns (total_returns)
);
    USE woo_master;
    TRUNCATE TABLE return_by_product;
    LOAD DATA LOCAL INFILE '$(pwd)/temp_return_by_product.tsv'
    INTO TABLE return_by_product
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (sku, product_name, parent_sku, categories, total_returns, last_return_date, first_return_date);
    DROP TABLE IF EXISTS woo_master.temp_return_skus;
  "

  rm -f temp_return_by_product.tsv
  echo "✅ Return By Product table built successfully!"
}
