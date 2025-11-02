-- =========================================================
-- 🌍 E-COMMERCE ANALYTICS DATABASE SCHEMA (v5.0 Production)
-- =========================================================
CREATE DATABASE IF NOT EXISTS metabase_app;  
CREATE DATABASE IF NOT EXISTS woo_master;   
CREATE DATABASE IF NOT EXISTS woo_tr;
CREATE DATABASE IF NOT EXISTS laravel_returns;    


USE woo_tr;

-- =========================================================
-- 1️⃣ Orders Table (per order, not per product)
-- =========================================================

DROP TABLE IF EXISTS woo_tr.orders;
CREATE TABLE IF NOT EXISTS orders (
  order_number_formatted VARCHAR(50),
  order_id BIGINT PRIMARY KEY,
  order_date DATETIME,
  order_status VARCHAR(50),
  customer_id BIGINT,
  country_code VARCHAR(5),
  channel VARCHAR(50),
  site VARCHAR(255),
  billing_country VARCHAR(100),
  billing_city VARCHAR(100),
  units_total INT,
  ordered_items_count INT,
  ordered_items_skus TEXT,
  payment_method VARCHAR(100),
  currency_code VARCHAR(10),
  total_price DECIMAL(12,2),
  gross_total DECIMAL(12,2),
  subtotal DECIMAL(12,2),
  cogs DECIMAL(12,2),
  tax_amount DECIMAL(12,2),
  shipping_fee DECIMAL(12,2),
  fee_amount DECIMAL(12,2),
  discount_amount DECIMAL(12,2),
  refunded_amount DECIMAL(12,2),
  ads_spend DECIMAL(12,2),
  logistics_cost DECIMAL(12,2),
  other_costs DECIMAL(12,2),
  net_profit DECIMAL(12,2),
  net_revenue DECIMAL(12,2),
  net_margin DECIMAL(8,2)
);

-- =========================================================
-- 2️⃣ Order Items Table (each ordered product)
-- =========================================================
DROP TABLE IF EXISTS woo_tr.order_items;
CREATE TABLE IF NOT EXISTS order_items (
  order_item_id BIGINT PRIMARY KEY,
  order_id BIGINT,
  product_id BIGINT,
  variation_id BIGINT,
  sku VARCHAR(100),                    -- 🆕 Added
  order_item_name VARCHAR(200),
  quantity INT DEFAULT 1,
  line_total DECIMAL(12,2),
  line_tax DECIMAL(12,2),
  refund_reference BIGINT,
  currency_code VARCHAR(10),           -- 🆕 Added
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- 4️⃣ Customers
-- =========================================================
DROP TABLE IF EXISTS woo_tr.customers;
CREATE TABLE IF NOT EXISTS customers (
  customer_id BIGINT,
  full_name VARCHAR(255),
  email VARCHAR(255),
  phone VARCHAR(50),
  registered_at DATETIME,
  first_order_date DATETIME,
  last_order_date DATETIME,
  orders_count INT,
  units_total INT,
  ltv DECIMAL(12,2),
  aov DECIMAL(12,2),
  refunds_total DECIMAL(12,2),
  source_store VARCHAR(50),
  billing_country VARCHAR(100),
  billing_city VARCHAR(100),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_id)
);

-- =========================================================
-- 5 🟢 MASTER DATABASE (Unified BI Schema)
-- =========================================================
USE woo_master;

-- 4.1 Master Products (OPS + PIM merged)
DROP TABLE IF EXISTS woo_master.products;
CREATE TABLE woo_master.products (
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


-- 4.2 Master Orders (merged across stores)
DROP TABLE IF EXISTS woo_master.orders;
CREATE TABLE woo_master.orders (
  order_number_formatted VARCHAR(50) NOT NULL,
  source_store VARCHAR(50),
  order_id BIGINT,
  order_date DATETIME,
  order_status VARCHAR(50),
  customer_id BIGINT,
  country_code VARCHAR(5),
  channel VARCHAR(50),
  site VARCHAR(255),
  billing_country VARCHAR(100),
  billing_city VARCHAR(100),
  units_total INT,
  ordered_items_count INT,
  ordered_items_skus TEXT,
  payment_method VARCHAR(100),
  currency_code VARCHAR(10),
  subtotal DECIMAL(12,2),
  gross_total DECIMAL(12,2),
  cogs DECIMAL(12,2),
  total_price DECIMAL(12,2),
  tax_amount DECIMAL(12,2),
  shipping_fee DECIMAL(12,2),
  fee_amount DECIMAL(12,2),
  discount_amount DECIMAL(12,2),
  refunded_amount DECIMAL(12,2),
  ads_spend DECIMAL(12,2),
  logistics_cost DECIMAL(12,2),
  other_costs DECIMAL(12,2),
  net_profit DECIMAL(12,2),
  net_revenue DECIMAL(12,2),
  net_margin DECIMAL(8,2),
  PRIMARY KEY (order_number_formatted),
  INDEX idx_orders_customer (customer_id)
);

-- 4.3 Master Order Items
DROP TABLE IF EXISTS woo_master.order_items;
CREATE TABLE woo_master.order_items (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,     
  order_item_id BIGINT,
  order_id BIGINT,
  product_id BIGINT,
  variation_id BIGINT,
  sku VARCHAR(100),
  order_item_name VARCHAR(200),
  quantity INT DEFAULT 1,
  line_total DECIMAL(12,2),
  line_tax DECIMAL(12,2),
  refund_reference BIGINT,
  currency_code VARCHAR(10),
  source_store VARCHAR(50),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_item_store (order_item_id, source_store),
  INDEX idx_order_items_order (order_id)
);

-- 4.4 Master Customers
DROP TABLE IF EXISTS woo_master.customers;
CREATE TABLE woo_master.customers (
  customer_number_formatted VARCHAR(50),
  customer_id BIGINT,
  full_name VARCHAR(255),
  email VARCHAR(255),
  phone VARCHAR(50),
  registered_at DATETIME,
  first_order_date DATETIME,
  last_order_date DATETIME,
  orders_count INT,
  units_total INT,
  ltv DECIMAL(12,2),
  aov DECIMAL(12,2),
  refunds_total DECIMAL(12,2),
  billing_country VARCHAR(100),
  billing_city VARCHAR(100),
  source_store VARCHAR(50),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (customer_id, source_store),
  INDEX idx_customers_email (email)
);

-- 4.5 Master Returns
DROP TABLE IF EXISTS woo_master.returns;
CREATE TABLE IF NOT EXISTS returns (
  id INT AUTO_INCREMENT PRIMARY KEY,
  order_number VARCHAR(50),
  order_email VARCHAR(255),
  order_date DATETIME,
  return_request_date DATETIME,
  order_site VARCHAR(255),
  shipping_method VARCHAR(255),
  payment_for_return_fee DECIMAL(12,2),
  return_request_status VARCHAR(100),
  return_requested_items_count INT,
  return_requested_items_sku TEXT,
  return_requested_items_attributes TEXT,
  return_reason TEXT,
  return_method VARCHAR(100),
  return_requested_total_amount DECIMAL(12,2),
  country_code VARCHAR(5),
  city VARCHAR(100),
  currency VARCHAR(10),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);


SELECT '✅ Production schema setup complete.' AS status;
