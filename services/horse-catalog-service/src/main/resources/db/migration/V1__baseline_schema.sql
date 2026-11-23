CREATE SCHEMA  IF NOT EXISTS stable;


CREATE TABLE IF NOT EXISTS  stable.horse_status (
  horse_status_id NUMERIC(3) PRIMARY KEY,
  horse_status VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS stable.horses (
  horse_id NUMERIC(3) NOT NULL PRIMARY KEY,
  horse_status_id NUMERIC(3) NOT NULL,
  horse_name VARCHAR(255),
  breed VARCHAR(255),
  age NUMERIC(3),
  color VARCHAR(255),
  coat_pattern VARCHAR(255),
  training_level VARCHAR(255),
  CONSTRAINT fk_horse_status_id FOREIGN KEY (horse_status_id) REFERENCES stable.horse_status(horse_status_id)
);

CREATE TABLE IF NOT EXISTS stable.horse_goods (
  goods_id NUMERIC(3) PRIMARY KEY,
  goods_name VARCHAR(255),
  good_description VARCHAR(255)

);


-- Create the customers table
CREATE TABLE IF NOT EXISTS stable.customers (
  customer_id NUMERIC(3) NOT NULL PRIMARY KEY,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  email VARCHAR(100) UNIQUE NOT NULL,
  phone VARCHAR(15)
);

-- Create the products table
CREATE TABLE IF NOT EXISTS stable.products (
  product_id NUMERIC(3) PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  description TEXT,
  price DECIMAL(10, 2) NOT NULL,
  stock INT NOT NULL
);

-- Create the orders table
CREATE TABLE IF NOT EXISTS stable.orders (
  order_id NUMERIC(3)  PRIMARY KEY,
  customer_id INT NOT NULL,
  order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(50) NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES stable.customers(customer_id)
);

-- Create the order_items table
CREATE TABLE IF NOT EXISTS stable.order_items (
  order_item_id NUMERIC(3)  PRIMARY KEY,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  quantity INT NOT NULL,
  price DECIMAL(10, 2) NOT NULL,
  FOREIGN KEY (order_id) REFERENCES stable.orders(order_id),
  FOREIGN KEY (product_id) REFERENCES stable.products(product_id)
);

INSERT INTO stable.horse_status (horse_status_id, horse_status) VALUES (1, 'Active');
INSERT INTO stable.horse_status (horse_status_id, horse_status) VALUES (2, 'Inactive');

INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (1, 1, 'Horse1', 'Thoroughbred', 5, 'Brown', 'Solid', 'Advanced');
INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (2, 1, 'Horse2', 'Quarter Horse', 8, 'Chestnut', 'Spotted', 'Intermediate');
INSERT INTO stable.horses (horse_id, horse_status_id, horse_name, breed, age, color, coat_pattern, training_level) VALUES (3, 2, 'Horse3', 'Arabian', 10, 'Gray', 'Pinto', 'Beginner');

INSERT INTO stable.horse_goods (goods_id, goods_name, good_description) VALUES (1, 'Saddle', 'Leather');
INSERT INTO stable.horse_goods (goods_id, goods_name, good_description) VALUES (2, 'Bridle', 'Leather');
INSERT INTO stable.horse_goods (goods_id, goods_name, good_description) VALUES (3, 'Blanket', 'Wool');