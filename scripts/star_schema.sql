-- create the product dimension table 
CREATE TABLE product_dim (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(255),
  product_brand VARCHAR(255),
  product_category VARCHAR(255),
  product_price DECIMAL(10, 2)
);
-- create the customer dimension table
CREATE TABLE customer_dim (
  customer_id int PRIMARY KEY,
  phone_number varchar(250),
  full_name varchar(250),
  email varchar(250),
  address varchar(250)
);
-- create the date dimension table
CREATE TABLE `date_dim` (
  date DATE PRIMARY KEY,
  day INT,
  quarter VARCHAR(10),
  month INT,
  year INT,
  month_name VARCHAR(20),
  dayName VARCHAR(20),
  dayOfWeek INT,
  dayOfMonth INT
);
-- create the order dimension table
CREATE TABLE `order_dim` (
  order_id INT,
  order_date DATE,
  customer_id INT,
  product_id INT,
  order_status VARCHAR(250),
  product_price INT,
  quantity INT,
  FOREIGN KEY (order_date) REFERENCES date_dim(`date`)
);