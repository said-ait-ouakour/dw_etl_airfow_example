CREATE TABLE `ecommercedb`.`customer` (
    `customer_id` INT,
    `first_name` VARCHAR(20),
    `last_name` VARCHAR(20),
    `email` VARCHAR(100),
    `phone_number` VARCHAR(100),
    `address` VARCHAR(100),
    `city` VARCHAR(20),
    `state` VARCHAR(50),
    `zip_code` VARCHAR(20),
    PRIMARY KEY (`customer_id`)
) ENGINE = InnoDB;
CREATE TABLE `ecommercedb`.`product` (
    `product_id` INT,
    `product_name` VARCHAR(20),
    `product_description` VARCHAR(50),
    `product_category` VARCHAR(20),
    `product_brand` VARCHAR(20),
    `product_price` DECIMAL,
    `product_quantity` INT,
    PRIMARY KEY (`product_id`)
) ENGINE = InnoDB;
CREATE TABLE `ecommercedb`.`order` (
    `order_id` INT,
    `customer_id` INT,
    `order_date` DATE,
    `order_status` VARCHAR(50),
    `shipping_address` VARCHAR(100),
    `billing_address` VARCHAR(100),
    PRIMARY KEY (`order_id`),
    FOREIGN KEY (`customer_id`) REFERENCES `customer`(`customer_id`)
) ENGINE = InnoDB;
CREATE TABLE `ecommercedb`.`order-item` (
    `order_item_id` INT,
    `order_id` INT,
    `product_id` INT,
    `product_name` VARCHAR(100),
    `product_price` DECIMAL,
    `quantity` INT,
    PRIMARY KEY (`order_item_id`),
    FOREIGN KEY (`product_id`) REFERENCES `product`(`product_id`),
    FOREIGN KEY (`order_id`) REFERENCES `order`(`order_id`)
) ENGINE = InnoDB;