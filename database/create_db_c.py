import mysql.connector
conn = mysql.connector.connect(host="localhost", user="root", database="vooray", password="mis5900vooray")

curs = conn.cursor()
print(curs)


def create_item():
    curs.execute(
        f'''
        CREATE TABLE `vooray_c`.`item` (
          `fnsku` VARCHAR(60) NULL,
          `sku` VARCHAR(45) NOT NULL,
          `product_name` VARCHAR(360) NULL,
          `asin` VARCHAR(60),
          PRIMARY KEY (`sku`));
        ''')

def create_inv_level():
    curs.execute(
        f'''
        CREATE TABLE `vooray_c`.`inv_level` (
          `s_date` VARCHAR(60),
          `date` DATE NOT NULL,
          `quantity` INT NULL,
          `fulfillment_center_id` VARCHAR(20) NULL,
          `detailed_disposition` VARCHAR(45) NULL,
          `country` VARCHAR(10) NULL,
          `sku` VARCHAR(45) NULL);
        ''')

def create_amz_price():
    curs.execute(
        f'''
        CREATE TABLE `vooray_c`.`amz_price` (
          `sku` VARCHAR(45) NULL,
          `p_date` VARCHAR(60),
          `date` DATE NOT NULL,
          `amazon_order_id` VARCHAR(45) NULL,
          `fulfillment_channel` VARCHAR(45) NULL,
          `sales_channel` VARCHAR(45) NULL,
          `currency` VARCHAR(10) NULL,
          `item_price` FLOAT NULL,
          `item_tax` FLOAT NULL,
          `shipping_price` FLOAT NULL,
          `shipping_tax` FLOAT NULL,
          INDEX `sku_idx` (`sku` ASC) VISIBLE,
          CONSTRAINT `sku`
            FOREIGN KEY (`sku`)
            REFERENCES `vooray_c`.`item` (`sku`)
            ON DELETE NO ACTION
            ON UPDATE NO ACTION);
        ''')


def create_quantity_sold():
    curs.execute(
        f'''
        CREATE TABLE `vooray_c`.`quantity_sold` (
          `sku` VARCHAR(45) NULL,
          `p_date` VARCHAR(60),
          `date` DATE NOT NULL,
          `quantity` INT NULL,
          `amazon_order_id` VARCHAR(45) NULL,
          `fulfillment_channel` VARCHAR(45) NULL,
          `sales_channel` VARCHAR(45) NULL,
          INDEX `sku_idx` (`sku` ASC) VISIBLE,
          CONSTRAINT ``
            FOREIGN KEY (`sku`)
            REFERENCES `vooray_c`.`item` (`sku`)
            ON DELETE NO ACTION
            ON UPDATE NO ACTION);
        ''')


def main():
    create_item()
    create_inv_level()
    create_amz_price()
    create_quantity_sold()

    print("DATABASE CREATED")


main()
