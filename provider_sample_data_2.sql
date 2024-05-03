/*************************************************************************************************************
Script:             Provider sample data 2
Create Date:        2023-11-15
Author:             B. Klein
Description:        Loads sample data
Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-11-15          B. Klein                            Initial Creation
2024-01-18          B. Klein                            Renaming to Dynamic Data Model Mapper
2024-05-01          B. Barker                           Updated Sample Data
*************************************************************************************************************/


/* set role and warehouse */
use role dmm_data_mapper_role;
call system$wait(5);
use warehouse dmm_data_mapper_wh;


create or replace TABLE dmm_customer_sample_db.sample_data.LOCATIONS (LOCATION_NAME VARCHAR(80), LOCATION_ID VARCHAR(80), LOCATION_CITY VARCHAR(16777216),  LOCATION_STATE VARCHAR(50)) CHANGE_TRACKING = TRUE;


create or replace TABLE dmm_customer_sample_db.sample_data.ITEMS (ITEM_NAME VARCHAR(200), ITEM_ID NUMBER(38,0), ITEM_DESCRIPTION VARCHAR(16777216), ITEM_PRICE FLOAT, ITEM_COST FLOAT)CHANGE_TRACKING = TRUE;


create or replace TABLE dmm_customer_sample_db.sample_data.INVENTORIES (LOCATION_ID VARCHAR(80), ITEM_ID NUMBER(38,0), AMOUNT FLOAT, LAST_CHECKED_DATE TIMESTAMP_NTZ(9))CHANGE_TRACKING = TRUE;


-- Insert Sample Locations
INSERT INTO dmm_customer_sample_db.sample_data.LOCATIONS
VALUES
('Warehouse A','1','Indianapolis','IN'),
('Warehouse B','2','Chicago','IL'),
('Warehouse C','3','Miami','FL'),
('Store 729','4','Los Angeles','CA'),
('Store 112','5','San Mateo','CA');

-- Insert Sample Items
INSERT INTO dmm_customer_sample_db.sample_data.ITEMS (ITEM_NAME, ITEM_ID, ITEM_DESCRIPTION, ITEM_PRICE, ITEM_COST)
VALUES
('Laptop', '1', 'A powerful laptop with the latest specs', 999.99, 799.99),
('Desktop PC', '2', 'High-performance desktop computer', 1299.99, 999.99),
('Monitor', '3', '27-inch Full HD monitor', 249.99, 199.99),
('Keyboard', '4', 'Mechanical gaming keyboard', 99.99, 69.99),
('Mouse', '5', 'Wireless ergonomic mouse', 49.99, 29.99),
('Headphones', '6', 'Noise-canceling headphones', 149.99, 99.99),
('External Hard Drive', '7', '2TB USB 3.0 External Hard Drive', 79.99, 59.99),
('SSD', '8', '500GB Solid State Drive', 99.99, 79.99),
('Router', '9', 'Dual-band Wi-Fi router', 79.99, 49.99),
('Printer', '10', 'Wireless all-in-one printer', 149.99, 99.99),
('Graphics Card', '11', 'Graphics RTB 9080', 699.99, 499.99),
('RAM', '12', '16GB DDR4 RAM', 79.99, 49.99),
('Webcam', '13', '1080p HD Webcam', 59.99, 39.99),
('USB Flash Drive', '14', '128GB USB 3.0 Flash Drive', 24.99, 14.99),
('Wireless Earbuds', '15', 'Bluetooth wireless earbuds', 79.99, 59.99),
('Software', '16', 'Office 24/7', 99.99, 79.99),
('Gaming Console', '17', 'Next-gen gaming console', 499.99, 399.99),
('Smartphone', '18', 'Latest smartphone model', 899.99, 699.99),
('Tablet', '19', '10-inch tablet with high-res display', 299.99, 199.99),
('Network Switch', '20', '8-port gigabit network switch', 49.99, 29.99);

-- Insert Sample Inventories
INSERT INTO dmm_customer_sample_db.sample_data.INVENTORIES (LOCATION_ID, ITEM_ID, AMOUNT, LAST_CHECKED_DATE)
VALUES
('1', '1', 10, '2024-05-01'),
('1', '2', 5, '2024-04-01'),
('1', '3', 20, '2024-03-01'),
('1', '4', 15, '2024-02-10'),
('1', '5', 30, '2023-05-01'),
('1', '6', 8, '2022-05-01'),
('1', '7', 12, '2024-04-11'),
('1', '8', 18, '2024-05-01'),
('1', '9', 10, '2024-05-01'),
('1', '10', 7, '2024-05-01'),
('1', '11', 3, '2024-05-01'),
('1', '12', 15, '2024-05-01'),
('1', '13', 25, '2024-05-01'),
('1', '14', 20, '2024-05-01'),
('1', '15', 10, '2024-05-01'),
('1', '16', 5, '2024-05-01'),
('1', '17', 4, '2024-05-01'),
('1', '18', 6, '2024-05-01'),
('1', '19', 8, '2024-05-01'),
('1', '20', 15, '2024-05-01'),
('2', '1', 8, '2024-05-01'),
('2', '2', 12, '2024-05-01'),
('2', '3', 25, '2024-05-01'),
('2', '4', 18, '2024-05-01'),
('2', '5', 22, '2024-05-01'),
('2', '6', 10, '2024-05-01'),
('2', '7', 15, '2024-05-01'),
('2', '8', 20, '2024-05-01'),
('2', '9', 12, '2024-05-01'),
('2', '10', 9, '2024-05-01'),
('2', '11', 5, '2024-05-01'),
('2', '12', 20, '2024-05-01'),
('3', '13', 30, '2024-05-01'),
('3', '14', 25, '2024-05-01'),
('3', '15', 15, '2024-05-01'),
('4', '16', 8, '2024-05-01'),
('4', '17', 6, '2024-05-01'),
('5', '18', 10, '2024-05-01'),
('5', '19', 12, '2024-05-01'),
('5', '20', 18, '2024-05-01');

select 'Run code in script file provider_app_setup.sql on the provider account' as DO_THIS_NEXT;