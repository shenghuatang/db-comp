-- SQL Script to create sample customer tables for testing db_compare application
-- Created: December 16, 2025
-- Purpose: Create customers1 and customers2 tables with sample data for comparison testing

-- Drop tables if they exist (clean slate for re-running)
DROP TABLE IF EXISTS customers1;
DROP TABLE IF EXISTS customers2;

-- Create customers1 table
CREATE TABLE customers1 (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    total_purchases DECIMAL(10, 2) NOT NULL,
    last_purchase_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL
);

-- Create customers2 table (identical structure to customers1)
CREATE TABLE customers2 (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    total_purchases DECIMAL(10, 2) NOT NULL,
    last_purchase_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL
);

-- Insert 10 rows of random data into customers1
INSERT INTO customers1 (customer_id, customer_name, email, total_purchases, last_purchase_date, status) VALUES
(1001, 'John Smith', 'john.smith@email.com', 1250.75, '2023-11-15', 'active'),
(1002, 'Sarah Johnson', 'sarah.j@email.com', 3450.00, '2023-12-01', 'active'),
(1003, 'Michael Brown', 'mbrown@email.com', 875.50, '2023-10-22', 'active'),
(1004, 'Emily Davis', 'emily.davis@email.com', 2100.25, '2023-11-28', 'inactive'),
(1005, 'David Wilson', 'dwilson@email.com', 5670.80, '2023-12-05', 'active'),
(1006, 'Jennifer Martinez', 'jmartinez@email.com', 1890.00, '2023-11-10', 'active'),
(1007, 'Robert Taylor', 'rtaylor@email.com', 4250.60, '2023-12-08', 'active'),
(1008, 'Lisa Anderson', 'landerson@email.com', 990.40, '2023-10-30', 'inactive'),
(1009, 'James Thomas', 'jthomas@email.com', 3120.15, '2023-11-20', 'active'),
(1010, 'Patricia Moore', 'pmoore@email.com', 2780.90, '2023-12-03', 'active');

-- Insert the same 10 rows into customers2
INSERT INTO customers2 (customer_id, customer_name, email, total_purchases, last_purchase_date, status) VALUES
(1001, 'John Smith', 'john.smith@email.com', 1250.75, '2023-11-15', 'active'),
(1002, 'Sarah Johnson', 'sarah.j@email.com', 3450.00, '2023-12-01', 'active'),
(1003, 'Michael Brown', 'mbrown@email.com', 875.50, '2023-10-22', 'active'),
(1004, 'Emily Davis', 'emily.davis@email.com', 2100.25, '2023-11-28', 'inactive'),
(1005, 'David Wilson', 'dwilson@email.com', 5670.80, '2023-12-05', 'active'),
(1006, 'Jennifer Martinez', 'jmartinez@email.com', 1890.00, '2023-11-10', 'active'),
(1007, 'Robert Taylor', 'rtaylor@email.com', 4250.60, '2023-12-08', 'active'),
(1008, 'Lisa Anderson', 'landerson@email.com', 990.40, '2023-10-30', 'inactive'),
(1009, 'James Thomas', 'jthomas@email.com', 3120.15, '2023-11-20', 'active'),
(1010, 'Patricia Moore', 'pmoore@email.com', 2780.90, '2023-12-03', 'active');

-- Verify the data
SELECT 'customers1' as table_name, COUNT(*) as row_count FROM customers1
UNION ALL
SELECT 'customers2' as table_name, COUNT(*) as row_count FROM customers2;

-- Optional: Show sample data from both tables
SELECT * FROM customers1 ORDER BY customer_id;
SELECT * FROM customers2 ORDER BY customer_id;

