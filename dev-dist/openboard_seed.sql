-- Openboard Examples — sample data for Armillary test pipelines
-- Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
-- SPDX-License-Identifier: MIT OR Apache-2.0

-- Customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    region VARCHAR(50) NOT NULL,
    signup_date DATE NOT NULL
);

INSERT INTO customers (first_name, last_name, email, region, signup_date) VALUES
('Alice', 'Chen', 'alice@example.com', 'West', '2024-03-15'),
('Bob', 'Martinez', 'bob@example.com', 'East', '2024-06-22'),
('Carol', 'Johnson', 'carol@example.com', 'West', '2024-01-10'),
('Dave', 'Kim', 'dave@example.com', 'Central', '2024-09-01'),
('Eve', 'Patel', 'eve@example.com', 'East', '2024-04-18'),
('Frank', 'Liu', 'frank@example.com', 'West', '2024-07-30'),
('Grace', 'Ng', 'grace@example.com', 'East', '2024-02-14'),
('Hank', 'Brown', 'hank@example.com', 'Central', '2024-11-05'),
('Iris', 'Wang', 'iris@example.com', 'West', '2024-05-28'),
('Jack', 'Davis', 'jack@example.com', 'East', '2024-08-12'),
('Karen', 'Wilson', 'karen@example.com', 'Central', '2025-01-03'),
('Leo', 'Thompson', 'leo@example.com', 'West', '2025-02-18'),
('Mia', 'Garcia', 'mia@example.com', 'East', '2025-03-07'),
('Nathan', 'Lee', 'nathan@example.com', 'Central', '2024-12-15'),
('Olivia', 'Taylor', 'olivia@example.com', 'West', '2024-10-22');

-- Products
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL
);

INSERT INTO products (product_name, category, unit_price) VALUES
('Widget Pro', 'Electronics', 49.99),
('Gizmo Max', 'Electronics', 129.99),
('Office Chair', 'Furniture', 299.99),
('Desk Lamp', 'Furniture', 34.99),
('Notebook Set', 'Office', 12.99),
('Wireless Mouse', 'Electronics', 24.99),
('Standing Desk', 'Furniture', 549.99),
('Monitor Arm', 'Furniture', 89.99),
('USB Hub', 'Electronics', 39.99),
('Pen Set', 'Office', 8.99);

-- Orders
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'completed'
);

INSERT INTO orders (customer_id, order_date, status) VALUES
(1, '2026-01-15', 'completed'),
(2, '2026-01-16', 'completed'),
(3, '2026-01-17', 'completed'),
(4, '2026-01-18', 'completed'),
(5, '2026-01-19', 'completed'),
(6, '2026-01-20', 'completed'),
(7, '2026-01-21', 'completed'),
(8, '2026-01-22', 'completed'),
(9, '2026-01-23', 'completed'),
(10, '2026-01-24', 'completed'),
(1, '2026-01-25', 'completed'),
(2, '2026-01-26', 'completed'),
(3, '2026-01-27', 'completed'),
(4, '2026-01-28', 'completed'),
(5, '2026-01-29', 'completed'),
(6, '2026-01-30', 'completed'),
(7, '2026-01-31', 'completed'),
(8, '2026-02-01', 'completed'),
(9, '2026-02-02', 'completed'),
(10, '2026-02-03', 'completed'),
(11, '2026-02-04', 'completed'),
(12, '2026-02-05', 'completed'),
(13, '2026-02-06', 'completed'),
(14, '2026-02-07', 'completed'),
(15, '2026-02-08', 'completed');

-- Order Lines
CREATE TABLE IF NOT EXISTS order_lines (
    line_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL
);

INSERT INTO order_lines (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 2, 49.99),
(2, 2, 1, 129.99),
(3, 1, 3, 49.99),
(4, 3, 1, 299.99),
(5, 4, 2, 34.99),
(6, 2, 2, 129.99),
(7, 5, 5, 12.99),
(8, 1, 1, 49.99),
(9, 3, 2, 299.99),
(10, 4, 1, 34.99),
(11, 5, 10, 12.99),
(12, 1, 4, 49.99),
(13, 2, 1, 129.99),
(14, 5, 3, 12.99),
(15, 1, 2, 49.99),
(16, 3, 1, 299.99),
(17, 4, 3, 34.99),
(18, 2, 1, 129.99),
(19, 1, 5, 49.99),
(20, 5, 2, 12.99),
(21, 7, 1, 549.99),
(22, 6, 2, 24.99),
(23, 8, 1, 89.99),
(24, 9, 2, 39.99),
(25, 10, 4, 8.99),
-- Some orders have multiple lines
(1, 6, 1, 24.99),
(3, 4, 1, 34.99),
(9, 8, 1, 89.99),
(15, 6, 1, 24.99),
(21, 10, 3, 8.99);
