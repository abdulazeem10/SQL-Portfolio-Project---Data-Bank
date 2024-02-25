USE data_bank;
CREATE TABLE Regions (
    region_id INT PRIMARY KEY,
    region_name VARCHAR(255)
);

-- Create the Customer Nodes table
CREATE TABLE Customer_Nodes (
    customer_id INT,
    region_id INT,
    node_id INT,
    start_date DATE,
    end_date DATE,
    FOREIGN KEY (region_id) REFERENCES Regions(region_id)
);

-- Create the Customer Transactions table
CREATE TABLE Customer_Transactions (
    customer_id INT,
    txn_date DATE,
    txn_type VARCHAR(50),
    txn_amount DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES Customer_Nodes(customer_id)
);

-- A. Customer Nodes Exploration
-- 1. How many unique nodes are there on the Data Bank system?
SELECT COUNT(DISTINCT node_id) AS unique_nodes
FROM Customer_Nodes;

-- 2. What is the number of nodes per region?
SELECT r.region_name, COUNT(DISTINCT cn.node_id) AS num_nodes
FROM customer_nodes cn
JOIN regions r ON cn.region_id = r.region_id
GROUP BY r.region_name;

-- 3. How many customers are allocated to each region?
SELECT r.region_name, COUNT(DISTINCT cn.customer_id) AS num_customers
FROM customer_nodes cn
JOIN regions r ON cn.region_id = r.region_id
GROUP BY r.region_name;

-- 4. How many days on average are customers reallocated to a different node?
SELECT AVG(DATEDIFF(end_date, start_date)) AS avg_reallocation_days
FROM customer_nodes;

-- 5. What is the median, 80th & 95th percentile for this same reallocation days metric for each region?
SELECT 
    region_name,
    APPROX_MEDIAN(reallocation_days) AS median_reallocation_days,
    APPROX_PERCENTILE(reallocation_days, 0.8) AS percentile_80,
    APPROX_PERCENTILE(reallocation_days, 0.95) AS percentile_95
FROM (
    SELECT 
        region_name,
        DATEDIFF(end_date, start_date) AS reallocation_days
    FROM 
        data_bank.customer_nodes cn
    JOIN 
        data_bank.regions r ON cn.region_id = r.region_id
) AS subquery
GROUP BY 
    region_name;

-- B. Customer Transactions
-- 1. What is the unique count and total amount for each transaction type?
SELECT 
    txn_type, 
    COUNT(*) AS unique_count, 
    SUM(txn_amount) AS total_amount 
FROM 
    customer_transactions
GROUP BY 
    txn_type
LIMIT 1000;

-- 2. What is the average total historical deposit counts and amounts for all customers?
SELECT 
    AVG(num_deposits) AS avg_deposit_count, 
    AVG(total_deposit_amount) AS avg_deposit_amount
FROM (
    SELECT 
        customer_id, 
        COUNT(*) AS num_deposits, 
        SUM(txn_amount) AS total_deposit_amount
    FROM 
        customer_transactions
    WHERE 
        txn_type = 'deposit'
    GROUP BY 
        customer_id
) AS deposit_summary;

-- 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
SELECT
    YEAR(txn_date) AS year,
    MONTH(txn_date) AS month,
    COUNT(DISTINCT customer_id) AS num_customers
FROM
    customer_transactions
WHERE
    txn_type IN ('deposit', 'purchase', 'withdrawal')
GROUP BY
    YEAR(txn_date), MONTH(txn_date)
HAVING
    COUNT(DISTINCT CASE WHEN txn_type = 'deposit' THEN customer_id END) > 1
    AND COUNT(DISTINCT CASE WHEN txn_type IN ('purchase', 'withdrawal') THEN customer_id END) >= 1;

-- 4. What is the closing balance for each customer at the end of the month?
SELECT
    customer_id,
    LAST_DAY(txn_date) AS end_of_month,
    SUM(CASE 
            WHEN txn_type = 'deposit' THEN txn_amount
            WHEN txn_type IN ('purchase', 'withdrawal') THEN -txn_amount
            ELSE 0 
        END) AS closing_balance
FROM
    customer_transactions
GROUP BY
    customer_id, LAST_DAY(txn_date)
LIMIT 1000;

-- 5. What is the percentage of customers who increase their closing balance by more than 5%?
WITH closing_balances AS (
    SELECT
        customer_id,
        LAST_DAY(txn_date) AS end_of_month,
        SUM(CASE 
                WHEN txn_type = 'deposit' THEN txn_amount
                WHEN txn_type IN ('purchase', 'withdrawal') THEN -txn_amount
                ELSE 0 
            END) AS closing_balance
    FROM
        customer_transactions
    GROUP BY
        customer_id, LAST_DAY(txn_date)
)
SELECT
    SUM(CASE WHEN closing_balance_increase > 0.05 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percentage_increase_above_5
FROM (
    SELECT
        customer_id,
        (MAX(closing_balance) - MIN(closing_balance)) / NULLIF(MIN(closing_balance), 0) AS closing_balance_increase
    FROM
        closing_balances
    GROUP BY
        customer_id
) AS balance_increase_summary;

-- To address the data allocation challenge, we need to calculate the running balance for each customer considering the three different options mentioned:

-- Option 1: Allocate data based on the amount of money at the end of the previous month.
-- Option 2: Allocate data based on the average amount of money kept in the account in the previous 30 days.
-- Option 3: Allocate data in real-time.
-- Let's break down the steps for each option:

-- Option 1:
-- For this option, we need to calculate the balance at the end of each month.
WITH monthly_balance_option1 AS (
    SELECT
        customer_id,
        LAST_DAY(txn_date) AS end_of_month,
        SUM(CASE 
                WHEN txn_type = 'deposit' THEN txn_amount
                WHEN txn_type IN ('purchase', 'withdrawal') THEN -txn_amount
                ELSE 0 
            END) AS closing_balance
    FROM
        customer_transactions
    WHERE
        txn_date < LAST_DAY(txn_date)
    GROUP BY
        customer_id, LAST_DAY(txn_date)
)
SELECT
    customer_id,
    end_of_month,
    closing_balance
FROM
    monthly_balance_option1;

-- Option 2:
-- For this option, we need to calculate the average balance over the previous 30 days for each day.
WITH daily_average_balance_option2 AS (
    SELECT
        customer_id,
        txn_date,
        AVG(CASE 
                WHEN txn_type = 'deposit' THEN txn_amount
                WHEN txn_type IN ('purchase', 'withdrawal') THEN -txn_amount
                ELSE 0 
            END) OVER (PARTITION BY customer_id ORDER BY txn_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS average_balance
    FROM
        customer_transactions
)
SELECT
    customer_id,
    txn_date,
    average_balance
FROM
    daily_average_balance_option2;

-- Option 3:
-- For this option, we would track the real-time balance after each transaction.
SELECT
    customer_id,
    txn_date,
    SUM(CASE 
            WHEN txn_type = 'deposit' THEN txn_amount
            WHEN txn_type IN ('purchase', 'withdrawal') THEN -txn_amount
            ELSE 0 
        END) OVER (PARTITION BY customer_id ORDER BY txn_date) AS real_time_balance
FROM
    customer_transactions;

-- D. Extra Challenge - Data Growth Calculation with Interest
-- Given:
-- Annual interest rate: 6%
-- Interest calculated on a daily basis
-- Initial calculation without compounding interest
-- To calculate the data growth based on the interest, 
-- we need to multiply the initial data allocation by the interest rate and then divide by 
-- the no of days in a year.
-- Simple Interest Calculation
SELECT 
    initial_data_allocation * (1 + (0.06 / 12)) AS simple_interest_monthly_growth
FROM 
    data_bank.customers;
    