-- Data Preparation & Cleaning
-- Customer demofraphics

/*
If the database has people born in the past 100 years, a reasonable assumption is:
YY > 25 → Born in the 1900s (1925–1999).
YY ≤ 30 → Born in the 2000s (2000–2030).
*/

-- Extracting age & gender
CREATE OR REPLACE VIEW CustomerProfile AS
WITH AgeGender AS(
	SELECT
		client_id,
        district_id,
		STR_TO_DATE(
			CONCAT(
				CASE 
					WHEN SUBSTR(birth_number, 1, 2) > 25 THEN CONCAT('19', SUBSTR(birth_number, 1, 2))
					ELSE CONCAT('20', SUBSTR(birth_number, 1, 2))
				END, '-',
				IF(CAST(SUBSTR(birth_number, 3, 2) AS UNSIGNED) > 50, 
				   CAST(SUBSTR(birth_number, 3, 2) AS UNSIGNED) - 50, 
				   CAST(SUBSTR(birth_number, 3, 2) AS UNSIGNED)), '-',
				SUBSTR(birth_number, 5, 2)
			), '%Y-%m-%d'
		) AS birth_date,

		CASE
			WHEN CAST(SUBSTR(birth_number, 3, 2) AS UNSIGNED) >= 50 THEN "female"
			ELSE "male"
		END AS gender
		
	FROM client)
    
    SELECT a.account_id, birth_date, gender, ag.district_id, d.A2 AS district_name, d.A3 AS region, d.A11 AS avg_salary,
	CASE
		WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) < 18 THEN 'Under 18'
		WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) BETWEEN 18 AND 24 THEN '18-24'
		WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) BETWEEN 25 AND 34 THEN '25-34'
		WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) BETWEEN 35 AND 44 THEN '35-44'
		WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) BETWEEN 45 AND 54 THEN '45-54'
		WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) BETWEEN 55 AND 64 THEN '55-64'
		WHEN TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) >= 65 THEN '65+'
	END AS age_buckets
    FROM AgeGender ag
    LEFT JOIN account a ON ag.district_id = a.district_id
    LEFT JOIN district d ON a.district_id = d.A1;

SELECT * FROM CustomerProfile;

-- Account Duration
SELECT
	account_id,
	CAST(date as date) AS account_creation_date, 
    CURDATE() AS today,
	DATEDIFF(CURDATE(),CAST(date as date)) AS account_duration_days,
    TIMESTAMPDIFF(YEAR, CAST(date as date), CURDATE()) AS account_duration_years
FROM account
ORDER BY account_id;

WITH spending_trends AS (
    SELECT 
        account_id,
        COUNT(trans_id) AS total_transactions,
        ROUND(SUM(CASE WHEN type = 'PRIJEM' THEN amount ELSE 0 END)) AS total_inflows,
        ROUND(SUM(CASE WHEN type = 'VYDAJ' THEN amount ELSE 0 END)) AS total_outflows,
        MAX(balance) AS latest_balance,
        MIN(balance) AS min_balance
    FROM trans
    GROUP BY account_id
)

SELECT 
    t.account_id, DATE_FORMAT(t.date, '%Y-%m-%d') AS date, t.amount, t.balance,
    a.total_transactions,
    a.total_inflows,
    a.total_outflows,
    a.latest_balance,
    
    -- Transaction categorization
    CASE  
        WHEN k_symbol IN ('DUCHOD', 'UROK') THEN 'Income'  
        WHEN operation IN ('VYBER KARTOU', 'VYBER') THEN 'Withdrawal'  
        WHEN k_symbol = 'UVER' THEN 'Loan Payment'  
        WHEN k_symbol IN ('SIPO', 'POJISTNE') THEN 'Household & Bills'  
        WHEN operation IN ('PREVOD Z UCTU', 'PREVOD NA UCET') THEN 'Bank Transfer'  
        WHEN k_symbol IN ('SLUZBY', 'SANKC. UROK') THEN 'Other Expenses'  
        ELSE 'Unknown'  
    END AS transaction_group,
    
    -- Net balance calculation
    (a.total_inflows - a.total_outflows) AS net_balance_change,
    
    -- Flag accounts with frequent negative balances
    CASE  
        WHEN a.min_balance < 0 THEN 'High Risk'  
        ELSE 'Normal'  
    END AS account_risk_category

FROM trans t
LEFT JOIN spending_trends a ON t.account_id = a.account_id;



-- Standarization of transaction data + View for Aggregated Customer Financial Data
CREATE OR REPLACE VIEW CustomerFinancialSummary AS
WITH account_summary AS (
    SELECT 
        account_id,
        COUNT(trans_id) AS total_transactions,
        ROUND(SUM(CASE WHEN type = 'PRIJEM' THEN amount ELSE 0 END)) AS total_inflows,
        ROUND(SUM(CASE WHEN type = 'VYDAJ' THEN amount ELSE 0 END)) AS total_outflows,
        MAX(balance) AS latest_balance,
        MIN(balance) AS min_balance
    FROM trans
    GROUP BY account_id
)
SELECT 
    t.account_id, DATE_FORMAT(t.date, '%Y-%m-%d') AS date, t.amount, t.balance,
    a.total_transactions,
    a.total_inflows,
    a.total_outflows,
    a.latest_balance,
    
    -- Transaction categorization
    CASE  
        WHEN k_symbol IN ('DUCHOD', 'UROK') THEN 'Income'  
        WHEN operation IN ('VYBER KARTOU', 'VYBER') THEN 'Withdrawal'  
        WHEN k_symbol = 'UVER' THEN 'Loan Payment'  
        WHEN k_symbol IN ('SIPO', 'POJISTNE') THEN 'Household & Bills'  
        WHEN operation IN ('PREVOD Z UCTU', 'PREVOD NA UCET') THEN 'Bank Transfer'  
        WHEN k_symbol IN ('SLUZBY', 'SANKC. UROK') THEN 'Other Expenses'  
        ELSE 'Unknown'  
    END AS transaction_group,
    
    -- Net balance calculation
    (a.total_inflows - a.total_outflows) AS net_balance_change,
    
    -- Flag accounts with frequent negative balances
    CASE  
        WHEN a.min_balance < 0 THEN 'High Risk'  
        ELSE 'Normal'  
    END AS account_risk_category

FROM trans t
LEFT JOIN account_summary a ON t.account_id = a.account_id;

SELECT * FROM CustomerFinancialSummary;


-- Segment customers based on the financial behavior
CREATE OR REPLACE VIEW CustomerFinancialBehavior AS
SELECT 
    account_id, 
    balance,
    CAST(date AS date) AS date,
    CASE
    WHEN NTILE(3) OVER (ORDER BY balance) = 1 THEN 'low-balance'
    WHEN NTILE(3) OVER (ORDER BY balance) = 2 THEN 'moderate-balance'
    WHEN NTILE(3) OVER (ORDER BY balance) = 3 THEN 'high-balance'
    END AS balance_category
FROM trans;

-- Low Balance Customers
WITH LowBalanceCustomers AS (
	SELECT
		account_id,
		balance,
		date,
		balance_category,
		CASE
			WHEN balance_category = 'low-balance' THEN 1
            ELSE 0
		END AS Low_Balance_Flag
    FROM CustomerFinancialBehavior
),

ConsistentLowBalance AS(
	SELECT
		account_id,
		SUM(Low_Balance_Flag) AS Low_Balance_Months,
		COUNT(*) AS Total_Active_Months
	FROM LowBalanceCustomers
    -- this is historical data, but if more recent data is available, we could look at only last 6 months
	-- WHERE date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
	GROUP BY account_id
)

SELECT
	account_id,
	Low_Balance_Months,
	Total_Months,
	CASE
		WHEN Low_Balance_Months >= 3 THEN 'At Risk'
		ELSE 'Stable'
	END AS Churn_Risk_Flag
FROM ConsistentLowBalance;

SELECT * FROM CustomerFinancialBehavior;

-- Customer Segmentation View

CREATE OR REPLACE VIEW CustomerSegmentation AS
WITH AggregatedData AS (
    SELECT 
        account_id,
        SUM(total_transactions) AS total_transactions,
        AVG(latest_balance) AS latest_balance
    FROM CustomerFinancialSummary
    GROUP BY account_id
),

TransactionBalanceRank AS (
    SELECT 
        account_id,
        total_transactions,
        latest_balance,
		-- Creating transaction frequency buckets: high, moderate, and low
		NTILE(3) OVER (ORDER BY total_transactions DESC) AS transaction_category,
        
        -- Creating balance buckets: high, moderate, and low
        NTILE(3) OVER (ORDER BY latest_balance DESC) AS balance_category
	FROM AggregatedData)
    
    SELECT 
		account_id,
        total_transactions,
        latest_balance,
        balance_category,
        CASE
        WHEN transaction_category = 3 AND balance_category = 3 THEN 'High-Value'
		WHEN transaction_category = 2 AND balance_category = 2 THEN 'Regular'
        WHEN transaction_category = 1 OR balance_category = 1 THEN 'Low-Engagement'
        WHEN transaction_category = 1 AND balance_category = 1 THEN 'At-Risk'
        END AS customer_segment
        
	FROM TransactionBalanceRank;

SELECT * FROM CustomerSegmentation;
SELECT balance_category FROM CustomerSegmentation;


-- Active vs. Inactive Customers View

CREATE OR REPLACE VIEW ActiveInactiveCustomers AS
WITH MonthlyActivity AS (
    SELECT a.account_id,
           DATE_FORMAT(t.date, '%Y-%m') AS transaction_month,
           COUNT(t.trans_id) AS transaction_count
    FROM account a
    LEFT JOIN trans t ON a.account_id = t.account_id
    GROUP BY a.account_id, DATE_FORMAT(t.date, '%Y-%m')
),
InactiveCustomers AS (
    SELECT account_id
    FROM MonthlyActivity
    WHERE transaction_count = 0  -- No transactions for this month
    GROUP BY account_id
    -- HAVING COUNT(transaction_month) >= 6  -- Inactive for the last 6 months
)
SELECT ma.account_id,
       ma.transaction_month,
       ma.transaction_count,
       CASE 
           WHEN ic.account_id IS NOT NULL THEN 'Inactive'
           WHEN ma.transaction_count > 1 THEN 'Active'
           ELSE 'Low Activity'
       END AS active_status
FROM MonthlyActivity ma
LEFT JOIN InactiveCustomers ic ON ma.account_id = ic.account_id;

SELECT * FROM ActiveInactiveCustomers;


-- Loan Risk View

CREATE OR REPLACE VIEW LoanRiskProfile AS
WITH CustomerData AS (
	SELECT a.account_id, l.amount, l.duration, l.payments, l.status, t.trans_id,
    t.balance
	FROM account a
	LEFT JOIN trans t ON a.account_id = t.account_id
	LEFT JOIN loan l ON t.account_id = l.account_id),
    
	RiskScoreRank AS (
	SELECT
		account_id,
		amount,
		NTILE(3) OVER (ORDER BY amount DESC) AS loan_weight,
		NTILE(3) OVER (ORDER BY balance DESC) AS balance_weight,    
		CASE
			WHEN status in ('A', 'C') THEN 'Low Risk'
			WHEN status = 'B' THEN 'Medium Risk'
			WHEN status = 'D' THEN 'High Risk'
		END AS payment_risk_level
    FROM CustomerData
)
SELECT
account_id,
MAX(CASE
	WHEN loan_weight = 3 AND balance_weight = 1 AND payment_risk_level = 'High Risk' THEN 'Very High Risk' 
	WHEN loan_weight = 3 AND balance_weight = 1 AND payment_risk_level = 'Medium Risk' THEN 'High Risk'
	WHEN loan_weight = 3 AND balance_weight = 1 AND payment_risk_level = 'Low Risk' THEN 'High Risk'
        
	WHEN loan_weight = 3 AND balance_weight = 2 AND payment_risk_level = 'High Risk' THEN 'High Risk'
	WHEN loan_weight = 3 AND balance_weight = 2 AND payment_risk_level = 'Medium Risk' THEN 'Medium Risk'
	WHEN loan_weight = 3 AND balance_weight = 2 AND payment_risk_level = 'Low Risk' THEN 'Medium Risk'
        
	WHEN loan_weight = 3 AND balance_weight = 3 AND payment_risk_level = 'High Risk' THEN 'Medium Risk'
	WHEN loan_weight = 3 AND balance_weight = 3 AND payment_risk_level = 'Medium Risk' THEN 'Low Risk'
	WHEN loan_weight = 3 AND balance_weight = 3 AND payment_risk_level = 'Low Risk' THEN 'Low Risk'
        
	WHEN loan_weight = 2 AND balance_weight = 1 AND payment_risk_level = 'High Risk' THEN 'High Risk'
	WHEN loan_weight = 2 AND balance_weight = 1 AND payment_risk_level = 'Medium Risk' THEN 'Medium Risk'
	WHEN loan_weight = 2 AND balance_weight = 1 AND payment_risk_level = 'Low Risk' THEN 'Medium Risk'
        
	WHEN loan_weight = 2 AND balance_weight = 2 AND payment_risk_level = 'High Risk' THEN 'Medium Risk'
	WHEN loan_weight = 2 AND balance_weight = 2 AND payment_risk_level = 'Medium Risk' THEN 'Low Risk'
	WHEN loan_weight = 2 AND balance_weight = 2 AND payment_risk_level = 'Low Risk' THEN 'Low Risk'
        
	WHEN loan_weight = 2 AND balance_weight = 3 AND payment_risk_level = 'High Risk' THEN 'Medium Risk'
	WHEN loan_weight = 2 AND balance_weight = 3 AND payment_risk_level = 'Medium Risk' THEN 'Low Risk'
	WHEN loan_weight = 2 AND balance_weight = 3 AND payment_risk_level = 'Low Risk' THEN 'Low Risk'
        
	WHEN loan_weight = 1 AND balance_weight = 1 AND payment_risk_level = 'High Risk' THEN 'Medium Risk'
	WHEN loan_weight = 1 AND balance_weight = 1 AND payment_risk_level = 'Medium Risk' THEN 'Low Risk'
	WHEN loan_weight = 1 AND balance_weight = 1 AND payment_risk_level = 'Low Risk' THEN 'Low Risk'
        
	WHEN loan_weight = 1 AND balance_weight = 2 AND payment_risk_level = 'High Risk' THEN 'Low Risk'
	WHEN loan_weight = 1 AND balance_weight = 2 AND payment_risk_level = 'Medium Risk' THEN 'Low Risk'
	WHEN loan_weight = 1 AND balance_weight = 2 AND payment_risk_level = 'Low Risk' THEN 'Low Risk'
        
	WHEN loan_weight = 1 AND balance_weight = 3 AND payment_risk_level = 'High Risk' THEN 'Low Risk'
	WHEN loan_weight = 1 AND balance_weight = 3 AND payment_risk_level = 'Medium Risk' THEN 'Low Risk'
	WHEN loan_weight = 1 AND balance_weight = 3 AND payment_risk_level = 'Low Risk' THEN 'Low Risk'
        
	ELSE 'Low Risk'
    END) AS overall_risk
FROM RiskScoreRank
GROUP BY account_id;

SELECT * FROM LoanRiskProfile;

-- Rolling Calculations & Window Functions
-- Rolling Average of Monthly Transactions
-- The rolling window size is defined as the current row and
-- the two previous rows (the last two days).
SELECT
	account_id,
	CAST(date AS date) AS date,
	amount,
	AVG(amount) OVER (
    PARTITION BY account_id
    ORDER BY CAST(date AS date)
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg
FROM trans;


-- Cumulative Deposits & Withdrawals per customer
/* 
This query helps analyze a customer’s deposit and withdrawal behavior over time,
providing both cumulative amount and ranking based on transaction amounts.
*/

CREATE OR REPLACE VIEW MonthlyTransactionTrends AS
WITH CustomerDepositsWithdrawals AS (
    SELECT
        account_id,
        DATE_FORMAT(CAST(date AS DATE), '%Y-%m') AS transaction_month,
        amount,
        CASE WHEN type = 'PRIJEM' THEN amount ELSE 0 END AS total_inflows,
        CASE WHEN type = 'VYDAJ' THEN amount ELSE 0 END AS total_outflows
    FROM trans
),
CumulativeDepositsWithdrawals AS (
    SELECT
        account_id,
        transaction_month,
        SUM(total_inflows) AS monthly_deposit,
        SUM(total_outflows) AS monthly_withdrawal,
        
        -- Cumulative deposit per customer over time
        SUM(SUM(total_inflows)) OVER (PARTITION BY account_id ORDER BY transaction_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_deposit,
        
        -- Cumulative withdrawal per customer over time
        SUM(SUM(total_outflows)) OVER (PARTITION BY account_id ORDER BY transaction_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_withdrawals
    FROM CustomerDepositsWithdrawals
    GROUP BY account_id, transaction_month
)
SELECT 
    account_id,
    transaction_month,
    monthly_deposit,
    monthly_withdrawal,
    cumulative_deposit,
    cumulative_withdrawals
FROM CumulativeDepositsWithdrawals;

SELECT * FROM MonthlyTransactionTrends;

-- Customer Lifetime Value (CLV) Estimation & 5 years estimation

WITH CustomerTransaction AS(
	SELECT
		a.account_id,
		t.trans_id,
        t.date,
		SUM(t.amount) AS total_transactions
	FROM account a
	LEFT JOIN trans t ON a.account_id = t.account_id
	GROUP BY a.account_id, t.date, t.trans_id),
    
    TransactionTrends AS(
    SELECT
		account_id,
        DATE_FORMAT(date, '%Y') AS date,
		AVG(total_transactions) AS avg_transaction_amount,
		COUNT(trans_id) / COUNT(DISTINCT DATE_FORMAT(date, '%Y')) AS avg_transaction_per_year
    FROM CustomerTransaction
    GROUP BY account_ID, DATE_FORMAT(date, '%Y'))
    
    SELECT
		account_id,
        date,
		avg_transaction_amount,
		avg_transaction_per_year,
		(avg_transaction_amount * avg_transaction_per_year * 5) AS estimated_five_yrs_revenue
	FROM TransactionTrends
    ORDER BY DATE, avg_transaction_amount DESC;

select count(account_id) from account;