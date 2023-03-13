INSERT INTO ZHUKOVANANYANDEXRU__DWH.global_metrics
SELECT
     	transaction_dt::date AS date_update
	,t.currency_code AS currency_from
	,SUM(amount*COALESCE(c.currency_with_div,1)) AS amount_total
	,COUNT(*) AS cnt_transactions
	,SUM(amount*COALESCE(c.currency_with_div,1))/COUNT(DISTINCT account_number_from) AS avg_transactions_per_account
	,COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
FROM ZHUKOVANANYANDEXRU__STAGING.transactions AS t
	LEFT JOIN ZHUKOVANANYANDEXRU__STAGING.currencies AS c 
		ON t.transaction_dt::date = c.date_update
		AND t.currency_code = c.currency_code
		AND c.currency_code_with = 420
WHERE 
	account_number_from >= 0
	AND transaction_dt::date = '{{ds}}'
GROUP BY
	 transaction_dt::date
	,t.currency_code
