CREATE TABLE IF NOT EXISTS ZHUKOVANANYANDEXRU__DWH.global_metrics
(date_update date,
currency_from int,
amount_total Numeric(14,2),
cnt_transactions int,
avg_transactions_per_account Numeric(14,2),
cnt_accounts_make_transactions int,
CONSTRAINT pk PRIMARY KEY (date_update,currency_from))
ORDER BY date_update
SEGMENTED BY hash(currency_from) all nodes
PARTITION BY date_update