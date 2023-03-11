CREATE TABLE IF NOT EXISTS ZHUKOVANANYANDEXRU__STAGING.transactions
(operation_id uuid primary key,
account_number_from int,
account_number_to int,
currency_code int,
country varchar(20),
status varchar(20),
transaction_type varchar(30),
amount int,
transaction_dt TIMESTAMP)
ORDER BY transaction_dt
SEGMENTED BY hash(operation_id) all nodes
PARTITION BY transaction_dt::date