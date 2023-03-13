CREATE TABLE IF NOT EXISTS ZHUKOVANANYANDEXRU__STAGING.currencies
(
  currency_code int,
  currency_code_with int,
  date_update DATE,
  currency_with_div NUMERIC (14,2),
  CONSTRAINT pk PRIMARY KEY (currency_code, currency_code_with, date_update) ENABLED
)
ORDER BY date_update
SEGMENTED BY hash(currency_code) all nodes
PARTITION BY date_update;

CREATE PROJECTION IF NOT EXISTS ZHUKOVANANYANDEXRU__STAGING.currencies_proj AS
SELECT
    currency_code,
    currency_code_with,
    date_update
    currency_with_div
FROM
    ZHUKOVANANYANDEXRU__STAGING.currencies
ORDER BY
    date_update
SEGMENTED BY hash(currency_code) ALL NODES;
