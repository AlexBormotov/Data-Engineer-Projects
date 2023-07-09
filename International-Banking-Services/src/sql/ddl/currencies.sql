DROP TABLE IF EXISTS AVBORMOTOVYANDEXRU__STAGING.currencies CASCADE

CREATE TABLE AVBORMOTOVYANDEXRU__STAGING.currencies
(
    date_update timestamp,
    currency_code int NOT NULL,
    currency_code_with int NOT NULL,
    currency_with_div float(2) CHECK (currency_with_div >= 0.01 or currency_with_div <= 1.00),
    CONSTRAINT C_PRIMARY PRIMARY KEY (date_update, currency_code, currency_code_with) DISABLED
)
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);


CREATE PROJECTION AVBORMOTOVYANDEXRU__STAGING.currencies
(
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
)
AS
 SELECT currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_with_div
 FROM AVBORMOTOVYANDEXRU__STAGING.currencies
 ORDER BY currencies.date_update
SEGMENTED BY hash(currencies.date_update) ALL NODES KSAFE 1;