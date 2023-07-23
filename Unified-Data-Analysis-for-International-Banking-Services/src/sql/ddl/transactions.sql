DROP TABLE IF EXISTS AVBORMOTOVYANDEXRU__STAGING.transactions CASCADE;

CREATE TABLE AVBORMOTOVYANDEXRU__STAGING.transactions
(
    operation_id uuid PRIMARY KEY DISABLED,
    account_number_from int NOT NULL,
    account_number_to int NOT NULL,
    currency_code int NOT NULL,
    country varchar(50) NOT NULL,
    status varchar(20) NOT NULL,
    transaction_type varchar(30) NOT NULL,
    amount int NOT NULL,
    transaction_dt timestamp(3) NOT NULL
);


CREATE PROJECTION AVBORMOTOVYANDEXRU__STAGING.transactions
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
)
AS
 SELECT transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
 FROM AVBORMOTOVYANDEXRU__STAGING.transactions
 ORDER BY transactions.transaction_dt, transactions.operation_id
SEGMENTED BY hash(transactions.transaction_dt, transactions.operation_id) ALL NODES KSAFE 1;
