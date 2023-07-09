CREATE TABLE AVBORMOTOVYANDEXRU__DWH.global_metrics (
	date_update date NOT NULL,
	currency_from int NOT NULL CHECK (currency_from > 0),
	amount_total float(2) NOT NULL,
	cnt_transactions bigint NOT NULL CHECK (cnt_transactions > 0),
	avg_transactions_per_account float(2) NOT NULL CHECK (avg_transactions_per_account > 0.0),
	cnt_accounts_make_transactions bigint NOT NULL CHECK (cnt_accounts_make_transactions > 0),
	CONSTRAINT C_PRIMARY PRIMARY KEY (date_update, currency_from) DISABLED
)
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);


CREATE PROJECTION AVBORMOTOVYANDEXRU__DWH.global_metrics
(
	date_update,
	currency_from,
	amount_total,
	cnt_transactions,
	avg_transactions_per_account,
	cnt_accounts_make_transactions 
)
AS
 SELECT global_metrics.date_update,
	global_metrics.currency_from,
	global_metrics.amount_total,
	global_metrics.cnt_transactions,
	global_metrics.avg_transactions_per_account,
	global_metrics.cnt_accounts_make_transactions 
 FROM AVBORMOTOVYANDEXRU__DWH.global_metrics
 ORDER BY global_metrics.date_update, global_metrics.currency_from
SEGMENTED BY hash(global_metrics.date_update, global_metrics.currency_from) ALL NODES KSAFE 1;