import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from stg.lib import VerticaConnect
from stg.lib import VerticaConnectionBuilder



def global_metrics_load(conn_info: VerticaConnect, execution_date: str):
    # it was possible to hide the query in a sql file, but it’s more convenient to work this way
    execution_date = str(execution_date)[0:10]

    query = f"""
    INSERT INTO AVBORMOTOVYANDEXRU__DWH.global_metrics(date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
    SELECT '{execution_date}'::date as date_update,
	    t.currency_code as currency_from,
		SUM(CASE WHEN t.status = 'done' AND t.currency_code != 420 THEN t.amount*c.currency_with_div
			WHEN t.status = 'done' AND t.currency_code = 420 THEN t.amount -- we separately consider transactions with the dollar, where there is no conversion
            WHEN t.status = 'chargeback' AND t.currency_code != 420 THEN t.amount*c.currency_with_div*(-1) -- chargeback in the source table is indicated without a backtick
            WHEN t.status = 'chargeback' AND t.currency_code = 420 THEN t.amount*(-1)
            END) as amount_total,
		COUNT(DISTINCT (t.operation_id||t.account_number_from||t.account_number_to)) as cnt_transactions, -- operation_id - not unique for different operations, it cannot be used
	    COUNT(DISTINCT (t.operation_id||t.account_number_from||t.account_number_to))/COUNT(DISTINCT t.account_number_from) as avg_transactions_per_account,
	    COUNT(DISTINCT t.account_number_from) as cnt_accounts_make_transactions 
    FROM AVBORMOTOVYANDEXRU__STAGING.transactions as t
    LEFT JOIN (SELECT date_update, currency_code, currency_with_div 
    			FROM AVBORMOTOVYANDEXRU__STAGING.currencies
    			WHERE currency_code_with = 420) c 
        ON t.currency_code = c.currency_code AND t.transaction_dt::date = c.date_update::date
    WHERE t.status IN ('done','chargeback') -- take into account returns
        AND t.account_number_from > 0 
        AND t.transaction_dt::date = ('{execution_date}'::date - interval '1 day')::date
    GROUP BY 1,2
    ORDER BY 1,2;
    """
    with conn_info.connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()


with DAG('global_metrics_etl',
        schedule_interval='0 0 * * 0-6',
        description='vertica-dwh',
        catchup=True,
        start_date=pendulum.datetime(2022, 10, 2, tz="UTC"),
        is_paused_upon_creation=True,
        tags=['final', 'dwh'],
) as dag:
    
    task_global_metrics= PythonOperator(
        task_id='dwh_load',
        python_callable=global_metrics_load,
        op_kwargs={
            'conn_info': VerticaConnectionBuilder.vc_conn("VC_CONNECTION"),
            'date': "{{ ds }}"
              }
    )

task_global_metrics
