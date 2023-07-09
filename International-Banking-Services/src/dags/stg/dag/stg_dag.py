import logging

import pendulum
from airflow.decorators import dag, task
from stg.dag.cur_loader import CurLoader
from stg.dag.trans_loader import TransLoader


from stg.lib import ConnectionBuilder
from stg.lib import VerticaConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/1 * * * *',  # We set the schedule for the execution of the dag - every minute.
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),  # Start date of the dag. Can be placed today.
    catchup=False,  # Whether to run a dag for previous periods (from start_date to today) - False (not needed).
    tags=['final', 'stg', 'origin'],  # Tags are used for filtering in the Airflow interface.
    is_paused_upon_creation=True  # Stopped/Started on Appearance. Launched immediately.
)
def stg_cur_and_trans_dag():
    # We create a connection to the dwh database.
    dwh_vc_connect = VerticaConnectionBuilder.vc_conn("VC_CONNECTION") # extra {"host": "51.250.75.20", "port": 5433, "user": "AVBORMOTOVYANDEXRU", "password": "u1623odeD3kaPt2", "database": "dwh"}

    # Create a connection to the postgresql database.
    pg_connect = ConnectionBuilder.pg_conn("PG_CONNECTION")

    # We declare a task that loads data.
    @task(task_id="currencies_load")
    def load_currencies():
        # we create an instance of the class in which the logic is implemented.
        rest_loader = CurLoader(pg_connect, dwh_vc_connect, log)
        rest_loader.load_currencies()  # We call a function that will pour the data.

    @task(task_id="transactions_load")
    def load_transactions():
        rest_loader = TransLoader(pg_connect, dwh_vc_connect, log)
        rest_loader.load_transactions()

    # We initialize the declared tasks.
    currency_dict = load_currencies()
    transaction_dict = load_transactions()

    # Next, we set the sequence of tasks execution.
    currency_dict >> transaction_dict

stg_cur_and_trans_dag = stg_cur_and_trans_dag()
