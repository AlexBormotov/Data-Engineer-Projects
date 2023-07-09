from logging import Logger
import logging
from typing import List
import uuid
from stg import EtlSetting, StgEtlSettingsRepository
from stg.lib import PgConnect
from stg.lib import VerticaConnect
from stg.lib.dict_util import json2str
import json
from psycopg.rows import class_row
from pydantic import BaseModel
import datetime as dt
import vertica_python
import pandas as pd

class TransObj(BaseModel):
    operation_id: uuid.UUID
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: int
    transaction_dt: str

class TransOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_currencies(self, threshold: str, limit: int) -> List[TransObj]: 
        with self._db.client().cursor(row_factory=class_row(TransObj)) as cur:
            cur.execute(
                """
                    SELECT operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt::text
                    FROM public.transactions
                    WHERE transaction_dt::timestamp > %(threshold)s::timestamp
                    ORDER BY transaction_dt
                    LIMIT %(limit)s;
                """,
                    {
                        "threshold": threshold,
                        "limit": limit
                    }
            )
            objs = cur.fetchall()
        return objs 


class TransDestRepository:
    def __init__(self, log: Logger) -> None:
        self.log = log

    def write_data(self, conn: vertica_python.Connection, cur_data: List, cols: str, dest_table: str):
        
        # implement in vertica
        logging.info('Vertica connection')

        cur = conn.cursor()
        new_df = pd.DataFrame([s.__dict__ for s in cur_data])

        self.log.info("______________________________")
        logging.info(new_df)
        self.log.info("______________________________")

        df = new_df.to_csv(sep=',', index=False, header=False) 

        # expression for vertica
        vert_expr = f"""COPY AVBORMOTOVYANDEXRU__STAGING.{dest_table} ({cols})
                from stdin DELIMITER ','
                REJECTED DATA AS TABLE
                AVBORMOTOVYANDEXRU__STAGING.{dest_table}_rej;"""

        # implementation
        logging.info('Start execute')
        cur.copy(vert_expr, df)
        conn.commit()
        logging.info('Finish execute')


class TransLoader:
    WF_KEY = 'transactions_to_stg_workflow'
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50000 

    def __init__(self, pg_origin: PgConnect, vc_dest: VerticaConnect, log: Logger) -> None:
        self.vc_dest = vc_dest
        self.origin = TransOriginRepository(pg_origin)
        self.stg = TransDestRepository(log)
        self.settings_repository = StgEtlSettingsRepository(self.WF_KEY)
        self.log = log

    def load_transactions(self):
        # open a transaction.
        # The transaction will be committed if the code in the with block succeeds (i.e. without errors).
        with self.vc_dest.connection() as conn:

            # Read loading status
            # If there is no setting yet, start it.
            wf_setting = self.settings_repository.get_setting(conn)
            # if there is no entry in the table yet, then we will create a basic wf_setting
            if not wf_setting:
                wf_setting = EtlSetting(id=uuid.uuid1(), workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: "1970-01-01 00:00:00.000"})

            self.log.info("______________________________")
            logging.info(wf_setting)
            self.log.info("______________________________")

            # Subtract the next batch of objects.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_currencies(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} rows to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            # Saving objects to the database dwh.
            self.stg.write_data(conn, load_queue, 'operation_id, account_number_from, account_number_to, currency_code,country, status, transaction_type, amount, transaction_dt', 'transactions')

            # Save progress.
            # We use the same connection, so the setting will be saved along with the objects
            # or all changes will be rolled back.
            last_date = str(max([dt.datetime.fromisoformat(i.transaction_dt) for i in load_queue]))[:-3] 
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_date # put in a dictionary 
            wf_setting.workflow_settings = json2str(wf_setting.workflow_settings)  # Let's convert to a string to put in the database

            dicty_dict = wf_setting.__dict__ # here it is more convenient to go to the dictionary to write to the pandas table and then to Vertica
            dicty_dict = {k: [v] for k, v in dicty_dict.items()}  # due to the specifics of data processing, all dictionary values must be represented as a list

            df = pd.DataFrame.from_dict(dicty_dict) 

            self.log.info("______________________________")
            self.log.info(dicty_dict)
            self.log.info(df)
            self.log.info("______________________________")

            self.settings_repository.save_setting(conn, df)

            self.log.info(f"Load finished on {json.loads(dicty_dict['workflow_settings'][0])[self.LAST_LOADED_ID_KEY]}")
            self.log.info("_______________IT'S_ALREADY_DONE_______________")