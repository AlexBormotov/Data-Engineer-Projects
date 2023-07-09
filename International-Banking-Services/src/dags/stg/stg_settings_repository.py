import json
import logging
from typing import Dict, Optional
import pandas as pd

from pydantic import BaseModel
import vertica_python 
import uuid


class EtlSetting(BaseModel):
    id: uuid.UUID
    workflow_key: str
    workflow_settings: Dict


class StgEtlSettingsRepository:
    def __init__(self, etl_key: str) -> None:
        self.etl_key = etl_key

    def get_setting(self, conn: vertica_python.Connection) -> Optional[EtlSetting]: # Dict
        with conn.cursor() as cur: # row_factory=class_row(EtlSetting)
            cur.execute(
                f"""
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM AVBORMOTOVYANDEXRU__STAGING.srv_wf_settings
                    WHERE workflow_key = '{self.etl_key}'
                    ORDER BY id;
                """
            )
            obj = cur.fetchone()
            try:
                # rebuilding the dictionary due to difficulties in the recording system in Vertica
                temp_list = obj[2].split('"')
                temp_dict = json.loads('{"'+temp_list[3]+'":"'+temp_list[7]+'"}')
                obj_dict = EtlSetting(id = obj[0], workflow_key = obj[1], workflow_settings = temp_dict)
                conn.commit()
                return obj_dict
            except:
                conn.commit()
                return obj

    def save_setting(self, conn: vertica_python.Connection, df: pd.DataFrame) -> None:
        with conn.cursor() as cur:
            df_csv = df.to_csv(index=False)
            vert_expr1 = f"""
                DELETE FROM AVBORMOTOVYANDEXRU__STAGING.srv_wf_settings
                WHERE workflow_key = '{self.etl_key}'
            """
            cur.execute(vert_expr1)

            vert_expr2 = f"""
                COPY AVBORMOTOVYANDEXRU__STAGING.srv_wf_settings (id, workflow_key, workflow_settings)
                from stdin DELIMITER ','
                REJECTED DATA AS TABLE
                AVBORMOTOVYANDEXRU__STAGING.srv_wf_settings_rej;"""
            cur.copy(vert_expr2, df_csv)

        conn.commit()

