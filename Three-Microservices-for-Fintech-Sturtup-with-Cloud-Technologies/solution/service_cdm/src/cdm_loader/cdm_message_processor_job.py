import time, json
from typing import Dict, List
from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository
import uuid


class CdmMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                cdm_repository: CdmRepository,
                logger: Logger,
                ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
        
            self._logger.info(f"{datetime.utcnow()}: Message received")

            order_id = msg["object_id"]
            order = msg["payload"]

            self._logger.info(f"{datetime.utcnow()}: ___________________________")
            self._logger.info(f"{datetime.utcnow()}: {msg}")
            self._logger.info(f"{datetime.utcnow()}: ___________________________")


            for i in order["products"]:
                self._cdm_repository.user_product_counters_insert(
                    order["user_id"],
                    i["product_id"],
                    i["product_name"],
                    i["quantity"])

                self._cdm_repository.user_category_counters_insert(
                    order["user_id"],
                    i["category_id"],
                    i["category_name"],
                    i["quantity"])

        self._logger.info(f"{datetime.utcnow()}: FINISH")

