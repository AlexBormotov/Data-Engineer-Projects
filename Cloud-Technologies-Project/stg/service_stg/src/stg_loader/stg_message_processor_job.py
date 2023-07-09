import time, json
from typing import Dict, List
from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository

class StgMessageProcessor:
    def __init__(self,
                     consumer: KafkaConsumer,
                     producer: KafkaProducer,
                     redis_client: RedisClient,
                     stg_repository: StgRepository,
                     batch_size,
                     logger: Logger) -> None:
            self._consumer = consumer
            self._producer = producer
            self._redis = redis_client
            self._stg_repository = stg_repository
            self._logger = logger
            self._batch_size = batch_size

    # a function that will be called according to the schedule.
    def run(self) -> None:
        # We write to the log that the job was launched.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg["payload"]
            self._stg_repository.order_events_insert(
                msg["object_id"],
                msg["object_type"],
                msg["sent_dttm"],
                json.dumps(msg["payload"]))

            user_id = msg["payload"]["user"]["id"]
            user = self._redis.get(user_id)
            user_name = user["name"]

            restaurant_id = msg["payload"]['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)
            restaurant_name = restaurant["name"]

            dst_msg = {
                "object_id": msg["object_id"],
                "object_type": msg["object_type"],
                "payload": {
                    "id": msg["object_id"],
                    "date": msg["payload"]["date"],
                    "cost": msg["payload"]["cost"],
                    "payment": msg["payload"]["payment"],
                    "status": msg["payload"]["final_status"],
                    "restaurant": self._format_restaurant(restaurant_id, restaurant_name),
                    "user": self._format_user(user_id, user_name),
                    "products": self._format_items(msg["payload"]["order_items"], restaurant)
                }
            }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        # We write to the log that the job has been successfully completed.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_restaurant(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_user(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_items(self, order_items, restaurant) -> List[Dict[str, str]]:
        items = []

        menu = restaurant["menu"]
        for it in order_items:
            menu_item = next(x for x in menu if x["_id"] == it["id"])
            dst_it = {
                "id": it["id"],
                "price": it["price"],
                "quantity": it["quantity"],
                "name": menu_item["name"],
                "category": menu_item["category"]
            }
            items.append(dst_it)

        return items
