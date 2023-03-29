from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaProducer, KafkaConsumer
from lib.redis import RedisClient
from stg_loader.repository import StgRepository
import json

class StgMessageProcessor:
    def __init__(self,
                consumer:KafkaConsumer,
                producer: KafkaProducer,
                redis:RedisClient,
                stg_repository:StgRepository,
                batch_size:int,
                logger: Logger) -> None:
        
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger
        

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")
        for i in range(self._batch_size):
            msg = self._consumer.consume()
            if msg:
                # отправляю сырое сообщение в Postgres
                self._stg_repository.order_events_insert(
                    object_id=int(msg["object_id"]),
                    object_type = msg["object_type"],
                    sent_dttm =  datetime.strptime(msg["sent_dttm"], '%Y-%m-%d %H:%M:%S'),
                    payload=json.dumps(msg["payload"])
                )
                # дополняю исходные данные значениями из Redis
                user_id = msg["payload"]["user"]["id"]
                restaurant_id = msg["payload"]["restaurant"]["id"]
                user_data = self._redis.get(user_id)
                restaurant_data = self._redis.get(restaurant_id)
                # собираю ответное сообщение
                output_msg = dict()
                output_msg['object_id'] = msg['object_id']
                output_msg['object_type'] = msg["object_type"]
                output_msg['payload'] = dict()
                output_msg['payload']['id'] = msg['object_id']
                output_msg['payload']['date'] = msg['payload']['date']
                output_msg['payload']['cost'] = msg['payload']['cost']
                output_msg['payload']['payment'] = msg['payload']['payment']
                output_msg['payload']['status'] = msg['payload']['final_status']
                output_msg['payload']['restaurant'] = msg['payload']['restaurant'].copy()
                output_msg['payload']['restaurant']['name'] = restaurant_data['name']
                output_msg['payload']['user'] = msg['payload']['user'].copy()
                output_msg['payload']['user']['name'] = user_data['name']
                output_msg['payload']['user']['login'] = user_data['login']
                output_msg['payload']['products'] = list()
                for product in msg['payload']['order_items']:
                    temp = dict()
                    for menu_item in restaurant_data['menu']:
                        if product['id'] in menu_item.values():
                            category = menu_item['category']
                            break
                    temp['id'] = product['id']
                    temp['price'] = product['price']
                    temp['quantity'] = product['quantity']
                    temp['name'] = product['name']
                    temp['category'] = category
                    output_msg['payload']['products'].append(temp)
                self._producer.produce(output_msg)
            else:
                break

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
        