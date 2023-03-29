from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaProducer, KafkaConsumer
from dds_loader.repository import DdsRepository
import uuid
from typing import Dict, List, Tuple

class DdsMessageProcessor:
    def __init__(self,
                consumer:KafkaConsumer,
                producer: KafkaProducer,
                dds_repository:DdsRepository,
                batch_size:int,
                logger: Logger) -> None:
        
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger
        # перечисление уникальных полей таблиц для разных типов таблиц, линки в виде списка, т.к. там можно сгенерировать поля
        self.hub_unique_fields = {
            "user":["user_id"], 
            "product":["product_id"], 
            "category":["category_name"], 
            "restaurant":["restaurant_id"],  
            "order":['order_id', 'order_dt']
            }
        self.links = [
            "order_product",
             "product_restaurant",
             "product_category",
             "order_user"]
        self.satellites = {"order_cost":["h_order_pk", "cost", "payment"],
                        "order_status":["h_order_pk", "status"],
                        "product_names": ["h_product_pk", "name"],
                        "restaurant_names": ["h_restaurant_pk", "name" ],
                        "user_names": ["h_user_pk", "username", "userlogin"]
        }
      
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        output_msg_fields = [
                    "h_user_pk",
                    "h_product_pk",
                    "h_order_pk",
                    "name",
                    "h_category_pk",
                    "category_name"]
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg:
                # create dict and list with main values and products entities
                values_dict, products_list = self.parse_msg(msg['payload'])
                # заполняю хабы с основными данными 
                for table_name in ['user', 'order', 'restaurant']:
                    self.populate_tables(table_name, values_dict, 'h')
                # заполняю линк между order и user
                self.populate_tables('order_user', values_dict, 'l')
                # заполняю сателлиты для основных данных
                for table_name in ["order_cost", "order_status", "restaurant_names", "user_names"]:
                    self.populate_tables(table_name, values_dict, 's')
                # обхожу список с продуктами, заполняю оставшиеся хабы, линки и сателлиты
                for product in products_list:
                    for table_name in ['product', 'category']:
                        self.populate_tables(table_name, product, 'h')
                    self.populate_tables('product_names', product, 's')
                    for table_name in ["order_product", "product_restaurant", "product_category"]:
                        self.populate_tables(table_name, values_dict|product, "l")
                # собираю ответное сообщение для CDM сервиса
                    output_msg = {}
                    output_msg = self.get_table_data(values_dict|product, output_msg_fields)
                    output_msg["user_id"] = str(output_msg.pop("h_user_pk"))
                    output_msg["product_name"] = output_msg.pop("name")
                    output_msg["category_id"] = str(output_msg.pop("h_category_pk"))
                    output_msg["order_id"] = str(output_msg.pop("h_order_pk"))
                    output_msg["product_id"] = str(output_msg.pop("h_product_pk"))
                    
                # отправляю сообщение для CDM в нужный топик в Kafka, количество сообщений равно количеству продуктов в заказе
                    self._producer.produce(output_msg)

    def populate_tables(self, 
                        table_name : str, 
                        values: Dict, 
                        table_type: str) -> None:
        additional_fields = []
        # добавляем ключевые поля в выборку в соответствии с типом таблицы
        if table_type == "h":
            additional_fields = self.hub_unique_fields[table_name] + [f"h_{table_name}_pk"]
        elif table_type == "l":
            additional_fields = [f"hk_{table_name}_pk", f"h_{table_name.split('_')[0]}_pk", f"h_{table_name.split('_')[1]}_pk"]
        elif table_type == "s":
            additional_fields = self.satellites[table_name] + [f"hk_{table_name}_pk"]
        else:
            return
        table_data = self.get_table_data(values, ["load_src", "load_dt"] + additional_fields)
        self._dds_repository.simple_insert(f"{table_type}_{table_name}", table_data)

# процедура для обработки входного сообщения
    @staticmethod
    def parse_msg(msg:Dict, 
                    source="Kafka-stg-service-orders") -> Tuple[Dict, List[Dict]]:
        res = dict()
        res["load_dt"] = str(datetime.now())
        res["load_src"] = source
        res["order_id"] = msg['id']
        res["order_dt"] = msg['date']
        res["cost"] = msg["cost"]
        res["payment"] = msg["payment"] 
        res["h_order_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["order_id"]))
        res["user_id"] = msg['user']['id']
        res["username"] = msg['user']['name']
        res["userlogin"] = msg['user'].get('login', 'login_was_not_loaded')
        res["h_user_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["user_id"]))
        res["restaurant_id"] = msg['restaurant']['id']
        # restaurant_name
        res["name"] = msg['restaurant']['name']
        res["h_restaurant_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["restaurant_id"]))
        res["status"] = msg["status"]
        res["hk_order_user_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["h_order_pk"]) + str(res["h_user_pk"]))
        res["hk_order_cost_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["h_order_pk"]) + str(res["cost"]))
        res["hk_order_status_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["h_order_pk"]) + str(res["status"]))
        res["hk_restaurant_names_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["h_restaurant_pk"]) + str(res["name"]))
        res["hk_user_names_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["h_user_pk"]) + str(res["username"]))
        products = []
        for product in msg['products']:
            product_record = {}
            product_record["product_id"] = product["id"]
            # product_name
            product_record["name"] = product["name"]
            product_record["price"] = product["price"]
            product_record["quantity"] = product["quantity"]
            product_record["category_name"] = product["category"]
            product_record["h_product_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(product_record["product_id"]))
            product_record["h_category_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(product_record["category_name"]))
            product_record["hk_product_category_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(product_record["h_product_pk"])+ str(product_record["h_category_pk"]))
            product_record["hk_product_names_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(product_record["h_product_pk"])+ str(product_record["name"]))
            product_record["hk_order_product_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(res["h_order_pk"])+ str(product_record["h_product_pk"]))
            product_record["hk_product_restaurant_pk"] = uuid.uuid3(uuid.NAMESPACE_OID, str(product_record["h_product_pk"])+ str(res["h_restaurant_pk"]))
            product_record["load_dt"] = res["load_dt"]
            product_record["load_src"] = res["load_src"]
            products.append(product_record)
        return res, products
# функция для фильтрации определеных полей из словаря    
    @staticmethod
    def get_table_data(data:Dict, 
                            fields:List[str]) -> Dict:
        return {x:data[x] for x in fields}
