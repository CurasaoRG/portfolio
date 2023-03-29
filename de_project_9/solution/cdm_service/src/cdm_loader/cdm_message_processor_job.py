from datetime import datetime
from logging import Logger
import uuid
from cdm_loader.repository import CDMRepository
from typing import Dict


from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                consumer:KafkaConsumer,
                cdm_repository:CDMRepository,
                batch_size:int,
                logger: Logger) -> None:
        
        self._logger = logger
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        product_cnt_dict = {}
        cat_count_dict = {}
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg:

# incoming msg sctructure: {"category_name": "\u0421\u0443\u043f\u044b", 
#                  "user_id": "c9e6fe6a-f87e-3d8b-a882-f31ddb1b23d2", 
#                  "product_name": "\u0427\u0438\u043a\u0435\u043d \u0428\u043e\u0440\u0431\u0430", 
#                  "category_id": "53350d4a-c113-31ae-8141-e9ff77d1e81a", 
#                  "order_id": "c6659afb-e735-3116-bf6e-fa6d1e118d1e", 
#                  "product_id": "1624026f-077f-31c0-8711-408a6439d9ed"}
# -----------CDM DDL
# CREATE SCHEMA IF NOT EXISTS cdm; 

# create table if not exists cdm.user_product_counters (
# id serial primary key, 
# user_id uuid not null,
# product_id uuid not null,
# product_name varchar not null, 
# order_cnt integer constraint order_cnt_check check(order_cnt>=0) not null
# );
# create unique index user_id_product_id_index on
# cdm.user_product_counters (user_id, product_id);

# drop table cdm.user_product_counters;

# create table if not exists cdm.user_category_counters (
# id serial primary key, 
# user_id uuid not null,
# category_id uuid not null,
# category_name varchar not null, 
# order_cnt integer constraint order_cnt_check check(order_cnt>=0) not null
# );
# create unique index user_id_category_id_index on
# cdm.user_category_counters (user_id, category_id);

                hk_user_product_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(msg["user_id"])+ str(msg["product_id"]))
                hk_user_category_pk = uuid.uuid3(uuid.NAMESPACE_OID, str(msg["user_id"])+ str(msg["category_id"]))
                if hk_user_product_pk in product_cnt_dict.keys():
                    product_cnt_dict[hk_user_product_pk]["order_cnt"] |= {msg["order_id"]} 
                else:
                    product_cnt_dict[hk_user_product_pk] = {"user_id":msg["user_id"], 
                                                                      "product_id":msg["product_id"], 
                                                                      "product_name":msg["product_name"],
                                                                      "order_cnt":{msg["order_id"]}
                    }
                
                if hk_user_category_pk in cat_count_dict.keys():
                    cat_count_dict[hk_user_category_pk]["order_cnt"] |= {msg["order_id"]} 
                else:
                    cat_count_dict[hk_user_category_pk] = {"user_id":msg["user_id"], 
                                                                      "category_id":msg["category_id"], 
                                                                      "category_name":msg["category_name"],
                                                                      "order_cnt":{msg["order_id"]}
                    }
            else:
                break
        # end for 
        for key in product_cnt_dict.keys():
            product_cnt_dict[key]["order_cnt"] = len(product_cnt_dict[key]["order_cnt"])
        for key in cat_count_dict.keys():
            cat_count_dict[key]["order_cnt"] = len(cat_count_dict[key]["order_cnt"])
        for key, val in {"user_product_counters":product_cnt_dict, 
                         "user_category_counters":cat_count_dict}.items():
            table_data = self.makeTuple(val)
            self._cdm_repository.simple_insert(key, table_data)
        

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    @staticmethod
    def makeTuple(input: Dict) -> tuple:
        res = tuple()
        for val in input.values():
            res += (val,)
        return res