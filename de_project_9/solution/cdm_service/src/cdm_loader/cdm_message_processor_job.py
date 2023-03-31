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