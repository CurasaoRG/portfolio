from src.connectors import KafkaConsumer
import datetime


class FromKafka:
    def __init__(self,
                 consumer:KafkaConsumer)->None:
        self._consumer = consumer

    def aaa(self):
        msg = self._consumer.consume()
        if msg:
            if msg['object_type'] == 'CURRENCY':
                return self.parse_currency(msg)
            elif msg['object_type'] == 'TRANSACTION':
                pass
            else:
                pass
    
    @staticmethod 
    def parse_currency(msg):
        msg = msg['payload']
        date_update = msg['date_update']
        currency_code = msg['currency_code']
        currency_code_with = msg['currency_code_with']
        currency_code_div = msg['currency_code_div']
        load_dt = datetime.datetime.now()
        load_src = 'Kafka-currency'
        return (date_update,currency_code, currency_code_with, currency_code_div, load_dt, load_src)
    
    @staticmethod
    def parse_transaction(msg):
        operation_id = msg['operation_id']
        account_number_from = msg['account_number_from']
        account_number_to = msg['account_number_to']
        currency_code = msg['currency_code']
        country = msg['country']
        status = msg['status']
        transaction_type = msg['transaction_type']
        amount = msg['amount']
        transaction_dt = msg['transaction_dt']
        load_dt = datetime.datetime.now()
        load_src = 'Kafka-transaction'
        return (operation_id, 
                account_number_from,
                account_number_to,
                currency_code,
                country,
                status,
                transaction_type,
                amount,
                transaction_dt,
                load_dt,
                load_src)


    # {"object_id": "5eeaf625-8161-4768-b031-d4686966778d", 
    #  "object_type": "TRANSACTION", 
    #  "sent_dttm": "2022-10-01T00:24:02", 
    #  "payload": {"operation_id": "5eeaf625-8161-4768-b031-d4686966778d", 
    #              "account_number_from": 903810, 
    #              "account_number_to": 2688072, 
    #              "currency_code": 420, 
    #              "country": "usa", 
    #              "status": "done", 
    #              "transaction_type": "sbp_incoming", 
    #              "amount": 50000, 
    #              "transaction_dt": "2022-10-01 00:24:02"}}
    

    # {"object_id": "604e3cbe-c876-5440-b9c2-77f9eb41ed3f", 
    #  "object_type": "CURRENCY", 
    #  "sent_dttm": "2022-10-02T00:00:00", 
    #  "payload": {"date_update": "2022-10-02 00:00:00", 
    #              "currency_code": 410, 
    #              "currency_code_with": 460, 
    #              "currency_with_div": 0.95}}

