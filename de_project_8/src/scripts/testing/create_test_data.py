import random
from datetime import datetime, timedelta
from json import dumps
import requests, os, sys

API_GEN_URL = 'https://www.uuidgenerator.net/api/version4'

columns = [
    "restaurant_id",
    "adv_campaign_id",
    "adv_campaign_content",
    "adv_campaign_owner",
    "adv_campaign_owner_contact",
    "adv_campaign_datetime_start",
    "adv_campaign_datetime_end",
    "datetime_created"
]

restaurants_DDL = """CREATE TABLE IF NOT EXISTS public.subscribers_restaurants(
id serial,
client_id varchar null,
restaurant_id varchar null
);
-- inserting dummy data to table subsribers_restaurants
insert into public.subscribers_restaurants (
    client_id, restaurant_id
)
values
"""

feedback_DDL = """CREATE TABLE if not exists public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);"""

try:
    num = int(sys.argv[1])
    test_data_folder = sys.argv[2]
except (IndexError, ValueError): 
    test_data_folder = '/home/rg/Documents/Study/temp/rg-folder/test_data'
    num = 10

def get_uuids_from_API():
    response = requests.get(f"{API_GEN_URL}/{num*3}")
    return response.text.split('\r\n')[:num], response.text.split('\r\n')[num:2*num], response.text.split('\r\n')[2*num:] 

def create_test_kafka_messages(restaurants, campaigns):
    d = {}
    with open(f"{test_data_folder}/kafka_text.txt", "w") as f:
        for i in range(len(restaurants)):
            gen = random.randint(0,len(restaurants)-1)
            restaurant_id = restaurants[i]
            adv_campaign_id = campaigns[gen]
            adv_campaign_content = ['1st content','2nd content', '3rd content'][gen%3]
            adv_campaign_owner = f"owner_{gen}"
            adv_campaign_owner_contact = f"info{gen}@{('mail.ru', 'google.com')[gen%2]}"
            adv_campaign_datetime_start = datetime.now() - timedelta(hours = random.randint(1,2))
            adv_campaign_datetime_end = adv_campaign_datetime_start + timedelta(hours = random.randint(1,4))
            datetime_created = (adv_campaign_datetime_start - timedelta(hours=1))
            adv_campaign_datetime_start = round(adv_campaign_datetime_start.timestamp())
            adv_campaign_datetime_end = round(adv_campaign_datetime_end.timestamp())
            datetime_created = round(datetime_created.timestamp())
            for key in columns:
                d[key] = eval(key)
            value = dumps(d)
            f.write(f"{i}:{value}\n")


def create_subscribers_restaurants_DDL(restaurants, users):
    with open(f"{test_data_folder}/restaurants.sql", "w") as f:
        f.write(restaurants_DDL)
        for i in range(num):
            f.write(f"('{users[i]}', '{restaurants[i]}'){[',',';'][i==(num-1)]}\n")

def create_subscribers_feedback_DDL():
    with open(f"{test_data_folder}/feedback.sql", "w") as f:
        f.write(feedback_DDL)

def main():
    restaurants, users, campaigns = get_uuids_from_API()
    if not os.path.exists(test_data_folder):
        os.mkdir(test_data_folder)
    create_subscribers_restaurants_DDL(restaurants, users)
    create_test_kafka_messages(restaurants, campaigns)
    create_subscribers_feedback_DDL()

if __name__ == "__main__":
    main()