--stg.api_restaurants
create table if not exists stg.api_restaurants(
    id serial primary key,
    content text,
    load_ts timestamp
);
--stg.api_couriers
create table if not exists stg.api_couriers(
    id serial primary key,
    content text,
    load_ts timestamp
);
--stg.api_deliveries
create table if not exists stg.api_deliveries(
    id serial primary key,
    content text,
    load_ts timestamp
);