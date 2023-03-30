--schema
create schema if not exists dds;
--dm_couriers
CREATE table if not exists dds.dm_api_couriers(
    id serial primary key,
    courier_id varchar(16) not null,
    name varchar not null
);
--dm_address
create table if not exists dds.dm_api_address(
    id serial primary key,
    city varchar(255) default 'Moscow' not null,
    street varchar(255) not null,
    house_number integer not null,
    apartment integer not null
);
--dm_restaurants
create table if not exists dds.dm_api_restaurants(
    id serial primary key,
    restaurant_id varchar(16) not null, 
    name text not null
);
--dm_orders
create table if not exists dds.dm_api_orders(
    id serial primary key,
    order_id varchar(16) not null,
    order_ts timestamp not null
);
--dm_delivery_details
create table if not exists dds.dm_api_delivery_details(
    id serial primary key,
    delivery_id varchar(16) not null,
    courier_id integer not null,
    address_id varchar not null,
    delivery_ts timestamp not null,
    rate smallint not null
);
alter table dds.dm_api_delivery_details add constraint dm_delivery_details_courier_id_fkey foreign key(courier_id) references dds.dm_api_couriers(id);
alter table dds.dm_api_delivery_details add constraint dm_delivery_details_address_id_fkey foreign key(address_id) references dds.dm_api_address(id);
--fct_sales
create table if not exists dds.fct_api_sales(
    id serial primary key,
    order_id integer not null,
    delivery_id integer not null,
    order_sum numeric (12,2) not null,
    tip_sum numeric(12,2) not null
);

alter table dds.fct_api_sales add constraint  fct_sales_order_id_fkey foreign key(order_id) references dds.dm_api_orders(id);
alter table dds.fct_api_sales add constraint fct_sales_delivery_id_fkey foreign key(delivery_id) references dds.dm__api_delivery_details(id);