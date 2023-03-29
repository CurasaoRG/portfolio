--создаю таблицу для витрины
create table if not exists mart.f_customer_retention (
new_customers_count bigint,
returning_customers_count bigint,
refunded_customer_count bigint,
period_name varchar(7) default 'weekly',
period_id smallint,
item_id int4,
new_customers_revenue numeric(10,2),
returning_customers_revenue numeric (10,2),
customers_refunded bigint
);
-- скрипт для создания вспомогательного представления с количеством заказов
create or replace view staging.customers_count as (
select week_id, item_id, customer_id, 
sum(case 
	when status = 'shipped' then 1
	else 0
	end) as shipped_qty, 
sum(case 
	when status = 'refunded' then 1
	else 0
	end) as refunded_qty,
sum(case
	when status = 'refunded' then payment_amount
	else 0
	end) as refunded,
sum(case 
	when status = 'shipped' then payment_amount
	else 0
	end) as payment_amount
from (
select 
s.customer_id, s.payment_amount, s.item_id, 
100*c.year_actual+c.week_of_year as week_id, 
status
from mart.f_sales s
left join mart.d_calendar c
on s.date_id = c.date_id) all_cust
group by week_id, customer_id, item_id);

--Скрипты дял представлений для отдельных компонентов витрины: данные по новым заказчикам, данные по возвращающимся заказчикам, данные по возвратам
CREATE or replace VIEW mart.f_new_customers as
select week_id, item_id, count(customer_id) as new_customers_count, sum(payment_amount) as new_customers_revenue from staging.customers_count where shipped_qty = 1
group by week_id, item_id;

CREATE VIEW mart.f_returning_customers as
select week_id, item_id, count(customer_id) as returning_customers_count, sum(payment_amount) as returning_customers_revenue from staging.customers_count where shipped_qty > 1
group by week_id, item_id;


CREATE or replace VIEW mart.f_refunded_customers as
select week_id, item_id, count(customer_id) as refunded_customers_count, sum(payment_amount) as customers_refunded from staging.customers_count where refunded_qty > 0
group by week_id, item_id;

--Скрипт для объединения данных из представлений в витрину
with wi as (select 
        distinct 100*c.year_actual+c.week_of_year as week_id, item_id
        from mart.f_sales s
        left join mart.d_calendar c
        on s.date_id = c.date_id)
insert into  mart.f_customer_retention 
        (new_customers_count, returning_customers_count, refunded_customers_count, 
        period_id, item_id, 
        new_customers_revenue, returning_customers_revenue, customers_refunded)
(select nc.new_customers_count, rc.returning_customers_count, rfc.refunded_customers_count, wi.week_id as period_id, wi.item_id, 
	nc.new_customers_revenue, rc.returning_customers_revenue, rfc.customers_refunded
from wi left join mart.f_new_customers nc 
	on wi.week_id = nc.week_id and wi.item_id = nc.item_id
left join mart.f_returning_customers rc
	on wi.week_id = rc.week_id and wi.item_id = rc.item_id
left join mart.f_refunded_customers rfc
	on wi.week_id = rfc.week_id and wi.item_id = rfc.item_id)