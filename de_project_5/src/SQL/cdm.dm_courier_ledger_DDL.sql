--creating auxiliary view
create or replace view cdm.courier_agg_with_rates as 
(select courier_id, name, orders_count, orders_total_sum, case 
	when rate<4 then greatest(0.05*orders_total_sum,100.0)
	when rate<4.5 then greatest(0.07*orders_total_sum,150.0)
	when rate<4.9 then greatest(0.08*orders_total_sum,175.0)
	when rate>4.9 then greatest(0.1*orders_total_sum,200.0)
end as courier_order_sum, tip_sum, year, month, rate

from (
select courier_id, name, count(order_id) as orders_count, sum(order_sum) as orders_total_sum,  
sum(tip_sum) as tip_sum, year, month, avg(rate) as rate
from cdm.courier_agg
group by year, month, courier_id, name
order by courier_id) agg);

--creating table cdm.dm_courier_ledger
create table if not exists cdm.dm_courier_ledger
(
id serial primary key,
courier_id integer,
courier_name varchar(255),
settlement_year smallint,
settlement_month smallint,
orders_count integer,
orders_total_sum numeric (12,2),
rate_avg numeric (4,3),
order_processing_fee numeric (12,2),
courier_order_sum numeric(12,2),
courier_tips_sum numeric(12,2), 
courier_reward_sum numeric(12,2)
);