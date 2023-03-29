create table if not exists
RASHITGAFAROVYANDEXRU__DWH.global_metrics
(    date_update date, 
    currency_from int, 
    amount_total numeric(7,3),
    cnt_transactions int, 
    avg_transactions_per_account numeric(7,3),
    cnt_accounts_make_transactions int,
	load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt 
SEGMENTED BY hash(date_update, currency_from) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);