CREATE TABLE IF NOT EXISTS RASHITGAFAROVYANDEXRU__STAGING.transactions (
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp NULL,
	load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt 
SEGMENTED BY hash(operation_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
--currencies
CREATE TABLE IF NOT EXISTS RASHITGAFAROVYANDEXRU__STAGING.currencies (
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL,
	load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt 
SEGMENTED BY hash(date_update, currency_code) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
--srv_wf_settings
CREATE TABLE IF NOT EXISTS RASHITGAFAROVYANDEXRU__STAGING.srv_wf_settings
(
    workflow_key varchar(80),
    workflow_settings varchar(80)
);

--transactions_proj
CREATE PROJECTION IF NOT EXISTS transactions_proj as 
select 
	operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	"status",
	transaction_type,
	amount,
	transaction_dt,
	load_dt,
    load_src
FROM
RASHITGAFAROVYANDEXRU__STAGING.transactions t
ORDER BY transaction_dt 
SEGMENTED BY hash(operation_id) all nodes;
--currencies_proj
CREATE PROJECTION IF NOT EXISTS currencies_proj
AS SELECT 
	date_update,
	currency_code,
	currency_code_with,	
	currency_with_div,
	load_dt ,
    load_src
FROM RASHITGAFAROVYANDEXRU__STAGING.currencies
ORDER BY date_update 
SEGMENTED BY hash(date_update, currency_code) all nodes;