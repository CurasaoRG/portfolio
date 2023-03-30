---dropping tables with outdated data
drop table if exists RASHITGAFAROVYANDEXRU__DWH.s_auth_history;
drop table if exists RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity;

----l_user_group_activity----

create table if not exists RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user_id REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

----s_auth_history----

create table if not exists RASHITGAFAROVYANDEXRU__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_user_group_activity REFERENCES RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(6),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
