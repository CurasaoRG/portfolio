drop table if exists RASHITGAFAROVYANDEXRU__DWH.h_users;

create table RASHITGAFAROVYANDEXRU__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;

drop table if exists RASHITGAFAROVYANDEXRU__DWH.h_dialogs;

create table RASHITGAFAROVYANDEXRU__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id      int,
    datetime datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;


drop table if exists RASHITGAFAROVYANDEXRU__DWH.h_groups;

create table RASHITGAFAROVYANDEXRU__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;



INSERT INTO RASHITGAFAROVYANDEXRU__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from RASHITGAFAROVYANDEXRU__STAGING.users
where hash(id) not in (select hk_user_id from RASHITGAFAROVYANDEXRU__DWH.h_users); 

INSERT INTO RASHITGAFAROVYANDEXRU__DWH.h_dialogs(hk_message_id, message_id, datetime, load_dt, load_src)
select
       hash(message_id) as  hk_message_id,
       message_id as message_id,
       message_ts as datetime,
       now() as load_dt,
       's3' as load_src
       from RASHITGAFAROVYANDEXRU__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from RASHITGAFAROVYANDEXRU__DWH.h_dialogs); 

INSERT INTO RASHITGAFAROVYANDEXRU__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from RASHITGAFAROVYANDEXRU__STAGING.groups
where hash(group_id) not in (select hk_group_id from RASHITGAFAROVYANDEXRU__DWH.h_groups); 


drop table if exists RASHITGAFAROVYANDEXRU__DWH.l_user_message;

create table RASHITGAFAROVYANDEXRU__DWH.l_user_message
(
hk_l_user_message bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_users (hk_user_id),
hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_dialogs (hk_message_id),
load_dt datetime,
load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



drop table if exists RASHITGAFAROVYANDEXRU__DWH.l_admins;

create table RASHITGAFAROVYANDEXRU__DWH.l_admins
(
hk_l_admin_id bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_admins_user REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_admins_group REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists RASHITGAFAROVYANDEXRU__DWH.l_groups_dialogs;

create table RASHITGAFAROVYANDEXRU__DWH.l_groups_dialogs
(
hk_l_groups_dialogs bigint primary key,
hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_dialogs (hk_message_id),
hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO RASHITGAFAROVYANDEXRU__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from RASHITGAFAROVYANDEXRU__STAGING.groups as g
left join RASHITGAFAROVYANDEXRU__DWH.h_users as hu on g.admin_id = hu.user_id
left join RASHITGAFAROVYANDEXRU__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from RASHITGAFAROVYANDEXRU__DWH.l_admins);


INSERT INTO RASHITGAFAROVYANDEXRU__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt,load_src)
select
hash(hu.hk_user_id, hd.hk_message_id) as hk_l_user_message,
hk_user_id,
hk_message_id,
now() as load_dt,
's3' as load_src
from RASHITGAFAROVYANDEXRU__STAGING.dialogs as d
left join RASHITGAFAROVYANDEXRU__DWH.h_users as hu on d.message_from = hu.user_id
left join RASHITGAFAROVYANDEXRU__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hu.hk_user_id, hd.hk_message_id) not in (select hk_l_user_message from RASHITGAFAROVYANDEXRU__DWH.l_user_message);
------------
INSERT INTO RASHITGAFAROVYANDEXRU__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt,load_src)
select
--hk_l_groups_dialogs bigint primary key,
--hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_dialogs (hk_message_id),
--hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES RASHITGAFAROVYANDEXRU__DWH.h_groups (hk_group_id),
hash(hg.hk_group_id, hd.hk_message_id) as hk_l_groups_dialogs,
hd.hk_message_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from RASHITGAFAROVYANDEXRU__STAGING.dialogs as d
right join RASHITGAFAROVYANDEXRU__DWH.h_groups as hg on d.message_group = hg.group_id
left join RASHITGAFAROVYANDEXRU__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hg.hk_group_id, hd.hk_message_id) not in (select hk_l_groups_dialogs from RASHITGAFAROVYANDEXRU__DWH.l_groups_dialogs);









