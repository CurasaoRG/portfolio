----users----
drop table if exists RASHITGAFAROVYANDEXRU__STAGING.users;
create table if not exists RASHITGAFAROVYANDEXRU__STAGING.users (
    id int primary key,
    chat_name varchar (200),
    registration_dt timestamp,
    country varchar(200),
    age int
    )
order by id 
SEGMENTED BY hash(id) all nodes
PARTITION BY registration_dt::date
;
----groups----
drop table if exists RASHITGAFAROVYANDEXRU__STAGING.groups;
create table if not exists RASHITGAFAROVYANDEXRU__STAGING.groups(
    id int primary key,
    admin_id int CONSTRAINT staging_groups_admin_id REFERENCES RASHITGAFAROVYANDEXRU__STAGING.users (id),
    group_name varchar(100),
    registration_dt datetime,
    is_private boolean
)
order by id, admin_id
SEGMENTED BY hash(id) all nodes
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

----dialogs----
drop table if exists RASHITGAFAROVYANDEXRU__STAGING.dialogs;
create table if not exists RASHITGAFAROVYANDEXRU__STAGING.dialogs(
    message_id int primary key,
    message_ts timestamp,
    message_from int CONSTRAINT staging_dialogs_message_from REFERENCES RASHITGAFAROVYANDEXRU__STAGING.users (id),
    message_to int CONSTRAINT staging_dialogs_message_to REFERENCES RASHITGAFAROVYANDEXRU__STAGING.users (id),
    message varchar(1000),
    message_group int
)
order by message_id
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

---group_log----
drop table if exists RASHITGAFAROVYANDEXRU__STAGING.group_log;
create table if not exists RASHITGAFAROVYANDEXRU__STAGING.group_log (
    group_id int primary key,
    user_id int,
    user_id_from int,
    event varchar(6),
    datetime timestamp
    )
order by group_id 
SEGMENTED BY hash(group_id) all nodes
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);
