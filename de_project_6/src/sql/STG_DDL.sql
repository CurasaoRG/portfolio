---group_log----
drop table if exists RASHITGAFAROVYANDEXRU__STAGING.group_log;
create table if not exists RASHITGAFAROVYANDEXRU__STAGING.group_log (
    group_id int primary key,
    user_id int,
    user_id_from int,
    event varchar(6),
    event_date timestamp
    )
order by group_id 
SEGMENTED BY hash(group_id) all nodes
PARTITION BY event_date::date
GROUP BY calendar_hierarchy_day(event_date::date, 3, 2);