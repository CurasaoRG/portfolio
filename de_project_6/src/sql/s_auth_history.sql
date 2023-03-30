truncate table RASHITGAFAROVYANDEXRU__DWH.s_auth_history;
----insert-to-s_auth_history----

INSERT INTO RASHITGAFAROVYANDEXRU__DWH.s_auth_history
(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt,load_src)
with cleaned_stg as (
select group_id, user_id, 
case 
	when user_id_from <> user_id then user_id_from
	else null
end as user_id_from, event, event_date
from (select group_id, user_id, user_id_from, event_date, lag(event) over (partition by user_id, group_id order by event_date), event, lead(event) over (partition by user_id, group_id order by event_date)from 
RASHITGAFAROVYANDEXRU__STAGING.group_log
order by group_id, user_id, event_date asc) s
where (s.event <> s.lag or s.lag is null)
) 
select
luga.hk_l_user_group_activity,
cleaned_stg.user_id_from, 
cleaned_stg.event,
cleaned_stg.event_date as event_dt,
now() as load_dt,
's3' as load_src
from cleaned_stg
left join RASHITGAFAROVYANDEXRU__DWH.h_groups hg 
on cleaned_stg.group_id = hg.group_id
left join  RASHITGAFAROVYANDEXRU__DWH.h_users hu
on cleaned_stg.user_id = hu.user_id
left join RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity luga 
on (hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id);