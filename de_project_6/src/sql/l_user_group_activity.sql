truncate table RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity;

insert into RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity 
(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
with dgau as (
select distinct group_id, user_id
from (select group_id, user_id, user_id_from, event_date, 
	lag(event) over (partition by user_id, group_id order by event_date), event, 
	lead(event) over (partition by user_id, group_id order by event_date)
	from RASHITGAFAROVYANDEXRU__STAGING.group_log) s
where (s.event <> s.lag or s.lag is null)) 

select
hash(hg.hk_group_id,hu.hk_user_id) as hk_l_user_group_activity,
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from dgau left join 
RASHITGAFAROVYANDEXRU__DWH.h_users hu 
on hu.user_id = dgau.user_id
left join RASHITGAFAROVYANDEXRU__DWH.h_groups hg 
on hg.group_id = dgau.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in 
(select hk_l_user_group_activity from RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity)
;