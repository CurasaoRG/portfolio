with user_group_log as (
    select
luga.hk_group_id,
count(distinct luga.hk_user_id) as cnt_added_users
from RASHITGAFAROVYANDEXRU__DWH.s_auth_history sah 
left join RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity luga
on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
where sah.event = 'add' 
group by luga.hk_group_id
having luga.hk_group_id in (select hk_group_id from RASHITGAFAROVYANDEXRU__DWH.h_groups hg 
order by registration_dt asc limit 10)
order by luga.hk_group_id
)

select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
;