create or replace view RASHITGAFAROVYANDEXRU__DWH.group_conversion as 
with user_group_log as (
select luga.hk_group_id,
count(distinct luga.hk_user_id) as cnt_added_users
from RASHITGAFAROVYANDEXRU__DWH.s_auth_history sah 
left join RASHITGAFAROVYANDEXRU__DWH.l_user_group_activity luga
on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
where sah.event = 'add' 
group by luga.hk_group_id
having luga.hk_group_id in (select hk_group_id from RASHITGAFAROVYANDEXRU__DWH.h_groups hg 
order by registration_dt asc limit 10)
order by luga.hk_group_id
), 
user_group_messages as (
select hg.hk_group_id, 
count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
from RASHITGAFAROVYANDEXRU__DWH.l_groups_dialogs lgd 
left join RASHITGAFAROVYANDEXRU__DWH.h_groups hg 
on hg.hk_group_id=lgd.hk_group_id
left join RASHITGAFAROVYANDEXRU__DWH.l_user_message lum 
on lgd.hk_message_id = lum.hk_message_id
group by hg.hk_group_id
order by hg.hk_group_id
)
(select 
user_group_log.hk_group_id, cnt_added_users, cnt_users_in_group_with_messages,
cnt_users_in_group_with_messages/cnt_added_users as group_conversion
from 
user_group_log left join
user_group_messages on 
user_group_messages.hk_group_id = user_group_log.hk_group_id
order by group_conversion desc);

select * from RASHITGAFAROVYANDEXRU__DWH.group_conversion order by group_conversion;