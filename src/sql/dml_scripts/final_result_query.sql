with user_group_log  as (
    select 
    hg.hk_group_id as hk_group_id,
    count(distinct hu.user_id) cnt_added_users
    from STV2024111143__DWH.h_groups hg
    left join STV2024111143__DWH.l_user_group_activity lu
    on lu.hk_group_id = hg.hk_group_id
    left join STV2024111143__DWH.h_users hu
    on hu.hk_user_id = lu.hk_user_id
    left join STV2024111143__DWH.s_auth_history sa
    on sa.hk_l_user_group_activity = lu.hk_l_user_group_activity and event = 'add'
    where hg.group_id in (select group_id from STV2024111143__DWH.h_groups order by registration_dt asc limit 10)
    group by hg.hk_group_id
),
user_group_messages as (
    select 
    hg.hk_group_id as hk_group_id,
    count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
    from STV2024111143__DWH.h_users hu
    inner join STV2024111143__DWH.l_user_group_activity luga 
    on luga.hk_user_id = hu.hk_user_id
    inner join STV2024111143__DWH.h_groups hg 
    on hg.hk_group_id = luga.hk_group_id
    inner join STV2024111143__DWH.s_dialog_info sd
    on hu.user_id = sd.mesage_to or hu.user_id = sd.message_from
    group by hg.hk_group_id
)
select  
ugl.hk_group_id,
cnt_added_users,
cnt_users_in_group_with_messages,
cnt_users_in_group_with_messages /ugl.cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc 