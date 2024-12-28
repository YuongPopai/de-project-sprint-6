insert into STV2024111143__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt,load_dt,load_src)
select 
lu.hk_l_user_group_activity,
g.user_id_from,
g.event,
g.datetime,
now(),
's3'
from STV2024111143__STAGING.group_log g
left  join STV2024111143__DWH.h_users hu
on g.user_id = hu.user_id 
left  join STV2024111143__DWH.h_groups hg
on hg.group_id = g.group_id 
left  join STV2024111143__DWH.l_user_group_activity lu
on lu.hk_group_id = hg.hk_group_id and hu.hk_user_id = lu.hk_user_id 