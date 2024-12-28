INSERT INTO STV2024111143__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV2024111143__STAGING.group_log as g
left join STV2024111143__DWH.h_users as hu on g.user_id = hu.user_id
left join STV2024111143__DWH.h_groups as hg on g.group_id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_user_group_activity from STV2024111143__DWH.l_user_group_activity);