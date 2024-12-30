create table STV2024111143__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null references STV2024111143__DWH.l_user_group_activity(hk_l_user_group_activity),
user_id_from int,
event varchar(20),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by event_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2); 