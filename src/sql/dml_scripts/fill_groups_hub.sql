INSERT INTO STV2024111143__DWH.h_groups (hk_group_id ,group_id,registration_dt,load_dt,load_src)
select
       hash(group_id),
       group_id,
       datetime,
       now(),
       's3'
       from STV2024111143__STAGING.group_log 
	   where hash(group_id) not in (select hk_group_id from STV2024111143__DWH.h_groups);