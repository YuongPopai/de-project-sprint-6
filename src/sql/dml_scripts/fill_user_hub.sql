INSERT INTO STV2024111143__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(user_id),
       user_id,
       datetime,
       now(),
       's3'
       from STV2024111143__STAGING.group_log 
	   where hash(user_id) not in (select hk_user_id from STV2024111143__DWH.h_users);