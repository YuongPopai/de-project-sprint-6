create table STV2024111143__STAGING.group_log
(
    group_id int,
    user_id int,
    user_id_from int,
    event varchar(100),
    datetime datetime,
    constraint pk_group primary key(group_id, user_id)
);

