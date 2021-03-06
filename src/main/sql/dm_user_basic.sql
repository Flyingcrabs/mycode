CREATE table if NOT EXISTS qfbap_dm.dm_user_basic
as
SELECT *
FROM
(
SELECT
user_id,
user_name,
user_gender,
user_birthday,
user_age,
constellation,
province,
city,
city_level,
e_mail,
op_mail,
mobile,
num_seg_mobile,
op_Mobile,
register_time,
login_ip,
login_source,
request_user,
total_score,
used_score,
is_blacklist,
is_married,
education,
monthly_income,
profession
FROM qfbap_dwd.dwd_user as a
JOIN
(
SELECT
user_id,
is_pregnant_woman,
is_have_children,
is_have_car,
phone_brand,
phone_brand_level,
phone_cnt,
change_phone_cnt,
is_maja,
majia_account_cnt,
loyal_model,
shopping_type_model,
weight,
height,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
FROM qfbap_dwd.dwd_user_extend
) b
ON b.user_id=a.user_id
) c
location '/qfbap/dm/dm_user_basic';