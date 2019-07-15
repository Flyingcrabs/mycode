Insert into table qfbap_dwd.dwd_code_category 
select 
first_category_id,
first_category_name,
second_category_id,
second_catery_name,
third_category_id,
third_category_name,
category_id,
 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
FROM qfbap_ods.ods_code_category

Insert into table qfbap_dwd.dwd_user_addr 
select 
*,
 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
FROM qfbap_ods.ods_user_addr

Insert into table qfbap_dwd.dwd_user_extend
select 
*,
 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
FROM qfbap_ods.ods_user_extend


Insert into table qfbap_dwd.dwd_user
select 
user_id    ,
  user_name   ,
  user_gender  ,
  user_birthday  ,
  user_age    ,
  constellation   ,
  province  ,
  city   ,
  city_level   ,
  e_mail   ,
  op_mail  ,
  mobile    ,
  num_seg_mobile    ,
  op_Mobile  ,
  register_time  ,
  login_ip ,
  login_source   ,
  request_user  ,
  total_score  ,
  used_score  ,
  is_blacklist   ,
  is_married ,
  education   ,
  monthly_income  ,
  profession   ,
 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
FROM qfbap_ods.ods_user


Insert overwrite table qfbap_dwd.dwd_order_delivery partition(dt='20190709')
select 
order_id,
order_no ,
 consignee,
 area_id,
 area_name,
 address,
 mobile,
 phone,
 coupon_id,
 coupon_money,
 carriage_money,
 create_time,
 update_time,
 addr_id,
 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
FROM qfbap_ods.ods_order_delivery


Insert overwrite table qfbap_dwd.dwd_us_order partition(dt='20190709')
select 
 order_id,
  order_no   ,
  order_date ,
  user_id   ,
  user_name  ,
  order_money   ,
  order_type   ,
  order_status  ,
  pay_status   ,
  pay_type  ,
  order_source  ,
  update_time   ,
 from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
FROM qfbap_ods.ods_us_order



Insert overwrite table qfbap_dwd.dwd_order_item partition(dt='20190709')
select 
user_id,
order_id,
order_no,
goods_id,
goods_no,
goods_name,
goods_amount,  
shop_id,
shop_name,
curr_price,
market_price,
discount,
cost_price,
first_cart,
first_cart_name,
second_cart,
second_cart_name,
third_cart,
third_cart_name,
goods_desc,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
FROM qfbap_ods.ods_order_item

insert overwrite table qfbap_dwd.dwd_user_pc_pv partition(dt=20190708)
select 
max(log_id),
user_id,
session_id,
cookie_id,
min(visit_time) in_time,
max(visit_time) out_time,
case when min(visit_time) = max(visit_time) then 3 else max(visit_time ) - min(visit_time)  end stay_time,
count(1) pv,
visit_os,
browser_name,
visit_ip,
province,
city,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') 
from
qfbap_ods.ods_user_pc_click_log
where dt=20190708
group by
user_id,
session_id,
cookie_id,
visit_os,
browser_name,
visit_ip,
province,
city;

INSERT overwrite TABLE  qfbap_dwd.dwd_user_app_pv partition(dt=20190708)
select
 log_id ,
  user_id  ,
  imei  ,
  log_time  ,
  hour(log_time)log_hour ,
  visit_os   ,
  os_version  ,
  app_name   ,
  app_version  ,
  device_token   ,
  visit_ip  ,
  province   ,
  city   ,
  from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') 
  from qfbap_ods.ods_user_app_click_log
  where dt=20190708

  