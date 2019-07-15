select t3.user_id ,t3.type,t3.cnt ,t3.content,row_number() over(partition by t3.user_id ,t3.type order by t3.cnt) as rn
FROM (
  select user_id,'visit_ip' as type, sum(pv) as cnt , visit_ip as content
  from qfbap_dwd.dwd_user_pc_pv
  group by user_id,visit_ip 
  
  
  union all
  
  select user_id,'浏览器' as type, sum(pv) as cnt ,browser_name  as content
  from qfbap_dwd.dwd_user_pc_pv
  group by user_id,browser_name 
  
  union all
  
  select user_id,'操作系统' as type, sum(pv) as cnt ,visit_os as content
  from qfbap_dwd.dwd_user_pc_pv
  group by user_id, visit_os 
  
  union all
  
  select user_id,'cookie_id' as type, sum(pv) as cnt ,cookie_id as content
  from qfbap_dwd.dwd_user_pc_pv
  group by user_id,cookie_id
  
) as t3 

