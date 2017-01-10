#! /bin/sh

source /etc/profile
source $HOME/.bashrc
SQL="create external table if not exists recom_yohobuy.add_cart_info_global(
 browser_uniq_id string,
 user_log_acct bigint,
 product_skn int,
 brand_id int,
 small_sort_id int,
 gender int,
 time_stamp_click bigint
)
partitioned by (date_id bigint)
row format delimited fields terminated by '\t'
stored as textfile
location '/data/yoho/recom_yohobuy/add_cart_info_global'
"
echo $SQL
spark-submit --class com.yoho.recom.yohobuy.basic.AddCartInfoGlobal --master local --executor-memory 1G --total-executor-cores 1 /var/lib/hadoop-hdfs/yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar local "$SQL"
#spark-submit --class com.yoho.recom.yohobuy.basic.AddCartInfoGlobal --master spark://172.31.54.156:7077 --executor-memory 1G --total-executor-cores 1 /home/hadoop/yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar spark://172.31.54.156:7077 "$SQL"

date_id=20161219
SQL1="
insert overwrite table recom_yohobuy.add_cart_info_global partition (date_id = $date_id)
select a.browser_uniq_id as browser_uniq_id,
 a.user_log_acct as user_log_acct,
 c.product_skn as product_skn,
 c.brand_id as brand_id,
 c.sort_three as small_sort_id,
 c.gender as gender,
 a.time_stamp_click as time_stamp_click
from
 (select browser_uniq_id,
   max(user_log_acct) as user_log_acct,
   product_skc,
   min(time_stamp_click) as time_stamp_click
  from
   (select a.udid as browser_uniq_id,
     a.uid as user_log_acct,
     cast(get_json_object(a.param,'$.PRD_SKC') as int) as product_skc,
     substr(a.time_stamp, 0, 10) as time_stamp_click
   from recom_yohobuy.fact_mobile_click as a
   where concat(year, month, day) = '$date_id'
     and oper_id='YB_GLOBAL_GDS_DT_BUY') a
  group by  browser_uniq_id,product_skc) a
join recom_yohobuy.tbl_product_skc as b
on a.product_skc = b.product_skc
 and b.date_id = $date_id
join recom_yohobuy.tbl_product as c
on b.product_skn = c.product_skn
 and c.date_id = $date_id
"

echo $SQL1

spark-submit --class com.yoho.recom.yohobuy.basic.AddCartInfoGlobal --master local --executor-memory 1G --total-executor-cores 1 /var/lib/hadoop-hdfs/yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar local "$SQL1"