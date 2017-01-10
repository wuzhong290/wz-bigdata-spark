#! /bin/sh

source /etc/profile
source $HOME/.bashrc
SQL="create external table if not exists src3 (
key int,
value string
)
PARTITIONED BY (date_id bigint)
row format delimited fields terminated by '\t'
stored as textfile
location '/test/src3'
"
echo $SQL
spark-submit --class com.yoho.recom.yohobuy.basic.AddCartInfoGlobal --master local --executor-memory 1G --total-executor-cores 1 /var/lib/hadoop-hdfs/yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar local "$SQL"
#spark-submit --class com.yoho.recom.yohobuy.basic.AddCartInfoGlobal --master spark://172.31.54.156:7077 --executor-memory 1G --total-executor-cores 1 /home/hadoop/yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar spark://172.31.54.156:7077 "$SQL"


SQL1="insert overwrite table src3 partition (date_id =20161220) select key,value from src where date_id=20161221"

echo $SQL1

spark-submit --class com.yoho.recom.yohobuy.basic.AddCartInfoGlobal --master local --executor-memory 1G --total-executor-cores 1 /var/lib/hadoop-hdfs/yoho-bigdata-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar local "$SQL1"


