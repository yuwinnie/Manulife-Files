sqoop-import --connect 'jdbc:datadirect:openedge://10.201.170.28:1692;databaseName=exc2' --username root -P --driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' --query 'SELECT * FROM PUB.gngnco WHERE $CONDITIONS' --split-by 'fst_bal' --enclosed-by '"' --target-dir '/user/goyalvi/dataphile/out/gngnco' --relaxed-isolation
sqoop-eval --connect 'jdbc:datadirect:openedge://10.101.96.30:1693;databaseName=exc2sm' --username progress -P --driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' --query "SELECT count(*) from PUB.mfsc "  --relaxed-isolation
sqoop-eval --connect 'jdbc:datadirect:openedge://10.201.170.28:1692;databaseName=exc2' --username root -P --driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' --query 'SELECT count(*)  FROM PUB.gngnco' --relaxed-isolation
sqoop-eval --connect 'jdbc:datadirect:openedge://10.201.170.28:1892;databaseName=exc2' --username root -P --driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' --query 'SELECT count(*)  FROM PUB.gngnco' --relaxed-isolation

#################Create External Tables#############################
CREATE TABLE inv_aum_raw_qa.test_dpl_gngnco (
addr STRING
, city STRING
, pc STRING
, ph STRING
, prov STRING
, name STRING
, company STRING
, last_usr STRING
, last_dt DATE
, last_time STRING
, fscl_yrnd STRING
, rtnd_earn INT
, fst_bal INT
, lst_bal INT
, fst_pl INT
, lst_pl INT
, crnt_fscl INT
, short_name STRING
, gst_ref STRING
, db_name STRING
, cr_rpts_cur STRING
, post_delay INT
, primary_mkt STRING
, consoldt STRING
, stmt_on STRING
, fund_type STRING
, language STRING
, addr2 STRING
, broker_code STRING)
COMMENT 'Data from gngnco'
PARTITIONED BY (process_date DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ("field.delim" = "\u0001")
LOCATION '/apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gngnco/';

describe formatted inv_aum_raw_qa.test_dpl_gngnco;

alter   table   inv_aum_raw_qa.test_dpl_gngnco      add partition   (process_date = '2020-01-01');
analyze table   inv_aum_raw_qa.test_dpl_gngnco          partition   (process_date) compute statistics;
alter   table   inv_aum_raw_qa.test_dpl_gngnco     drop partition   (process_date = '2020-01-01');

select * from  inv_aum_raw_qa.test_dpl_gngnco a;
show partitions inv_aum_raw_qa.test_dpl_gngnco;
####################################################################
 
#################Create pwd alias#############################
hadoop credential create dpl.sqoop.password.alias -provider jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks
dpl.sqoop.password.alias

-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--password-alias dpl.sqoop.password.alias \
####################################################################

#################Sample sqoop script################################
sqoop import \
-Dmapreduce.job.queuename=inv \
-Dhadoop.security.credential.provider.path=jceks://hdfs/apps/inv_qa/aum/security/dpl.sqoop.password.jceks \
--connect "jdbc:datadirect:openedge:// 10.101.96.30:1692;databaseName=exc2" \
--username root \
--password-alias dpl.sqoop.password.alias \
--driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' \
--target-dir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gngnco/process_date=2020-01-01 \
--query "SELECT  a.*,'MSI' as brokder_code FROM  PUB.gngnco a  where \$CONDITIONS" \
--boundary-query "SELECT MIN(fst_bal), MAX(fst_bal) FROM PUB.gngnco WHERE fst_bal IN (SELECT fst_bal FROM PUB.gngnco)" \
--split-by fst_bal \
--hive-drop-import-delims \
--fields-terminated-by "\001" \
--null-string \\\\N \
--null-non-string \\\\N \
--temporary-rootdir /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/tmp/dataphile \
-m 10 &>${log_path}/gngnco.log \
--relaxed-isolation

while ! grep -q Retrieved ${log_path}/gngnco.log; do echo -n ".";sleep 10; done
tail -n1 ${log_path}/gngnco.log
echo "[Done]"

##change /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_gngnco to 755.
####################################################################

#################Remove Partitions##################################
hdfs dfs -rm -r /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_actran/process_date=2020-01-02
hdfs dfs -rm -r /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfrr/process_date=2020-01-02

select count(*) from  inv_aum_raw_qa.test_dpl_tpkptl a;
show partitions inv_aum_raw_qa.test_dpl_tpkptl;

hdfs dfs -rm -r /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_mfclac/
hdfs dfs -ls /apps/inv_qa/aum/raw/qa_test/dataphile/16_tables/test_dpl_acpos
####################################################################
#################Create External Tables#############################
sqoop-eval --connect 'jdbc:datadirect:openedge://10.101.96.30:1692;databaseName=exc2' --username root -P --driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' --query "SELECT count(*) from PUB.mfclac"  --relaxed-isolation
sqoop-eval --connect 'jdbc:datadirect:openedge://10.101.96.30:1692;databaseName=exc2' --username root -P --driver 'com.ddtek.jdbc.openedge.OpenEdgeDriver' --query "SELECT count(*) from PUB.tpkptl"  --relaxed-isolation
####################################################################