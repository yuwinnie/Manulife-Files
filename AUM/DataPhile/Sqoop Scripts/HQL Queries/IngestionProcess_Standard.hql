1) change date
​2) echo "last_load_date=2020-01-01" | hadoop fs -put - /apps/inv_qa/aum/code/ingestion/oozie/dpl/trigger/dpl_daily_trigger.dat
​3) run workflow
​Vijay Sharma: 4379950482

--Alter table 

hdfs dfs -cat /apps/inv_qa/aum/code/ingestion/oozie/dpl/hql/alter_tables_dpl_daily.hql
hdfs dfs -rm /apps/inv_qa/aum/code/ingestion/oozie/dpl/hql/alter_tables_dpl_daily.hql
hdfs dfs -put alter_tables_dpl_daily.hql /apps/inv_qa/aum/code/ingestion/oozie/dpl/hql/alter_tables_dpl_daily.hql


--#####Modify dpl_done.txt file
hdfs dfs  -rm -skipTrash /apps/inv_qa/aum/code/ingestion/oozie/dpl/shell/dpl_done.txt
echo "last_load_date=2019-12-01" | hadoop fs -put - /apps/inv_qa/aum/code/ingestion/oozie/dpl/shell/dpl_done.txt
hdfs dfs  -cat /apps/inv_qa/aum/code/ingestion/oozie/dpl/shell/dpl_done.txt

--##### clean raw/typed tables
hdfs dfs -rm -r -skipTrash /apps/inv_qa/aum/raw/dpl/*                                                                           */
cd /data-01/apps/inv_qa/aum/code/ingestion/ddl/
sh ddl_shell.sh dpl_all_ddl/raw_ddl.hql
sh ddl_shell.sh dpl_all_ddl/typed_ddl.hql

--#####clean test tables

--Trigger file:
cd /data-01/inv_landing_qa/aum/dpl/
>DPL_Load_20200413.txt

--Start Workflow  for DPL ingestion:
cd /data-01/apps/inv_qa/aum/code/ingestion/shell
sh runcoordwf.sh wf dpl_daily run

/data-01/apps/inv_qa/aum/code/ingestion/config