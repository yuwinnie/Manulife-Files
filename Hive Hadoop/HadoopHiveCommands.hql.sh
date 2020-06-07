Hadoop command:

hadoop dfs -copyToLocal /apps/inv_qa/aum/code/curation/oozie/sdh-workflow.xml /data-01/inv_landing_qa/aum

--remove files when there is space in file name, use single quote.
hdfs dfs -rm /apps/inv_qa/aum/raw/archive/jhi/trowe_daily_assets/'*'

--View a file
hdfs dfs -cat /apps/inv_qa/aum/raw/archive/jhi/trowe_daily_assets/

--change name
hdfs dfs -mv /apps/inv_qa/aum/code/ingestion/oozie/wf_dpl_daily.xml /apps/inv_qa/aum/code/ingestion/oozie/wf_dpl_daily_may5.xml

--download file to current location
hdfs dfs -get /apps/inv_qa/aum/raw/typed/load_bd_dis/bd_distributor.csv
wc -l <file>

--upload files to HDFS
hadoop fs -put /<local machime path> /<hdfs path>

hdfs dfs -rm /apps/inv_qa/aum/code/curation/hql/load_bf_investor_holding_jhisdh.hql
hdfs dfs -rm /apps/inv_qa/aum/code/curation/hql/load_bf_investor_transaction_jhisdh.hql

hdfs dfs -put load_bf_investor_holding_jhisdh.hql /apps/inv_qa/aum/code/curation/hql
hdfs dfs -put load_bf_investor_transaction_jhisdh.hql /apps/inv_qa/aum/code/curation/hql

               load_bf_source_of_funds_jhisdh.hql
/              load_bf_investor_holding_jhisdh.hql
               load_bf_investor_transaction_jhisdh.hql
--Change owner
hadoop fs -chown invaumsvcqa /apps/inv_qa/aum/code/ingestion/oozie/sdh/config/sdh_last_change_ts_daily.conf

--HIVE Commands

use <schema>;
show locks <table>; 

desc <schema>.<table>;
show partitions <schema>.<table>; --to display all partitions values 

describe formatted table_name; 
--backup and insert
create table inv_curated_eod_qa.20200227_bkp_SDH_bf_source_of_funds like inv_curated_eod_qa.bf_source_of_funds;
insert into inv_curated_eod_qa.20200227_bkp_SDH_bf_source_of_funds partition(effective_dt,src) select * from inv_curated_eod_qa.bf_source_of_funds where src = 'JHISDH';

--Check backup tables
select  'bf_source_of_funds' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_curated_eod_qa.20200227_bkp_SDH_bf_source_of_funds) as t,
        (select count(*) as cn from inv_curated_eod_qa.bf_source_of_funds where src = 'JHISDH') as o;

--***********Manage Patitions***********
--How to drop partitions

    --external table, you have to drop the partition and then delete the data file manuly;
Alter table inv_jhi_typed_qa.jhi_trowe_daily_assets  drop if exists partition (process_date='2020-02-14');
hdfs dfs -rm -r /apps/inv_qa/aum/raw/typed/jhi_trowe_daily_assets/process_date=2020-02-14
    --* the path can be found when describe formatted table.

    --managed table (internal table)
Alter table inv_jhi_typed_qa.jhi_trowe_daily_assets  drop partition (process_date='2020-02-14');

--add new partitions
alter table inv_jhi_raw_qa.test_sdh_asset_position add partition (process_timestamp='20200325_100000');
analyze table inv_jhi_raw_qa.test_sdh_asset_position partition (process_timestamp) compute statistics;


--Use the below command to know all the partitions for your data.

hadoop fs -ls -R /apps/hive/warehouse/inv_curated_eod_qa.db/bf_source_of_funds/* | grep 'JHISDH';     

/apps/hive/warehouse/inv_curated_eod_qa.db/bf_source_of_funds/effective_dt=2021-11-03/src=HSBC


                                                 */
        -- Use your src/ table name accordingly

2	
use inv_curated_eod_qa; -- use your DB
alter table bf_source_of_funds drop if exists partition(src='JHISDH', effective_dt='2019-10-22'); 
        -- Here src and effective_dt are the partition columns



--***********Manage Patitions***********END/
--***********Manage Patitions***********




--date and time functions
unix_timestamp(concat(s.trade_dt,' 00:00:00')) - unix_timestamp(d.bus_eff_date_from) >=  0 --compare 2 timastamp

--nvl and coalesce
coalesce(value1, value2, …valuen) --This will return the first value that is not NULL, or NULL if all values's are NULL

use <schema>;
show locks <table>;