set hivevar:s_process_date = '2020-04-01';
set hivevar:t_process_date = '2020-05-11';


--Registry
select  source, topic,process_timestamp,status
select  *
from    inv_aum_typed_qa.registry 
where   source = 'dpl' 
order by process_timestamp desc;

and     to_date(process_timestamp) = cast('2020-05-11' as date);
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
| registry.source  |   registry.topic    |   registry.filename    | registry.user_name  |  registry.hdfs_location   | registry.process_timestamp  | registry.record_count  | registry.status  | registry.process_endtime  | registry.table_name  |
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200501.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-06 00:30:53.0       | 1                      | SUCCESS          | 2020-05-06 01:39:00.0     |                      |
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
1 row selected (8.589 seconds)

--error log
select  filename,
        message,
        process_timestamp
from    inv_aum_typed_qa.error_log
where   to_date(process_timestamp) = cast('2020-05-06' as date);
+------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------+--+
|        filename        |                                                                                 message                                                                                 |   process_timestamp    |
+------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------+--+
| DPL_Load_20200501.txt  | JA008: File does not exist: hdfs://azcedlmstv001.v01caedl.manulife.com:8020/apps/inv_qa/aum/code/ingestion/oozie/dpl/shell/dpl_create_done_ts.sh#dpl_create_done_ts.sh  | 2020-05-06 00:30:53.0  |
+------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------+--+
1 row selected (65.955 seconds)