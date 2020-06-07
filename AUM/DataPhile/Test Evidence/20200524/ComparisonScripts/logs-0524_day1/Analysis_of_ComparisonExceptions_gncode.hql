select  source,topic,filename,user_name,hdfs_location,process_timestamp,record_count,status,process_endtime,table_name

select  source,process_timestamp,process_endtime
from    inv_aum_typed_qa.registry 
where   source = 'dpl' 
and     to_date(process_timestamp) = cast('2020-05-24' as date);
+---------+---------------------+------------------------+--------------+---------------------------+------------------------+---------------+----------+------------------------+-------------+--+
| source  |        topic        |        filename        |  user_name   |       hdfs_location       |   process_timestamp    | record_count  |  status  |    process_endtime     | table_name  |
+---------+---------------------+------------------------+--------------+---------------------------+------------------------+---------------+----------+------------------------+-------------+--+
| dpl     | DPL-DAILY-Workflow  | DPL_Load_20200502.txt  | invaumsvcqa  | /apps/inv_qa/aum/raw/dpl  | 2020-05-24 14:30:47.0  | 1             | SUCCESS  | 2020-05-24 16:06:00.0  |             |
+---------+---------------------+------------------------+--------------+---------------------------+------------------------+---------------+----------+------------------------+-------------+--+
1 row selected (20.146 seconds)

--Above query and result shows the ingestion job was completed @16:06 (utc time zone) of May 24. EST time is 12:06 pm.

--Below INV_TYPED queries and resuts shows the records were updated during the wf ingestion. 
--Sqoop was completed for gncode at below times (refer to sqoop logs):
    --MSI:      20/05/24 10:51:22
    --MSISI:    20/05/24 11:09:25
    --MSII:     20/05/24 11:20:53

--Comments: the 8 records are mismatch between INV_TYPED table and test table. Reason cuased the mismatch is because those records are updated between sqoop and Ingestion workflow.
--
select
  code
, code_value
, last_dt
, descr
, subsystem
, last_time
, last_usr
, table_name
, dec_value
, broker_code
from    inv_aum_typed_qa.dpl_gncode 
where   process_date='2020-05-24'
and     code in ('BISTL_EXC2','BISTL_EXC2SM') and table_name = 'CTRL';
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
|     code      | code_value  |   last_dt   |                                  descr                                   | subsystem  | last_time  |  last_usr  | table_name  | dec_value  | broker_code  |
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
| BISTL_EXC2    |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 28),(0% of current BI chain of 14).  |            | 11:42:02   | bi-ctrl.p  | CTRL        | 0          | MSII         |
| BISTL_EXC2SM  |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 11:42:02   | bi-ctrl.p  | CTRL        | 0          | MSII         |
| BISTL_EXC2    |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 45),(0% of current BI chain of 14).  |            | 11:42:02   | bi-ctrl.p  | CTRL        | 0          | MSISI        |
| BISTL_EXC2SM  |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 11:42:02   | bi-ctrl.p  | CTRL        | 0          | MSISI        |
| BISTL_EXC2    |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 45),(0% of current BI chain of 14).  |            | 11:42:01   | bi-ctrl.p  | CTRL        | 0          | MSI          |
| BISTL_EXC2SM  |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 11:42:01   | bi-ctrl.p  | CTRL        | 0          | MSI          |
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
6 rows selected (18.448 seconds)



select
  code
, code_value
, last_dt
, descr
, subsystem
, last_time
, last_usr
, table_name
, dec_value
, broker_code
from    inv_aum_raw_qa.test_dpl_gncode 
where   process_date='2020-05-02'
and     code in ('BISTL_EXC2','BISTL_EXC2SM') and table_name = 'CTRL';
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
|     code      | code_value  |   last_dt   |                                  descr                                   | subsystem  | last_time  |  last_usr  | table_name  | dec_value  | broker_code  |
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
| BISTL_EXC2    |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 45),(0% of current BI chain of 14).  |            | 11:08:01   | bi-ctrl.p  | CTRL        | 0          | MSISI        |
| BISTL_EXC2SM  |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 11:08:01   | bi-ctrl.p  | CTRL        | 0          | MSISI        |
| BISTL_EXC2    |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 45),(0% of current BI chain of 14).  |            | 10:50:01   | bi-ctrl.p  | CTRL        | 0          | MSI          |
| BISTL_EXC2SM  |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 10:50:01   | bi-ctrl.p  | CTRL        | 0          | MSI          |
| BISTL_EXC2    |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 28),(0% of current BI chain of 14).  |            | 11:20:02   | bi-ctrl.p  | CTRL        | 0          | MSII         |
| BISTL_EXC2SM  |             | 2020-05-24  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 11:20:02   | bi-ctrl.p  | CTRL        | 0          | MSII         |
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
6 rows selected (9.32 seconds)
