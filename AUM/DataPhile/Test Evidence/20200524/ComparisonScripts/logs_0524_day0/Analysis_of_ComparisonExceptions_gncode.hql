select  source,topic,filename,user_name,hdfs_location,process_timestamp,record_count,status,process_endtime,table_name
from    inv_aum_typed_qa.registry 
where   source = 'dpl' 
and     to_date(process_timestamp) = cast('2020-05-17' as date);
+---------+---------------------+------------------------+--------------+---------------------------+------------------------+---------------+----------+------------------------+-------------+--+
| source  |        topic        |        filename        |  user_name   |       hdfs_location       |   process_timestamp    | record_count  |  status  |    process_endtime     | table_name  |
+---------+---------------------+------------------------+--------------+---------------------------+------------------------+---------------+----------+------------------------+-------------+--+
| dpl     | DPL-DAILY-Workflow  | DPL_Load_20200501.txt  | invaumsvcqa  | /apps/inv_qa/aum/raw/dpl  | 2020-05-17 17:22:41.0  | 1             | SUCCESS  | 2020-05-17 17:58:00.0  |             |
+---------+---------------------+------------------------+--------------+---------------------------+------------------------+---------------+----------+------------------------+-------------+--+

--Above query and result shows the ingestion job was completed @ 17:58 (utc time zone) of May 17. EST time is 13:38 pm.

--Below INV_TYPED queries and resuts shows the records were updated during the wf ingestion. 
--Sqoop was completed for gncode at below times (refer to sqoop logs):
    --MSI:      20/05/17 12:20:31
    --MSISI:    20/05/17 12:45:21
    --MSII:     20/05/17 12:57:56

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
where   process_date='2020-05-17'
and     code = 'BISTL_EXC2' and table_name = 'CTRL';
+-------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
|    code     | code_value  |   last_dt   |                                  descr                                   | subsystem  | last_time  |  last_usr  | table_name  | dec_value  | broker_code  |
+-------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
| BISTL_EXC2  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 28),(0% of current BI chain of 14).  |            | 13:30:03   | bi-ctrl.p  | CTRL        | 0          | MSII         |
| BISTL_EXC2  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 45),(0% of current BI chain of 14).  |            | 13:30:01   | bi-ctrl.p  | CTRL        | 0          | MSI          |
| BISTL_EXC2  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 45),(0% of current BI chain of 14).  |            | 13:31:01   | bi-ctrl.p  | CTRL        | 0          | MSISI        |
+-------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
3 rows selected (18.329 seconds)


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
from inv_aum_raw_qa.test_dpl_gncode where process_date='2020-05-01'
and     code = 'BISTL_EXC2' and table_name = 'CTRL';
+-------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
|    code     | code_value  |   last_dt   |                                  descr                                   | subsystem  | last_time  |  last_usr  | table_name  | dec_value  | broker_code  |
+-------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
| BISTL_EXC2  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 45),(0% of current BI chain of 14).  |            | 12:19:00   | bi-ctrl.p  | CTRL        | 0          | MSI          |
| BISTL_EXC2  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 45),(0% of current BI chain of 14).  |            | 12:44:01   | bi-ctrl.p  | CTRL        | 0          | MSISI        |
| BISTL_EXC2  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 28),(0% of current BI chain of 14).  |            | 12:57:01   | bi-ctrl.p  | CTRL        | 0          | MSII         |
+-------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
3 rows selected (17.62 seconds)


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
where   process_date='2020-05-17'
and     code = 'KYFLDA' and table_name = 'SEQ' and broker_code = 'MSI';

+---------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
|  code   | code_value  |   last_dt   | descr  | subsystem  | last_time  | last_usr  | table_name  | dec_value  | broker_code  |
+---------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
| KYFLDA  | 371808      | 2020-05-17  |        |            | 12:37:17   | lcharko   | SEQ         | 0          | MSI          |
+---------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
1 row selected (21.472 seconds)

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
from inv_aum_raw_qa.test_dpl_gncode where process_date='2020-05-01'
and     code = 'KYFLDA' and table_name = 'SEQ' and broker_code = 'MSI';
+---------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
|  code   | code_value  |   last_dt   | descr  | subsystem  | last_time  | last_usr  | table_name  | dec_value  | broker_code  |
+---------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
| KYFLDA  | 371807      | 2020-05-16  |        |            | 14:42:58   | dhenders  | SEQ         | 0          | MSI          |
+---------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
1 row selected (14.291 seconds)

--################################
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
where   process_date='2020-05-17'
and     code = 'BISTL_EXC2SM' and table_name = 'CTRL'
order by    broker_code;
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
|     code      | code_value  |   last_dt   |                                  descr                                   | subsystem  | last_time  |  last_usr  | table_name  | dec_value  | broker_code  |
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
| BISTL_EXC2SM  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 13:30:02   | bi-ctrl.p  | CTRL        | 0          | MSI          |
| BISTL_EXC2SM  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 13:30:04   | bi-ctrl.p  | CTRL        | 0          | MSII         |
| BISTL_EXC2SM  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 13:31:02   | bi-ctrl.p  | CTRL        | 0          | MSISI        |
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
3 rows selected (11.501 seconds)

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
where   process_date='2020-05-01'
and     code = 'BISTL_EXC2SM' and table_name = 'CTRL'
order by    broker_code;

+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
|     code      | code_value  |   last_dt   |                                  descr                                   | subsystem  | last_time  |  last_usr  | table_name  | dec_value  | broker_code  |
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
| BISTL_EXC2SM  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 12:19:01   | bi-ctrl.p  | CTRL        | 0          | MSI          |
| BISTL_EXC2SM  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 12:57:01   | bi-ctrl.p  | CTRL        | 0          | MSII         |
| BISTL_EXC2SM  |             | 2020-05-17  | 0 BI cluster(s) in use (0% of total 59),(0% of current BI chain of 14).  |            | 12:44:01   | bi-ctrl.p  | CTRL        | 0          | MSISI        |
+---------------+-------------+-------------+--------------------------------------------------------------------------+------------+------------+------------+-------------+------------+--------------+--+
3 rows selected (11.336 seconds)

--################################
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
where   process_date='2020-05-17'
and     code = 'ACCT_SEQ' and table_name = 'SEQ' and broker_code = 'MSI';
+-----------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
|   code    | code_value  |   last_dt   | descr  | subsystem  | last_time  | last_usr  | table_name  | dec_value  | broker_code  |
+-----------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
| ACCT_SEQ  |             | 2020-05-17  |        |            | 12:38:02   | lcharko   | SEQ         | 440862     | MSI          |
+-----------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
1 row selected (8.365 seconds)

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
from inv_aum_raw_qa.test_dpl_gncode where process_date='2020-05-01'
and     code = 'ACCT_SEQ' and table_name = 'SEQ' and broker_code = 'MSI';
+-----------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
|   code    | code_value  |   last_dt   | descr  | subsystem  | last_time  | last_usr  | table_name  | dec_value  | broker_code  |
+-----------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
| ACCT_SEQ  |             | 2020-05-16  |        |            | 14:58:10   | dhenders  | SEQ         | 440861     | MSI          |
+-----------+-------------+-------------+--------+------------+------------+-----------+-------------+------------+--------------+--+
1 row selected (10.489 seconds)
