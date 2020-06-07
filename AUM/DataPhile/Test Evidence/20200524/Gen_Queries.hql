--Registry
select  *
from    inv_aum_typed_qa.registry 
where   source = 'dpl' 
and     to_date(process_timestamp) = cast('2020-05-24' as date);
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
| registry.source  |   registry.topic    |   registry.filename    | registry.user_name  |  registry.hdfs_location   | registry.process_timestamp  | registry.record_count  | registry.status  | registry.process_endtime  | registry.table_name  |
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200502.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-24 14:30:47.0       | 1                      | SUCCESS          | 2020-05-24 16:06:00.0     |                      |
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
1 row selected (19.725 seconds)

select  *
from    inv_aum_typed_qa.registry 
where   source = 'dpl' 
and     to_date(process_timestamp) = cast('2020-05-17' as date);
order by process_timestamp;

+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
| registry.source  |   registry.topic    |   registry.filename    | registry.user_name  |  registry.hdfs_location   | registry.process_timestamp  | registry.record_count  | registry.status  | registry.process_endtime  | registry.table_name  |
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200415.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-04-30 15:27:44.0       | 1                      | SUCCESS          | 2020-04-30 16:28:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200511.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-11 20:18:46.0       | 1                      | SUCCESS          | 2020-05-11 21:00:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200501.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-15 21:09:23.0       | 1                      | SUCCESS          | 2020-05-17 14:20:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200511.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-11 17:01:22.0       | 1                      | SUCCESS          | 2020-05-11 18:02:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200501.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-06 00:30:53.0       | 1                      | SUCCESS          | 2020-05-06 01:39:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200501.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-15 21:09:23.0       | 1                      | SUCCESS          | 2020-05-17 15:09:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200414.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-04-29 20:16:47.0       | 1                      | SUCCESS          | 2020-04-29 20:52:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200501.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-15 21:09:23.0       | 1                      | SUCCESS          | 2020-05-15 22:27:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200501.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-17 17:22:41.0       | 1                      | SUCCESS          | 2020-05-17 17:58:00.0     |                      |
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200502.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-24 14:30:47.0       | 1                      | SUCCESS          | 2020-05-24 16:06:00.0     |                      |
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
10 rows selected (9.089 seconds)

select  source,process_timestamp,process_endtime
from    inv_aum_typed_qa.registry 
where   source = 'dpl' 
and     to_date(process_timestamp) = cast('2020-05-17' as date);

+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
| registry.source  |   registry.topic    |   registry.filename    | registry.user_name  |  registry.hdfs_location   | registry.process_timestamp  | registry.record_count  | registry.status  | registry.process_endtime  | registry.table_name  |
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
| dpl              | DPL-DAILY-Workflow  | DPL_Load_20200501.txt  | invaumsvcqa         | /apps/inv_qa/aum/raw/dpl  | 2020-05-17 17:22:41.0       | 1                      | SUCCESS          | 2020-05-17 17:58:00.0     |                      |
+------------------+---------------------+------------------------+---------------------+---------------------------+-----------------------------+------------------------+------------------+---------------------------+----------------------+--+
1 row selected (26.872 seconds)

