/* *********    backup all tables with pre_prod records.
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-02-01  |
| process_date=2020-03-01  |
| process_date=2020-04-01  |
+--------------------------+--+
*/

create table inv_aum_raw_qa.20200515_bkp_test_dpl_actran like inv_aum_raw_qa.test_dpl_actran;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_gngnco like inv_aum_raw_qa.test_dpl_gngnco;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_mfclac like inv_aum_raw_qa.test_dpl_mfclac;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_mfacct like inv_aum_raw_qa.test_dpl_mfacct;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_mfrr   like inv_aum_raw_qa.test_dpl_mfrr;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_esmgr  like inv_aum_raw_qa.test_dpl_esmgr;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_acpos  like inv_aum_raw_qa.test_dpl_acpos;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_gnexch like inv_aum_raw_qa.test_dpl_gnexch;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_tpkptl like inv_aum_raw_qa.test_dpl_tpkptl;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_mfclcl like inv_aum_raw_qa.test_dpl_mfclcl;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_mfsc   like inv_aum_raw_qa.test_dpl_mfsc;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_tpcont like inv_aum_raw_qa.test_dpl_tpcont;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_mfbr   like inv_aum_raw_qa.test_dpl_mfbr;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_mfcl   like inv_aum_raw_qa.test_dpl_mfcl;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_mfrrus like inv_aum_raw_qa.test_dpl_mfrrus;
create table inv_aum_raw_qa.20200515_bkp_test_dpl_gncode like inv_aum_raw_qa.test_dpl_gncode;



insert into inv_aum_raw_qa.20200515_bkp_test_dpl_actran partition(process_date) select * from inv_aum_raw_qa.test_dpl_actran;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_gngnco partition(process_date) select * from inv_aum_raw_qa.test_dpl_gngnco;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_mfclac partition(process_date) select * from inv_aum_raw_qa.test_dpl_mfclac;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_mfacct partition(process_date) select * from inv_aum_raw_qa.test_dpl_mfacct;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_mfrr   partition(process_date) select * from inv_aum_raw_qa.test_dpl_mfrr;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_esmgr  partition(process_date) select * from inv_aum_raw_qa.test_dpl_esmgr;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_acpos  partition(process_date) select * from inv_aum_raw_qa.test_dpl_acpos;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_gnexch partition(process_date) select * from inv_aum_raw_qa.test_dpl_gnexch;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_tpkptl partition(process_date) select * from inv_aum_raw_qa.test_dpl_tpkptl;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_mfclcl partition(process_date) select * from inv_aum_raw_qa.test_dpl_mfclcl;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_mfsc   partition(process_date) select * from inv_aum_raw_qa.test_dpl_mfsc;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_tpcont partition(process_date) select * from inv_aum_raw_qa.test_dpl_tpcont;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_mfbr   partition(process_date) select * from inv_aum_raw_qa.test_dpl_mfbr;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_mfcl   partition(process_date) select * from inv_aum_raw_qa.test_dpl_mfcl;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_mfrrus partition(process_date) select * from inv_aum_raw_qa.test_dpl_mfrrus;
insert into inv_aum_raw_qa.20200515_bkp_test_dpl_gncode partition(process_date) select * from inv_aum_raw_qa.test_dpl_gncode;


select  '01_dpl_actran' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_actran) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_actran) as o
union all
select  '02_dpl_gngnco' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_gngnco) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_gngnco) as o
union all
select  '03_dpl_mfclac' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfclac) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfclac) as o
union all
select  '04_dpl_mfacct' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfacct) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfacct) as o
union all
select  '05_dpl_mfrr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfrr) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfrr) as o
union all
select  '06_dpl_esmgr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_esmgr) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_esmgr) as o
union all
select  '07_dpl_acpos' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_acpos) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_acpos) as o
union all
select  '08_dpl_gnexch' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_gnexch) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_gnexch) as o
union all
select  '09_dpl_tpkptl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_tpkptl) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_tpkptl) as o
union all
select  '10_dpl_mfclcl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfclcl) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfclcl) as o
union all
select  '11_dpl_mfsc' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfsc) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfsc) as o
union all
select  '12_dpl_tpcont' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_tpcont) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_tpcont) as o
union all
select  '13_dpl_mfbr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfbr) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfbr) as o
union all
select  '14_dpl_mfcl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfcl) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfcl) as o
union all
select  '15_dpl_mfrrus' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfrrus) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfrrus) as o
union all
select  '16_dpl_gncode' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_gncode) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_gncode) as o;
+----------------+---------------+-----------------+------------+--+
|  u1.tablename  | u1.backup_rc  | u1.original_cn  | u1.result  |
+----------------+---------------+-----------------+------------+--+
| 01_dpl_actran  | 238322        | 238322          | PASS       |
| 10_dpl_mfclcl  | 94            | 94              | PASS       |
| 11_dpl_mfsc    | 2270135       | 2270135         | PASS       |
| 12_dpl_tpcont  | 43328         | 43328           | PASS       |
| 13_dpl_mfbr    | 1958          | 1958            | PASS       |
| 14_dpl_mfcl    | 1349604       | 1349604         | PASS       |
| 15_dpl_mfrrus  | 27            | 27              | PASS       |
| 16_dpl_gncode  | 1923721       | 1923721         | PASS       |
| 02_dpl_gngnco  | 5             | 5               | PASS       |
| 03_dpl_mfclac  | 2112508       | 2112508         | PASS       |
| 04_dpl_mfacct  | 425           | 425             | PASS       |
| 05_dpl_mfrr    | 22432         | 22432           | PASS       |
| 06_dpl_esmgr   | 1426          | 1426            | PASS       |
| 07_dpl_acpos   | 1975767       | 1975767         | PASS       |
| 08_dpl_gnexch  | 14            | 14              | PASS       |
| 09_dpl_tpkptl  | 77865         | 77865           | PASS       |
+----------------+---------------+-----------------+------------+--+
16 rows selected (31.13 seconds)


        
select  '01_dpl_actran' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_actran) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_actran) as o
union all
select  '02_dpl_gngnco' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_gngnco) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_gngnco) as o
union all
select  '03_dpl_mfclac' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfclac) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfclac) as o
union all
select  '04_dpl_mfacct' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfacct) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfacct) as o
union all
select  '05_dpl_mfrr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfrr) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfrr) as o
union all
select  '06_dpl_esmgr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_esmgr) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_esmgr) as o
union all
select  '07_dpl_acpos' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_acpos) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_acpos) as o
union all
select  '08_dpl_gnexch' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_gnexch) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_gnexch) as o
union all
select  '09_dpl_tpkptl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_tpkptl) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_tpkptl) as o
union all
select  '10_dpl_mfclcl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfclcl) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfclcl) as o
union all
select  '11_dpl_mfsc' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfsc) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfsc) as o
union all
select  '12_dpl_tpcont' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_tpcont) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_tpcont) as o
union all
select  '13_dpl_mfbr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfbr) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfbr) as o
union all
select  '14_dpl_mfcl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfcl) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfcl) as o
union all
select  '15_dpl_mfrrus' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_mfrrus) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_mfrrus) as o
union all
select  '16_dpl_gncode' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_raw_qa.20200515_bkp_test_dpl_gncode) as t,
        (select count(*) as cn from inv_aum_raw_qa.test_dpl_gncode) as o;

--show partitions --typed
show partitions inv_aum_typed_qa.dpl_actran;
show partitions inv_aum_typed_qa.dpl_gngnco;
show partitions inv_aum_typed_qa.dpl_mfclac;
show partitions inv_aum_typed_qa.dpl_mfacct;
show partitions inv_aum_typed_qa.dpl_mfrr;
show partitions inv_aum_typed_qa.dpl_esmgr;
show partitions inv_aum_typed_qa.dpl_acpos;
show partitions inv_aum_typed_qa.dpl_gnexch;
show partitions inv_aum_typed_qa.dpl_tpkptl;
show partitions inv_aum_typed_qa.dpl_mfclcl;
show partitions inv_aum_typed_qa.dpl_mfsc;
show partitions inv_aum_typed_qa.dpl_tpcont;
show partitions inv_aum_typed_qa.dpl_mfbr;
show partitions inv_aum_typed_qa.dpl_mfcl;
show partitions inv_aum_typed_qa.dpl_mfrrus;
show partitions inv_aum_typed_qa.dpl_gncode;
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_actran;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.482 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_gngnco;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.466 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_mfclac;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
| process_date=2020-05-12  |
+--------------------------+--+
2 rows selected (0.468 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_mfacct;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.509 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_mfrr;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
| process_date=2020-05-12  |
+--------------------------+--+
2 rows selected (0.472 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_esmgr;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.484 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_acpos;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.466 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_gnexch;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.476 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_tpkptl;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.481 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_mfclcl;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.477 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_mfsc;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.474 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_tpcont;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.488 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_mfbr;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.486 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_mfcl;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.508 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_mfrrus;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.506 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli> show partitions inv_aum_typed_qa.dpl_gncode;
Getting log thread is interrupted, since query is done!
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-05-11  |
+--------------------------+--+
1 row selected (0.52 seconds)
0: jdbc:hive2://azcedlmstv001.v01caedl.manuli>

--show partitions - Raw
show partitions inv_aum_raw_qa.test_dpl_actran;
show partitions inv_aum_raw_qa.test_dpl_gngnco;
show partitions inv_aum_raw_qa.test_dpl_mfclac;
show partitions inv_aum_raw_qa.test_dpl_mfacct;
show partitions inv_aum_raw_qa.test_dpl_mfrr;
show partitions inv_aum_raw_qa.test_dpl_esmgr;
show partitions inv_aum_raw_qa.test_dpl_acpos;
show partitions inv_aum_raw_qa.test_dpl_gnexch;
show partitions inv_aum_raw_qa.test_dpl_tpkptl;
show partitions inv_aum_raw_qa.test_dpl_mfclcl;
show partitions inv_aum_raw_qa.test_dpl_mfsc;
show partitions inv_aum_raw_qa.test_dpl_tpcont;
show partitions inv_aum_raw_qa.test_dpl_mfbr;
show partitions inv_aum_raw_qa.test_dpl_mfcl;
show partitions inv_aum_raw_qa.test_dpl_mfrrus;
show partitions inv_aum_raw_qa.test_dpl_gncode;

--**************************** Typed tables
/* *********    backup all tables with pre_prod records.
+--------------------------+--+
|        partition         |
+--------------------------+--+
| process_date=2020-02-01  |
| process_date=2020-03-01  |
| process_date=2020-04-01  |
+--------------------------+--+
*/
set hivevar:t_process_date = '2020-04-01';

alter   table   inv_aum_raw_qa.test_dpl_actran    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_gngnco    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfclac    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfacct    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfrr      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_esmgr     drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_acpos     drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_gnexch    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_tpkptl    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfclcl    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfsc      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_tpcont    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfbr      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfcl      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_mfrrus    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_raw_qa.test_dpl_gncode    drop partition   (process_date = ${t_process_date});

analyze table   inv_aum_raw_qa.test_dpl_actran    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_gngnco    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfclac    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfacct    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfrr      partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_esmgr     partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_acpos     partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_gnexch    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_tpkptl    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfclcl    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfsc      partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_tpcont    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfbr      partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfcl      partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_mfrrus    partition       (process_date) compute statistics;
analyze table   inv_aum_raw_qa.test_dpl_gncode    partition       (process_date) compute statistics;
