create table inv_aum_typed_qa.20200513_bkp_dpl_actran like inv_aum_typed_qa.dpl_actran;
create table inv_aum_typed_qa.20200513_bkp_dpl_gngnco like inv_aum_typed_qa.dpl_gngnco;
create table inv_aum_typed_qa.20200513_bkp_dpl_mfclac like inv_aum_typed_qa.dpl_mfclac;
create table inv_aum_typed_qa.20200513_bkp_dpl_mfacct like inv_aum_typed_qa.dpl_mfacct;
create table inv_aum_typed_qa.20200513_bkp_dpl_mfrr   like inv_aum_typed_qa.dpl_mfrr;
create table inv_aum_typed_qa.20200513_bkp_dpl_esmgr  like inv_aum_typed_qa.dpl_esmgr;
create table inv_aum_typed_qa.20200513_bkp_dpl_acpos  like inv_aum_typed_qa.dpl_acpos;
create table inv_aum_typed_qa.20200513_bkp_dpl_gnexch like inv_aum_typed_qa.dpl_gnexch;
create table inv_aum_typed_qa.20200513_bkp_dpl_tpkptl like inv_aum_typed_qa.dpl_tpkptl;
create table inv_aum_typed_qa.20200513_bkp_dpl_mfclcl like inv_aum_typed_qa.dpl_mfclcl;
create table inv_aum_typed_qa.20200513_bkp_dpl_mfsc   like inv_aum_typed_qa.dpl_mfsc;
create table inv_aum_typed_qa.20200513_bkp_dpl_tpcont like inv_aum_typed_qa.dpl_tpcont;
create table inv_aum_typed_qa.20200513_bkp_dpl_mfbr   like inv_aum_typed_qa.dpl_mfbr;
create table inv_aum_typed_qa.20200513_bkp_dpl_mfcl   like inv_aum_typed_qa.dpl_mfcl;
create table inv_aum_typed_qa.20200513_bkp_dpl_mfrrus like inv_aum_typed_qa.dpl_mfrrus;
create table inv_aum_typed_qa.20200513_bkp_dpl_gncode like inv_aum_typed_qa.dpl_gncode;



insert into inv_aum_typed_qa.20200513_bkp_dpl_actran partition(process_date) select * from inv_aum_typed_qa.dpl_actran;
insert into inv_aum_typed_qa.20200513_bkp_dpl_gngnco partition(process_date) select * from inv_aum_typed_qa.dpl_gngnco;
insert into inv_aum_typed_qa.20200513_bkp_dpl_mfclac partition(process_date) select * from inv_aum_typed_qa.dpl_mfclac;
insert into inv_aum_typed_qa.20200513_bkp_dpl_mfacct partition(process_date) select * from inv_aum_typed_qa.dpl_mfacct;
insert into inv_aum_typed_qa.20200513_bkp_dpl_mfrr   partition(process_date) select * from inv_aum_typed_qa.dpl_mfrr;
insert into inv_aum_typed_qa.20200513_bkp_dpl_esmgr  partition(process_date) select * from inv_aum_typed_qa.dpl_esmgr;
insert into inv_aum_typed_qa.20200513_bkp_dpl_acpos  partition(process_date) select * from inv_aum_typed_qa.dpl_acpos;
insert into inv_aum_typed_qa.20200513_bkp_dpl_gnexch partition(process_date) select * from inv_aum_typed_qa.dpl_gnexch;
insert into inv_aum_typed_qa.20200513_bkp_dpl_tpkptl partition(process_date) select * from inv_aum_typed_qa.dpl_tpkptl;
insert into inv_aum_typed_qa.20200513_bkp_dpl_mfclcl partition(process_date) select * from inv_aum_typed_qa.dpl_mfclcl;
insert into inv_aum_typed_qa.20200513_bkp_dpl_mfsc   partition(process_date) select * from inv_aum_typed_qa.dpl_mfsc;
insert into inv_aum_typed_qa.20200513_bkp_dpl_tpcont partition(process_date) select * from inv_aum_typed_qa.dpl_tpcont;
insert into inv_aum_typed_qa.20200513_bkp_dpl_mfbr   partition(process_date) select * from inv_aum_typed_qa.dpl_mfbr;
insert into inv_aum_typed_qa.20200513_bkp_dpl_mfcl   partition(process_date) select * from inv_aum_typed_qa.dpl_mfcl;
insert into inv_aum_typed_qa.20200513_bkp_dpl_mfrrus partition(process_date) select * from inv_aum_typed_qa.dpl_mfrrus;
insert into inv_aum_typed_qa.20200513_bkp_dpl_gncode partition(process_date) select * from inv_aum_typed_qa.dpl_gncode;


select  '01_dpl_actran' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_actran) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_actran) as o
union all
select  '02_dpl_gngnco' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_gngnco) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_gngnco) as o
union all
select  '03_dpl_mfclac' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfclac) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfclac) as o
union all
select  '04_dpl_mfacct' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfacct) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfacct) as o
union all
select  '05_dpl_mfrr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfrr) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfrr) as o
union all
select  '06_dpl_esmgr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_esmgr) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_esmgr) as o
union all
select  '07_dpl_acpos' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_acpos) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_acpos) as o
union all
select  '08_dpl_gnexch' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_gnexch) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_gnexch) as o
union all
select  '09_dpl_tpkptl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_tpkptl) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_tpkptl) as o
union all
select  '10_dpl_mfclcl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfclcl) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfclcl) as o
union all
select  '11_dpl_mfsc' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfsc) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfsc) as o
union all
select  '12_dpl_tpcont' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_tpcont) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_tpcont) as o
union all
select  '13_dpl_mfbr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfbr) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfbr) as o
union all
select  '14_dpl_mfcl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfcl) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfcl) as o
union all
select  '15_dpl_mfrrus' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfrrus) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfrrus) as o
union all
select  '16_dpl_gncode' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn,
        case when t.cn = o.cn then 'PASS' else 'FAIL' end as result
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_gncode) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_gncode) as o;

+----------------+---------------+-----------------+--+
|  u1.tablename  | u1.backup_rc  | u1.original_cn  |
+----------------+---------------+-----------------+--+
| 01_dpl_actran  | 41825414      | 41825414        |
| 10_dpl_mfclcl  | 94            | 94              |
| 11_dpl_mfsc    | 2202091       | 2202091         |
| 12_dpl_tpcont  | 5182906       | 5182906         |
| 13_dpl_mfbr    | 1641          | 1641            |
| 14_dpl_mfcl    | 1304232       | 1304232         |
| 15_dpl_mfrrus  | 17092         | 17092           |
| 16_dpl_gncode  | 1128937       | 1128937         |
| 02_dpl_gngnco  | 4             | 4               |
| 03_dpl_mfclac  | 2034999       | 2034999         |
| 04_dpl_mfacct  | 328           | 328             |
| 05_dpl_mfrr    | 20409         | 20409           |
| 06_dpl_esmgr   | 1017          | 1017            |
| 07_dpl_acpos   | 1784040       | 1784040         |
| 08_dpl_gnexch  | 689           | 689             |
| 09_dpl_tpkptl  | 9958531       | 9958531         |
+----------------+---------------+-----------------+--+
16 rows selected (32.777 seconds)


        
select  '01_dpl_actran' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_actran) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_actran) as o
union all
select  '02_dpl_gngnco' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_gngnco) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_gngnco) as o
union all
select  '03_dpl_mfclac' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfclac) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfclac) as o
union all
select  '04_dpl_mfacct' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfacct) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfacct) as o
union all
select  '05_dpl_mfrr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfrr) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfrr) as o
union all
select  '06_dpl_esmgr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_esmgr) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_esmgr) as o
union all
select  '07_dpl_acpos' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_acpos) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_acpos) as o
union all
select  '08_dpl_gnexch' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_gnexch) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_gnexch) as o
union all
select  '09_dpl_tpkptl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_tpkptl) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_tpkptl) as o
union all
select  '10_dpl_mfclcl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfclcl) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfclcl) as o
union all
select  '11_dpl_mfsc' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfsc) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfsc) as o
union all
select  '12_dpl_tpcont' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_tpcont) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_tpcont) as o
union all
select  '13_dpl_mfbr' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfbr) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfbr) as o
union all
select  '14_dpl_mfcl' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfcl) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfcl) as o
union all
select  '15_dpl_mfrrus' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_mfrrus) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_mfrrus) as o
union all
select  '16_dpl_gncode' as tableName, t.cn  as  backup_rc, o.cn  as  original_cn
from    (select count(*) as cn from inv_aum_typed_qa.20200513_bkp_dpl_gncode) as t,
        (select count(*) as cn from inv_aum_typed_qa.dpl_gncode) as o;
+---------------+---------------+-----------------+--+
| u1.tablename  | u1.backup_rc  | u1.original_cn  |
+---------------+---------------+-----------------+--+
| 1_dpl_actran  | 41825414      | 41825414        |
| 1_dpl_mfclcl  | 94            | 94              |
| 1_dpl_mfsc    | 2202091       | 2202091         |
| 1_dpl_tpcont  | 5182906       | 5182906         |
| 1_dpl_mfbr    | 1641          | 1641            |
| 1_dpl_mfcl    | 1304232       | 1304232         |
| 1_dpl_mfrrus  | 17092         | 17092           |
| 1_dpl_gncode  | 1128937       | 1128937         |
| 1_dpl_gngnco  | 4             | 4               |
| 1_dpl_mfclac  | 2034999       | 2034999         |
| 1_dpl_mfacct  | 328           | 328             |
| 1_dpl_mfrr    | 20409         | 20409           |
| 1_dpl_esmgr   | 1017          | 1017            |
| 1_dpl_acpos   | 1784040       | 1784040         |
| 1_dpl_gnexch  | 689           | 689             |
| 1_dpl_tpkptl  | 9958531       | 9958531         |
+---------------+---------------+-----------------+--+
16 rows selected (200.767 seconds)

--show partitions
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
show partitions inv_aum_raw_qa.test_dpl_gncode;;

--show partitions
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

--**************************** Typed tables
set hivevar:t_process_date = '2020-05-10';

alter   table   inv_aum_typed_qa.dpl_actran    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_gngnco    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfclac    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfacct    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfrr      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_esmgr     drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_acpos     drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_gnexch    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_tpkptl    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfclcl    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfsc      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_tpcont    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfbr      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfcl      drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_mfrrus    drop partition   (process_date = ${t_process_date});
alter   table   inv_aum_typed_qa.dpl_gncode    drop partition   (process_date = ${t_process_date});

analyze table   inv_aum_typed_qa.dpl_actran    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_gngnco    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfclac    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfacct    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfrr      partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_esmgr     partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_acpos     partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_gnexch    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_tpkptl    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfclcl    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfsc      partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_tpcont    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfbr      partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfcl      partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_mfrrus    partition       (process_date) compute statistics;
analyze table   inv_aum_typed_qa.dpl_gncode    partition       (process_date) compute statistics;
