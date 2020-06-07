insert into inv_aum_typed_qa.dpl_actran partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_actran;
insert into inv_aum_typed_qa.dpl_gngnco partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_gngnco;
insert into inv_aum_typed_qa.dpl_mfclac partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_mfclac;
insert into inv_aum_typed_qa.dpl_mfacct partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_mfacct;
insert into inv_aum_typed_qa.dpl_mfrr   partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_mfrr;
insert into inv_aum_typed_qa.dpl_esmgr  partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_esmgr;
insert into inv_aum_typed_qa.dpl_acpos  partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_acpos;
insert into inv_aum_typed_qa.dpl_gnexch partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_gnexch;
insert into inv_aum_typed_qa.dpl_tpkptl partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_tpkptl;
insert into inv_aum_typed_qa.dpl_mfclcl partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_mfclcl;
insert into inv_aum_typed_qa.dpl_mfsc   partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_mfsc;
insert into inv_aum_typed_qa.dpl_tpcont partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_tpcont;
insert into inv_aum_typed_qa.dpl_mfbr   partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_mfbr;
insert into inv_aum_typed_qa.dpl_mfcl   partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_mfcl;
insert into inv_aum_typed_qa.dpl_mfrrus partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_mfrrus;
insert into inv_aum_typed_qa.dpl_gncode partition(process_date) select * from inv_aum_typed_qa.20200513_bkp_dpl_gncode;

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
| 05_dpl_mfrr    | 20409         | 0               |
| 06_dpl_esmgr   | 1017          | 1017            |
| 07_dpl_acpos   | 1784040       | 1784040         |
| 08_dpl_gnexch  | 689           | 689             |
| 09_dpl_tpkptl  | 9958531       | 9958531         |
+----------------+---------------+-----------------+--+
16 rows selected (20.471 seconds)