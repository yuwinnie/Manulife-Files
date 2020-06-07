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